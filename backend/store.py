from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple


log = logging.getLogger(__name__)


def _isoformat(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_timestamp(value: Any) -> Optional[float]:
    """将各种时间字段转换为秒级时间戳。"""
    ts = _as_float(value, default=-1.0)
    if ts <= 0.0:
        return None
    # 13 位或更大的数值通常表示毫秒
    if ts >= 1_000_000_000_000:
        return ts / 1000.0
    return ts


class MetricStore:
    """线程安全的内存缓存，用于给前端提供快照。"""

    def __init__(
        self,
        *,
        xdp_points: int,
        integrity_points: int,
        retention_seconds: int,
    ) -> None:
        self._xdp_points = xdp_points
        self._integrity_points = integrity_points
        self._retention_seconds = retention_seconds
        self._lock = threading.Lock()
        self._xdp_series: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self._integrity_series: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self._alerts: Deque[Dict[str, Any]] = deque()

    def _prune_deque(self, series: Deque[Dict[str, Any]], cutoff: float) -> None:
        while series and _as_float(series[0].get("timestamp"), default=0.0) < cutoff:
            series.popleft()

    def _prune_locked(self, reference_ts: Optional[float] = None) -> None:
        if self._retention_seconds <= 0:
            return
        now = reference_ts if reference_ts is not None else time.time()
        cutoff = now - self._retention_seconds
        for key in list(self._xdp_series.keys()):
            series = self._xdp_series[key]
            self._prune_deque(series, cutoff)
            if not series:
                del self._xdp_series[key]
        for key in list(self._integrity_series.keys()):
            series = self._integrity_series[key]
            self._prune_deque(series, cutoff)
            if not series:
                del self._integrity_series[key]
        self._prune_deque(self._alerts, cutoff)

    # xdp ------------------------------------------------------------------
    def add_xdp_payload(self, payload: Dict[str, Any], *, source: str | None = None) -> Tuple[str, Dict[str, Any]]:
        host = str(payload.get("hostname") or "")
        iface = str(payload.get("interface") or "")
        key = f"{host}|{iface}"

        metrics = payload.get("metrics") or {}
        timestamp = _as_float(payload.get("timestamp"), default=time.time())
        timestamp_iso = str(payload.get("timestamp_iso") or _isoformat(timestamp))

        avg_bps = _as_float(
            metrics.get("bps_avg")
            or metrics.get("avg_bps")
            or metrics.get("avg")
            or metrics.get("bps_mean")
        )
        max_bps = _as_float(
            metrics.get("bps_max")
            or metrics.get("max_bps")
            or metrics.get("max")
        )

        entry = {
            "hostname": host,
            "interface": iface,
            "timestamp": timestamp,
            "timestamp_iso": timestamp_iso,
            "avg_bps": avg_bps,
            "max_bps": max_bps,
            "avg_mbps": avg_bps / 1_000_000.0,
            "max_mbps": max_bps / 1_000_000.0,
            "avg_MBps": avg_bps / 8_000_000.0,
            "max_MBps": max_bps / 8_000_000.0,
            "bytes_total": _as_int(metrics.get("bytes_total")),
            "packets_total": _as_int(metrics.get("packets_total")),
            "mode": payload.get("mode"),
            "window": payload.get("window"),
            "samples": _as_int(payload.get("samples")),
            "source": source,
        }

        with self._lock:
            series = self._xdp_series[key]
            is_new_key = len(series) == 0
            series.append(entry)
            self._prune_locked(reference_ts=timestamp)

        if is_new_key:
            log.info("首次收到 XDP 数据: key=%s host=%s iface=%s", key, host, iface)

        return key, entry

    # integrity ------------------------------------------------------------
    def add_integrity_payload(
        self,
        payload: Dict[str, Any],
        *,
        source: str | None = None,
        defaults: Dict[str, Any] | None = None,
    ) -> Tuple[str, Dict[str, Any], bool]:
        exchange = str(payload.get("exchange") or "")
        symbol = str(payload.get("symbol") or "")
        defaults = defaults or {}
        hostname_raw = defaults.get("hostname") or ""
        interface_raw = defaults.get("interface") or ""
        hostname = str(hostname_raw) if hostname_raw else ""
        interface = str(interface_raw) if interface_raw else ""

        key_parts: List[str] = []
        if hostname:
            key_parts.append(hostname)
        if interface:
            key_parts.append(interface)
        if not key_parts:
            if exchange:
                key_parts.append(exchange)
            if symbol:
                key_parts.append(symbol)
        key = "|".join([part for part in key_parts if part])
        if not key:
            key = exchange or symbol or (source or "integrity")

        timestamp = (
            _coerce_timestamp(payload.get("timestamp_ms"))
            or _coerce_timestamp(payload.get("period_end_ts"))
            or _coerce_timestamp(payload.get("tp"))
            or _coerce_timestamp(payload.get("timestamp"))
            or time.time()
        )
        timestamp_iso = _isoformat(timestamp)

        status = str(payload.get("status") or "").lower()
        is_ok = status == "ok"

        event_type = str(payload.get("type") or "")

        entry = {
            "exchange": exchange,
            "symbol": symbol,
            "timestamp": timestamp,
            "timestamp_iso": timestamp_iso,
            "minute": _as_int(payload.get("minute")),
            "status": status,
            "detail": payload.get("detail"),
            "type": event_type,
            "is_ok": is_ok,
            "source": source,
            "hostname": hostname,
            "interface": interface,
        }

        if payload.get("trade_batch"):
            batch_items: List[Dict[str, Any]] = []
            raw_items = payload.get("trade_batch_items") or []
            for raw_item in raw_items:
                if not isinstance(raw_item, dict):
                    continue
                item_symbol = str(raw_item.get("symbol") or "")
                item_status = str(raw_item.get("status") or "").lower()
                item_ts = _coerce_timestamp(raw_item.get("timestamp"))
                if item_ts is None:
                    item_ts = timestamp
                item_detail = raw_item.get("detail")
                item_minute = _as_int(raw_item.get("minute"))
                batch_items.append(
                    {
                        "symbol": item_symbol,
                        "status": item_status,
                        "detail": item_detail,
                        "minute": item_minute,
                        "timestamp": item_ts,
                        "timestamp_iso": _isoformat(item_ts) if item_ts is not None else None,
                    }
                )
            entry["trade_batch"] = True
            entry["trade_batch_items"] = batch_items
            entry["trade_batch_size"] = _as_int(
                payload.get("trade_batch_size"),
                default=len(batch_items),
            )
            failure_count = _as_int(
                payload.get("trade_batch_failures"),
                default=sum(1 for item in batch_items if item.get("status") != "ok"),
            )
            entry["trade_batch_failures"] = failure_count
        else:
            entry["trade_batch"] = False
            entry["trade_batch_items"] = []
            entry["trade_batch_size"] = 0
            entry["trade_batch_failures"] = 0

        with self._lock:
            series = self._integrity_series[key]
            is_new_key = len(series) == 0
            series.append(entry)
            if not is_ok:
                self._alerts.append(entry)
            self._prune_locked(reference_ts=timestamp)

        if is_new_key:
            log.info("首次收到完整性数据: key=%s exchange=%s symbol=%s", key, exchange, symbol)
        if not is_ok:
            log.warning(
                "完整性检测异常: key=%s exchange=%s symbol=%s status=%s detail=%s",
                key,
                exchange,
                symbol,
                status,
                entry.get("detail"),
            )

        return key, entry, not is_ok

    # snapshots ------------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            self._prune_locked()
            return {
                "xdp": {key: list(series) for key, series in self._xdp_series.items()},
                "integrity": {key: list(series) for key, series in self._integrity_series.items()},
                "alerts": list(self._alerts),
            }

    def xdp_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            self._prune_locked()
            return {key: list(series) for key, series in self._xdp_series.items()}

    def integrity_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            self._prune_locked()
            return {key: list(series) for key, series in self._integrity_series.items()}

    def alerts(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._prune_locked()
            return list(self._alerts)

    def integrity_keys(self) -> List[Dict[str, str]]:
        records: List[Dict[str, str]] = []
        with self._lock:
            self._prune_locked()
            for key, series in self._integrity_series.items():
                if not series:
                    continue
                latest = series[-1]
                hostname = str(latest.get("hostname") or "")
                interface = str(latest.get("interface") or "")
                types = sorted({str(point.get("type") or "") for point in series if point.get("type")})
                records.append(
                    {
                        "key": key,
                        "hostname": hostname,
                        "interface": interface,
                        "types": [t for t in types if t],
                    }
                )
        records.sort(key=lambda item: (item["hostname"], item["interface"]))
        return records

    def integrity_series(
        self,
        *,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        hostname: Optional[str] = None,
        interface: Optional[str] = None,
        type_filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        with self._lock:
            self._prune_locked()
            for key, series in self._integrity_series.items():
                for point in series:
                    ex = str(point.get("exchange") or "")
                    sym = str(point.get("symbol") or "")
                    host = str(point.get("hostname") or "")
                    iface = str(point.get("interface") or "")
                    if exchange and ex != exchange:
                        continue
                    if symbol and sym != symbol:
                        continue
                    if hostname and host != hostname:
                        continue
                    if interface and iface != interface:
                        continue
                    point_type = str(point.get("type") or "")
                    if type_filter and point_type != type_filter:
                        continue
                    record = dict(point)
                    record["key"] = key
                    record["exchange"] = ex
                    record["symbol"] = sym
                    record["hostname"] = host
                    record["interface"] = iface
                    record["type"] = point_type
                    records.append(record)

        records.sort(key=lambda item: float(item.get("timestamp") or 0.0))
        if limit and limit > 0 and len(records) > limit:
            records = records[-limit:]
        return records

    def latest_xdp_entry(self) -> Optional[Dict[str, Any]]:
        """返回最近一次的 XDP 数据点。"""
        latest_key: Optional[str] = None
        latest_point: Optional[Dict[str, Any]] = None
        latest_ts: float = 0.0
        with self._lock:
            self._prune_locked()
            for key, series in self._xdp_series.items():
                if not series:
                    continue
                candidate = series[-1]
                ts = float(candidate.get("timestamp") or 0.0)
                if latest_point is None or ts >= latest_ts:
                    latest_point = candidate
                    latest_key = key
                    latest_ts = ts
        if latest_point is None:
            return None
        return {"key": latest_key, "entry": latest_point}

    def xdp_buckets(self) -> List[Dict[str, Any]]:
        """将所有 XDP 样本转换为桶视图，兼容旧版前端。"""
        buckets: List[Dict[str, Any]] = []
        with self._lock:
            for key, series in self._xdp_series.items():
                for point in series:
                    window = point.get("window") or {}
                    start_ts = float(window.get("start") or point.get("timestamp") or 0.0)
                    duration = float(window.get("duration") or 0.0)
                    end_ts = float(window.get("end") or (start_ts + duration))
                    window_duration = duration if duration > 0 else max(end_ts - start_ts, 0.0)

                    avg_bps = 0.0
                    avg_source: Optional[str] = None
                    for key_name in ("avg_bps", "bps_avg", "avg"):
                        raw_value = point.get(key_name)
                        if raw_value is None:
                            continue
                        try:
                            avg_bps = float(raw_value)
                            avg_source = key_name
                            break
                        except (TypeError, ValueError):
                            log.info(
                                "avg_bps 字段解析失败: host=%s iface=%s key=%s value=%r",
                                point.get("hostname"),
                                point.get("interface"),
                                key_name,
                                raw_value,
                            )
                    if avg_bps == 0.0 and window_duration > 0:
                        bytes_total = float(point.get("bytes_total") or 0.0)
                        if bytes_total > 0:
                            avg_bps = (bytes_total * 8.0) / window_duration
                            avg_source = "bytes_total"
                            log.info(
                                "根据 bytes_total 回推 avg_bps: host=%s iface=%s start_ts=%.3f duration=%.3f bytes=%s avg_bps=%.2f",
                                point.get("hostname"),
                                point.get("interface"),
                                start_ts,
                                window_duration,
                                bytes_total,
                                avg_bps,
                            )

                    max_bps = 0.0
                    max_source: Optional[str] = None
                    for key_name in ("max_bps", "bps_max", "max"):
                        raw_value = point.get(key_name)
                        if raw_value is None:
                            continue
                        try:
                            max_bps = float(raw_value)
                            max_source = key_name
                            break
                        except (TypeError, ValueError):
                            log.info(
                                "max_bps 字段解析失败: host=%s iface=%s key=%s value=%r",
                                point.get("hostname"),
                                point.get("interface"),
                                key_name,
                                raw_value,
                            )

                    buckets.append(
                        {
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "max_bps": max_bps,
                            "avg_bps": avg_bps,
                            "bps_max": max_bps,
                            "bps_avg": avg_bps,
                            "avg_source": avg_source,
                            "max_source": max_source,
                            "sample_count": int(point.get("samples") or 0),
                        }
                    )

        buckets.sort(key=lambda item: item["start_ts"])
        return buckets
