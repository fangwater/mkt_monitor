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
    ) -> None:
        self._xdp_points = xdp_points
        self._integrity_points = max(integrity_points, 1)
        self._lock = threading.Lock()
        self._xdp_series: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self._integrity_series: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self._xdp_stream_counts: Dict[str, int] = defaultdict(int)
        self._integrity_retention: Dict[str, int] = {}
        self._alerts: Deque[Dict[str, Any]] = deque(maxlen=max(self._integrity_points, 128))

    def set_integrity_retention(self, stream: str, points: Optional[int]) -> None:
        value: Optional[int] = None
        if points is not None:
            try:
                candidate = int(points)
            except (TypeError, ValueError):
                candidate = None
            else:
                if candidate > 0:
                    value = candidate
        with self._lock:
            if value is None:
                self._integrity_retention.pop(stream, None)
            else:
                self._integrity_retention[stream] = value

    def _integrity_retention_limit(self, stream: str) -> int:
        return self._integrity_retention.get(stream, self._integrity_points)

    def _prune_locked(self, reference_ts: Optional[float] = None) -> None:
        """保留接口以兼容旧逻辑，当前仅依赖条数控制，不做时间裁剪。"""
        return

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
            stream_name = str(source or "default")
            self._xdp_stream_counts[stream_name] += 1
            while len(series) > self._xdp_points:
                removed = series.popleft()
                removed_source = str(removed.get("source") or "default")
                current = self._xdp_stream_counts.get(removed_source, 0)
                if current <= 1:
                    self._xdp_stream_counts.pop(removed_source, None)
                else:
                    self._xdp_stream_counts[removed_source] = current - 1
            self._prune_locked(reference_ts=timestamp)
            counts_snapshot = dict(self._xdp_stream_counts)
            limit = self._xdp_points

        if is_new_key:
            log.info("首次收到 XDP 数据: key=%s host=%s iface=%s", key, host, iface)

        for stream_id, count in sorted(counts_snapshot.items()):
            log.info("XDP 缓存状态: stream=%s count=%d limit=%d", stream_id, count, limit)

        return key, entry

    # integrity ------------------------------------------------------------
    def add_integrity_payload(
        self,
        payload: Dict[str, Any],
        *,
        source: str | None = None,
        defaults: Dict[str, Any] | None = None,
    ) -> List[Tuple[str, Dict[str, Any], bool]]:
        if not isinstance(payload, dict):
            return []

        defaults = defaults or {}
        exchange = str(payload.get("exchange") or "")
        event_type = str(payload.get("type") or "").lower()
        stage_raw = payload.get("stage") or payload.get("mode") or ""
        stage = str(stage_raw) if stage_raw is not None else ""

        hostname = str((defaults.get("hostname") or payload.get("hostname") or "") or "")
        interface = str((defaults.get("interface") or payload.get("interface") or "") or "")

        timestamp = (
            _coerce_timestamp(payload.get("timestamp"))
            or _coerce_timestamp(payload.get("timestamp_ms"))
            or _coerce_timestamp(payload.get("period_end_ts"))
            or _coerce_timestamp(payload.get("close_tp"))
            or _coerce_timestamp(payload.get("tp"))
            or time.time()
        )
        timestamp_iso = _isoformat(timestamp)

        period = _as_int(payload.get("period"))

        base_entry: Dict[str, Any] = {
            "exchange": exchange,
            "timestamp": timestamp,
            "timestamp_iso": timestamp_iso,
            "type": event_type,
            "stage": stage,
            "period": period,
            "status": str(payload.get("status") or "").lower(),
            "detail": payload.get("detail"),
            "source": source,
            "hostname": hostname,
            "interface": interface,
        }

        normalized_results: List[Dict[str, Any]] = []
        failed_symbols: List[str] = []
        failed_requests: List[str] = []

        raw_results = payload.get("results")
        if isinstance(raw_results, list):
            for raw_item in raw_results:
                if not isinstance(raw_item, dict):
                    continue
                symbol_raw = raw_item.get("symbol")
                symbol = str(symbol_raw or "")
                symbol_norm = symbol.upper() if symbol else ""
                status = str(raw_item.get("status") or "").lower()
                detail_value = raw_item.get("detail")

                normalized_result: Dict[str, Any] = {
                    "symbol": symbol_norm,
                    "status": status,
                }
                if detail_value is not None:
                    normalized_result["detail"] = detail_value

                requests: List[Dict[str, Any]] = []
                raw_requests = raw_item.get("requests")
                if isinstance(raw_requests, list):
                    for request_item in raw_requests:
                        if not isinstance(request_item, dict):
                            continue
                        request_name_raw = request_item.get("request") or request_item.get("name") or ""
                        request_name = str(request_name_raw or "")
                        request_status = str(request_item.get("status") or "").lower()
                        request_detail = request_item.get("detail")
                        normalized_request: Dict[str, Any] = {
                            "name": request_name,
                            "status": request_status,
                        }
                        if request_detail is not None:
                            normalized_request["detail"] = request_detail
                        requests.append(normalized_request)
                        if request_status != "ok":
                            label = request_name or str(request_detail or "unknown")
                            if symbol_norm:
                                label = f"{symbol_norm}:{label}"
                            failed_requests.append(label)
                    if requests:
                        normalized_result["requests"] = requests

                normalized_results.append(normalized_result)
                if status and status != "ok":
                    failed_symbols.append(symbol_norm or symbol)

        failed_symbols = [sym for sym in dict.fromkeys(sym for sym in failed_symbols if sym)]
        failed_requests = [req for req in dict.fromkeys(req for req in failed_requests if req)]

        entry = dict(base_entry)
        entry["symbol"] = str(payload.get("symbol") or "").upper()
        entry["is_ok"] = entry["status"] == "ok" if entry.get("status") else not failed_symbols and not failed_requests
        if entry.get("status") in (None, "") and (failed_symbols or failed_requests):
            entry["status"] = "fail"
            entry["is_ok"] = False

        entry["results"] = normalized_results
        entry["results_count"] = len(normalized_results)
        entry["request_count"] = sum(
            len(result.get("requests") or []) for result in normalized_results if isinstance(result, dict)
        )
        entry["failed_symbols"] = failed_symbols
        entry["failed_count"] = len(failed_symbols)
        entry["failed_requests"] = failed_requests
        entry["failed_request_count"] = len(failed_requests)
        if not entry.get("detail"):
            if failed_symbols:
                entry["detail"] = "失败合约: " + ", ".join(failed_symbols)
            elif failed_requests:
                entry["detail"] = "失败请求: " + ", ".join(failed_requests)
        entry["stream"] = source
        entry["source"] = source

        updates: List[Tuple[str, Dict[str, Any], bool]] = []
        latest_ts = timestamp

        with self._lock:
            stream_name = source or "integrity"
            key = self._compose_integrity_key(
                stream_name=stream_name,
                hostname=hostname,
                interface=interface,
                exchange=exchange,
                symbol="",
                stage=stage,
                event_type=event_type,
            )
            entry["key"] = key
            entry["stream"] = stream_name
            series = self._integrity_series[key]
            is_new_key = len(series) == 0
            series.append(entry)
            retention_limit = self._integrity_retention_limit(stream_name)
            while len(series) > retention_limit:
                series.popleft()
            if not entry.get("is_ok"):
                self._alerts.append(entry)
            series_ts = _as_float(entry.get("timestamp"), default=timestamp)
            if series_ts > latest_ts:
                latest_ts = series_ts
            updates.append((key, entry, not entry.get("is_ok")))
            if is_new_key:
                log.info(
                    "首次收到完整性数据: key=%s stream=%s exchange=%s type=%s stage=%s",
                    key,
                    stream_name,
                    exchange,
                    event_type,
                    stage,
                )
            if not entry.get("is_ok"):
                log.warning(
                    "完整性检测异常: key=%s stream=%s exchange=%s type=%s stage=%s detail=%s",
                    key,
                    stream_name,
                    exchange,
                    event_type,
                    stage,
                    entry.get("detail"),
                )

            self._prune_locked(reference_ts=latest_ts)

        return updates

    def _compose_integrity_key(
        self,
        *,
        stream_name: str,
        hostname: str,
        interface: str,
        exchange: str,
        symbol: str,
        stage: str,
        event_type: str,
    ) -> str:
        parts: List[str] = []
        if stream_name:
            parts.append(stream_name.lower())
        if hostname:
            parts.append(hostname)
        if interface:
            parts.append(interface)
        if exchange:
            parts.append(exchange.lower())
        if stage:
            parts.append(stage.lower())
        if event_type:
            parts.append(event_type.lower())
        if symbol:
            parts.append(symbol.upper())
        if not parts:
            return "integrity"
        return "|".join(parts)

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
                stages = sorted({str(point.get("stage") or "") for point in series if point.get("stage")})
                stream_name = str(latest.get("stream") or latest.get("source") or "")
                exchange = str(latest.get("exchange") or "")
                records.append(
                    {
                        "key": key,
                        "hostname": hostname,
                        "interface": interface,
                        "types": [t for t in types if t],
                        "stages": [s for s in stages if s],
                        "stream": stream_name,
                        "exchange": exchange,
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
        stage: Optional[str] = None,
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
                    point_stage = str(point.get("stage") or "")
                    if type_filter and point_type != type_filter:
                        continue
                    if stage and point_stage != stage:
                        continue
                    record = dict(point)
                    record["key"] = key
                    record["exchange"] = ex
                    record["symbol"] = sym
                    record["hostname"] = host
                    record["interface"] = iface
                    record["type"] = point_type
                    record["stage"] = point_stage
                    record["stream"] = str(point.get("stream") or point.get("source") or "")
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
