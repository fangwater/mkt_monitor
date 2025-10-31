from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Tuple


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


class MetricStore:
    """线程安全的内存缓存，用于给前端提供快照。"""

    def __init__(self, *, xdp_points: int, integrity_points: int) -> None:
        self._xdp_points = xdp_points
        self._integrity_points = integrity_points
        self._lock = threading.Lock()
        self._xdp_series: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=self._xdp_points))
        self._integrity_series: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self._integrity_points)
        )
        self._alerts: Deque[Dict[str, Any]] = deque(maxlen=self._integrity_points)

    # xdp ------------------------------------------------------------------
    def add_xdp_payload(self, payload: Dict[str, Any], *, source: str | None = None) -> Tuple[str, Dict[str, Any]]:
        host = str(payload.get("hostname") or "")
        iface = str(payload.get("interface") or "")
        key = f"{host}|{iface}"

        metrics = payload.get("metrics") or {}
        timestamp = _as_float(payload.get("timestamp"), default=time.time())
        timestamp_iso = str(payload.get("timestamp_iso") or _isoformat(timestamp))

        avg_bps = _as_float(metrics.get("bps_avg"))
        max_bps = _as_float(metrics.get("bps_max"))

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
            self._xdp_series[key].append(entry)

        return key, entry

    # integrity ------------------------------------------------------------
    def add_integrity_payload(
        self, payload: Dict[str, Any], *, source: str | None = None
    ) -> Tuple[str, Dict[str, Any], bool]:
        exchange = str(payload.get("exchange") or "")
        symbol = str(payload.get("symbol") or "")
        key = f"{exchange}|{symbol}"

        ts_ms = _as_float(payload.get("timestamp_ms"))
        timestamp = ts_ms / 1000.0 if ts_ms else time.time()
        timestamp_iso = _isoformat(timestamp)

        status = str(payload.get("status") or "").lower()
        is_ok = status == "ok"

        entry = {
            "exchange": exchange,
            "symbol": symbol,
            "timestamp": timestamp,
            "timestamp_iso": timestamp_iso,
            "minute": _as_int(payload.get("minute")),
            "status": status,
            "detail": payload.get("detail"),
            "type": payload.get("type"),
            "is_ok": is_ok,
            "source": source,
        }

        with self._lock:
            self._integrity_series[key].append(entry)
            if not is_ok:
                self._alerts.append(entry)

        return key, entry, not is_ok

    # snapshots ------------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "xdp": {key: list(series) for key, series in self._xdp_series.items()},
                "integrity": {key: list(series) for key, series in self._integrity_series.items()},
                "alerts": list(self._alerts),
            }

    def xdp_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            return {key: list(series) for key, series in self._xdp_series.items()}

    def integrity_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            return {key: list(series) for key, series in self._integrity_series.items()}

    def alerts(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._alerts)
