from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, asdict
from typing import Deque, Dict, List, Optional

from src.xdp_bandwidth import Sample, XDPBandwidthMonitor

from .config import AggregationSettings, XDPSettings

log = logging.getLogger(__name__)


@dataclass
class Bucket:
    bucket_id: int
    start_ts: float
    end_ts: float
    max_bps: float
    sample_count: int

    def to_dict(self) -> Dict[str, float]:
        return {
            "bucket_id": self.bucket_id,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "max_bps": self.max_bps,
            "sample_count": self.sample_count,
        }


class TrafficCollector:
    """后台线程：消费 10ms tick 并生成 3min max 桶。"""

    def __init__(self, xdp_cfg: XDPSettings, agg_cfg: AggregationSettings) -> None:
        self.xdp_cfg = xdp_cfg
        self.agg_cfg = agg_cfg

        self._history: Deque[Bucket] = deque(maxlen=agg_cfg.history_buckets)
        self._current: Optional[Bucket] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._latest_sample: Optional[Sample] = None
        self._error: Optional[Exception] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="traffic-collector", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def latest_sample(self) -> Optional[Dict]:
        with self._lock:
            if self._latest_sample is None:
                return None
            return asdict(self._latest_sample)

    def buckets(self) -> List[Dict]:
        with self._lock:
            data = list(self._history)
            if self._current:
                data.append(self._current)
            return [bucket.to_dict() for bucket in data]

    def last_error(self) -> Optional[str]:
        err = self._error
        return None if err is None else repr(err)

    # internal ---------------------------------------------------------------
    def _run(self) -> None:
        monitor = XDPBandwidthMonitor(
            iface=self.xdp_cfg.interface,
            interval=self.xdp_cfg.tick_ms / 1000.0,
            mode=self.xdp_cfg.mode,
        )

        try:
            with monitor:
                for sample in monitor.stream():
                    if self._stop_event.is_set():
                        break
                    self._handle_sample(sample)
        except Exception as exc:  # noqa: BLE001
            log.exception("collector crashed")
            self._error = exc

    def _handle_sample(self, sample: Sample) -> None:
        bucket_span = self.agg_cfg.window_seconds
        bucket_id = int(sample.timestamp // bucket_span)
        start_ts = bucket_id * bucket_span
        end_ts = start_ts + bucket_span

        with self._lock:
            self._latest_sample = sample

            if self._current is None or self._current.bucket_id != bucket_id:
                if self._current is not None:
                    self._history.append(self._current)
                self._current = Bucket(
                    bucket_id=bucket_id,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    max_bps=sample.bps,
                    sample_count=1,
                )
            else:
                self._current.sample_count += 1
                if sample.bps > self._current.max_bps:
                    self._current.max_bps = sample.bps
