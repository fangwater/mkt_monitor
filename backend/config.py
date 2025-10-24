from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any, Dict

import yaml


@dataclass(frozen=True)
class XDPSettings:
    interface: str
    tick_ms: float
    mode: str


@dataclass(frozen=True)
class AggregationSettings:
    window_seconds: int
    history_hours: int

    @property
    def history_buckets(self) -> int:
        return int((self.history_hours * 3600) / self.window_seconds)


@dataclass(frozen=True)
class BackendSettings:
    host: str
    port: int


@dataclass(frozen=True)
class FrontendSettings:
    refresh_interval_ms: int
    alert_threshold_bps: int


@dataclass(frozen=True)
class AppConfig:
    xdp: XDPSettings
    aggregation: AggregationSettings
    backend: BackendSettings
    frontend: FrontendSettings


def parse_threshold(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        suffix = text[-1].upper()
        multipliers = {"K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}
        if suffix in multipliers:
            number_part = text[:-1].strip()
            try:
                base = float(number_part)
            except ValueError as err:
                raise ValueError(f"无法解析阈值 {value!r}") from err
            return int(base * multipliers[suffix])
        try:
            return int(float(text))
        except ValueError as err:
            raise ValueError(f"无法解析阈值 {value!r}") from err
    raise ValueError(f"无法解析阈值类型: {type(value).__name__}")


def load_config(path: str | pathlib.Path) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("配置文件格式不正确")

    def require(section: str) -> Dict[str, Any]:
        value = raw.get(section)
        if not isinstance(value, dict):
            raise ValueError(f"配置项 {section} 缺失或格式错误")
        return value

    xdp_raw = require("xdp")
    agg_raw = require("aggregation")
    backend_raw = require("backend")
    frontend_raw = require("frontend")

    return AppConfig(
        xdp=XDPSettings(
            interface=str(xdp_raw.get("interface", "")),
            tick_ms=float(xdp_raw.get("tick_ms", 10)),
            mode=str(xdp_raw.get("mode", "auto")),
        ),
        aggregation=AggregationSettings(
            window_seconds=int(agg_raw.get("window_seconds", 180)),
            history_hours=int(agg_raw.get("history_hours", 72)),
        ),
        backend=BackendSettings(
            host=str(backend_raw.get("host", "0.0.0.0")),
            port=int(backend_raw.get("port", 8000)),
        ),
        frontend=FrontendSettings(
            refresh_interval_ms=int(frontend_raw.get("refresh_interval_ms", 5000)),
            alert_threshold_bps=parse_threshold(frontend_raw.get("alert_threshold")),
        ),
    )
