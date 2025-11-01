from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

import yaml


@dataclass(frozen=True)
class ServerSettings:
    host: str = "0.0.0.0"
    port: int = 8000


@dataclass(frozen=True)
class RetentionSettings:
    xdp_points: int = 72
    integrity_points: int = 72
    retention_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.xdp_points <= 0:
            raise ValueError("xdp_points 必须为正整数")
        if self.integrity_points <= 0:
            raise ValueError("integrity_points 必须为正整数")
        if self.retention_seconds <= 0:
            raise ValueError("retention_seconds 必须为正整数")


@dataclass(frozen=True)
class ZMQStreamConfig:
    name: str
    endpoint: str
    topic: str = ""
    hostname: str | None = None
    interface: str | None = None

    def __post_init__(self) -> None:
        if not self.endpoint:
            raise ValueError(f"流 {self.name!r} 的 endpoint 不能为空")
        object.__setattr__(self, "topic", str(self.topic or ""))
        if self.hostname is not None:
            object.__setattr__(self, "hostname", str(self.hostname))
        if self.interface is not None:
            object.__setattr__(self, "interface", str(self.interface))


@dataclass(frozen=True)
class AppConfig:
    server: ServerSettings
    retention: RetentionSettings
    xdp_streams: Tuple[ZMQStreamConfig, ...]
    integrity_streams: Tuple[ZMQStreamConfig, ...]
    frontend: "FrontendSettings"


@dataclass(frozen=True)
class FrontendSettings:
    alert_threshold_bps: float = 0.0
    refresh_interval_ms: int = 5000

    def __post_init__(self) -> None:
        if self.alert_threshold_bps < 0:
            raise ValueError("alert_threshold_bps 不能为负")
        if self.refresh_interval_ms <= 0:
            raise ValueError("refresh_interval_ms 必须为正整数")


def _ensure_dict(root: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = root.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{key} 配置必须是对象")
    return value


def _ensure_list(root: Dict[str, Any], key: str) -> Iterable[Dict[str, Any]]:
    value = root.get(key, [])
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{key} 配置必须是列表")
    return value


def _parse_streams(items: Iterable[Dict[str, Any]], *, fallback_topic: str) -> Tuple[ZMQStreamConfig, ...]:
    streams = []
    for pos, raw_item in enumerate(items):
        if not isinstance(raw_item, dict):
            raise ValueError(f"第 {pos} 个流配置不是对象")
        name = str(raw_item.get("name") or f"stream-{pos}")
        endpoint = raw_item.get("endpoint")
        if not endpoint or not isinstance(endpoint, str):
            raise ValueError(f"{name} 缺少 endpoint")
        topic = raw_item.get("topic")
        if topic is None:
            topic = fallback_topic
        if not isinstance(topic, str):
            raise ValueError(f"{name} 的 topic 必须是字符串")
        hostname = raw_item.get("hostname")
        interface = raw_item.get("interface")
        if hostname is not None and not isinstance(hostname, str):
            raise ValueError(f"{name} 的 hostname 必须是字符串")
        if interface is not None and not isinstance(interface, str):
            raise ValueError(f"{name} 的 interface 必须是字符串")
        streams.append(
            ZMQStreamConfig(
                name=name,
                endpoint=endpoint,
                topic=topic,
                hostname=hostname,
                interface=interface,
            )
        )
    return tuple(streams)


def load_config(path: str | pathlib.Path) -> AppConfig:
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict):
        raise ValueError("配置文件必须是对象")

    server_cfg = _ensure_dict(raw, "server")
    retention_cfg = _ensure_dict(raw, "retention")
    frontend_cfg = _ensure_dict(raw, "frontend")

    fallback_xdp_topic = ""
    fallback_integrity_topic = "integrity_trade"

    xdp_items = _ensure_list(raw, "xdp_streams")
    integrity_items = _ensure_list(raw, "integrity_streams")

    if not xdp_items:
        raise ValueError("至少配置一个 xdp_streams")
    if not integrity_items:
        raise ValueError("至少配置一个 integrity_streams")

    server = ServerSettings(
        host=str(server_cfg.get("host", "0.0.0.0")),
        port=int(server_cfg.get("port", 8000)),
    )

    retention = RetentionSettings(
        xdp_points=int(retention_cfg.get("xdp_points", 72)),
        integrity_points=int(retention_cfg.get("integrity_points", 72)),
        retention_seconds=int(retention_cfg.get("retention_seconds", 3600) or 3600),
    )

    frontend = FrontendSettings(
        alert_threshold_bps=float(frontend_cfg.get("alert_threshold_bps", 0.0) or 0.0),
        refresh_interval_ms=int(frontend_cfg.get("refresh_interval_ms", 5000) or 5000),
    )

    xdp_streams = _parse_streams(xdp_items, fallback_topic=fallback_xdp_topic)
    integrity_streams = _parse_streams(integrity_items, fallback_topic=fallback_integrity_topic)

    return AppConfig(
        server=server,
        retention=retention,
        xdp_streams=xdp_streams,
        integrity_streams=integrity_streams,
        frontend=frontend,
    )
