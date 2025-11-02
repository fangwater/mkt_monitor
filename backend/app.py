from __future__ import annotations

import asyncio
import logging
import os
import pathlib
from collections import OrderedDict
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .config import AppConfig, load_config
from .store import MetricStore
from .subscriber import ZMQSubscriber

ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config.yaml"
FRONTEND_DIR = ROOT_DIR / "frontend"
CONFIG_ENV_VAR = "MKT_MONITOR_CONFIG"
CONFIGS_ENV_VAR = "MKT_MONITOR_CONFIGS"
CONFIG_GLOB_PATTERN = "config-*.yaml"


def _resolve_config_path(path: str | pathlib.Path) -> pathlib.Path:
    candidate = pathlib.Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = (ROOT_DIR / candidate).resolve()
    else:
        candidate = candidate.resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"配置文件不存在: {candidate}")
    return candidate


def _parse_config_map(raw: str) -> Dict[str, pathlib.Path]:
    mapping: Dict[str, pathlib.Path] = {}
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"多配置模式的条目 {item!r} 缺少 '=' 分隔符，格式应为 name=path")
        name, path_str = item.split("=", 1)
        name = name.strip()
        if not name:
            raise ValueError("多配置条目名称不能为空")
        if "/" in name or name.startswith("."):
            raise ValueError(f"配置名称 {name!r} 不能包含 '/' 或以 '.' 开头")
        if name in mapping:
            raise ValueError(f"检测到重复的配置名称: {name}")
        try:
            mapping[name] = _resolve_config_path(path_str.strip())
        except FileNotFoundError as exc:
            raise ValueError(f"配置 {name!r} 指定的文件不存在: {path_str!r}") from exc
    if not mapping:
        raise ValueError("多配置模式至少需要提供一个有效条目，例如 primary=config_primary.yaml")
    return mapping


def _discover_config_map() -> Dict[str, pathlib.Path]:
    entries: OrderedDict[str, pathlib.Path] = OrderedDict()
    for path in sorted(ROOT_DIR.glob(CONFIG_GLOB_PATTERN)):
        if not path.is_file():
            continue
        slug = path.stem
        if slug.startswith("config-"):
            slug = slug[len("config-") :]
        slug = slug.strip().lower().replace("_", "-")
        slug = slug.strip("-")
        if not slug:
            continue
        if "/" in slug or slug.startswith("."):
            continue
        if slug in entries:
            raise ValueError(f"检测到重复的配置别名: {slug}")
        entries[slug] = path.resolve()

    if entries and CONFIG_PATH.exists():
        if "primary" not in entries and "default" not in entries:
            merged: OrderedDict[str, pathlib.Path] = OrderedDict()
            merged["primary"] = CONFIG_PATH.resolve()
            for slug, path in entries.items():
                merged[slug] = path
            entries = merged

    if len(entries) >= 2:
        return OrderedDict(entries)

    return {}


def _setup_logging() -> None:
    logger = logging.getLogger("backend")
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


class WebsocketHub:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._connections: List[WebSocket] = []

    async def add(self, ws: WebSocket, snapshot: Dict[str, Any]) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.append(ws)
        await ws.send_json({"type": "snapshot", "payload": snapshot})

    async def remove(self, ws: WebSocket) -> None:
        async with self._lock:
            try:
                self._connections.remove(ws)
            except ValueError:
                pass

    async def broadcast(self, message: Dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._connections)

        stale: List[WebSocket] = []
        for conn in targets:
            try:
                await conn.send_json(message)
            except Exception:  # noqa: BLE001
                stale.append(conn)

        if not stale:
            return

        async with self._lock:
            for conn in stale:
                if conn in self._connections:
                    self._connections.remove(conn)


def _create_dashboard_app(
    config_path: pathlib.Path,
    *,
    dashboard_name: str | None = None,
) -> FastAPI:
    _setup_logging()
    log_name = "backend.integrity"
    if dashboard_name:
        log_name = f"{log_name}.{dashboard_name}"
    log = logging.getLogger(log_name)
    cfg: AppConfig = load_config(config_path)

    store = MetricStore(
        xdp_points=cfg.retention.xdp_points,
        integrity_points=cfg.retention.integrity_points,
    )
    for stream_cfg in cfg.integrity_streams:
        store.set_integrity_retention(stream_cfg.name, stream_cfg.retention_points)
    hub = WebsocketHub()

    app = FastAPI(title="Market Integrity Monitor", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.config = cfg
    app.state.config_path = pathlib.Path(config_path)
    app.state.dashboard = dashboard_name or "default"
    app.state.store = store
    app.state.hub = hub
    app.state.loop = None
    app.state.subscribers: List[ZMQSubscriber] = []

    logging.getLogger("backend").info(
        "启动仪表盘: name=%s config=%s",
        app.state.dashboard,
        app.state.config_path,
    )

    def _schedule(message: Dict[str, Any]) -> None:
        loop: asyncio.AbstractEventLoop | None = getattr(app.state, "loop", None)
        if loop is None:
            return
        asyncio.run_coroutine_threadsafe(hub.broadcast(message), loop)

    def _handle_xdp(stream_cfg, topic: str, payload: Dict[str, Any]) -> None:
        key, entry = store.add_xdp_payload(payload, source=stream_cfg.name)
        broadcast_entry = dict(entry)
        broadcast_entry["topic"] = topic
        _schedule(
            {
                "type": "update",
                "scope": "xdp",
                "series": {"key": key, "point": broadcast_entry},
            }
        )

    def _handle_integrity(stream_cfg, topic: str, payload: Dict[str, Any]) -> None:
        defaults = {
            "hostname": getattr(stream_cfg, "hostname", None),
            "interface": getattr(stream_cfg, "interface", None),
        }
        updates = store.add_integrity_payload(
            payload,
            source=stream_cfg.name,
            defaults=defaults,
        )
        if not updates:
            log.debug(
                "忽略空的完整性事件: dashboard=%s stream=%s topic=%s payload_keys=%s",
                app.state.dashboard,
                stream_cfg.name,
                topic,
                list(sorted(payload.keys())),
            )
            return

        for key, entry, alerted in updates:
            log_func = log.warning if alerted else log.debug
            log_func(
                "integrity event: dashboard=%s stream=%s topic=%s key=%s type=%s stage=%s symbol=%s status=%s detail=%s",
                app.state.dashboard,
                stream_cfg.name,
                topic,
                key,
                entry.get("type"),
                entry.get("stage"),
                entry.get("symbol"),
                entry.get("status"),
                entry.get("detail"),
            )
            broadcast_entry = dict(entry)
            broadcast_entry["topic"] = topic
            _schedule(
                {
                    "type": "update",
                    "scope": "integrity",
                    "series": {"key": key, "point": broadcast_entry},
                    "alert": alerted,
                }
            )

    @app.on_event("startup")
    async def _startup() -> None:
        loop = asyncio.get_running_loop()
        app.state.loop = loop

        subscribers: List[ZMQSubscriber] = []
        for stream_cfg in cfg.xdp_streams:
            sub = ZMQSubscriber(stream_cfg, _handle_xdp)
            sub.start()
            subscribers.append(sub)
        for stream_cfg in cfg.integrity_streams:
            sub = ZMQSubscriber(stream_cfg, _handle_integrity)
            sub.start()
            subscribers.append(sub)
        app.state.subscribers = subscribers

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        for sub in app.state.subscribers:
            sub.stop()
        for sub in app.state.subscribers:
            sub.join(timeout=1.5)
        app.state.subscribers.clear()
        app.state.loop = None

    @app.get("/api/config")
    async def get_config() -> JSONResponse:
        payload = asdict(cfg)
        payload["dashboard"] = app.state.dashboard
        payload["config_path"] = str(app.state.config_path)
        return JSONResponse(payload)

    @app.get("/api/status")
    async def get_status() -> JSONResponse:
        latest = store.latest_xdp_entry()

        interface = ""
        window_seconds = 0.0
        tick_ms = 0.0
        mode = ""

        if latest is not None:
            entry = latest["entry"]
            interface = str(entry.get("interface") or "")
            mode = str(entry.get("mode") or "")
            window = entry.get("window") or {}
            duration = float(window.get("duration") or 0.0)
            if duration <= 0.0:
                start = float(window.get("start") or 0.0)
                end = float(window.get("end") or 0.0)
                if end > start:
                    duration = end - start
            if duration <= 0.0 and entry.get("samples"):
                # 采样数存在但 duration 缺失，无法推断具体窗口，只能保持默认 0
                duration = 0.0
            window_seconds = duration

            samples = int(entry.get("samples") or 0)
            if samples > 0 and duration > 0.0:
                tick_ms = (duration / samples) * 1000.0

        config_payload = {
            "interface": interface,
            "window_seconds": round(window_seconds, 3) if window_seconds else 0,
            "tick_ms": round(tick_ms, 3) if tick_ms else 0,
            "mode": mode,
            "alert_threshold_bps": cfg.frontend.alert_threshold_bps,
            "refresh_interval_ms": cfg.frontend.refresh_interval_ms,
            "dashboard": app.state.dashboard,
        }

        return JSONResponse({"config": config_payload, "last_error": None})

    @app.get("/api/snapshot")
    async def get_snapshot() -> JSONResponse:
        snapshot = store.snapshot()
        snapshot["dashboard"] = app.state.dashboard
        return JSONResponse(snapshot)

    @app.get("/api/alerts")
    async def get_alerts() -> JSONResponse:
        return JSONResponse({"alerts": store.alerts(), "dashboard": app.state.dashboard})

    @app.get("/api/buckets")
    async def get_buckets(debug: bool = Query(False, description="返回额外调试信息")) -> JSONResponse:
        data = store.xdp_buckets()
        payload: Dict[str, Any] = {"data": data, "dashboard": app.state.dashboard}
        if debug:
            snapshot = store.xdp_snapshot()
            payload["meta"] = {"raw_series": snapshot}
        return JSONResponse(payload)

    @app.get("/api/integrity")
    async def get_integrity(
        exchange: Optional[str] = Query(None, description="交易所过滤，例如 binance-futures"),
        symbol: Optional[str] = Query(None, description="交易对过滤，例如 BTCUSDT"),
        hostname: Optional[str] = Query(None, description="来源主机名，例如 cc-jp-yf-srv-195"),
        interface: Optional[str] = Query(None, description="来源网卡，例如 ens18"),
        stage: Optional[str] = Query(None, description="阶段过滤，例如 1m/5m"),
        event_type: Optional[str] = Query(
            None,
            alias="type",
            description="事件类型过滤，例如 trade/inc_seq",
        ),
        limit: int = Query(180, ge=1, le=2000, description="返回的最大记录条数"),
        meta: bool = Query(False, description="是否附带可选 key 列表"),
    ) -> JSONResponse:
        data = store.integrity_series(
            exchange=exchange,
            symbol=symbol,
            hostname=hostname,
            interface=interface,
            type_filter=event_type,
            stage=stage,
            limit=limit,
        )
        payload: Dict[str, Any] = {"data": data, "dashboard": app.state.dashboard}
        if meta:
            payload["meta"] = {"keys": store.integrity_keys()}
        return JSONResponse(payload)

    @app.websocket("/ws/metrics")
    async def metrics_ws(ws: WebSocket) -> None:
        await hub.add(ws, store.snapshot())
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            await hub.remove(ws)
        except Exception:  # noqa: BLE001
            await hub.remove(ws)

    @app.get("/.well-known/appspecific/com.chrome.devtools.json")
    async def get_chrome_devtools_descriptor() -> JSONResponse:
        # Returning an empty manifest avoids noisy 404s from Chrome DevTools probes.
        return JSONResponse({"targets": []})

    if FRONTEND_DIR.exists():
        app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

    return app


def _create_multi_dashboard_app(config_map: Dict[str, pathlib.Path]) -> FastAPI:
    _setup_logging()
    root_app = FastAPI(title="Market Integrity Monitor", version="1.0.0")
    root_app.state.dashboards = {name: str(path) for name, path in config_map.items()}
    root_app.state.sub_apps: Dict[str, FastAPI] = {}

    first_prefix: str | None = None
    for name, path in config_map.items():
        prefix = f"/{name.strip('/')}"
        sub_app = _create_dashboard_app(path, dashboard_name=name)
        root_app.mount(prefix, sub_app)
        root_app.state.sub_apps[prefix] = sub_app

        @root_app.get(prefix, include_in_schema=False)
        async def redirect_without_slash(prefix_path: str = prefix) -> RedirectResponse:
            return RedirectResponse(url=f"{prefix_path}/", status_code=307)

        if first_prefix is None:
            first_prefix = prefix

    @root_app.get("/api/dashboards")
    async def list_dashboards() -> JSONResponse:
        return JSONResponse({"dashboards": list(config_map.keys())})

    if first_prefix:
        @root_app.get("/")
        async def redirect_root() -> RedirectResponse:
            return RedirectResponse(url=f"{first_prefix}/", status_code=307)

    @root_app.on_event("startup")
    async def _startup_sub_apps() -> None:
        for prefix, sub_app in root_app.state.sub_apps.items():
            logging.getLogger("backend").info("启动子应用: prefix=%s", prefix)
            await sub_app.router.startup()

    @root_app.on_event("shutdown")
    async def _shutdown_sub_apps() -> None:
        for prefix, sub_app in root_app.state.sub_apps.items():
            logging.getLogger("backend").info("停止子应用: prefix=%s", prefix)
            await sub_app.router.shutdown()

    return root_app


def create_app(config_path: pathlib.Path = CONFIG_PATH) -> FastAPI:
    _setup_logging()
    multi_raw = os.getenv(CONFIGS_ENV_VAR, "").strip()
    if multi_raw:
        config_map = _parse_config_map(multi_raw)
        return _create_multi_dashboard_app(config_map)

    discovered_map = _discover_config_map()
    if discovered_map:
        logging.getLogger("backend").info(
            "检测到多配置文件: %s",
            ", ".join(f"{name}={path}" for name, path in discovered_map.items()),
        )
        return _create_multi_dashboard_app(discovered_map)

    env_path = os.getenv(CONFIG_ENV_VAR)
    if env_path:
        resolved = _resolve_config_path(env_path)
    else:
        resolved = _resolve_config_path(config_path)
    return _create_dashboard_app(resolved)


app = create_app()
