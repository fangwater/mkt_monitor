from __future__ import annotations

import asyncio
import logging
import pathlib
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import AppConfig, load_config
from .store import MetricStore
from .subscriber import ZMQSubscriber

CONFIG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config.yaml"
FRONTEND_DIR = pathlib.Path(__file__).resolve().parent.parent / "frontend"


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
def create_app(config_path: pathlib.Path = CONFIG_PATH) -> FastAPI:
    _setup_logging()
    log = logging.getLogger("backend.integrity")
    cfg: AppConfig = load_config(config_path)

    store = MetricStore(
        xdp_points=cfg.retention.xdp_points,
        integrity_points=cfg.retention.integrity_points,
        retention_seconds=cfg.retention.retention_seconds,
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
    app.state.store = store
    app.state.hub = hub
    app.state.loop = None
    app.state.subscribers: List[ZMQSubscriber] = []

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
                "忽略空的完整性事件: stream=%s topic=%s payload_keys=%s",
                stream_cfg.name,
                topic,
                list(sorted(payload.keys())),
            )
            return

        for key, entry, alerted in updates:
            log_func = log.warning if alerted else log.debug
            log_func(
                "integrity event: stream=%s topic=%s key=%s type=%s stage=%s symbol=%s status=%s detail=%s",
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
        return JSONResponse(asdict(cfg))

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
        }

        return JSONResponse({"config": config_payload, "last_error": None})

    @app.get("/api/snapshot")
    async def get_snapshot() -> JSONResponse:
        return JSONResponse(store.snapshot())

    @app.get("/api/alerts")
    async def get_alerts() -> JSONResponse:
        return JSONResponse({"alerts": store.alerts()})

    @app.get("/api/buckets")
    async def get_buckets(debug: bool = Query(False, description="返回额外调试信息")) -> JSONResponse:
        data = store.xdp_buckets()
        if debug:
            snapshot = store.xdp_snapshot()
            return JSONResponse({"data": data, "meta": {"raw_series": snapshot}})
        return JSONResponse({"data": data})

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
        meta_payload: Dict[str, Any] | None = None
        if meta:
            meta_payload = {"keys": store.integrity_keys()}
        payload: Dict[str, Any] = {"data": data}
        if meta_payload:
            payload["meta"] = meta_payload
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


app = create_app()
