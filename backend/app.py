from __future__ import annotations

import asyncio
import pathlib
from dataclasses import asdict
from typing import Any, Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import AppConfig, load_config
from .store import MetricStore
from .subscriber import ZMQSubscriber

CONFIG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config.yaml"
FRONTEND_DIR = pathlib.Path(__file__).resolve().parent.parent / "frontend"


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
    cfg: AppConfig = load_config(config_path)

    store = MetricStore(
        xdp_points=cfg.retention.xdp_points,
        integrity_points=cfg.retention.integrity_points,
    )
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
        key, entry, alerted = store.add_integrity_payload(payload, source=stream_cfg.name)
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

    @app.get("/api/snapshot")
    async def get_snapshot() -> JSONResponse:
        return JSONResponse(store.snapshot())

    @app.get("/api/alerts")
    async def get_alerts() -> JSONResponse:
        return JSONResponse({"alerts": store.alerts()})

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

    if FRONTEND_DIR.exists():
        app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

    return app


app = create_app()
