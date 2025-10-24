from __future__ import annotations

import pathlib

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .collector import TrafficCollector
from .config import AppConfig, load_config

CONFIG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config.yaml"
FRONTEND_DIR = pathlib.Path(__file__).resolve().parent.parent / "frontend"


def create_app(config_path: pathlib.Path = CONFIG_PATH) -> FastAPI:
    cfg: AppConfig = load_config(config_path)

    collector = TrafficCollector(cfg.xdp, cfg.aggregation)

    app = FastAPI(title="XDP 3min Max Monitor", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    async def _startup() -> None:  # noqa: D401
        collector.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:  # noqa: D401
        collector.stop()

    @app.get("/api/buckets")
    async def get_buckets():
        error = collector.last_error()
        if error:
            raise HTTPException(status_code=500, detail=error)
        return JSONResponse({"data": collector.buckets()})

    @app.get("/api/status")
    async def get_status():
        return JSONResponse(
            {
                "config": {
                    "interface": cfg.xdp.interface,
                    "tick_ms": cfg.xdp.tick_ms,
                    "mode": cfg.xdp.mode,
                    "window_seconds": cfg.aggregation.window_seconds,
                    "history_hours": cfg.aggregation.history_hours,
                    "refresh_interval_ms": cfg.frontend.refresh_interval_ms,
                    "alert_threshold_bps": cfg.frontend.alert_threshold_bps,
                },
                "latest_sample": collector.latest_sample(),
                "last_error": collector.last_error(),
            }
        )

    if FRONTEND_DIR.exists():
        app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

    return app


app = create_app()
