from __future__ import annotations

import asyncio
import logging
import pathlib
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

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


def _coerce_trade_timestamp(value: Any) -> Optional[float]:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts <= 0.0:
        return None
    if ts >= 1_000_000_000_000:
        return ts / 1000.0
    return ts


def _extract_trade_timestamp(payload: Dict[str, Any]) -> Optional[float]:
    for field in ("timestamp_ms", "period_end_ts", "tp", "timestamp"):
        ts = _coerce_trade_timestamp(payload.get(field))
        if ts is not None:
            return ts
    return None


class TradeBatchAggregator:
    """Simple delayed aggregator for trade integrity events."""

    def __init__(
        self,
        *,
        delay_seconds: float,
        processor: Callable[[Any, str, List[Dict[str, Any]], Dict[str, Any]], None],
    ) -> None:
        self._delay = max(delay_seconds, 0.0)
        self._processor = processor
        self._lock = threading.Lock()
        self._batches: Dict[str, Dict[str, Any]] = {}
        self._log = logging.getLogger("backend.integrity")

    def add(
        self,
        stream_cfg: Any,
        topic: str,
        payload: Dict[str, Any],
        defaults: Dict[str, Any],
    ) -> None:
        if not isinstance(payload, dict):
            return
        key = self._make_key(stream_cfg, topic, payload, defaults)
        payload_copy = dict(payload)
        defaults_copy = dict(defaults or {})
        with self._lock:
            batch = self._batches.get(key)
            if batch is None:
                timer = threading.Timer(self._delay, self._flush, args=(key,))
                timer.daemon = True
                self._batches[key] = {
                    "stream_cfg": stream_cfg,
                    "topic": topic,
                    "payloads": [payload_copy],
                    "defaults": defaults_copy,
                    "timer": timer,
                }
                timer.start()
            else:
                batch["payloads"].append(payload_copy)
                batch["topic"] = topic
                merged_defaults = dict(batch.get("defaults") or {})
                for key_name, value in defaults_copy.items():
                    if value:
                        merged_defaults[key_name] = value
                batch["defaults"] = merged_defaults

    def _flush(self, batch_key: str) -> None:
        with self._lock:
            batch = self._batches.pop(batch_key, None)
        if not batch:
            return
        payloads = batch.get("payloads") or []
        if not payloads:
            return
        try:
            self._processor(
                batch.get("stream_cfg"),
                batch.get("topic") or "",
                payloads,
                batch.get("defaults") or {},
            )
        except Exception:  # noqa: BLE001
            self._log.exception("处理 trade 聚合批次失败: key=%s size=%d", batch_key, len(payloads))

    def _make_key(
        self,
        stream_cfg: Any,
        topic: str,
        payload: Dict[str, Any],
        defaults: Dict[str, Any],
    ) -> str:
        host = str((defaults or {}).get("hostname") or "")
        interface = str((defaults or {}).get("interface") or "")
        exchange = str(payload.get("exchange") or "").lower()
        minute_raw = payload.get("minute")
        minute_part = str(minute_raw) if minute_raw is not None else ""
        ts = _extract_trade_timestamp(payload)
        bucket = str(int(ts // 60)) if ts is not None else ""
        topic_part = topic or getattr(stream_cfg, "topic", "")
        event_type = str(payload.get("type") or "").lower()
        return "|".join(
            [
                getattr(stream_cfg, "name", "unknown"),
                topic_part,
                host,
                interface,
                exchange,
                minute_part,
                bucket,
                event_type,
            ]
        )

    def drain(self) -> None:
        while True:
            with self._lock:
                if not self._batches:
                    return
                batch_key, batch = self._batches.popitem()
            timer = batch.get("timer")
            if timer:
                timer.cancel()
            payloads = batch.get("payloads") or []
            if not payloads:
                continue
            try:
                self._processor(
                    batch.get("stream_cfg"),
                    batch.get("topic") or "",
                    payloads,
                    batch.get("defaults") or {},
                )
            except Exception:  # noqa: BLE001
                self._log.exception("同步刷新 trade 聚合批次失败: key=%s size=%d", batch_key, len(payloads))


def create_app(config_path: pathlib.Path = CONFIG_PATH) -> FastAPI:
    _setup_logging()
    log = logging.getLogger("backend.integrity")
    cfg: AppConfig = load_config(config_path)

    store = MetricStore(
        xdp_points=cfg.retention.xdp_points,
        integrity_points=cfg.retention.integrity_points,
        retention_seconds=cfg.retention.retention_seconds,
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

    def _process_trade_batch(
        stream_cfg: Any,
        topic: str,
        payloads: List[Dict[str, Any]],
        defaults: Dict[str, Any],
    ) -> None:
        if not payloads:
            return

        exchange = ""
        minute = None
        batch_items: List[Dict[str, Any]] = []
        timestamps: List[float] = []

        for raw in payloads:
            if not isinstance(raw, dict):
                continue
            exchange_candidate = str(raw.get("exchange") or "")
            if exchange_candidate and not exchange:
                exchange = exchange_candidate
            if raw.get("minute") is not None:
                minute = raw.get("minute")
            item_ts = _extract_trade_timestamp(raw)
            if item_ts is not None:
                timestamps.append(item_ts)
            status = str(raw.get("status") or "").lower()
            detail = raw.get("detail")
            symbol = str(raw.get("symbol") or "")
            item_payload = {
                "exchange": exchange_candidate,
                "symbol": symbol,
                "status": status,
                "detail": detail,
                "minute": raw.get("minute"),
                "timestamp": item_ts,
                "timestamp_iso": datetime.fromtimestamp(item_ts, tz=timezone.utc).isoformat()
                if item_ts is not None
                else None,
            }
            batch_items.append(item_payload)

        if not exchange and payloads:
            exchange = str(payloads[-1].get("exchange") or "")

        timestamp = max(timestamps) if timestamps else time.time()
        failures = [item for item in batch_items if item.get("status") != "ok"]
        status = "ok" if not failures else "error"

        if failures:
            fragments = []
            for item in failures:
                symbol_label = item.get("symbol") or "*"
                if item.get("detail"):
                    fragments.append(f"{symbol_label}({item['detail']})")
                else:
                    fragments.append(symbol_label)
            detail_text = f"异常符号: {', '.join(fragments)}"
        else:
            detail_text = f"{len(batch_items)} 个交易对正常"

        aggregate_payload: Dict[str, Any] = {
            "type": "trade",
            "exchange": exchange,
            "symbol": "__batch__",
            "status": status,
            "detail": detail_text,
            "minute": minute,
            "timestamp": timestamp,
            "trade_batch": True,
            "trade_batch_size": len(batch_items),
            "trade_batch_failures": len(failures),
            "trade_batch_items": batch_items,
        }

        key, entry, alerted = store.add_integrity_payload(
            aggregate_payload,
            source=getattr(stream_cfg, "name", None),
            defaults=defaults,
        )

        log_func = log.warning if failures else log.info
        log_func(
            "trade integrity batch: stream=%s topic=%s exchange=%s minute=%s size=%d failures=%d status=%s",
            getattr(stream_cfg, "name", None),
            topic,
            exchange,
            minute,
            len(batch_items),
            len(failures),
            status,
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

    trade_batch_aggregator = TradeBatchAggregator(
        delay_seconds=0.1,
        processor=_process_trade_batch,
    )
    app.state.trade_batch_aggregator = trade_batch_aggregator

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
        fallback = {
            "hostname": getattr(stream_cfg, "hostname", None),
            "interface": getattr(stream_cfg, "interface", None),
        }
        event_type = str(payload.get("type") or "").lower()
        if event_type == "trade" and trade_batch_aggregator is not None:
            log.debug(
                "trade integrity raw event queued: stream=%s topic=%s exchange=%s symbol=%s status=%s minute=%s",
                stream_cfg.name,
                topic,
                payload.get("exchange"),
                payload.get("symbol"),
                payload.get("status"),
                payload.get("minute"),
            )
            trade_batch_aggregator.add(stream_cfg, topic, payload, fallback)
            return
        key, entry, alerted = store.add_integrity_payload(
            payload,
            source=stream_cfg.name,
            defaults=fallback,
        )
        if entry.get("type") == "trade":
            log.info(
                "trade integrity event: stream=%s topic=%s key=%s exchange=%s symbol=%s status=%s host=%s iface=%s minute=%s timestamp=%.3f",
                stream_cfg.name,
                topic,
                key,
                entry.get("exchange"),
                entry.get("symbol"),
                entry.get("status"),
                entry.get("hostname"),
                entry.get("interface"),
                entry.get("minute"),
                float(entry.get("timestamp") or 0.0),
            )
        else:
            log.debug(
                "integrity event: type=%s stream=%s topic=%s key=%s status=%s host=%s iface=%s minute=%s",
                entry.get("type"),
                stream_cfg.name,
                topic,
                key,
                entry.get("status"),
                entry.get("hostname"),
                entry.get("interface"),
                entry.get("minute"),
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
        if trade_batch_aggregator is not None:
            trade_batch_aggregator.drain()
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
