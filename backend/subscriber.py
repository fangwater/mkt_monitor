from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Iterable, Tuple

import zmq

from .config import ZMQStreamConfig

log = logging.getLogger(__name__)


PayloadHandler = Callable[[ZMQStreamConfig, str, Dict[str, Any]], None]


class ZMQSubscriber(threading.Thread):
    """简单的 ZMQ SUB 背景线程。"""

    def __init__(self, config: ZMQStreamConfig, handler: PayloadHandler) -> None:
        name = f"zmq-sub:{config.name}"
        super().__init__(name=name, daemon=True)
        self._cfg = config
        self._handler = handler
        self._stop_event = threading.Event()
        self._ctx = zmq.Context.instance()
        self._socket: zmq.Socket | None = None
        self._message_counter = 0
        self._last_log = 0.0

    def stop(self) -> None:
        self._stop_event.set()
        if self._socket is not None:
            try:
                self._socket.close(0)
            except Exception:  # noqa: BLE001
                pass

    def run(self) -> None:
        socket = self._ctx.socket(zmq.SUB)
        self._socket = socket

        try:
            socket.connect(self._cfg.endpoint)
            socket.setsockopt_string(zmq.SUBSCRIBE, self._cfg.topic)
            log.info(
                "订阅线程启动: stream=%s endpoint=%s topic='%s'",
                self._cfg.name,
                self._cfg.endpoint,
                self._cfg.topic,
            )

            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)

            while not self._stop_event.is_set():
                events = dict(poller.poll(1000))
                if socket not in events:
                    continue
                try:
                    frames = socket.recv_multipart()
                except zmq.Again:
                    continue
                except zmq.Error as exc:
                    if self._stop_event.is_set():
                        break
                    log.warning("ZMQ 接收失败 (%s): %s", self._cfg.name, exc)
                    continue

                topic, payload = self._decode_frames(frames)
                if payload is None:
                    continue

                try:
                    self._handler(self._cfg, topic, payload)
                    self._message_counter += 1
                    now = time.time()
                    if self._message_counter == 1 or (now - self._last_log) >= 30:
                        self._last_log = now
                        log.info(
                            "订阅到消息: stream=%s total=%d last_topic='%s'",
                            self._cfg.name,
                            self._message_counter,
                            topic,
                        )
                except Exception:  # noqa: BLE001
                    log.exception("处理 ZMQ 消息失败 (%s)", self._cfg.name)

        except Exception:  # noqa: BLE001
            if not self._stop_event.is_set():
                log.exception("ZMQ 订阅线程异常退出 (%s)", self._cfg.name)
        finally:
            try:
                socket.close(0)
            except Exception:  # noqa: BLE001
                pass
            self._socket = None

    @staticmethod
    def _decode_frames(frames: Iterable[bytes]) -> Tuple[str, Dict[str, Any] | None]:
        frames = list(frames)
        if not frames:
            return "", None

        if len(frames) == 1:
            topic = ""
            data = frames[0]
        else:
            topic = frames[0].decode("utf-8", errors="ignore")
            data = frames[-1]

        try:
            payload = json.loads(data.decode("utf-8"))
        except json.JSONDecodeError as exc:
            log.warning("无法解析 JSON (topic=%s): %s", topic, exc)
            return topic, None

        return topic, payload
