#!/usr/bin/env python3

"""
简单的 ZeroMQ SUB 接收端，用于验证 XDP 采样推送。

默认连接 tcp://127.0.0.1:16666，可通过 --endpoint 覆盖。
支持解析 JSON 消息，并友好地打印出来。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Optional

try:
    import zmq
except ImportError as exc:  # pragma: no cover
    raise SystemExit("缺少 pyzmq 依赖，请先安装: pip install pyzmq") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ZeroMQ 采样数据订阅测试脚本")
    parser.add_argument(
        "--endpoint",
        default="tcp://127.0.0.1:16666",
        help="连接到已绑定 PUB socket 的地址，例如 tcp://127.0.0.1:16666",
    )
    parser.add_argument(
        "--bind",
        help="将 SUB 端绑定到指定地址，例如 tcp://*:16666（与 endpoint 二选一）",
    )
    parser.add_argument(
        "--topic",
        default="",
        help="订阅的主题前缀（默认订阅全部）",
    )
    parser.add_argument(
        "--dump-raw",
        action="store_true",
        help="同时打印原始消息体，便于调试非 JSON 数据",
    )
    return parser.parse_args()


def pretty_print(payload: bytes) -> None:
    text = payload.decode("utf-8", errors="replace")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        print(f"[{time.strftime('%H:%M:%S')}] <raw> {text}")
        return

    formatted = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"[{time.strftime('%H:%M:%S')}] <json>\n{formatted}")


def recv_loop(endpoint: str, bind: Optional[str], topic: str, dump_raw: bool) -> None:
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.SUB)

    topic_bytes = topic.encode("utf-8")
    socket.setsockopt(zmq.SUBSCRIBE, topic_bytes)

    if bind:
        socket.bind(bind)
        print(f"已绑定在 {bind}，等待 PUB 端连接。按 Ctrl+C 退出。")
    else:
        socket.connect(endpoint)
        print(f"已连接到 {endpoint}，订阅主题前缀 '{topic}'。按 Ctrl+C 退出。")

    try:
        while True:
            parts = socket.recv_multipart()
            if topic:
                # PUB 端发送 theme + payload 时，需要先匹配主题
                if len(parts) >= 2:
                    received_topic = parts[0].decode("utf-8", errors="replace")
                    payload = parts[1]
                else:
                    received_topic = topic
                    payload = parts[0]
            else:
                received_topic = ""
                payload = parts[-1]

            if dump_raw:
                print(f"[{time.strftime('%H:%M:%S')}] <topic='{received_topic}'> payload={payload!r}")

            pretty_print(payload)
    except KeyboardInterrupt:
        print("\n已停止接收。")
    finally:
        socket.close(0)
        ctx.term()


def main() -> None:
    args = parse_args()
    try:
        recv_loop(args.endpoint, args.bind, args.topic, args.dump_raw)
    except Exception as exc:  # noqa: BLE001
        print(f"接收过程中发生错误: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
