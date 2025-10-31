#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于 XDP 的高分辨率带宽监测工具。

本模块既可直接运行，也可作为库被其它程序调用。

核心功能：
  * 在 XDP 层累加每个包的字节/报文数
  * 用户态按照指定 tick (默认 50ms，可低至 10ms) 轮询
  * 支持 driver/native 与 SKB 模式，并在 auto 模式下自动回退
  * 提供 JSON 行输出，便于下游聚合/可视化
"""

from __future__ import annotations

import argparse
import ctypes as ct
import json
import resource
import signal
import socket
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from bcc import BPF

try:
    XDP_FLAGS_UPDATE_IF_NOEXIST = BPF.XDP_FLAGS_UPDATE_IF_NOEXIST  # type: ignore[attr-defined]
except AttributeError:
    XDP_FLAGS_UPDATE_IF_NOEXIST = 1 << 0

try:
    XDP_FLAGS_SKB_MODE = BPF.XDP_FLAGS_SKB_MODE  # type: ignore[attr-defined]
except AttributeError:
    XDP_FLAGS_SKB_MODE = 1 << 1

try:
    XDP_FLAGS_DRV_MODE = BPF.XDP_FLAGS_DRV_MODE  # type: ignore[attr-defined]
except AttributeError:
    XDP_FLAGS_DRV_MODE = 1 << 2

XDP_PROGRAM = r"""
#include <uapi/linux/bpf.h>

struct datarec {
    __u64 bytes;
    __u64 packets;
};

BPF_PERCPU_ARRAY(stats_map, struct datarec, 1);

int xdp_bandwidth(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;
    __u64 len = (__u64)(data_end - data);
    __u32 key = 0;
    struct datarec *rec;

    rec = stats_map.lookup(&key);
    if (!rec)
        return XDP_PASS;

    rec->bytes += len;
    rec->packets += 1;

    return XDP_PASS;
}
"""


class DataRec(ct.Structure):
    _fields_ = [
        ("bytes", ct.c_ulonglong),
        ("packets", ct.c_ulonglong),
    ]


@dataclass
class Sample:
    """单个采样点数据。"""

    timestamp: float          # Unix time (秒)
    interval: float           # 本次采样间隔（秒）
    bytes: int
    packets: int
    bps: float
    pps: float


class ZMQPublisher:
    """简单的 ZeroMQ PUSH 发布封装。"""

    def __init__(self, endpoint: str, pattern: str = "push", bind: bool = False) -> None:
        try:
            import zmq  # type: ignore[import]
        except ImportError as exc:
            raise SystemExit("缺少 pyzmq 依赖，请先安装: pip install pyzmq") from exc

        self._endpoint = endpoint
        self._ctx = zmq.Context.instance()
        pattern = pattern.lower()
        if pattern == "push":
            socket_type = zmq.PUSH
        elif pattern == "pub":
            socket_type = zmq.PUB
        else:
            raise SystemExit(f"不支持的 zmq pattern: {pattern}")
        self._pattern = pattern
        self._bind = bind
        self._socket = self._ctx.socket(socket_type)
        if bind:
            self._socket.bind(endpoint)
        else:
            self._socket.connect(endpoint)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def pattern(self) -> str:
        return self._pattern

    @property
    def bind(self) -> bool:
        return self._bind

    def send(self, payload: Dict[str, Any]) -> None:
        try:
            self._socket.send_json(payload, ensure_ascii=False)
        except Exception as exc:  # noqa: BLE001
            print(f"ZMQ 发送失败 ({self._endpoint}): {exc}", file=sys.stderr)

    def close(self) -> None:
        try:
            self._socket.close(0)
        except Exception:  # noqa: BLE001
            pass


def ensure_memlock_limit() -> None:
    """将 MEMLOCK 调整到无限制，避免 bcc 抛出权限错误。"""
    try:
        resource.setrlimit(resource.RLIMIT_MEMLOCK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
    except (ValueError, PermissionError, OSError):
        pass


def confirm_iface(name: str) -> None:
    try:
        socket.if_nametoindex(name)
    except OSError:
        raise SystemExit(f"接口 {name} 不存在") from None


def read_totals(table) -> Tuple[int, int]:
    """读取 PERCPU array 的累加值。"""
    key = ct.c_uint(0)
    try:
        aggregated = table.sum(key)
        return int(aggregated.bytes), int(aggregated.packets)
    except Exception:  # noqa: BLE001
        values = table[key]
        total_bytes = 0
        total_packets = 0
        for cpu_val in values:
            total_bytes += int(cpu_val.bytes)
            total_packets += int(cpu_val.packets)
        return total_bytes, total_packets


def format_rate(bps: float) -> str:
    units = ["bps", "Kbps", "Mbps", "Gbps", "Tbps"]
    value = bps
    for unit in units:
        if value < 1000.0:
            return f"{value:7.2f}{unit}"
        value /= 1000.0
    return f"{value:7.2f}Pbps"


def iso_timestamp(ts: float) -> str:
    """将时间戳格式化为 ISO8601 字符串（UTC）。"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def parse_bool(value: Any) -> bool:
    """将布尔或字符串解析为 bool。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    raise ValueError("expect bool or string")


class XDPBandwidthMonitor:
    """封装 XDP 采样流程，便于重复使用。"""

    def __init__(
        self,
        iface: str,
        interval: float = 0.05,
        mode: str = "auto",
        debug_bpf: bool = False,
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be positive")
        if mode not in {"auto", "drv", "skb"}:
            raise ValueError("mode must be one of {'auto','drv','skb'}")

        self.iface = iface
        self.interval = interval
        self.mode = mode
        self.debug_bpf = debug_bpf

        self._bpf: Optional[BPF] = None
        self._stats_map = None
        self._flags = 0
        self._prev_bytes = 0
        self._prev_packets = 0
        self._last_ts = 0.0

    def __enter__(self) -> "XDPBandwidthMonitor":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def start(self) -> None:
        confirm_iface(self.iface)
        ensure_memlock_limit()

        self._bpf = BPF(text=XDP_PROGRAM, debug=int(self.debug_bpf))
        fn = self._bpf.load_func("xdp_bandwidth", BPF.XDP)

        def attach(flags: int) -> Optional[int]:
            try:
                self._bpf.attach_xdp(self.iface, fn, flags)
                return flags
            except Exception:
                return None

        chosen = None
        if self.mode in {"auto", "drv"}:
            flags = XDP_FLAGS_DRV_MODE | XDP_FLAGS_UPDATE_IF_NOEXIST
            chosen = attach(flags)
        if chosen is None and self.mode in {"auto", "skb"}:
            flags = XDP_FLAGS_SKB_MODE | XDP_FLAGS_UPDATE_IF_NOEXIST
            chosen = attach(flags)

        if chosen is None:
            raise RuntimeError("无法挂载 XDP 程序，请检查驱动是否支持或尝试手动开启 skb 模式。")

        self._flags = chosen
        self._stats_map = self._bpf.get_table("stats_map")

        self._prev_bytes, self._prev_packets = read_totals(self._stats_map)
        self._last_ts = time.monotonic()

    def stop(self) -> None:
        if self._bpf is not None:
            try:
                self._bpf.remove_xdp(self.iface, self._flags)
            except Exception:  # noqa: BLE001
                pass
            finally:
                self._bpf = None
                self._stats_map = None

    def sample(self) -> Sample:
        """阻塞 interval 秒，返回一次采样。"""
        if self._bpf is None or self._stats_map is None:
            raise RuntimeError("monitor not started")

        time.sleep(self.interval)
        now_monotonic = time.monotonic()
        elapsed = max(1e-9, now_monotonic - self._last_ts)
        self._last_ts = now_monotonic

        curr_bytes, curr_packets = read_totals(self._stats_map)
        delta_bytes = max(0, curr_bytes - self._prev_bytes)
        delta_packets = max(0, curr_packets - self._prev_packets)
        self._prev_bytes, self._prev_packets = curr_bytes, curr_packets

        bps = (delta_bytes * 8.0) / elapsed
        pps = delta_packets / elapsed
        return Sample(
            timestamp=time.time(),
            interval=elapsed,
            bytes=delta_bytes,
            packets=delta_packets,
            bps=bps,
            pps=pps,
        )

    def stream(self) -> Generator[Sample, None, None]:
        """持续生成采样数据。"""
        while True:
            yield self.sample()


DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("xdp_cfg.yaml")


def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    """加载 YAML 配置，如果不存在则返回空 dict。"""
    if config_path:
        path = Path(config_path).expanduser()
    else:
        path = DEFAULT_CONFIG_PATH

    if not path.exists():
        if config_path:
            raise SystemExit(f"指定的配置文件不存在: {path}")
        return {}

    try:
        import yaml  # type: ignore[import]
    except ImportError as exc:  # pragma: no cover - 运行环境问题
        raise SystemExit("缺少 PyYAML 依赖，请先安装: pip install pyyaml") from exc

    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    if not isinstance(data, dict):
        raise SystemExit("配置文件格式错误：顶层应为 key/value 结构")

    return data


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="XDP 小窗口带宽监测 (支持读取 xdp_cfg.yaml)")
    parser.add_argument(
        "-f",
        "--iface",
        help="目标网卡名称（若未提供将尝试读取配置文件 xdp.interface）",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=None,
        help="采样间隔（秒），默认读取配置 xdp.tick_ms（毫秒），否则 0.05",
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=float,
        default=None,
        help="运行总时长（秒，默认 0 表示一直运行）",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "drv", "skb"],
        default=None,
        help="XDP 挂载模式，默认读取配置 xdp.mode 或 auto",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="使用 JSON 行输出，便于机器解析",
    )
    parser.add_argument(
        "--debug-bpf",
        action="store_true",
        help="开启 BPF 编译日志",
    )
    parser.add_argument(
        "--config",
        help=f"配置文件路径，默认 {DEFAULT_CONFIG_PATH}",
    )

    args = parser.parse_args()

    cfg = load_config(args.config)
    xdp_section = cfg.get("xdp", {})
    cfg_xdp = xdp_section if isinstance(xdp_section, dict) else {}
    zmq_section = cfg.get("zmq", {})
    cfg_zmq = zmq_section if isinstance(zmq_section, dict) else {}

    iface = args.iface or cfg_xdp.get("interface")
    if not iface:
        raise SystemExit("未指定网卡。请通过 -f/--iface 或配置文件 xdp.interface 指定。")

    interval = args.interval
    if interval is None:
        tick_ms = cfg_xdp.get("tick_ms")
        if tick_ms is not None:
            try:
                interval = float(tick_ms) / 1000.0
            except (TypeError, ValueError):
                raise SystemExit("配置 xdp.tick_ms 必须为数字")
    if interval is None:
        interval = 0.05
    if interval <= 0:
        raise SystemExit("采样间隔必须大于 0")

    mode = args.mode or cfg_xdp.get("mode") or "auto"
    if mode not in {"auto", "drv", "skb"}:
        raise SystemExit("配置 xdp.mode 仅支持 auto/drv/skb")

    duration = args.duration
    if duration is None:
        duration = cfg_xdp.get("duration_seconds", 0.0)
    try:
        duration = float(duration)
    except (TypeError, ValueError):
        raise SystemExit("运行时长必须为数字")
    if duration < 0:
        raise SystemExit("运行时长不能为负数")

    push_interval = cfg_zmq.get("push_interval_sec", 5)
    try:
        push_interval = float(push_interval)
    except (TypeError, ValueError):
        raise SystemExit("配置 zmq.push_interval_sec 必须为数字")
    if push_interval < 0:
        raise SystemExit("配置 zmq.push_interval_sec 不能为负数")

    port = cfg_zmq.get("send_port")
    if port is None:
        publisher = None
    else:
        try:
            port_int = int(port)
        except (TypeError, ValueError):
            raise SystemExit("配置 zmq.send_port 必须为整数")

        host = cfg_zmq.get("host", "0.0.0.0")
        endpoint = f"tcp://{host}:{port_int}"
        pattern = "pub"
        publisher = ZMQPublisher(endpoint, pattern=pattern, bind=True)

    if publisher is not None:
        action = "绑定" if publisher.bind else "连接"
        print(f"ZeroMQ {publisher.pattern.upper()} 已{action}: {publisher.endpoint}")

    debug_bpf = args.debug_bpf
    output_json = args.json

    stop_flag = {"stop": False}

    def _signal_handler(signum, frame):
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    hostname = socket.gethostname()
    aggregator_samples: List[Sample] = []
    last_push_wall = time.time()

    def maybe_emit(force: bool = False) -> None:
        nonlocal aggregator_samples, last_push_wall
        if publisher is None or not aggregator_samples:
            if force:
                aggregator_samples.clear()
            return

        now_wall = time.time()
        if not force and push_interval > 0 and (now_wall - last_push_wall) < push_interval:
            return

        total_samples = len(aggregator_samples)
        total_bytes = sum(s.bytes for s in aggregator_samples)
        total_packets = sum(s.packets for s in aggregator_samples)
        sum_bps = sum(s.bps for s in aggregator_samples)
        sum_pps = sum(s.pps for s in aggregator_samples)
        max_bps = max(s.bps for s in aggregator_samples)
        max_pps = max(s.pps for s in aggregator_samples)
        window_start = aggregator_samples[0].timestamp
        window_end = aggregator_samples[-1].timestamp
        window_duration = max(window_end - window_start, aggregator_samples[-1].interval)

        payload = {
            "hostname": hostname,
            "interface": iface,
            "mode": mode,
            "window": {
                "start": window_start,
                "end": window_end,
                "duration": window_duration,
                "start_iso": iso_timestamp(window_start),
                "end_iso": iso_timestamp(window_end),
            },
            "samples": total_samples,
            "metrics": {
                "bps_avg": sum_bps / total_samples if total_samples else 0.0,
                "bps_max": max_bps,
                "pps_avg": sum_pps / total_samples if total_samples else 0.0,
                "pps_max": max_pps,
                "bytes_total": total_bytes,
                "packets_total": total_packets,
            },
            "timestamp": now_wall,
            "timestamp_iso": iso_timestamp(now_wall),
            "config": {
                "sample_interval_sec": interval,
                "push_interval_sec": push_interval,
                "endpoint": publisher.endpoint,
                "pattern": publisher.pattern,
                "bind": publisher.bind,
            },
        }

        publisher.send(payload)
        avg_bps = sum_bps / total_samples if total_samples else 0.0
        avg_pps = sum_pps / total_samples if total_samples else 0.0
        print(
            f"[{time.strftime('%H:%M:%S')}] 推送 {total_samples} 样本，窗口 {window_duration:.2f}s "
            f"avg={format_rate(avg_bps)} max={format_rate(max_bps)} "
            f"avg_pps={avg_pps:,.0f} max_pps={max_pps:,.0f} -> {publisher.endpoint}"
        )
        sys.stdout.flush()
        aggregator_samples = []
        last_push_wall = now_wall

    try:
        with XDPBandwidthMonitor(iface, interval, mode, debug_bpf) as monitor:
            start = time.time()
            print(f"已在 {iface} 挂载 XDP 程序，按 Ctrl+C 结束。")
            for sample in monitor.stream():
                if stop_flag["stop"]:
                    if publisher is not None:
                        aggregator_samples.append(sample)
                        maybe_emit(force=True)
                    break

                if publisher is not None:
                    aggregator_samples.append(sample)
                    maybe_emit()
                else:
                    if output_json:
                        print(json.dumps(asdict(sample), ensure_ascii=False))
                    else:
                        timestamp = time.strftime("%H:%M:%S", time.localtime(sample.timestamp))
                        print(
                            f"{timestamp} | interval={sample.interval*1000:.1f}ms "
                            f"bytes={sample.bytes:10d} packets={sample.packets:8d} "
                            f"rate={format_rate(sample.bps)} pps={sample.pps:10.0f}"
                        )

                if duration and (time.time() - start) >= duration:
                    if publisher is not None:
                        maybe_emit(force=True)
                    break
    finally:
        maybe_emit(force=True)
        if publisher is not None:
            publisher.close()


if __name__ == "__main__":
    try:
        run_cli()
    except BrokenPipeError:
        pass
