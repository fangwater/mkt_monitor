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
from typing import Generator, Optional, Tuple

from bcc import BPF

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
            flags = BPF.XDP_FLAGS_DRV_MODE | BPF.XDP_FLAGS_UPDATE_IF_NOEXIST
            chosen = attach(flags)
        if chosen is None and self.mode in {"auto", "skb"}:
            flags = BPF.XDP_FLAGS_SKB_MODE | BPF.XDP_FLAGS_UPDATE_IF_NOEXIST
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


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="XDP 小窗口带宽监测")
    parser.add_argument("-f", "--iface", required=True, help="目标网卡名称")
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=0.05,
        help="采样间隔（秒），默认 0.05 即 50ms",
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=float,
        default=0.0,
        help="运行总时长（秒，0 表示一直运行）",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "drv", "skb"],
        default="auto",
        help="XDP 挂载模式，默认 auto（先尝试 driver，再回退 skb）",
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

    args = parser.parse_args()
    if args.duration < 0:
        raise SystemExit("运行时长不能为负数")

    stop_flag = {"stop": False}

    def _signal_handler(signum, frame):
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    with XDPBandwidthMonitor(args.iface, args.interval, args.mode, args.debug_bpf) as monitor:
        start = time.time()
        print(f"已在 {args.iface} 挂载 XDP 程序，按 Ctrl+C 结束。")
        for sample in monitor.stream():
            if stop_flag["stop"]:
                break

            if args.json:
                print(json.dumps(asdict(sample), ensure_ascii=False))
            else:
                timestamp = time.strftime("%H:%M:%S", time.localtime(sample.timestamp))
                print(
                    f"{timestamp} | interval={sample.interval*1000:.1f}ms "
                    f"bytes={sample.bytes:10d} packets={sample.packets:8d} "
                    f"rate={format_rate(sample.bps)} pps={sample.pps:10.0f}"
                )

            if args.duration and (time.time() - start) >= args.duration:
                break


if __name__ == "__main__":
    try:
        run_cli()
    except BrokenPipeError:
        pass
