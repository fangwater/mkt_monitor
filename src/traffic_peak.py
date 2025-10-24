#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 eBPF (BCC) 在高精度下检测网卡流量峰值。

功能：
  * 基于 tracepoint 捕获网卡收发字节数
  * 按指定采样间隔统计吞吐率，自动跟踪峰值
  * 支持按接口过滤、设置告警阈值
"""

import argparse
import ctypes as ct
import signal
import socket
import time
from datetime import datetime
from typing import Dict, Iterable, Optional, Tuple

from bcc import BPF

BPF_PROGRAM = r"""
#include <uapi/linux/bpf.h>

#define IFNAMSIZ 16

struct datarec {
    __u64 bytes;
    __u64 packets;
};

struct dev_key {
    char name[IFNAMSIZ];
};

BPF_HASH(rx_stats, struct dev_key, struct datarec);
BPF_HASH(tx_stats, struct dev_key, struct datarec);

static __always_inline int read_dev_name(struct dev_key *key, void *ctx, __u32 loc)
{
    __u16 offset = loc & 0xFFFF;
    const char *src = (const char *)ctx + offset;
    int ret = bpf_probe_read_str(key->name, sizeof(key->name), src);
    if (ret <= 0) {
        return -1;
    }
    return 0;
}

static __always_inline void count_packet(void *map, void *ctx, __u32 name_loc, __u32 len)
{
    struct dev_key key = {};
    struct datarec zero = {};
    struct datarec *val;

    if (read_dev_name(&key, ctx, name_loc) < 0) {
        return;
    }

    val = bpf_map_lookup_elem(map, &key);
    if (!val) {
        bpf_map_update_elem(map, &key, &zero, BPF_NOEXIST);
        val = bpf_map_lookup_elem(map, &key);
        if (!val) {
            return;
        }
    }

    __sync_fetch_and_add(&val->packets, 1);
    __sync_fetch_and_add(&val->bytes, len);
}

TRACEPOINT_PROBE(net, netif_receive_skb)
{
    count_packet(&rx_stats, args, args->name, args->len);
    return 0;
}

TRACEPOINT_PROBE(net, net_dev_queue)
{
    count_packet(&tx_stats, args, args->name, args->len);
    return 0;
}
"""


class DevKey(ct.Structure):
    _fields_ = [
        ("name", ct.c_char * 16),
    ]


class DataRec(ct.Structure):
    _fields_ = [
        ("bytes", ct.c_ulonglong),
        ("packets", ct.c_ulonglong),
    ]


def resolve_ifaces(filters: Optional[Iterable[str]]) -> Optional[Dict[int, str]]:
    if not filters:
        return None

    result = set()
    for name in filters:
        try:
            ifindex = socket.if_nametoindex(name)
        except OSError:
            raise SystemExit(f"接口 {name} 不存在") from None
        result.add(name)
    return result


def decode_ifname(raw: bytes) -> str:
    name = raw.split(b"\x00", 1)[0].decode("utf-8", "replace")
    return name or "<unknown>"


def snapshot(table) -> Dict[str, Tuple[int, int]]:
    stats: Dict[str, Tuple[int, int]] = {}
    for key, leaf in table.items():
        if hasattr(key, "name"):
            ifname = decode_ifname(bytes(bytearray(key.name)))
        else:
            ifname = str(key)

        if hasattr(leaf, "bytes"):
            stats[ifname] = (int(leaf.bytes), int(leaf.packets))
        else:
            data = ct.cast(ct.pointer(leaf), ct.POINTER(DataRec)).contents
            stats[ifname] = (int(data.bytes), int(data.packets))
    return stats


def format_rate(bps: float) -> str:
    units = ["bps", "Kbps", "Mbps", "Gbps", "Tbps"]
    value = bps
    for unit in units:
        if value < 1000.0:
            return f"{value:7.2f}{unit}"
        value /= 1000.0
    return f"{value:7.2f}Pbps"


def install_signal_handlers(stop_flag):
    def _handler(signum, frame):
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main():
    parser = argparse.ArgumentParser(description="检测网卡流量峰值 (eBPF/BCC)")
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=1.0,
        help="采样间隔（秒，默认 1.0）",
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=float,
        default=0.0,
        help="运行总时长（秒，默认不限）",
    )
    parser.add_argument(
        "-f",
        "--iface",
        action="append",
        help="仅监控指定接口，可重复使用多次",
    )
    parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=0.0,
        help="峰值告警阈值（bps，默认 0 表示不告警）",
    )
    parser.add_argument(
        "--debug-bpf",
        action="store_true",
        help="打印 BPF 编译日志",
    )

    args = parser.parse_args()

    if args.interval <= 0:
        raise SystemExit("采样间隔必须大于 0")
    if args.duration < 0:
        raise SystemExit("运行时长不能为负数")

    filters = resolve_ifaces(args.iface)

    bpf = BPF(text=BPF_PROGRAM, debug=int(args.debug_bpf))
    bpf.attach_tracepoint(tp="net:netif_receive_skb", fn_name="tracepoint__net__netif_receive_skb")
    bpf.attach_tracepoint(tp="net:net_dev_queue", fn_name="tracepoint__net__net_dev_queue")

    rx_table = bpf.get_table("rx_stats")
    tx_table = bpf.get_table("tx_stats")

    prev_rx: Dict[str, Tuple[int, int]] = {}
    prev_tx: Dict[str, Tuple[int, int]] = {}
    peak_total: Dict[str, float] = {}
    peak_rx: Dict[str, float] = {}
    peak_tx: Dict[str, float] = {}

    stop_flag = {"stop": False}
    install_signal_handlers(stop_flag)

    start_time = time.time()
    last_sample = start_time
    first_loop = True

    print("开始监控，按 Ctrl+C 结束")
    while not stop_flag["stop"]:
        time.sleep(args.interval)
        now = time.time()
        elapsed = now - last_sample
        last_sample = now

        if elapsed <= 0:
            continue

        rx_snap = snapshot(rx_table)
        tx_snap = snapshot(tx_table)

        if filters is not None:
            rx_snap = {k: v for k, v in rx_snap.items() if k in filters}
            tx_snap = {k: v for k, v in tx_snap.items() if k in filters}

        timestamp = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{timestamp} | interval={elapsed:.3f}s")

        ifaces = set(rx_snap) | set(tx_snap)
        if not ifaces:
            print("  暂无数据 (可能接口未收到/发送流量)")
            continue

        for ifname in sorted(ifaces):
            curr_rx = rx_snap.get(ifname, (0, 0))
            curr_tx = tx_snap.get(ifname, (0, 0))
            prev_rx_entry = prev_rx.get(ifname, curr_rx)
            prev_tx_entry = prev_tx.get(ifname, curr_tx)

            rx_bytes = max(0, curr_rx[0] - prev_rx_entry[0])
            tx_bytes = max(0, curr_tx[0] - prev_tx_entry[0])

            rx_bps = (rx_bytes * 8) / elapsed
            tx_bps = (tx_bytes * 8) / elapsed
            total_bps = rx_bps + tx_bps

            if first_loop:
                peak_rx[ifname] = rx_bps
                peak_tx[ifname] = tx_bps
                peak_total[ifname] = total_bps
                new_peak = False
            else:
                new_peak = False
                if rx_bps > peak_rx.get(ifname, 0.0):
                    peak_rx[ifname] = rx_bps
                if tx_bps > peak_tx.get(ifname, 0.0):
                    peak_tx[ifname] = tx_bps
                prev_total_peak = peak_total.get(ifname, 0.0)
                if total_bps > prev_total_peak:
                    peak_total[ifname] = total_bps
                    new_peak = True

            flags = []
            if args.threshold > 0 and total_bps >= args.threshold:
                flags.append("ALERT")
            if not first_loop and new_peak and total_bps > 0:
                flags.append("PEAK↑")

            flag_str = f" [{' & '.join(flags)}]" if flags else ""

            print(
                f"  {ifname:10s} "
                f"rx={format_rate(rx_bps)} "
                f"tx={format_rate(tx_bps)} "
                f"total={format_rate(total_bps)} "
                f"(peak: rx={format_rate(peak_rx[ifname])}, "
                f"tx={format_rate(peak_tx[ifname])}, "
                f"total={format_rate(peak_total[ifname])})"
                f"{flag_str}"
            )

            prev_rx[ifname] = curr_rx
            prev_tx[ifname] = curr_tx

        first_loop = False

        if args.duration and (now - start_time) >= args.duration:
            break

    print("\n监控结束")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        pass
