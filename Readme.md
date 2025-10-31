行情观测工具，辅助实时行情数据的连续性

backend
python后段，需求如下
一个配置的cfg，用yaml文件存储，包括

目的是检测行情的连续性，机器分为主、备两份

主机、备机的ip
主机、备机，都会运行xdp的观测脚本，观察网卡的下行流量。后段需要通过zmq订阅，如下所示:
zmq_subscriber_test.py
[23:56:02] <json>
{
  "config": {
    "bind": true,
    "endpoint": "tcp://0.0.0.0:16666",
    "pattern": "pub",
    "push_interval_sec": 5.0,
    "sample_interval_sec": 0.01
  },
  "hostname": "cc-jp-yf-srv-195",
  "interface": "ens18",
  "metrics": {
    "bps_avg": 26021671.733675566,
    "bps_max": 126547896.44183041,
    "bytes_total": 16294002,
    "packets_total": 35622,
    "pps_avg": 7111.077590938274,
    "pps_max": 21598.299857284645
  },
  "mode": "auto",
  "samples": 494,
  "timestamp": 1761839762.520018,
  "timestamp_iso": "2025-10-30T15:56:02.520018+00:00",
  "window": {
    "duration": 4.99836802482605,
    "end": 1761839762.5200133,
    "end_iso": "2025-10-30T15:56:02.520013+00:00",
    "start": 1761839757.5216453,
    "start_iso": "2025-10-30T15:55:57.521645+00:00"
  }
}

我需要持续接收数据，用hostname + interface作为key，还需要时间数据。5s一个数据，一分钟12条，
一个小时720条，数据尽量只保存我需要的，最多保存72的数据在内存中。

然后，这是前端的展示基础。我需要你用一个折线图（带有一定的数据弹性，少就不显示）。通过websocket和前端建立连接，连接建立的时候，需要缓存的全量数据，之后推送增量数据节约内存。
这样前端我有多少个订阅服务，就会有hostname + interface作为key的折线图。x轴是时间，y轴是max和avg，要换算成mb的带宽。这是第一部分。


[2025-10-31 00:08:09.572] Topic=integrity_trade
{
  "detail": "ok",
  "exchange": "binance-futures",
  "minute": 29364006,
  "status": "ok",
  "symbol": "DOGEUSDT",
  "timestamp_ms": 1761840360000,
  "type": "trade"
}

[2025-10-31 00:08:09.572] Topic=integrity_trade
{
  "detail": "ok",
  "exchange": "binance-futures",
  "minute": 29364006,
  "status": "ok",
  "symbol": "BNBUSDT",
  "timestamp_ms": 1761840360000,
  "type": "trade"
}

类似这样，我还会提供两个addr，对应主机、备机器。你需要关注zmq的integrity_trade这个topic。同样的思路去记录数据。对于每个symbol + exchange作为key。来进行维护。
你需要关注的事情是，叠加到之前的折线图上，采样的频率是5s。而integrity_trade是1min。如果对应的检查位置，存在非ok，需要记录下来。展示在前端上。如果没有，你建议用什么方式在折线图上体现，这个时间点的检查时正常的？

[2025-10-31 00:08:10.661] Topic=rest_summary
{
  "close_tp": 1761840480000,
  "entries": [
    {
      "detail": "ts=1761840420000",
      "request": "premium-index",
      "status": "ok"
    },
    {
      "detail": "ts=1761840485302",
      "request": "open-interest",
      "status": "ok"
    }
  ],
  "exchange": "binance-futures",
  "stage": "1m",
  "symbol": "ETHUSDT",
  "type": "rest_summary"
}
另外，还有rest请求

