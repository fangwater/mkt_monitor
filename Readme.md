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

## 多面板配置

在仓库根目录直接放置多个 `config-*.yaml`（例如 `config-primary.yaml`、`config-secondary.yaml`）。启动后会自动发现这些配置文件，并分别挂载到 `/<名称>/` 路径；若同时存在 `config.yaml`，它会作为 `primary` 参与展示。无需再设置环境变量即可同时访问多个页面。
