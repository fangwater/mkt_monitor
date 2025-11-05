# 市场带宽与完整性监测面板

## 项目简介

本项目用于监控交易所链路的网络带宽与数据完整性。后端通过 ZeroMQ 订阅多条数据流：

- **XDP 带宽流**（5 秒一个采样），用于展示实时带宽折线图，并对超过阈值的区间做标记。
- **完整性流**（`integrity_trade`、`integrity_inc`、`rest_summary` 等），按 1 分钟或更长周期推送各交易对、各阶段的检查结果，异常时需要在前端高亮。

前端是一个单页应用，直接由 FastAPI 静态托管。页面会同时显示带宽折线、完整性事件列表以及关键指标摘要，支持根据流名称过滤。

## 目录概览

```
backend/      FastAPI 后端，负责订阅 ZMQ、缓存数据、提供 REST/WebSocket 接口
frontend/     前端静态资源（HTML/JS/CSS）
config*.yaml  运行时配置文件（ZMQ 端点、保留条数、阈值等）
scripts/      辅助脚本（部署、环境检查）
src/          XDP 带宽采集相关工具脚本与配置
```

## 快速开始

1. **安装依赖**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r backend/requirements.txt
   ```

2. **准备配置**

   - 默认读取仓库根目录的 `config.yaml`。
   - 如果需要主备两套数据源，可复制为 `config-primary.yaml`、`config-secondary.yaml` 并按需修改端点。

3. **启动服务**

   ```bash
   python -m uvicorn backend.app:app --host 0.0.0.0 --port 12345 --log-level info
   ```

   浏览器访问 `http://<主机>:12345/primary/`（或 `/secondary/`）即可查看面板。

## 多面板配置

在仓库根目录放置多个 `config-*.yaml`（例如 `config-primary.yaml`、`config-secondary.yaml`）。后端会自动扫描这些文件，并将每个配置挂载到 `/<名称>/` 路径：

- `config-primary.yaml` → `/primary/`
- `config-secondary.yaml` → `/secondary/`

若同时存在 `config.yaml`，会自动作为 `primary` 参与展示。无需额外的环境变量配置。

## 使用 pm2 守护运行

生产环境可以使用 [pm2](https://pm2.keymetrics.io/) 来管理 uvicorn 进程：

```bash
# 启动并命名进程
pm2 start "python3 -m uvicorn backend.app:app --host 0.0.0.0 --port 12345 --log-level info" --name mkt-monitor 

# 查看状态与日志
pm2 status
pm2 logs mkt-monitor

# 开机自启（可选）
pm2 save
pm2 startup   # 按提示执行
```

如需修改端口或日志等级，只需在命令中调整 uvicorn 的参数。

## 调试与验证

- 后端启动后日志会打印 `订阅线程启动` 和 `订阅到消息`，可确认 ZMQ 连通性。
- 可以使用接口快速检查数据是否就绪：

  ```bash
  curl http://127.0.0.1:12345/primary/api/status
  curl http://127.0.0.1:12345/primary/api/buckets
  ```

- 前端若出现缓存问题，执行浏览器强制刷新（Ctrl+Shift+R）或清理缓存。

如需扩展到更多数据流，可在对应的配置文件中新增 ZMQ 端点并补充前端映射。若有新的可视化需求，只需修改 `frontend/app.js` 即可。欢迎根据实盘运行情况继续完善。


负载部署nginx
