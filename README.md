# 实时数据入湖平台

面向流式数据接入、实时转换、增量合并与湖上物化查询的实时数据入湖治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 主题注册 | `topic_registry` | 数据主题 | 主题编号、主题名称、消息格式 |
| 流式计算 | `flink_job` | 流式任务 | 任务编号、消费主题、转换逻辑 |
| 消息接入 | `kafka_connector` | 接入配置 | 配置编号、集群地址、消费组名 |
| Schema演进 | `schema_evolve` | 演进规则 | 规则编号、关联主题、新增字段 |
| 增量合并 | `upsert_merge` | 合并策略 | 策略编号、目标表名、合并键列 |
| 湖上分区 | `lake_partition` | 分区定义 | 分区编号、目标表名、分区列名 |
| 流式质量 | `stream_quality` | 质量规则 | 规则编号、监控主题、校验类型 |
| Iceberg管理 | `iceberg_table` | 表元数据 | 表编号、表名称、存储格式 |
| 水位追踪 | `watermark_track` | 水位标记 | 标记编号、数据主题、分区编号 |
| 死信处理 | `dead_letter` | 死信记录 | 记录编号、原始主题、原始分区 |
| 物化查询 | `materialized_view` | 物化视图 | 视图编号、查询SQL、基表名 |
| CDC捕获 | `change_capture` | CDC配置 | 配置编号、源数据库、源表名 |
| 合并清理 | `compact_policy` | 清理策略 | 策略编号、目标表名、触发条件 |
| 流式血缘 | `stream_lineage` | 血缘记录 | 记录编号、上游主题、下游表名 |
| 流式补数 | `backfill_stream` | 补数任务 | 任务编号、目标表名、起始偏移 |
| 下沉分发 | `sink_dispatch` | 分发规则 | 规则编号、湖表名、目标系统 |
| 数据生命周期 | `ttl_policy` | 生命周期 | 策略编号、目标表名、热数据保留 |
| 实时指标 | `realtime_metrics` | 指标定义 | 指标编号、指标名称、聚合窗口 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 订单吞吐指标增量结果表

旧实现每次刷新都全量重扫 `topic_registry` 与 `flink_job`，刷新越频繁耗时越长，
且中断后没有已消费版本只能重算。增量方案把链路拆成三段（实现见
`backend/app/services/metric_increment.py`）：

| 产物表 | 作用 |
| --- | --- |
| `metric_source_versions` | 短窗口源版本流，新版本只取数一次，不重扫历史主题与流任务 |
| `metric_increment_results` | 指标增量结果表，按 `(metric_id, source_version, window_start)` 去重，保留新增聚合值与写入时的结果表版本 |
| `metric_increment_cursors` | 每个指标的已消费版本、结果表当前版本（最后成功位置） |
| `ttl_delivery_log` | 实时指标 → 数据生命周期的跨模块交付记录，保留交付时的结果表版本 |

关键规则：

- 增量刷新只处理「已消费版本之后」的连续版本；全部窗口写入成功后才推进已消费版本，
  固定「先并入新增窗口（去重）→ 再扩展已消费版本」的冲突取舍，结果表版本每次提交 +1。
- 写入中断时部分窗口落表但游标不前进；`recover` 从上次已消费版本之后补齐，
  已写窗口自动跳过，同一窗口不会重复累计。出现版本断档时只推进连续区间并上报缺口版本。
- 数据生命周期经 `GET /api/ttl_policy/retention/metrics` 接入结果表，
  按主题保留时长判断来源主题是否仍处于可消费保留期。
- 前端：实时指标页支持按时间窗口筛选与增量刷新/中断恢复；
  指标工作台（`/metric_workbench`）同步显示结果表，关联记录刷新后保持其写入时版本。

### 存量迁移与回滚

本次改动均为增量新增（新服务、新路由、新页面、新产物表），不修改既有模块的数据契约。
按部署流程上线后，旧的全量计算接口 `/api/pipeline/metrics/{id}/compute` 仍然保留；
若上线失败，回滚到旧环境部署即可，新增表为空时不影响原有 18 个模块的任何链路。

## 领域
领域：数据工程
子方向：数仓与数据湖
