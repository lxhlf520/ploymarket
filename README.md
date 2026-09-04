# ploymarket 交易数据采集

Polymarket 全量交易数据采集系统：通过 Data API 拉取全市场历史成交与用户活动流，并经 Polygon RPC 回填每笔交易的链上收据，落地 PostgreSQL。

## 数据架构

| 表 | 说明 |
|---|---|
| `trades` | 每笔交易/活动明细（19+ 字段 + raw_json），自然键去重，Phase A/B 共用 |
| `users` | 用户聚合画像（笔数、时间范围、昵称/头像，从 trades 聚合刷新） |
| `tx_receipts` | Polygon 链上收据（区块号/区块时间/gas/status/from/to 等），按哈希去重回填一次 |
| `blocks` | 区块时间戳缓存（避免重复 `eth_getBlockByNumber`） |
| `scrape_progress` | 断点续传进度（scope 粒度：`market:{conditionId}` / `user:{wallet}`） |

## 目录结构

```
ploymarket/
├── main_trades.py          # CLI 编排入口（各阶段/试跑/估算）
├── scraper_trades.py       # Phase A 全市场交易 + Phase B 用户活动流
├── scraper_tx_enrich.py    # Phase C Polygon RPC 链上回填
├── data_api_client.py      # Data API 客户端（限流/重试/end 窗口深翻页）
├── db_pg.py                # PostgreSQL 建库建表、批量 upsert、进度管理
├── config.py               # 全部配置（数据库连接从 .env / 环境变量读取）
├── export_condition_ids.py # 从旧 SQLite 库导出 conditionId 种子文件（可选）
├── condition_ids.txt       # 市场种子（65k+ conditionId，Phase A 数据源）
├── requirements.txt
└── .env.example            # 数据库配置模板（复制为 .env 后填写）
```

## 环境要求

- Python 3.10+（开发环境 3.12）
- PostgreSQL 15+（建议空闲磁盘 ≥ 40GB，全量数据 20-40GB）
- 网络可达 `data-api.polymarket.com` 与 Polygon RPC 节点

## 安装

```bash
git clone https://github.com/lxhlf520/ploymarket.git
cd ploymarket
pip install -r requirements.txt
```

## 数据库配置

```bash
cp .env.example .env     # Windows: copy .env.example .env
# 编辑 .env，填写目标服务器的 PostgreSQL 连接信息
```

`.env` 中的同名环境变量优先于 `config.py` 默认值，密码不会硬编码进代码。也可以不建 `.env`，直接在启动环境中 export 这些变量。

## 初始化（幂等）

```bash
python main_trades.py --stage initdb   # 自动建库 polymarket + 建表 + 建索引
```

## 采集三阶段

| 阶段 | 命令 | 说明 |
|---|---|---|
| Phase A 市场 | `python main_trades.py --stage markets` | 全市场 /trades 全量历史（`--limit N` 试跑前 N 个） |
| Phase B 用户 | `python main_trades.py --stage users` | 去重钱包 /activity 回补（usdcSize/type 及非 TRADE 活动） |
| Phase C 回填 | `python main_trades.py --stage enrich` | Polygon RPC 链上收据回填（`--batch` 每批哈希数） |

辅助命令：

```bash
python main_trades.py --stage all --dry-run   # 仅估算各阶段请求数（抽样探测）
python main_trades.py --stage verify          # 回填覆盖率统计
python main_trades.py --stage stats           # 库汇总统计
python main_trades.py --stage markets --market <conditionId>  # 定向单市场（对账用）
```

常用参数：`--limit N`（试跑/限量）、`--concurrency N`（并发，默认 16）、`--skip-refresh-users`（分多次跑时跳过末尾的用户聚合）、`-v`（DEBUG 日志）。

## 断点续传

任意阶段可随时 Ctrl+C 中断，已完成的 scope 记录在 `scrape_progress`，重跑同命令自动跳过。所有 upsert 幂等，按自然键去重，重复采集不会产生脏数据。

## 数据验证

- `--stage verify`：链上回填覆盖率（enriched / total_hashes / failed）
- 对账示例：Kraken IPO 2025 市场已知 3,190 笔，定向重采后 `inserted=0, updated=3190` 即幂等正确
- 全量 Phase A 结束后建议跑一次 `--stage users`（刷新聚合）或 `--skip-refresh-users` 分次跑完后统一刷新

## 服务器部署注意

1. **磁盘**：全量预计 20-40GB（含 4 个索引），确认 PG 数据目录所在盘余量
2. **限流**：Data API 全局 200 req/10s（代码令牌桶对齐）；Cloudflare 可能阶段性升级限流，中断冷却后续跑即可
3. **RPC 限额**：Phase C 数百万次请求，`config.POLYGON_RPC_URLS` 可追加带免费 key 的节点（Alchemy/QuickNode）大幅提速；备选公共节点为非归档节点，仅作故障轮换
4. **耗时**：全量中性估计约 4-7 天（A 半天 / B 1-2 天 / C 2-5 天），断点续传支持跨天分批推进

## 分布式多任务采集（activity/trades）

> 当前主入口为 `main_collect.py`（全模块采集）；本章节为 trades 全量采集的多进程并行方案。

### 原理

- Data API 限流（200 req/10s）按**出口 IP** 计——单机加线程无益，**多 worker 进程各走独立隧道代理**（独立出口 IP）才能线性扩吞吐
- 任务队列存 PG `activity_tasks` 表（event 级状态机：`pending 等待 → running 采集中 → done 完成 / failed 错误`）
- 领取用 `FOR UPDATE SKIP LOCKED` 原子操作：多 worker 并发领取**永不重复**；worker 被强杀后其 running 任务**租约超时自动回收**，其他 worker 接管
- 多 worker 写同一 PG 库：trades upsert 幂等，无脏数据

### 启动

多开 PowerShell 窗口，每个窗口一个 worker、各带不同代理端口：

```powershell
# 窗口 1
python worker_activity.py --proxy http://127.0.0.1:7890 --worker-id w1 --jobs 3

# 窗口 2
python worker_activity.py --proxy http://127.0.0.1:7891 --worker-id w2 --jobs 3

# 窗口 3（直连）
python worker_activity.py --worker-id w3 --jobs 3

# 或一键启动（start_workers.bat，按需改代理端口）
start_workers.bat
```

参数：`--proxy`（本进程出口代理，不填=直连）、`--jobs`（同时采的事件数，默认 3）、`--lease-min`（租约分钟，默认 15）、`--max-attempts`（重试上限，默认 5，超过标 failed 死信）、`--idle-wait`（队列空轮询秒数，默认 30，0=领空即退出）。

### 状态监控

```sql
SELECT status, count(*) FROM activity_tasks GROUP BY status;

-- 看当前谁在采什么
SELECT event_id, worker_id, lease_until, attempts FROM activity_tasks WHERE status = 'running';

-- 排查失败原因
SELECT event_id, attempts, last_error FROM activity_tasks WHERE status = 'failed';

-- 失败任务重新入队（人工排查后）
UPDATE activity_tasks SET status = 'pending', attempts = 0 WHERE status = 'failed';
```

### 注意事项

1. **不要混跑**：worker 模式与 `main_collect.py --stage activity` 单机模式使用不同进度表（`activity_tasks` vs `scrape_progress`），同时跑会重复采集（幂等不脏数据，但浪费配额）。worker 启动时会把 `scrape_progress` 中已完成的 done 断点**单向迁移**进任务队列
2. **代理要求**：每个 worker 一个独立出口 IP 的隧道代理；多个 worker 共用同一出口 IP 无扩展效果（限流共享）
3. **jobs 建议 2-4**：单 IP 配额 200 req/10s，jobs 过大只是排队
4. **优雅退出**：Ctrl+C 停止领新任务、等在采事件完成；强杀场景租约超时（默认 15 分钟）自动回收
