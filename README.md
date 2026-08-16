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
