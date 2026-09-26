# ploymarket 交易数据采集

Polymarket 全量交易数据采集系统：通过 Data API 拉取全市场历史成交与用户活动流，并经 Polygon RPC 回填每笔交易的链上收据，落地 PostgreSQL。

## 快速开始

```powershell
pip install -r requirements.txt   # 首次
copy .env.example .env            # 首次：填 PostgreSQL 连接信息
python poly.py                    # 唯一入口：建库 → 代理池配置 → mihomo 实例 → events → N 个 worker
```

`python poly.py` 幂等可反复跑；Ctrl+C 一次全停（worker + 本次启动的 mihomo 实例）；`python poly.py status` 看状态，`python poly.py stop` 清残留，`python poly.py retry-failed` 死信重新入队。worker 异常重启、实例掉线自愈、日志落盘与心跳见下文《稳定性与自愈》；长期无人值守可另跑 `watchdog_poly.py`（poly 进程被强杀/机器重启后自动拉起）。

## 数据架构

| 表 | 说明 |
|---|---|
| `trades` | 每笔交易/活动明细（19+ 字段 + raw_json），自然键去重，Phase A/B 共用 |
| `events` | 事件 + 嵌套市场（Gamma 采集落地，也是 worker 任务队列的数据源） |
| `markets` | 市场详情（clobTokenIds / 描述等，details 阶段补拉） |
| `activity_tasks` | worker 任务队列（event 级状态机 pending/running/done/failed + 租约） |
| `users` | 用户聚合画像（笔数、时间范围、昵称/头像，从 trades 聚合刷新） |
| `tx_receipts` | Polygon 链上收据（区块号/区块时间/gas/status/from/to 等），按哈希去重回填一次 |
| `blocks` | 区块时间戳缓存（避免重复 `eth_getBlockByNumber`） |
| `scrape_progress` | 断点续传进度（scope 粒度：`market:{conditionId}` / `user:{wallet}`） |

其余表（`comments` / `holders` / `positions` / `price_history` / `orderbook` / `market_clarifications`）对应 `main_collect.py` 的各个阶段，完整字段见 `DATABASE_SCHEMA.md`。

## 目录结构

```
ploymarket/
├── poly.py                 # ★ 统一入口：run（一键跑起来）/ status / stop / retry-failed
├── watchdog_poly.py        # poly 进程意外退出后自动拉起（无人值守兜底；人工 stop 过的不拉）
├── main_collect.py         # 全模块采集 CLI（initdb/stats/events/details/comments/
│                           #   holders/positions/activity/prices/orderbook/all）
├── worker_activity.py      # 分布式 worker（领任务→采集→写库；poly 同进程并发 N 个）
├── make_worker_clash.py    # 代理池配置生成（订阅 → clash_worker/w1..N.yaml + 内核探测/解压）
├── clash_pool.py           # mihomo 节点轮换器（429/403 自动切节点换出口 IP）
├── db_pg.py                # PostgreSQL 建库建表、批量 upsert、任务队列与进度管理
├── config.py               # 全部配置（数据库连接从 .env / 环境变量读取）
├── data_api_client.py      # Data API 客户端（限流/重试/翻页）
├── gamma_client.py         # Gamma API 客户端（事件/市场/评论）
├── clob_client.py          # CLOB API 客户端（价格历史/盘口）
├── scraper_gamma.py        # events / details / clarifications / comments 采集
├── scraper_data.py         # holders / positions / activity 采集
├── scraper_clob.py         # prices / orderbook 采集
├── main_trades.py          # 早期 trades 三阶段 CLI（Phase A/B/C，保留）
├── scraper_trades.py       # Phase A 全市场交易 + Phase B 用户活动流
├── scraper_tx_enrich.py    # Phase C Polygon RPC 链上回填
├── export_cookie.py        # 导出登录态 cookie（comments 需要）
├── condition_ids.txt       # 市场种子（65k+ conditionId，Phase A 数据源）
├── requirements.txt
├── .env.example            # 数据库配置模板（复制为 .env 后填写）
├── clash_config.yaml       # 机场订阅（手动放入，不入库，见《新机器部署》）
├── mihomo_core/            # mihomo 内核（手动放入，不入库；或放 mihomo.zip 首次运行自动解压）
└── logs/                   # 运行日志（poly.log / w1..wN.log / watchdog.log）
```

## 环境要求

- **Windows 10/11**：`poly.py` 的实例管理依赖 `tasklist` / `taskkill` / `netstat` / `winreg`，当前仅支持 Windows
- Python 3.10+（开发环境 3.12）
- PostgreSQL 15+（建议空闲磁盘 ≥ 40GB，全量数据 20-40GB；不需要任何扩展）
- 网络可达 `data-api.polymarket.com` 与 Polygon RPC 节点

## 新机器部署

```powershell
# 1. 拉代码
git clone https://github.com/lxhlf520/ploymarket.git
cd ploymarket

# 2. 装依赖
pip install -r requirements.txt

# 3. 拷 3 个 git 之外的文件（见下表），并把 .env 的 PG_PASSWORD 改成新机器的密码

# 4. 自检：只打印不落库/不启实例（PG 连通 / 订阅解析 / 内核探测）
python poly.py --dry-run

# 5. 正式跑：建库 → 代理池配置 → mihomo 实例 → events → N 个 worker
python poly.py
```

### 需要手动拷贝的文件（都不在 git 里）

| 文件 | 放到 | 必需性 | 说明 |
|---|---|---|---|
| `.env` | `ploymarket/.env` | **必需** | 从旧机器拷贝或按 `.env.example` 新建；`PG_PASSWORD` 必须是**新机器**的 PostgreSQL 密码 |
| `clash_config.yaml` | `ploymarket/clash_config.yaml` | **必需** | 机场订阅（含节点凭据，敏感不入库）；poly 按此路径自动探测，放根目录最省事 |
| mihomo 内核 | `ploymarket/mihomo_core/mihomo-windows-amd64.exe` | **必需** | 直接拷 `mihomo_core/` 整个目录；或只拷 `mihomo.zip` 到根目录，首次运行自动解压；机器上装有快安/Clash Verge 也可兜底 |
| `cookies.json` | `ploymarket/cookies.json` | 可选 | 仅 `main_collect.py` 采集评论需要；可在新机器 `python export_cookie.py --json "{...}"` 重新导出 |

**不需要拷**：`clash_worker/`（首次运行自动生成 w1..wN.yaml 与实例配置）、`logs/`、`__pycache__/`、`condition_ids.txt`（已在仓库）。

### 部署自检

`python poly.py status` 显示「实例可用 3/3」；日志出现 `[poly] 心跳: 实例 3/3 | 队列 ...`；`python poly.py stop` 后 6 个端口（7901-7903 / 9101-9103）全部关闭。

## 数据库配置

```bash
cp .env.example .env     # Windows: copy .env.example .env
# 编辑 .env，填写目标服务器的 PostgreSQL 连接信息
```

`.env` 中的同名环境变量优先于 `config.py` 默认值，密码不会硬编码进代码。也可以不建 `.env`，直接在启动环境中 export 这些变量。

`python poly.py` 首次运行会自动 `CREATE DATABASE` 并建表，要求 `.env` 中的用户有建库权限（postgres 超级用户即可）。

## 初始化（幂等）

```bash
python main_collect.py --stage initdb   # 自动建库 polymarket + 建表 + 建索引（python poly.py 的第一步）
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

> 全模块采集 CLI 是 `main_collect.py`；本章节是 activity/trades 分布式并行方案，日常入口只有 `poly.py`（实例 + 数据 + worker 一条命令）。

### 原理

- Data API 限流（200 req/10s）按**出口 IP** 计——单机加线程无益，**多 worker 进程各走独立隧道代理**（独立出口 IP）才能线性扩吞吐
- 任务队列存 PG `activity_tasks` 表（event 级状态机：`pending 等待 → running 采集中 → done 完成 / failed 错误`）
- 领取用 `FOR UPDATE SKIP LOCKED` 原子操作：多 worker 并发领取**永不重复**；worker 被强杀后其 running 任务**租约超时自动回收**，其他 worker 接管
- 多 worker 写同一 PG 库：trades upsert 幂等，无脏数据

### 启动（唯一入口：poly.py）

```powershell
python poly.py            # 一键跑起来（幂等，反复跑不会重复起）
python poly.py status     # 状态：DB 统计（含死信数）/ 实例与 controller / poly 主进程
python poly.py stop       # 清残留：停掉全部 mihomo 实例（含端口兜底），并记停止标记
python poly.py retry-failed   # 死信（failed）重新入队（--limit N 限制条数）
```

`poly.py` 的 run 流程（每步先检查现状再动作）：

| 步骤 | 动作 |
|---|---|
| [1/5] 建库建表 | `main_collect.py --stage initdb`（幂等） |
| [2/5] 代理池配置 | `clash_worker/w1..wN.yaml` 缺失/无节点/内核未就位 → 自动生成（自动探测订阅与 mihomo 内核，`mihomo*.zip` 自动解压） |
| [3/5] 实例 | 后台无窗口起 N 个 mihomo（日志 `clash_worker/mihomo-wN.log`）；已监听且 controller/secret/节点组校验通过才复用，否则清掉残留重建 |
| [4/5] 事件数据 | `events` 表为空 → 自动采集（经 w1 实例出口）；已非空则跳过 |
| [5/5] worker | 同进程并发 N 个 worker（日志带 `[w1]`/`[w2]` 前缀），后台每 60 秒自动把 events 灌入任务队列 |

- Ctrl+C **一次全停**（worker + 本次启动的 mihomo 实例）；关窗口/强杀后用 `python poly.py stop` 清残留实例
- events 补采后**不需要重启 worker**：队列每 60s 自动补充（幂等）
- worker 启动时会把 `scrape_progress` 里已完成的 activity 断点单向迁移进任务队列
- 日志落盘：全部日志进 `logs/poly.log`，每个 worker 另有分文件 `logs/w1.log`…（含 clash_pool 的换节点日志，能查到"谁在什么时候换了节点"）

```powershell
python poly.py                        # 部署 / 日常重启都用它
python poly.py --dry-run              # 先空跑检查：只打印将执行的动作
python poly.py --collect-events 2     # 试跑：强制小量采 2 个事件验证链路（不带数字=全量补采）
python poly.py --duration 60          # 挂 60 秒验证全链路后自动优雅停止
python poly.py --n 5 --jobs 4         # 5 个实例/worker，每个并发 4 个事件
python poly.py status                 # 看 events/队列/trades 统计 + 实例端口 + 主进程
```

run 参数：`--n`（实例数=worker 数，默认 3）、`--jobs`（每 worker 并发事件数，默认 3）、`--collect-events [N]`（强制采集 events；跟数字=先小量试跑，不带数字=全量补采）、`--no-events`（跳过事件检查）、`--duration N`（跑 N 秒后自动优雅停止，0=一直跑）、`--dry-run`。

单实例调试（不走 poly，手动指定代理或直连）：

```powershell
python worker_activity.py --proxy http://127.0.0.1:7901 --clash-base http://127.0.0.1:9101 --worker-id w1 --jobs 3
python worker_activity.py --worker-id w4 --jobs 3   # 不填 --proxy = 直连
```

worker 参数：`--proxy`、`--jobs`（默认 3）、`--lease-min`（租约分钟，默认 15）、`--max-attempts`（重试上限，默认 5，超过标 failed 死信）、`--idle-wait`（队列空轮询秒数，默认 30，0=领空即退出）、`--rotate-after`（每 N 请求主动换节点，默认 150，0=仅限流时切换）、`--max-delay`（节点预筛延迟上限 ms，默认 10000）。

### 限流自动切节点（mihomo 代理池，推荐）

复用机场订阅，为每个 worker 起一个专属 mihomo(Clash.Meta) 实例（独立 mixed 端口 + 独立节点组），worker 内置节点轮换器（`clash_pool.AsyncNodeRotator`，移植自 glassdoor 项目实战经验）：

- **触发**：429 累计 3 次（防抖）/ 403 / 连接级错误（死节点）→ 自动切换
- **切换逻辑**：ban 当前节点 + 出口 IP（冷却 15 分钟）→ 轮询组内下一可用节点（切不动/出口 IP 仍在冷却的自动跳过）→ 验证新出口 IP → 暂停 5 秒等生效 → 继续采集
- **主动轮换**：每 150 次请求主动换一个 IP（`--rotate-after`，0=仅限流时切换），摊薄单 IP 请求量
- **启动预筛**：实例就绪后先测一遍节点延迟，超过 `--max-delay`（默认 10000ms）的跳过，避免一上来撞死节点
- 切换**无需重建 HTTP 客户端**：采集连接都走实例 mixed 端口，组切换后新请求自动走新节点

配置生成与实例起停**全部由 `poly.py` 自动完成**（`clash_worker/` 下的 w1..wN.yaml 与 mihomo 实例都归它管）：

```powershell
python poly.py            # 生成缺失配置 + 后台起实例 + 起 worker（日常只用这条）
python poly.py status     # 看实例端口是否在监听
python poly.py stop       # 停掉 poly 起的全部实例

# 只想重新生成配置（如机场订阅换了节点）：
python make_worker_clash.py --n 3
```

端口配对（w1 示例）：mixed `7901` ↔ controller `9101`，w2 → `7902`/`9102`，以此类推；controller secret 统一 `pm-worker`，节点组名 `PM`。

说明：
- `--clash-base` 指向该 worker 专属实例的 external-controller；不配则退化为静态 `--proxy`（行为不变）
- `clash_worker/` 含机场节点凭据，已在 .gitignore 中排除，每台机器自行生成
- mihomo 内核探测顺序：项目目录 `mihomo*.zip`（自动解压到 `mihomo_core/`）→ `clash_worker/mihomo*.exe` → `mihomo_core/mihomo*.exe` → 本机快安 / Clash Verge 自带内核；也可手动把内核 exe 放进前两个目录
- 节点池越大越好：死节点自动跳过（短冷却 5 分钟），被限流节点/出口 IP 冷却 15 分钟

### 稳定性与自愈（无人值守）

长跑不看窗口也不会静默减员，每种故障都有日志可查：

| 故障 | 系统行为 | 日志长什么样 |
|---|---|---|
| worker 异常退出 | 指数退避自动重启（5s→…→60s 封顶），不静默少一个 worker | `[poly] w2 异常退出（第 1 次: ...），5s 后自动重启` |
| 领取任务时 DB 抖动 | 退避重试，worker 循环不退出（DB 层另有 4 次带连接池重建的重试） | `[w2] 领取任务失败（DB 抖动？...），2s 后重试` |
| 代理实例掉线 | worker 暂停领新任务（在采事件继续采完，不白烧成死信），实例恢复自动继续 | `[w2] 代理实例不可用…暂停领新任务` / `…已恢复，继续领任务` |
| mihomo 实例挂了 | poly 每 30s 巡检（端口 + controller 双重校验），自动清理重启并确认恢复 | `[poly] 实例 w2 掉线（mixed 7902 / ctl 9102）→ 自动重启` / `实例 w2 已恢复（pid …）` |
| controller 不可用 | 节点轮换不拉黑节点（否则实例恢复后仍被冷却挡住），暂停 30s 再试 | `clash_pool: controller 不可用（…），暂停 30 秒后再试切换` |
| 启动时旧实例残留 | 不只查端口：controller/secret/节点组校验不通过自动清掉重建 | `w2: controller 不可用（旧实例 / secret 不匹配）→ 清理残留并重建` |
| 死信（failed） | 不自动重试（先查原因），数量变化时告警；确认后用 `retry-failed` 重新入队 | `注意: 队列有 N 个死信（failed），确认原因后: python poly.py retry-failed` |
| 整个 poly 进程没了 | `watchdog_poly.py` 兜底拉起（人工 stop 过的不拉） | `logs/watchdog.log` |

心跳（每 5 分钟一条，一眼判断系统是否活着）：

```
19:42:37 INFO [poly] 心跳: 实例 3/3 | 队列 pending=0 running=0 done=88 failed=0
```

死信处理（不自动回队：先看 `last_error` 和 `logs/wN.log` 确认原因）：

```powershell
python poly.py status              # failed 数量 > 0 会直接提示
python poly.py retry-failed        # 全部重新入队（--limit N 限制条数）
```

### 无人值守（watchdog_poly.py，可选）

poly 进程本身被强杀 / OOM / 机器重启后，由看门狗把它拉起来：

```powershell
python watchdog_poly.py                        # 常驻（每 60s 检查一次）
python watchdog_poly.py --once                 # 只检查一次（适合注册计划任务/启动项）
python watchdog_poly.py --n 3 --jobs 3         # 拉起时透传给 poly 的参数
```

- 判定规则：PID 记录里 poly 活着 → 不动；`clash_worker/.poly_stopped` 存在（`poly.py stop` / 正常 Ctrl+C 会写）→ 不动；都没有 → 拉起
- 想恢复自动拉起：手动 `python poly.py` 启动一次即可（run 会清除停止标记）
- 日志：看门狗 `logs/watchdog.log`；被拉起的 poly 输出 → `logs/poly-stdout.log`
- 连续拉起失败指数退避（最长 30 分钟一次），配置错误时不会刷屏空转
- 它只兜“整个进程没了”这一层；实例/worker 级别自愈由 poly.py 自己负责

### 状态监控

```sql
SELECT status, count(*) FROM activity_tasks GROUP BY status;

-- 看当前谁在采什么
SELECT event_id, worker_id, lease_until, attempts FROM activity_tasks WHERE status = 'running';

-- 排查失败原因
SELECT event_id, attempts, last_error FROM activity_tasks WHERE status = 'failed';

-- 失败任务重新入队（人工排查后）：python poly.py retry-failed（等价，且会一并清理 worker_id/租约）
```

### 注意事项

1. **不要混跑**：worker 模式与 `main_collect.py --stage activity` 单机模式使用不同进度表（`activity_tasks` vs `scrape_progress`），同时跑会重复采集（幂等不脏数据，但浪费配额）。worker 启动时会把 `scrape_progress` 中已完成的 done 断点**单向迁移**进任务队列
2. **代理要求**：每个 worker 一个独立出口 IP（独立隧道代理端口，或 mihomo 代理池模式下的专属实例）；多个 worker 共用同一出口 IP 无扩展效果（限流共享）
3. **jobs 建议 2-4**：单 IP 配额 200 req/10s，jobs 过大只是排队
4. **优雅退出**：Ctrl+C 停止领新任务、等在采事件完成；`poly.py` 模式下 Ctrl+C 会同时停掉本次启动的 mihomo 实例。强杀/关窗口场景租约超时（默认 15 分钟）自动回收任务，mihomo 残留用 `python poly.py stop` 清理；整机重启/进程被杀后的自动拉起用 `watchdog_poly.py`（见《无人值守》）
