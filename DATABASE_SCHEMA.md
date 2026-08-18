# Polymarket 数据库 Schema 与关联查询手册

> 数据库：PostgreSQL `polymarket`（本地）
> 来源：`db_pg.py` SCHEMA_SQL（幂等建表 + COMMENT），采集实测（2026-08）

## 1. 总览

共 13 张表，按职责分 4 组：

| 分组 | 表 | 职责 |
|---|---|---|
| 核心元数据 | `events` / `markets` | 事件（盘口组）与市场（选项） |
| 交易数据 | `trades` / `users` / `tx_receipts` / `blocks` | 成交明细、用户聚合、链上回填 |
| 市场扩展 | `holders` / `positions` / `price_history` / `orderbook` | 持仓者、持仓、价格历史、盘口快照 |
| 辅助数据 | `comments` / `market_clarifications` / `scrape_progress` | 评论、Rules 澄清、断点续传 |

## 2. 实体关系图（ER）

```
                        ┌──────────┐
                        │  events  │ (事件/盘口组，如 Alito 事件 212877)
                        └────┬─────┘
                             │ 1
                             │
        ┌────────────────────┼─────────────────────┐
        │ events.id          │ events.id            │ events.id
        │ N                  │ N                    │ N
   ┌────┴─────┐        ┌─────┴──────┐         ┌─────┴─────┐
   │ markets  │        │  comments  │         │  trades   │
   │ (选项盘口)│        │ (评论)     │         │ (成交明细) │
   └────┬─────┘        └────────────┘         └─────┬─────┘
        │ markets.condition_id                      │ trades.proxy_wallet
        │ N                                         │ N
   ┌────┴──────────────┐                      ┌─────┴─────┐
   │ market_           │                      │   users   │
   │ clarifications    │ (Rules 澄清)          │ (用户聚合) │
   └───────────────────┘                      └───────────┘
        │ markets.raw_json->>'clobTokenIds'（每市场 2 个 token：Yes/No）
        │
   ┌────┴───────────────────────────────┐
   │  token_id 维度子表                  │
   │  holders / positions               │
   │  price_history / orderbook         │
   └────────────────────────────────────┘
        │ trades.transaction_hash = tx_receipts.transaction_hash
   ┌────┴──────────┐   tx_receipts.block_number = blocks.block_number
   │  tx_receipts  │ ──────────────────────────► blocks
   └───────────────┘
```

## 3. 表关系详解（关联查询）

### 3.1 events ↔ markets（事件 1 : 市场 N）—— 核心关系

一个事件（盘口组）下多个二元市场（选项），官网"一个盘口多个选项"即此结构。

```sql
-- 按事件查全部选项市场
SELECT m.question, m.volume, m.closed
FROM markets m
JOIN events e ON m.event_id = e.id::text      -- 注意：events.id 是 TEXT，markets.event_id 是 TEXT
WHERE e.slug = 'will-samuel-alito-announce-his-retirement-by'
ORDER BY m.volume DESC;
```

### 3.2 markets ↔ trades（市场 1 : 交易 N）

```sql
SELECT t.timestamp, t.side, t.price, t.size, t.proxy_wallet
FROM trades t
JOIN markets m ON t.condition_id = m.condition_id
WHERE m.slug = 'will-samuel-alito-announce-his-retirement-by-september-30-2026';
```

### 3.3 events ↔ trades（事件 1 : 交易 N）

`trades.event_id` 是 activity 阶段补写的冗余列，可直连 events 免二次 join。

```sql
SELECT count(*) FROM trades WHERE event_id = '212877';
```

### 3.4 events ↔ comments（事件 1 : 评论 N）

```sql
SELECT c.body, c.author_name, c.created_at
FROM comments c
JOIN events e ON c.event_id = e.id
WHERE e.slug = 'will-samuel-alito-announce-his-retirement-by';
```

### 3.5 markets ↔ market_clarifications（市场 1 : 澄清 N）—— 特殊键

⚠️ **澄清接口只接受市场数字 id（`markets.raw_json->>'id'`），不是 condition_id**。`market_clarifications.market_id` 存的是数字 id。

```sql
SELECT mc.clarification
FROM market_clarifications mc
JOIN markets m ON mc.market_id = m.raw_json->>'id'
WHERE m.condition_id = '0x63b2ff2a...';
```

### 3.6 markets ↔ token 维度子表（holders / positions / price_history / orderbook）

每个市场有 2 个 token（Yes/No），token ID 存于 `markets.raw_json->>'clobTokenIds'`（JSON 数组）。子表以 `token_id` 为关联键。

```sql
-- 从 market 找到 token 再关联子表
SELECT ph.t, ph.p
FROM markets m
CROSS JOIN LATERAL jsonb_array_elements_text(m.raw_json->'clobTokenIds') AS t(token_id)
JOIN price_history ph ON ph.token_id = t.token_id
WHERE m.condition_id = '0x63b2ff2a...'
ORDER BY ph.t DESC LIMIT 10;

-- positions 表额外冗余 condition_id，可直接 join markets
SELECT p.proxy_wallet, p.size, p.avg_price, p.curr_price
FROM positions p
JOIN markets m ON p.condition_id = m.condition_id
WHERE m.slug = 'will-samuel-alito-announce-his-retirement-by-september-30-2026';
```

### 3.7 trades ↔ users（交易 N : 用户 1，聚合关系）

`users` 表由 `refresh_users()` 从 trades 聚合生成，`proxy_wallet` 为公共键。

```sql
SELECT u.proxy_wallet, u.pseudonym, u.trade_count
FROM users u
ORDER BY u.trade_count DESC LIMIT 10;

-- 某个用户的全部交易
SELECT t.timestamp, t.side, t.price, t.size
FROM trades t JOIN users u ON t.proxy_wallet = u.proxy_wallet
WHERE u.proxy_wallet = '0x156b1a35...';
```

### 3.8 trades ↔ tx_receipts ↔ blocks（链上回填链）

```sql
SELECT tr.transaction_hash, tr.status, tr.gas_used,
       b.timestamp AS block_ts
FROM trades t
JOIN tx_receipts tr ON t.transaction_hash = tr.transaction_hash
JOIN blocks b ON tr.block_number = b.block_number
LIMIT 10;
```

### 3.9 holders / positions ↔ users（画像关联）

holder/position 行内冗余了用户画像（name/pseudonym/bio），也可按 wallet 关联 users 取最新画像。

```sql
SELECT h.proxy_wallet, h.amount, u.pseudonym
FROM holders h
LEFT JOIN users u ON h.proxy_wallet = u.proxy_wallet
WHERE h.token_id = '5446811838378153175720784620384399222070';
```

### 3.10 scrape_progress

断点续传元数据，不关联业务表。`scope` 格式：
`market:{cid}` / `users:{wallet}` / `activity:{eid}` / `markets:{0|1}` / `event_tags` / `holders:{cid}` / `positions:{cid}` / `prices:{cid}` / `orderbook:{cid}` / `comments:{eid}`。

## 4. 数据流（阶段 → 写入表）

| 采集阶段 | 入口 | 写入表 |
|---|---|---|
| events（活跃+已结算桶 + keyset 展开） | `scraper_gamma.scrape_events` | events, markets |
| details（补详情） | `scraper_gamma.scrape_market_details` | markets |
| clarifications（Rules） | `scraper_gamma.scrape_clarifications` | market_clarifications |
| comments（评论） | `scraper_gamma.scrape_comments` | comments |
| holders（Top Holders） | `scraper_data.scrape_holders` | holders |
| positions（持仓） | `scraper_data.scrape_positions` | positions |
| activity（事件 CASH 交易） | `scraper_data.scrape_activity` | trades → 随后 `refresh_users()` → users |
| prices（价格历史） | `scraper_clob.scrape_prices` | price_history |
| orderbook（盘口快照） | `scraper_clob.scrape_orderbook` | orderbook |
| Phase C（链上回填，main_trades） | `scraper_tx_enrich` | tx_receipts, blocks |

## 5. 字段注释（COMMENT 全集）

### 5.1 trades — 交易/活动明细（自然键去重，Phase A/B 共用）

| 字段 | 类型 | 注释 |
|---|---|---|
| transaction_hash | TEXT | 链上交易哈希（自然键之一） |
| condition_id | TEXT | 市场条件 ID（conditionId） |
| asset | TEXT | 交易资产（代币地址/资产标识） |
| outcome_index | SMALLINT | 结果索引（0=是/1=否） |
| size | NUMERIC | 交易数量（股数） |
| price | NUMERIC | 成交价格 |
| side | TEXT | 方向（BUY/SELL） |
| timestamp | BIGINT | 交易时间戳（秒） |
| proxy_wallet | TEXT | 交易代理钱包地址 |
| usdc_size | NUMERIC | USDC 名义金额 |
| type | TEXT | 交易类型（TRADE/REDEEM/MERGE/SPLIT/REWARD/CONVERSION） |
| title | TEXT | 市场标题（冗余快照） |
| slug | TEXT | 市场 slug（冗余快照） |
| event_slug | TEXT | 所属事件 slug |
| outcome | TEXT | 结果名称（冗余快照） |
| icon | TEXT | 市场图标 URL |
| name | TEXT | 交易者昵称（冗余快照） |
| pseudonym | TEXT | 交易者匿名名（冗余快照） |
| bio | TEXT | 交易者简介（冗余快照） |
| profile_image | TEXT | 交易者头像 URL（冗余快照） |
| profile_image_optimized | TEXT | 交易者头像优化版 URL（冗余快照） |
| event_id | TEXT | 所属事件 ID（activity 维度补充） |
| raw_json | JSONB | API 原始响应 JSON |
| created_at | TIMESTAMPTZ | 入库时间 |

主键：`(transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side)`

### 5.2 users — 用户聚合画像（由 trades 聚合刷新）

| 字段 | 类型 | 注释 |
|---|---|---|
| proxy_wallet | TEXT | 钱包地址（主键） |
| name | TEXT | 昵称（最新非空值） |
| pseudonym | TEXT | 匿名名（最新非空值） |
| bio | TEXT | 简介（最新非空值） |
| profile_image | TEXT | 头像 URL（最新非空值） |
| profile_image_optimized | TEXT | 头像优化版 URL（最新非空值） |
| trade_count | BIGINT | 交易笔数（聚合） |
| first_trade_ts | BIGINT | 首笔交易时间戳（秒） |
| last_trade_ts | BIGINT | 最近交易时间戳（秒） |
| updated_at | TIMESTAMPTZ | 聚合刷新时间 |

### 5.3 tx_receipts — Polygon 链上交易收据（回填）

| 字段 | 类型 | 注释 |
|---|---|---|
| transaction_hash | TEXT | 交易哈希（主键） |
| block_number | BIGINT | 所在区块号 |
| block_timestamp | BIGINT | 区块时间戳（秒） |
| tx_from | TEXT | 发送方地址 |
| tx_to | TEXT | 接收方地址 |
| gas_used | BIGINT | gas 用量 |
| effective_gas_price | BIGINT | 实际 gas 价格 |
| status | SMALLINT | 交易状态（1=成功，0=失败） |
| tx_type | SMALLINT | 交易类型（0=legacy，2=EIP-1559） |
| tx_index | SMALLINT | 区块内交易索引 |
| cumulative_gas_used | BIGINT | 区块累计 gas 用量 |
| failed | BOOLEAN | 回填失败标记（冷却窗口后自动重试） |
| fetched_at | TIMESTAMPTZ | 回填时间 |

### 5.4 blocks — 区块时间戳缓存

| 字段 | 类型 | 注释 |
|---|---|---|
| block_number | BIGINT | 区块号（主键） |
| timestamp | BIGINT | 区块时间戳（秒） |
| fetched_at | TIMESTAMPTZ | 缓存写入时间 |

### 5.5 scrape_progress — 断点续传进度

| 字段 | 类型 | 注释 |
|---|---|---|
| scope | TEXT | 作用域键（market:{cid}/user:{wallet}/activity:{eid}/markets:{0\|1} 等） |
| state | TEXT | 状态（done=完成，其余为续传游标值） |
| fetched_count | BIGINT | 已处理数量 |
| updated_at | TIMESTAMPTZ | 最近更新时间 |

### 5.6 markets — 市场（Gamma API，含嵌套事件反推）

| 字段 | 类型 | 注释 |
|---|---|---|
| condition_id | TEXT | 市场条件 ID（主键，链上唯一标识） |
| question | TEXT | 市场问题文本 |
| slug | TEXT | 市场 slug |
| end_date | TIMESTAMPTZ | 结算/结束时间 |
| start_date | TIMESTAMPTZ | 开始时间 |
| image | TEXT | 市场图片 URL |
| icon | TEXT | 市场图标 URL |
| description | TEXT | 市场描述 |
| outcomes | TEXT | 结果列表（如 ["Yes","No"]） |
| outcome_prices | TEXT | 结果价格 JSON（如 ["0.5","0.5"]） |
| volume | NUMERIC | 累计成交量 |
| liquidity | NUMERIC | 流动性 |
| active | BOOLEAN | 是否活跃 |
| closed | BOOLEAN | 是否已结算 |
| created_at | TIMESTAMPTZ | 创建时间 |
| updated_at | TIMESTAMPTZ | 更新时间 |
| closed_time | TIMESTAMPTZ | 结算时间 |
| submitted_by | TEXT | 提交人（UMA 地址） |
| resolved_by | TEXT | 仲裁人地址 |
| restricted | BOOLEAN | 是否受限市场 |
| archived | BOOLEAN | 是否归档 |
| group_item_title | TEXT | 分组条目标题（groupItemTitle） |
| group_item_threshold | TEXT | 分组条目阈值（groupItemThreshold） |
| question_id | TEXT | 问题 ID（questionID） |
| enable_order_book | BOOLEAN | 是否启用订单簿 |
| order_price_min_tick_size | NUMERIC | 订单簿最小价格变动 |
| order_min_size | NUMERIC | 订单簿最小下单量 |
| volume_24hr | NUMERIC | 24 小时成交量 |
| uma_resolution_status | TEXT | UMA 仲裁状态 |
| event_id | TEXT | 所属事件 ID（补列） |
| tags | JSONB | 事件标签 JSON（补列，冗余自 events） |
| uma_resolution_statuses | TEXT | UMA 仲裁状态列表 JSON（补列） |
| fee_schedule | JSONB | 费率计划 JSON（补列） |
| raw_json | JSONB | API 原始响应 JSON |
| imported_at | TIMESTAMPTZ | 入库时间 |

> 备注：token 列表在 `raw_json->>'clobTokenIds'`（JSON 数组，每市场 2 个 Yes/No token）；数字市场 ID 在 `raw_json->>'id'`。

### 5.7 comments — 事件评论（未登录每页仅 10 条）

| 字段 | 类型 | 注释 |
|---|---|---|
| id | TEXT | 评论 ID（主键） |
| event_id | TEXT | 所属事件 ID |
| body | TEXT | 评论正文 |
| user_address | TEXT | 评论者钱包地址 |
| author_name | TEXT | 评论者昵称 |
| reaction_count | INTEGER | 点赞数 |
| report_count | INTEGER | 举报数 |
| created_at | TIMESTAMPTZ | 评论发布时间 |
| updated_at | TIMESTAMPTZ | 评论更新时间 |
| profile_json | JSONB | 评论者画像 JSON |
| parent_comment_id | TEXT | 父评论 ID（楼层回复，补列） |
| parent_entity_type | TEXT | 父实体类型（补列） |
| reactions | JSONB | 点赞明细 JSON（补列） |
| raw_json | JSONB | API 原始响应 JSON |
| imported_at | TIMESTAMPTZ | 入库时间 |

### 5.8 events — 事件（/markets 嵌套反推 + /events/keyset?slug= 补 tags）

| 字段 | 类型 | 注释 |
|---|---|---|
| id | TEXT | 事件 ID（主键） |
| slug | TEXT | 事件 slug |
| ticker | TEXT | 事件代码 |
| title | TEXT | 事件标题 |
| description | TEXT | 事件描述 |
| tags | JSONB | 标签列表 JSON（[{id,label,slug,...}]） |
| volume | NUMERIC | 累计成交量（= 各选项市场 volume 之和） |
| liquidity | NUMERIC | 流动性 |
| open_interest | NUMERIC | 未平仓量 |
| volume_24hr | NUMERIC | 24 小时成交量 |
| comment_count | INTEGER | 评论数 |
| neg_risk | BOOLEAN | 是否负风险市场 |
| active | BOOLEAN | 是否活跃 |
| closed | BOOLEAN | 是否已结算 |
| archived | BOOLEAN | 是否归档 |
| created_at | TIMESTAMPTZ | 创建时间 |
| updated_at | TIMESTAMPTZ | 更新时间 |
| raw_json | JSONB | API 原始响应 JSON |
| imported_at | TIMESTAMPTZ | 入库时间 |

### 5.9 market_clarifications — 市场 Rules 澄清

| 字段 | 类型 | 注释 |
|---|---|---|
| id | TEXT | 澄清 ID（主键） |
| market_id | TEXT | 市场数字 ID（/market-clarifications 仅接受数字 id） |
| clarification | TEXT | 澄清文本（Rules） |
| created_at | TIMESTAMPTZ | 发布时间 |
| updated_at | TIMESTAMPTZ | 更新时间 |
| raw_json | JSONB | API 原始响应 JSON |
| imported_at | TIMESTAMPTZ | 入库时间 |

### 5.10 holders — 市场 Top Holders 快照（按 token 分组展平）

| 字段 | 类型 | 注释 |
|---|---|---|
| token_id | TEXT | 代币 ID（主键之一） |
| proxy_wallet | TEXT | 持仓钱包地址（主键之一） |
| name | TEXT | 用户昵称 |
| pseudonym | TEXT | 用户匿名名 |
| bio | TEXT | 用户简介 |
| amount | NUMERIC | 持仓数量 |
| outcome_index | SMALLINT | 结果索引 |
| verified | BOOLEAN | 是否已验证 |
| profile_image | TEXT | 用户头像 URL |
| snapshot_at | TIMESTAMPTZ | 快照时间 |

### 5.11 positions — 市场持仓快照（每 token 组 Top limit 条）

| 字段 | 类型 | 注释 |
|---|---|---|
| token_id | TEXT | 代币 ID（主键之一） |
| proxy_wallet | TEXT | 钱包地址（主键之一） |
| condition_id | TEXT | 市场条件 ID |
| name | TEXT | 用户昵称 |
| avg_price | NUMERIC | 平均成本价 |
| size | NUMERIC | 持仓数量 |
| curr_price | NUMERIC | 当前价格 |
| current_value | NUMERIC | 当前价值 |
| cash_pnl | NUMERIC | 现金盈亏 |
| realized_pnl | NUMERIC | 已实现盈亏 |
| total_pnl | NUMERIC | 总盈亏 |
| total_bought | NUMERIC | 累计买入额 |
| outcome | TEXT | 结果名称 |
| outcome_index | SMALLINT | 结果索引 |
| snapshot_at | TIMESTAMPTZ | 快照时间 |

### 5.12 price_history — 价格历史（CLOB 价格时间序列）

| 字段 | 类型 | 注释 |
|---|---|---|
| token_id | TEXT | 代币 ID（主键之一） |
| t | BIGINT | 时间戳（秒，主键之一） |
| p | NUMERIC | 价格 |
| fidelity | INTEGER | 采样精度（分钟） |
| fetched_at | TIMESTAMPTZ | 拉取时间 |

### 5.13 orderbook — 订单簿快照（每次采集一批快照行）

| 字段 | 类型 | 注释 |
|---|---|---|
| token_id | TEXT | 代币 ID（主键之一） |
| side | TEXT | 方向（BUY/SELL，主键之一） |
| price | NUMERIC | 价格（主键之一） |
| size | NUMERIC | 数量（主键之一） |
| snapshot_at | TIMESTAMPTZ | 快照时间（主键之一） |

## 6. 常用关联查询速查

```sql
-- ① 一个事件的全部选项市场（Alito 示例）
SELECT m.question, m.volume, m.closed
FROM markets m JOIN events e ON m.event_id = e.id
WHERE e.slug = 'will-samuel-alito-announce-his-retirement-by'
ORDER BY m.volume DESC;

-- ② 事件成交额（events.volume 应为各市场之和）
SELECT e.title, e.volume, sum(m.volume) AS markets_sum
FROM events e JOIN markets m ON m.event_id = e.id
WHERE e.id = '212877' GROUP BY e.id;

-- ③ 某市场最近交易
SELECT t.timestamp, t.side, t.price, t.size, t.proxy_wallet, t.outcome
FROM trades t JOIN markets m ON t.condition_id = m.condition_id
WHERE m.slug LIKE 'will-samuel-alito%'
ORDER BY t.timestamp DESC LIMIT 50;

-- ④ 某市场当前盘口（最近一次快照）
SELECT side, price, size FROM orderbook ob
WHERE ob.token_id = (
    SELECT (raw_json->'clobTokenIds'->>0) FROM markets WHERE condition_id = '0x63b2ff2a...'
)
AND ob.snapshot_at = (SELECT max(snapshot_at) FROM orderbook)
ORDER BY side, price;

-- ⑤ 活跃用户 Top N
SELECT proxy_wallet, pseudonym, trade_count
FROM users ORDER BY trade_count DESC LIMIT 20;

-- ⑥ 按分类标签查事件与市场（tags 为 JSONB 数组）
SELECT e.id, e.title, count(m.condition_id) AS market_cnt
FROM events e LEFT JOIN markets m ON m.event_id = e.id
WHERE e.tags @> '[{"slug": "politics"}]'
GROUP BY e.id LIMIT 20;
```

## 7. 注意事项

1. **类型对齐**：`events.id` / `markets.event_id` / `trades.event_id` / `comments.event_id` 均为 TEXT；`markets.condition_id` / `trades.condition_id` 亦为 TEXT，join 无需转换。
2. **澄清键特殊**：`market_clarifications.market_id` = 市场数字 id（`markets.raw_json->>'id'`），与 condition_id 不同。
3. **token 关联**：holders/positions/price_history/orderbook 均以 `token_id` 为键，token 列表在 `markets.raw_json->'clobTokenIds'`（数组，0=Yes，1=No）。
4. **users 为派生表**：由 `refresh_users()` 从 trades 聚合（trade_count/first_trade_ts/last_trade_ts + 最新非空画像），非独立采集源。
5. **快照型表**：holders/positions/orderbook 带 `snapshot_at`，同键多次采集会保留多份快照（upsert 以 token+wallet / token+price+time 区分）。
6. **断点续传**：清库重采需一并 TRUNCATE `scrape_progress`，否则旧断点会跳过新任务。
