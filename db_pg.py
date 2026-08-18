# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Polymarket PostgreSQL 数据库管理（交易采集扩展）

本地 PostgreSQL 新建库 polymarket，保存：
- trades: 每笔交易/活动明细（自然键去重，Phase A/B 共用）
- users: 用户聚合画像
- tx_receipts / blocks: Polygon 链上回填（收据 + 区块时间戳缓存）
- scrape_progress: 断点续传进度
"""

import json
import logging
from datetime import datetime

import asyncpg

import config

logger = logging.getLogger(__name__)

_pool = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    transaction_hash        TEXT        NOT NULL,
    condition_id            TEXT        NOT NULL,
    asset                   TEXT        NOT NULL,
    outcome_index           SMALLINT    NOT NULL,
    size                    NUMERIC     NOT NULL,
    price                   NUMERIC     NOT NULL,
    side                    TEXT        NOT NULL,
    timestamp               BIGINT      NOT NULL,
    proxy_wallet            TEXT        NOT NULL,
    usdc_size               NUMERIC,
    type                    TEXT        NOT NULL DEFAULT 'TRADE',
    title                   TEXT,
    slug                    TEXT,
    event_slug              TEXT,
    outcome                 TEXT,
    icon                    TEXT,
    name                    TEXT,
    pseudonym               TEXT,
    bio                     TEXT,
    profile_image           TEXT,
    profile_image_optimized TEXT,
    raw_json                JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side)
);

CREATE INDEX IF NOT EXISTS idx_trades_condition_id ON trades (condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet ON trades (proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_transaction_hash ON trades (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades (proxy_wallet, timestamp DESC);

CREATE TABLE IF NOT EXISTS users (
    proxy_wallet            TEXT PRIMARY KEY,
    name                    TEXT,
    pseudonym               TEXT,
    bio                     TEXT,
    profile_image           TEXT,
    profile_image_optimized TEXT,
    trade_count             BIGINT NOT NULL DEFAULT 0,
    first_trade_ts          BIGINT,
    last_trade_ts           BIGINT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tx_receipts (
    transaction_hash    TEXT PRIMARY KEY,
    block_number        BIGINT,
    block_timestamp     BIGINT,
    tx_from             TEXT,
    tx_to               TEXT,
    gas_used            BIGINT,
    effective_gas_price BIGINT,
    status              SMALLINT,
    tx_type             SMALLINT,
    tx_index            SMALLINT,
    cumulative_gas_used BIGINT,
    failed              BOOLEAN NOT NULL DEFAULT FALSE,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS blocks (
    block_number BIGINT PRIMARY KEY,
    timestamp    BIGINT NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scrape_progress (
    scope         TEXT PRIMARY KEY,
    state         TEXT NOT NULL DEFAULT 'pending',
    fetched_count BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS markets (
    condition_id            TEXT PRIMARY KEY,
    question                TEXT,
    slug                    TEXT,
    end_date                TIMESTAMPTZ,
    start_date              TIMESTAMPTZ,
    image                   TEXT,
    icon                    TEXT,
    description             TEXT,
    outcomes                TEXT,
    outcome_prices          TEXT,
    volume                  NUMERIC(18,2),
    liquidity               NUMERIC,
    active                  BOOLEAN,
    closed                  BOOLEAN,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    closed_time             TIMESTAMPTZ,
    submitted_by            TEXT,
    resolved_by             TEXT,
    restricted              BOOLEAN,
    archived                BOOLEAN,
    group_item_title        TEXT,
    group_item_threshold    TEXT,
    question_id             TEXT,
    enable_order_book       BOOLEAN,
    order_price_min_tick_size NUMERIC,
    order_min_size          NUMERIC,
    volume_24hr             NUMERIC,
    uma_resolution_status   TEXT,
    raw_json                JSONB,
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS comments (
    id              TEXT PRIMARY KEY,
    event_id        TEXT,
    body            TEXT,
    user_address    TEXT,
    author_name     TEXT,
    reaction_count  INTEGER DEFAULT 0,
    report_count    INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    profile_json    JSONB,
    raw_json        JSONB,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    slug            TEXT,
    ticker          TEXT,
    title           TEXT,
    description     TEXT,
    tags            JSONB,
    volume          NUMERIC,
    liquidity       NUMERIC,
    open_interest   NUMERIC,
    volume_24hr     NUMERIC,
    comment_count   INTEGER DEFAULT 0,
    neg_risk        BOOLEAN,
    active          BOOLEAN,
    closed          BOOLEAN,
    archived        BOOLEAN,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    raw_json        JSONB,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS market_clarifications (
    id              TEXT PRIMARY KEY,
    market_id       TEXT,
    clarification   TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    raw_json        JSONB,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS holders (
    token_id        TEXT NOT NULL,
    proxy_wallet    TEXT NOT NULL,
    name            TEXT,
    pseudonym       TEXT,
    bio             TEXT,
    amount          NUMERIC,
    outcome_index   SMALLINT,
    verified        BOOLEAN,
    profile_image   TEXT,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, proxy_wallet)
);

CREATE TABLE IF NOT EXISTS positions (
    token_id        TEXT NOT NULL,
    proxy_wallet    TEXT NOT NULL,
    condition_id    TEXT,
    name            TEXT,
    avg_price       NUMERIC,
    size            NUMERIC,
    curr_price      NUMERIC,
    current_value   NUMERIC,
    cash_pnl        NUMERIC,
    realized_pnl    NUMERIC,
    total_pnl       NUMERIC,
    total_bought    NUMERIC,
    outcome         TEXT,
    outcome_index   SMALLINT,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, proxy_wallet)
);

CREATE TABLE IF NOT EXISTS price_history (
    token_id        TEXT NOT NULL,
    t               BIGINT NOT NULL,
    p               NUMERIC NOT NULL,
    fidelity        INTEGER DEFAULT 10,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, t)
);

CREATE TABLE IF NOT EXISTS orderbook (
    token_id        TEXT NOT NULL,
    side            TEXT NOT NULL,
    price           NUMERIC NOT NULL,
    size            NUMERIC NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, side, price, snapshot_at)
);

-- 存量表补列（幂等）
ALTER TABLE markets ADD COLUMN IF NOT EXISTS event_id TEXT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS tags JSONB;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS uma_resolution_statuses TEXT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS fee_schedule JSONB;
ALTER TABLE comments ADD COLUMN IF NOT EXISTS parent_comment_id TEXT;
ALTER TABLE comments ADD COLUMN IF NOT EXISTS parent_entity_type TEXT;
ALTER TABLE comments ADD COLUMN IF NOT EXISTS reactions JSONB;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS event_id TEXT;
CREATE INDEX IF NOT EXISTS idx_events_slug ON events (slug);
CREATE INDEX IF NOT EXISTS idx_markets_event_id ON markets (event_id);
CREATE INDEX IF NOT EXISTS idx_clarifications_market ON market_clarifications (market_id);
CREATE INDEX IF NOT EXISTS idx_holders_token ON holders (token_id);
CREATE INDEX IF NOT EXISTS idx_positions_token ON positions (token_id);
CREATE INDEX IF NOT EXISTS idx_price_history_token ON price_history (token_id, t);
CREATE INDEX IF NOT EXISTS idx_orderbook_token ON orderbook (token_id, snapshot_at);

-- ==================== 表/字段注释（COMMENT，幂等） ====================
COMMENT ON TABLE trades IS '交易/活动明细（自然键去重，Phase A/B 共用）';
COMMENT ON COLUMN trades.transaction_hash IS '链上交易哈希（自然键之一）';
COMMENT ON COLUMN trades.condition_id IS '市场条件 ID（conditionId）';
COMMENT ON COLUMN trades.asset IS '交易资产（代币地址/资产标识）';
COMMENT ON COLUMN trades.outcome_index IS '结果索引（0=是/1=否）';
COMMENT ON COLUMN trades.size IS '交易数量（股数）';
COMMENT ON COLUMN trades.price IS '成交价格';
COMMENT ON COLUMN trades.side IS '方向（BUY/SELL）';
COMMENT ON COLUMN trades.timestamp IS '交易时间戳（秒）';
COMMENT ON COLUMN trades.proxy_wallet IS '交易代理钱包地址';
COMMENT ON COLUMN trades.usdc_size IS 'USDC 名义金额';
COMMENT ON COLUMN trades.type IS '交易类型（TRADE/REDEEM/MERGE/SPLIT/REWARD/CONVERSION）';
COMMENT ON COLUMN trades.title IS '市场标题（冗余快照）';
COMMENT ON COLUMN trades.slug IS '市场 slug（冗余快照）';
COMMENT ON COLUMN trades.event_slug IS '所属事件 slug';
COMMENT ON COLUMN trades.outcome IS '结果名称（冗余快照）';
COMMENT ON COLUMN trades.icon IS '市场图标 URL';
COMMENT ON COLUMN trades.name IS '交易者昵称（冗余快照）';
COMMENT ON COLUMN trades.pseudonym IS '交易者匿名名（冗余快照）';
COMMENT ON COLUMN trades.bio IS '交易者简介（冗余快照）';
COMMENT ON COLUMN trades.profile_image IS '交易者头像 URL（冗余快照）';
COMMENT ON COLUMN trades.profile_image_optimized IS '交易者头像优化版 URL（冗余快照）';
COMMENT ON COLUMN trades.event_id IS '所属事件 ID（activity 维度补充）';
COMMENT ON COLUMN trades.raw_json IS 'API 原始响应 JSON';
COMMENT ON COLUMN trades.created_at IS '入库时间';

COMMENT ON TABLE users IS '用户聚合画像（由 trades 聚合刷新）';
COMMENT ON COLUMN users.proxy_wallet IS '钱包地址（主键）';
COMMENT ON COLUMN users.name IS '昵称（最新非空值）';
COMMENT ON COLUMN users.pseudonym IS '匿名名（最新非空值）';
COMMENT ON COLUMN users.bio IS '简介（最新非空值）';
COMMENT ON COLUMN users.profile_image IS '头像 URL（最新非空值）';
COMMENT ON COLUMN users.profile_image_optimized IS '头像优化版 URL（最新非空值）';
COMMENT ON COLUMN users.trade_count IS '交易笔数（聚合）';
COMMENT ON COLUMN users.first_trade_ts IS '首笔交易时间戳（秒）';
COMMENT ON COLUMN users.last_trade_ts IS '最近交易时间戳（秒）';
COMMENT ON COLUMN users.updated_at IS '聚合刷新时间';

COMMENT ON TABLE tx_receipts IS 'Polygon 链上交易收据（回填）';
COMMENT ON COLUMN tx_receipts.transaction_hash IS '交易哈希（主键）';
COMMENT ON COLUMN tx_receipts.block_number IS '所在区块号';
COMMENT ON COLUMN tx_receipts.block_timestamp IS '区块时间戳（秒）';
COMMENT ON COLUMN tx_receipts.tx_from IS '发送方地址';
COMMENT ON COLUMN tx_receipts.tx_to IS '接收方地址';
COMMENT ON COLUMN tx_receipts.gas_used IS 'gas 用量';
COMMENT ON COLUMN tx_receipts.effective_gas_price IS '实际 gas 价格';
COMMENT ON COLUMN tx_receipts.status IS '交易状态（1=成功，0=失败）';
COMMENT ON COLUMN tx_receipts.tx_type IS '交易类型（0=legacy，2=EIP-1559）';
COMMENT ON COLUMN tx_receipts.tx_index IS '区块内交易索引';
COMMENT ON COLUMN tx_receipts.cumulative_gas_used IS '区块累计 gas 用量';
COMMENT ON COLUMN tx_receipts.failed IS '回填失败标记（冷却窗口后自动重试）';
COMMENT ON COLUMN tx_receipts.fetched_at IS '回填时间';

COMMENT ON TABLE blocks IS '区块时间戳缓存';
COMMENT ON COLUMN blocks.block_number IS '区块号（主键）';
COMMENT ON COLUMN blocks.timestamp IS '区块时间戳（秒）';
COMMENT ON COLUMN blocks.fetched_at IS '缓存写入时间';

COMMENT ON TABLE scrape_progress IS '断点续传进度';
COMMENT ON COLUMN scrape_progress.scope IS '作用域键（market:{cid}/user:{wallet}/activity:{eid}/markets:{0|1} 等）';
COMMENT ON COLUMN scrape_progress.state IS '状态（done=完成，其余为续传游标值）';
COMMENT ON COLUMN scrape_progress.fetched_count IS '已处理数量';
COMMENT ON COLUMN scrape_progress.updated_at IS '最近更新时间';

COMMENT ON TABLE markets IS '市场（Gamma API，含嵌套事件反推）';
COMMENT ON COLUMN markets.condition_id IS '市场条件 ID（主键，链上唯一标识）';
COMMENT ON COLUMN markets.question IS '市场问题文本';
COMMENT ON COLUMN markets.slug IS '市场 slug';
COMMENT ON COLUMN markets.end_date IS '结算/结束时间';
COMMENT ON COLUMN markets.start_date IS '开始时间';
COMMENT ON COLUMN markets.image IS '市场图片 URL';
COMMENT ON COLUMN markets.icon IS '市场图标 URL';
COMMENT ON COLUMN markets.description IS '市场描述';
COMMENT ON COLUMN markets.outcomes IS '结果列表（如 ["Yes","No"]）';
COMMENT ON COLUMN markets.outcome_prices IS '结果价格 JSON（如 ["0.5","0.5"]）';
COMMENT ON COLUMN markets.volume IS '累计成交量';
COMMENT ON COLUMN markets.liquidity IS '流动性';
COMMENT ON COLUMN markets.active IS '是否活跃';
COMMENT ON COLUMN markets.closed IS '是否已结算';
COMMENT ON COLUMN markets.created_at IS '创建时间';
COMMENT ON COLUMN markets.updated_at IS '更新时间';
COMMENT ON COLUMN markets.closed_time IS '结算时间';
COMMENT ON COLUMN markets.submitted_by IS '提交人（UMA 地址）';
COMMENT ON COLUMN markets.resolved_by IS '仲裁人地址';
COMMENT ON COLUMN markets.restricted IS '是否受限市场';
COMMENT ON COLUMN markets.archived IS '是否归档';
COMMENT ON COLUMN markets.group_item_title IS '分组条目标题（groupItemTitle）';
COMMENT ON COLUMN markets.group_item_threshold IS '分组条目阈值（groupItemThreshold）';
COMMENT ON COLUMN markets.question_id IS '问题 ID（questionID）';
COMMENT ON COLUMN markets.enable_order_book IS '是否启用订单簿';
COMMENT ON COLUMN markets.order_price_min_tick_size IS '订单簿最小价格变动';
COMMENT ON COLUMN markets.order_min_size IS '订单簿最小下单量';
COMMENT ON COLUMN markets.volume_24hr IS '24 小时成交量';
COMMENT ON COLUMN markets.uma_resolution_status IS 'UMA 仲裁状态';
COMMENT ON COLUMN markets.raw_json IS 'API 原始响应 JSON';
COMMENT ON COLUMN markets.imported_at IS '入库时间';
COMMENT ON COLUMN markets.event_id IS '所属事件 ID（补列）';
COMMENT ON COLUMN markets.tags IS '事件标签 JSON（补列，冗余自 events）';
COMMENT ON COLUMN markets.uma_resolution_statuses IS 'UMA 仲裁状态列表 JSON（补列）';
COMMENT ON COLUMN markets.fee_schedule IS '费率计划 JSON（补列）';

COMMENT ON TABLE comments IS '事件评论（未登录每页仅 10 条）';
COMMENT ON COLUMN comments.id IS '评论 ID（主键）';
COMMENT ON COLUMN comments.event_id IS '所属事件 ID';
COMMENT ON COLUMN comments.body IS '评论正文';
COMMENT ON COLUMN comments.user_address IS '评论者钱包地址';
COMMENT ON COLUMN comments.author_name IS '评论者昵称';
COMMENT ON COLUMN comments.reaction_count IS '点赞数';
COMMENT ON COLUMN comments.report_count IS '举报数';
COMMENT ON COLUMN comments.created_at IS '评论发布时间';
COMMENT ON COLUMN comments.updated_at IS '评论更新时间';
COMMENT ON COLUMN comments.profile_json IS '评论者画像 JSON';
COMMENT ON COLUMN comments.parent_comment_id IS '父评论 ID（楼层回复，补列）';
COMMENT ON COLUMN comments.parent_entity_type IS '父实体类型（补列）';
COMMENT ON COLUMN comments.reactions IS '点赞明细 JSON（补列）';
COMMENT ON COLUMN comments.raw_json IS 'API 原始响应 JSON';
COMMENT ON COLUMN comments.imported_at IS '入库时间';

COMMENT ON TABLE events IS '事件（/markets 嵌套反推 + /events/keyset?slug= 补 tags）';
COMMENT ON COLUMN events.id IS '事件 ID（主键）';
COMMENT ON COLUMN events.slug IS '事件 slug';
COMMENT ON COLUMN events.ticker IS '事件代码';
COMMENT ON COLUMN events.title IS '事件标题';
COMMENT ON COLUMN events.description IS '事件描述';
COMMENT ON COLUMN events.tags IS '标签列表 JSON（[{id,label,slug,...}]）';
COMMENT ON COLUMN events.volume IS '累计成交量';
COMMENT ON COLUMN events.liquidity IS '流动性';
COMMENT ON COLUMN events.open_interest IS '未平仓量';
COMMENT ON COLUMN events.volume_24hr IS '24 小时成交量';
COMMENT ON COLUMN events.comment_count IS '评论数';
COMMENT ON COLUMN events.neg_risk IS '是否负风险市场';
COMMENT ON COLUMN events.active IS '是否活跃';
COMMENT ON COLUMN events.closed IS '是否已结算';
COMMENT ON COLUMN events.archived IS '是否归档';
COMMENT ON COLUMN events.created_at IS '创建时间';
COMMENT ON COLUMN events.updated_at IS '更新时间';
COMMENT ON COLUMN events.raw_json IS 'API 原始响应 JSON';
COMMENT ON COLUMN events.imported_at IS '入库时间';

COMMENT ON TABLE market_clarifications IS '市场 Rules 澄清';
COMMENT ON COLUMN market_clarifications.id IS '澄清 ID（主键）';
COMMENT ON COLUMN market_clarifications.market_id IS '市场数字 ID（/market-clarifications 仅接受数字 id）';
COMMENT ON COLUMN market_clarifications.clarification IS '澄清文本（Rules）';
COMMENT ON COLUMN market_clarifications.created_at IS '发布时间';
COMMENT ON COLUMN market_clarifications.updated_at IS '更新时间';
COMMENT ON COLUMN market_clarifications.raw_json IS 'API 原始响应 JSON';
COMMENT ON COLUMN market_clarifications.imported_at IS '入库时间';

COMMENT ON TABLE holders IS '市场 Top Holders 快照（按 token 分组展平）';
COMMENT ON COLUMN holders.token_id IS '代币 ID（主键之一）';
COMMENT ON COLUMN holders.proxy_wallet IS '持仓钱包地址（主键之一）';
COMMENT ON COLUMN holders.name IS '用户昵称';
COMMENT ON COLUMN holders.pseudonym IS '用户匿名名';
COMMENT ON COLUMN holders.bio IS '用户简介';
COMMENT ON COLUMN holders.amount IS '持仓数量';
COMMENT ON COLUMN holders.outcome_index IS '结果索引';
COMMENT ON COLUMN holders.verified IS '是否已验证';
COMMENT ON COLUMN holders.profile_image IS '用户头像 URL';
COMMENT ON COLUMN holders.snapshot_at IS '快照时间';

COMMENT ON TABLE positions IS '市场持仓快照（每 token 组 Top limit 条，offset 无效不可翻页）';
COMMENT ON COLUMN positions.token_id IS '代币 ID（主键之一）';
COMMENT ON COLUMN positions.proxy_wallet IS '钱包地址（主键之一）';
COMMENT ON COLUMN positions.condition_id IS '市场条件 ID';
COMMENT ON COLUMN positions.name IS '用户昵称';
COMMENT ON COLUMN positions.avg_price IS '平均成本价';
COMMENT ON COLUMN positions.size IS '持仓数量';
COMMENT ON COLUMN positions.curr_price IS '当前价格';
COMMENT ON COLUMN positions.current_value IS '当前价值';
COMMENT ON COLUMN positions.cash_pnl IS '现金盈亏';
COMMENT ON COLUMN positions.realized_pnl IS '已实现盈亏';
COMMENT ON COLUMN positions.total_pnl IS '总盈亏';
COMMENT ON COLUMN positions.total_bought IS '累计买入额';
COMMENT ON COLUMN positions.outcome IS '结果名称';
COMMENT ON COLUMN positions.outcome_index IS '结果索引';
COMMENT ON COLUMN positions.snapshot_at IS '快照时间';

COMMENT ON TABLE price_history IS '价格历史（CLOB 价格时间序列）';
COMMENT ON COLUMN price_history.token_id IS '代币 ID（主键之一）';
COMMENT ON COLUMN price_history.t IS '时间戳（秒，主键之一）';
COMMENT ON COLUMN price_history.p IS '价格';
COMMENT ON COLUMN price_history.fidelity IS '采样精度（分钟）';
COMMENT ON COLUMN price_history.fetched_at IS '拉取时间';

COMMENT ON TABLE orderbook IS '订单簿快照（每次采集一批快照行）';
COMMENT ON COLUMN orderbook.token_id IS '代币 ID（主键之一）';
COMMENT ON COLUMN orderbook.side IS '方向（BUY/SELL，主键之一）';
COMMENT ON COLUMN orderbook.price IS '价格（主键之一）';
COMMENT ON COLUMN orderbook.size IS '数量（主键之一）';
COMMENT ON COLUMN orderbook.snapshot_at IS '快照时间（主键之一）';
"""

UPSERT_TRADES_SQL = """
INSERT INTO trades (
    transaction_hash, condition_id, asset, outcome_index, size, price, side,
    timestamp, proxy_wallet, usdc_size, type, title, slug, event_slug, outcome,
    icon, name, pseudonym, bio, profile_image, profile_image_optimized, event_id, raw_json
)
SELECT * FROM (
    SELECT DISTINCT ON (transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side) *
    FROM unnest(
        $1::text[], $2::text[], $3::text[], $4::smallint[], $5::numeric[], $6::numeric[],
        $7::text[], $8::bigint[], $9::text[], $10::numeric[], $11::text[], $12::text[],
        $13::text[], $14::text[], $15::text[], $16::text[], $17::text[], $18::text[],
        $19::text[], $20::text[], $21::text[], $22::text[], $23::jsonb[]
    ) AS t(transaction_hash, condition_id, asset, outcome_index, size, price, side,
           timestamp, proxy_wallet, usdc_size, type, title, slug, event_slug, outcome,
           icon, name, pseudonym, bio, profile_image, profile_image_optimized, event_id, raw_json)
    ORDER BY transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side
) d
ON CONFLICT (transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side)
DO UPDATE SET
    usdc_size = COALESCE(trades.usdc_size, EXCLUDED.usdc_size),
    type      = EXCLUDED.type,
    event_id  = COALESCE(trades.event_id, EXCLUDED.event_id),
    raw_json  = COALESCE(trades.raw_json, EXCLUDED.raw_json)
RETURNING (xmax = 0) AS inserted
"""

UPSERT_RECEIPTS_SQL = """
INSERT INTO tx_receipts (
    transaction_hash, block_number, block_timestamp, tx_from, tx_to, gas_used,
    effective_gas_price, status, tx_type, tx_index, cumulative_gas_used
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (transaction_hash) DO UPDATE SET
    block_number = EXCLUDED.block_number,
    block_timestamp = EXCLUDED.block_timestamp,
    tx_from = EXCLUDED.tx_from,
    tx_to = EXCLUDED.tx_to,
    gas_used = EXCLUDED.gas_used,
    effective_gas_price = EXCLUDED.effective_gas_price,
    status = EXCLUDED.status,
    tx_type = EXCLUDED.tx_type,
    tx_index = EXCLUDED.tx_index,
    cumulative_gas_used = EXCLUDED.cumulative_gas_used,
    failed = FALSE,
    fetched_at = now()
"""

UPSERT_BLOCKS_SQL = """
INSERT INTO blocks (block_number, timestamp) VALUES ($1, $2)
ON CONFLICT (block_number) DO NOTHING
"""


def ensure_database() -> None:
    """创建 polymarket 库（不存在时），同步用 psycopg2 连接管理库执行"""
    import psycopg2

    conn = psycopg2.connect(config.PG_ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.PG_DB,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{config.PG_DB}"')
        logger.info('已创建数据库 %s', config.PG_DB)
    cur.close()
    conn.close()


async def get_pool() -> asyncpg.Pool:
    """获取全局连接池（惰性创建）"""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=config.PG_DSN,
            min_size=config.PG_POOL_MIN,
            max_size=config.PG_POOL_MAX,
        )
    return _pool


async def reset_pool() -> None:
    """连接池损坏时重建（InterfaceError 触发）"""
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
        except Exception:
            pass
        _pool = None


async def execute_with_retry(coro_fn, *args, **kwargs):
    """执行 DB 操作；InterfaceError 时重建池并重试一次（asyncpg 连接池回收陷阱）"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await coro_fn(conn, *args, **kwargs)
    except asyncpg.exceptions.InterfaceError as exc:
        logger.warning('连接池损坏，重建后重试: %s', exc)
        await reset_pool()
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await coro_fn(conn, *args, **kwargs)


async def init_schema() -> None:
    """幂等建表"""
    await execute_with_retry(lambda conn: conn.execute(SCHEMA_SQL))
    logger.info('schema 初始化完成')


def _dedupe_rows(rows: list) -> list:
    """同批内按自然键去重：重复行保留信息更全的一条（有 usdcSize 者优先）。
    分页边界会重复返回同一行，unnest 批量 INSERT 撞到同批重复键会直接报错。"""
    seen = {}
    for r in rows:
        key = (
            r.get('transactionHash') or '',
            r.get('asset') or '',
            int(r.get('outcomeIndex')) if r.get('outcomeIndex') is not None else -1,
            float(r.get('size') or 0),
            int(r.get('timestamp') or 0),
            r.get('proxyWallet') or '',
            r.get('side') or '',
        )
        prev = seen.get(key)
        if prev is None or (prev.get('usdcSize') is None and r.get('usdcSize') is not None):
            seen[key] = r
    return list(seen.values())


def _rows_to_arrays(rows: list) -> list:
    """把 API 原始行归一化为 unnest 数组（22 列）"""
    tx_hashes, cids, assets, oidxs, sizes, prices = [], [], [], [], [], []
    sides, tss, wallets, usdcs, types_ = [], [], [], [], []
    titles, slugs, eslugs, outcomes, icons = [], [], [], [], []
    names, pseu, bios, pimgs, pimgs_o, evt_ids, raws = [], [], [], [], [], [], []

    for r in rows:
        tx_hashes.append(r.get('transactionHash') or '')
        cids.append(r.get('conditionId') or '')
        assets.append(r.get('asset') or '')
        oi = r.get('outcomeIndex')
        oidxs.append(int(oi) if oi is not None else -1)
        sizes.append(float(r.get('size') or 0))
        prices.append(float(r.get('price') or 0))
        sides.append(r.get('side') or '')
        tss.append(int(r.get('timestamp') or 0))
        wallets.append(r.get('proxyWallet') or '')
        usdcs.append(float(r['usdcSize']) if r.get('usdcSize') is not None else None)
        types_.append(r.get('type') or 'TRADE')
        titles.append(r.get('title'))
        slugs.append(r.get('slug'))
        eslugs.append(r.get('eventSlug'))
        outcomes.append(r.get('outcome'))
        icons.append(r.get('icon'))
        names.append(r.get('name'))
        pseu.append(r.get('pseudonym'))
        bios.append(r.get('bio'))
        pimgs.append(r.get('profileImage'))
        pimgs_o.append(r.get('profileImageOptimized'))
        evt_ids.append(str(r['eventId']) if r.get('eventId') is not None else None)
        raws.append(json.dumps(r, ensure_ascii=False))

    return [tx_hashes, cids, assets, oidxs, sizes, prices, sides, tss, wallets,
            usdcs, types_, titles, slugs, eslugs, outcomes, icons, names, pseu,
            bios, pimgs, pimgs_o, evt_ids, raws]


async def upsert_trades(rows: list) -> tuple:
    """批量 upsert 交易行，返回 (inserted, updated)。自然键去重，冲突时富化 usdc_size/type。"""
    if not rows:
        return 0, 0

    rows = _dedupe_rows(rows)
    arrays = _rows_to_arrays(rows)

    async def _do(conn):
        res = await conn.fetch(UPSERT_TRADES_SQL, *arrays)
        inserted = sum(1 for rec in res if rec['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def refresh_users() -> dict:
    """从 trades 聚合刷新 users 表（笔数、时间范围、画像字段），返回统计"""
    async def _do(conn):
        await conn.execute(
            """
            INSERT INTO users (proxy_wallet, trade_count, first_trade_ts, last_trade_ts)
            SELECT proxy_wallet, count(*), min(timestamp), max(timestamp)
            FROM trades GROUP BY proxy_wallet
            ON CONFLICT (proxy_wallet) DO UPDATE SET
                trade_count = EXCLUDED.trade_count,
                first_trade_ts = EXCLUDED.first_trade_ts,
                last_trade_ts = EXCLUDED.last_trade_ts,
                updated_at = now()
            """
        )
        total = await conn.fetchval("SELECT count(*) FROM users")
        # 画像字段取每个用户最新非空值
        await conn.execute(
            """
            INSERT INTO users (proxy_wallet, name, pseudonym, bio, profile_image, profile_image_optimized)
            SELECT DISTINCT ON (proxy_wallet) proxy_wallet, name, pseudonym, bio, profile_image, profile_image_optimized
            FROM trades
            WHERE COALESCE(name, '') <> '' OR COALESCE(pseudonym, '') <> ''
            ORDER BY proxy_wallet, timestamp DESC
            ON CONFLICT (proxy_wallet) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, users.name),
                pseudonym = COALESCE(EXCLUDED.pseudonym, users.pseudonym),
                bio = COALESCE(EXCLUDED.bio, users.bio),
                profile_image = COALESCE(EXCLUDED.profile_image, users.profile_image),
                profile_image_optimized = COALESCE(EXCLUDED.profile_image_optimized, users.profile_image_optimized),
                updated_at = now()
            """
        )
        return {'user_count': total}

    return await execute_with_retry(_do)


async def upsert_tx_receipts(receipts: list) -> int:
    """批量 upsert 链上收据，返回写入条数"""
    if not receipts:
        return 0

    async def _do(conn):
        async with conn.transaction():
            for r in receipts:
                await conn.execute(
                    UPSERT_RECEIPTS_SQL,
                    r['transaction_hash'], r.get('block_number'), r.get('block_timestamp'),
                    r.get('tx_from'), r.get('tx_to'), r.get('gas_used'),
                    r.get('effective_gas_price'), r.get('status'), r.get('tx_type'),
                    r.get('tx_index'), r.get('cumulative_gas_used'),
                )
        return len(receipts)

    return await execute_with_retry(_do)


async def upsert_blocks(block_rows: list) -> None:
    """批量写入区块时间戳缓存"""
    if not block_rows:
        return

    async def _do(conn):
        async with conn.transaction():
            for bn, ts in block_rows:
                await conn.execute(UPSERT_BLOCKS_SQL, bn, ts)

    await execute_with_retry(_do)


async def get_block_timestamps(block_numbers: list) -> dict:
    """查询区块时间戳缓存"""
    if not block_numbers:
        return {}

    async def _do(conn):
        res = await conn.fetch(
            "SELECT block_number, timestamp FROM blocks WHERE block_number = ANY($1::bigint[])",
            block_numbers,
        )
        return {r['block_number']: r['timestamp'] for r in res}

    return await execute_with_retry(_do)


async def get_pending_tx_hashes(after_hash: str, limit: int) -> list:
    """取未回填的交易哈希（字典序 keyset 游标，可断点续扫）。
    failed 哈希进入冷却窗口（TX_RETRY_COOLDOWN_HOURS 后自动重试），
    避免备选非归档节点误标 failed 后的重试风暴。"""
    async def _do(conn):
        res = await conn.fetch(
            """
            SELECT transaction_hash
            FROM trades
            WHERE transaction_hash <> '' AND transaction_hash > $1
              AND NOT EXISTS (
                  SELECT 1 FROM tx_receipts r
                  WHERE r.transaction_hash = trades.transaction_hash
                    AND (NOT r.failed
                         OR r.fetched_at > now() - make_interval(hours => $3::int))
              )
            GROUP BY transaction_hash
            ORDER BY transaction_hash
            LIMIT $2
            """,
            after_hash, limit, config.TX_RETRY_COOLDOWN_HOURS,
        )
        return [r['transaction_hash'] for r in res]

    return await execute_with_retry(_do)


async def count_pending_tx_hashes() -> int:
    """未回填哈希总数（--dry-run 估算用）"""
    async def _do(conn):
        return await conn.fetchval(
            """
            SELECT count(*) FROM (
                SELECT DISTINCT transaction_hash
                FROM trades
                WHERE transaction_hash <> ''
                  AND NOT EXISTS (
                      SELECT 1 FROM tx_receipts r
                      WHERE r.transaction_hash = trades.transaction_hash AND NOT r.failed
                  )
            ) t
            """
        )

    return await execute_with_retry(_do)


async def count_pending_wallets() -> int:
    """未完成采集的用户数（--dry-run 估算用）"""
    async def _do(conn):
        return await conn.fetchval(
            """
            SELECT count(DISTINCT proxy_wallet)
            FROM trades
            WHERE proxy_wallet <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM scrape_progress p
                  WHERE p.scope = 'user:' || proxy_wallet AND p.state = 'done'
              )
            """
        )

    return await execute_with_retry(_do)


async def upsert_markets(rows: list) -> tuple:
    """批量 upsert markets 表，返回 (inserted, updated)"""
    if not rows:
        return 0, 0

    def _parse_ts(s):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None

    def _to_json(s):
        if not s:
            return None
        try:
            return json.loads(s) if isinstance(s, str) else s
        except (json.JSONDecodeError, TypeError):
            return None

    def _to_num(s):
        if s is None:
            return None
        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    cids, questions, slugs = [], [], []
    end_dates, start_dates = [], []
    images, icons, descriptions = [], [], []
    outcomes_list, outcome_prices_list = [], []
    volumes, liquidities = [], []
    actives, closeds = [], []
    created_ats, updated_ats, closed_times = [], [], []
    submitted_bys, resolved_bys = [], []
    restricteds, archiveds = [], []
    group_item_titles, group_item_thresholds = [], []
    question_ids = []
    enable_order_books = []
    order_price_min_tick_sizes, order_min_sizes = [], []
    volume_24hrs = []
    uma_resolution_statuses = []
    event_ids, tags_list, uma_statuses_list, fee_schedules = [], [], [], []
    raw_jsons = []

    for r in rows:
        cids.append(r.get('conditionId'))
        questions.append(r.get('question'))
        slugs.append(r.get('slug'))
        end_dates.append(_parse_ts(r.get('endDate')))
        start_dates.append(_parse_ts(r.get('startDate')))
        images.append(r.get('image'))
        icons.append(r.get('icon'))
        descriptions.append(r.get('description'))
        outcomes_list.append(r.get('outcomes'))
        outcome_prices_list.append(r.get('outcomePrices'))
        volumes.append(_to_num(r.get('volume') or r.get('volumeNum')))
        liquidities.append(_to_num(r.get('liquidity') or r.get('liquidityNum')))
        actives.append(bool(r.get('active')))
        closeds.append(bool(r.get('closed')))
        created_ats.append(_parse_ts(r.get('createdAt')))
        updated_ats.append(_parse_ts(r.get('updatedAt')))
        closed_times.append(_parse_ts(r.get('closedTime')))
        submitted_bys.append(r.get('submitted_by'))
        resolved_bys.append(r.get('resolvedBy'))
        restricteds.append(bool(r.get('restricted')))
        archiveds.append(bool(r.get('archived')))
        group_item_titles.append(r.get('groupItemTitle'))
        group_item_thresholds.append(r.get('groupItemThreshold'))
        question_ids.append(r.get('questionID'))
        enable_order_books.append(bool(r.get('enableOrderBook')))
        order_price_min_tick_sizes.append(_to_num(r.get('orderPriceMinTickSize')))
        order_min_sizes.append(_to_num(r.get('orderMinSize')))
        volume_24hrs.append(_to_num(r.get('volume24hr')))
        uma_resolution_statuses.append(r.get('umaResolutionStatus'))
        event_ids.append(str(r['eventId']) if r.get('eventId') is not None else None)
        tags_list.append(json.dumps(r['tags'], ensure_ascii=False) if r.get('tags') else None)
        uma_statuses_list.append(json.dumps(r['umaResolutionStatuses'], ensure_ascii=False) if r.get('umaResolutionStatuses') else None)
        fee_schedules.append(json.dumps(r['feeSchedule'], ensure_ascii=False) if r.get('feeSchedule') else None)
        raw_jsons.append(json.dumps(r, ensure_ascii=False))

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO markets (
                condition_id, question, slug, end_date, start_date, image, icon, description,
                outcomes, outcome_prices, volume, liquidity, active, closed, created_at, updated_at,
                closed_time, submitted_by, resolved_by, restricted, archived, group_item_title,
                group_item_threshold, question_id, enable_order_book, order_price_min_tick_size,
                order_min_size, volume_24hr, uma_resolution_status, event_id, tags,
                uma_resolution_statuses, fee_schedule, raw_json
            )
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::timestamptz[],
                $6::text[], $7::text[], $8::text[], $9::text[], $10::text[],
                $11::numeric[], $12::numeric[], $13::boolean[], $14::boolean[],
                $15::timestamptz[], $16::timestamptz[], $17::timestamptz[], $18::text[],
                $19::text[], $20::boolean[], $21::boolean[], $22::text[], $23::text[],
                $24::text[], $25::boolean[], $26::numeric[], $27::numeric[], $28::numeric[],
                $29::text[], $30::text[], $31::jsonb[], $32::text[], $33::jsonb[], $34::jsonb[]
            )
            ON CONFLICT (condition_id) DO UPDATE SET
                question = EXCLUDED.question, slug = EXCLUDED.slug, end_date = EXCLUDED.end_date,
                start_date = EXCLUDED.start_date, image = EXCLUDED.image, icon = EXCLUDED.icon,
                description = EXCLUDED.description, outcomes = EXCLUDED.outcomes,
                outcome_prices = EXCLUDED.outcome_prices, volume = EXCLUDED.volume,
                liquidity = EXCLUDED.liquidity, active = EXCLUDED.active, closed = EXCLUDED.closed,
                updated_at = EXCLUDED.updated_at, closed_time = EXCLUDED.closed_time,
                submitted_by = EXCLUDED.submitted_by, resolved_by = EXCLUDED.resolved_by,
                restricted = EXCLUDED.restricted, archived = EXCLUDED.archived,
                group_item_title = EXCLUDED.group_item_title,
                group_item_threshold = EXCLUDED.group_item_threshold,
                question_id = EXCLUDED.question_id, enable_order_book = EXCLUDED.enable_order_book,
                order_price_min_tick_size = EXCLUDED.order_price_min_tick_size,
                order_min_size = EXCLUDED.order_min_size, volume_24hr = EXCLUDED.volume_24hr,
                uma_resolution_status = EXCLUDED.uma_resolution_status,
                event_id = EXCLUDED.event_id, tags = EXCLUDED.tags,
                uma_resolution_statuses = EXCLUDED.uma_resolution_statuses,
                fee_schedule = EXCLUDED.fee_schedule, raw_json = EXCLUDED.raw_json,
                imported_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            cids, questions, slugs, end_dates, start_dates, images, icons, descriptions,
            outcomes_list, outcome_prices_list, volumes, liquidities, actives, closeds,
            created_ats, updated_ats, closed_times, submitted_bys, resolved_bys, restricteds,
            archiveds, group_item_titles, group_item_thresholds, question_ids, enable_order_books,
            order_price_min_tick_sizes, order_min_sizes, volume_24hrs, uma_resolution_statuses,
            event_ids, tags_list, uma_statuses_list, fee_schedules, raw_jsons,
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def upsert_comments(rows: list) -> tuple:
    """批量 upsert comments 表，返回 (inserted, updated)

    行字段兼容两种命名（DB 下划线 / API camelCase），楼层、实体类型、reactions 存新列。
    """
    if not rows:
        return 0, 0

    def _parse_ts(s):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None

    def _to_json(s):
        if not s:
            return None
        try:
            return json.loads(s) if isinstance(s, str) else s
        except (json.JSONDecodeError, TypeError):
            return None

    ids, event_ids, bodies = [], [], []
    user_addresses, author_names = [], []
    reaction_counts, report_counts = [], []
    created_ats, updated_ats = [], []
    profile_json_list, raw_json_list = [], []
    parent_comment_ids, parent_entity_types, reactions_list = [], [], []

    for r in rows:
        ids.append(r.get('id'))
        event_ids.append(r.get('event_id') or r.get('eventId'))
        bodies.append(r.get('body'))
        user_addresses.append(r.get('user_address') or r.get('userAddress'))
        author_names.append(r.get('author_name') or r.get('authorName'))
        reaction_counts.append(int(r.get('reaction_count', 0) or r.get('reactionCount', 0) or 0))
        report_counts.append(int(r.get('report_count', 0) or r.get('reportCount', 0) or 0))
        created_ats.append(_parse_ts(r.get('created_at') or r.get('createdAt')))
        updated_ats.append(_parse_ts(r.get('updated_at') or r.get('updatedAt')))
        profile_json_list.append(json.dumps(_to_json(r.get('profile_json') or r.get('profileJson') or r.get('profile')), ensure_ascii=False) if (r.get('profile_json') or r.get('profileJson') or r.get('profile')) else None)
        parent_comment_ids.append(r.get('parent_comment_id') or r.get('parentCommentId') or r.get('parentCommentID'))
        parent_entity_types.append(r.get('parent_entity_type') or r.get('parentEntityType'))
        reactions_list.append(json.dumps(_to_json(r.get('reactions')), ensure_ascii=False) if r.get('reactions') else None)
        raw_json_list.append(json.dumps(r, ensure_ascii=False))

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO comments (
                id, event_id, body, user_address, author_name, reaction_count, report_count,
                created_at, updated_at, profile_json, parent_comment_id, parent_entity_type,
                reactions, raw_json
            )
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                $6::integer[], $7::integer[], $8::timestamptz[], $9::timestamptz[],
                $10::jsonb[], $11::text[], $12::text[], $13::jsonb[], $14::jsonb[]
            )
            ON CONFLICT (id) DO UPDATE SET
                event_id = EXCLUDED.event_id, body = EXCLUDED.body,
                user_address = EXCLUDED.user_address, author_name = EXCLUDED.author_name,
                reaction_count = EXCLUDED.reaction_count, report_count = EXCLUDED.report_count,
                updated_at = EXCLUDED.updated_at, profile_json = EXCLUDED.profile_json,
                parent_comment_id = EXCLUDED.parent_comment_id,
                parent_entity_type = EXCLUDED.parent_entity_type,
                reactions = EXCLUDED.reactions, raw_json = EXCLUDED.raw_json,
                imported_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            ids, event_ids, bodies, user_addresses, author_names,
            reaction_counts, report_counts, created_ats, updated_ats,
            profile_json_list, parent_comment_ids, parent_entity_types,
            reactions_list, raw_json_list,
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def mark_tx_failed(hashes: list) -> None:
    """标记回填失败的哈希。冷却窗口过后 get_pending 会自动重试；
    已有成功收据的行不被覆盖。"""
    if not hashes:
        return

    async def _do(conn):
        await conn.execute(
            """
            INSERT INTO tx_receipts (transaction_hash, failed) 
            SELECT h, TRUE FROM unnest($1::text[]) AS h
            ON CONFLICT (transaction_hash) DO UPDATE SET
                failed = TRUE, fetched_at = now()
            WHERE tx_receipts.failed = TRUE
            """,
            hashes,
        )

    await execute_with_retry(_do)


async def get_tx_stats() -> dict:
    """链上回填覆盖度统计"""
    async def _do(conn):
        total = await conn.fetchval(
            "SELECT count(DISTINCT transaction_hash) FROM trades WHERE transaction_hash <> ''"
        )
        done = await conn.fetchval(
            "SELECT count(*) FROM tx_receipts WHERE NOT failed"
        )
        failed = await conn.fetchval(
            "SELECT count(*) FROM tx_receipts WHERE failed"
        )
        return {'total_hashes': total, 'enriched': done, 'failed': failed}

    return await execute_with_retry(_do)


async def get_done_scopes(prefix: str) -> set:
    """已完成的采集 scope 集合（market:/user: 前缀）"""
    async def _do(conn):
        res = await conn.fetch(
            "SELECT scope FROM scrape_progress WHERE scope LIKE $1 || '%' AND state = 'done'",
            prefix,
        )
        return {r['scope'] for r in res}

    return await execute_with_retry(_do)


async def mark_scope_done(scope: str, fetched_count: int) -> None:
    """标记 scope 完成"""
    async def _do(conn):
        await conn.execute(
            """
            INSERT INTO scrape_progress (scope, state, fetched_count)
            VALUES ($1, 'done', $2)
            ON CONFLICT (scope) DO UPDATE SET
                state = 'done', fetched_count = EXCLUDED.fetched_count, updated_at = now()
            """,
            scope, fetched_count,
        )

    await execute_with_retry(_do)


async def get_pending_wallets(limit: int) -> list:
    """取尚未完成采集的用户钱包（按字典序）"""
    async def _do(conn):
        res = await conn.fetch(
            """
            SELECT DISTINCT proxy_wallet
            FROM trades
            WHERE proxy_wallet <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM scrape_progress p
                  WHERE p.scope = 'user:' || proxy_wallet AND p.state = 'done'
              )
            ORDER BY proxy_wallet
            LIMIT $1
            """,
            limit,
        )
        return [r['proxy_wallet'] for r in res]

    return await execute_with_retry(_do)


async def get_stats() -> dict:
    """采集汇总统计"""
    async def _do(conn):
        row = await conn.fetchrow(
            """
            SELECT
                (SELECT count(*) FROM trades) AS trades,
                (SELECT count(*) FROM users) AS users,
                (SELECT count(*) FROM tx_receipts) AS receipts,
                (SELECT count(*) FROM scrape_progress WHERE state = 'done') AS done_scopes
            """
        )
        return dict(row)

    return await execute_with_retry(_do)


# ==================== 全模块采集扩展（events/clarifications/holders/positions/price/orderbook） ====================


def _parse_ts(s):
    """ISO 时间字符串转 datetime（camelCase API 与下划线 DB 字段通用）"""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def _to_json(s):
    if not s:
        return None
    try:
        return json.loads(s) if isinstance(s, str) else s
    except (json.JSONDecodeError, TypeError):
        return None


def _to_num(s):
    if s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


async def upsert_events(rows: list) -> tuple:
    """批量 upsert events 表，返回 (inserted, updated)。tags 存 JSONB。"""
    if not rows:
        return 0, 0

    ids, slugs, tickers, titles, descriptions = [], [], [], [], []
    tags_list, volumes, liquidities, open_interests, volume_24hrs = [], [], [], [], []
    comment_counts, neg_risks, actives, closeds, archiveds = [], [], [], [], []
    created_ats, updated_ats, raw_jsons = [], [], []

    for r in rows:
        ids.append(str(r.get('id')) if r.get('id') is not None else None)
        slugs.append(r.get('slug'))
        tickers.append(r.get('ticker'))
        titles.append(r.get('title'))
        descriptions.append(r.get('description'))
        tags_list.append(json.dumps(r['tags'], ensure_ascii=False) if r.get('tags') else None)
        volumes.append(_to_num(r.get('volume')))
        liquidities.append(_to_num(r.get('liquidity')))
        open_interests.append(_to_num(r.get('openInterest')))
        volume_24hrs.append(_to_num(r.get('volume24hr')))
        comment_counts.append(int(r.get('commentCount') or 0))
        neg_risks.append(bool(r.get('negRisk')) if r.get('negRisk') is not None else None)
        actives.append(bool(r.get('active')) if r.get('active') is not None else None)
        closeds.append(bool(r.get('closed')) if r.get('closed') is not None else None)
        archiveds.append(bool(r.get('archived')) if r.get('archived') is not None else None)
        created_ats.append(_parse_ts(r.get('createdAt')))
        updated_ats.append(_parse_ts(r.get('updatedAt')))
        raw_jsons.append(json.dumps(r, ensure_ascii=False))

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO events (
                id, slug, ticker, title, description, tags, volume, liquidity,
                open_interest, volume_24hr, comment_count, neg_risk, active, closed,
                archived, created_at, updated_at, raw_json
            )
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                $6::jsonb[], $7::numeric[], $8::numeric[], $9::numeric[], $10::numeric[],
                $11::integer[], $12::boolean[], $13::boolean[], $14::boolean[], $15::boolean[],
                $16::timestamptz[], $17::timestamptz[], $18::jsonb[]
            )
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug, ticker = EXCLUDED.ticker, title = EXCLUDED.title,
                description = EXCLUDED.description, tags = EXCLUDED.tags,
                volume = EXCLUDED.volume, liquidity = EXCLUDED.liquidity,
                open_interest = EXCLUDED.open_interest, volume_24hr = EXCLUDED.volume_24hr,
                comment_count = EXCLUDED.comment_count, neg_risk = EXCLUDED.neg_risk,
                active = EXCLUDED.active, closed = EXCLUDED.closed, archived = EXCLUDED.archived,
                updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json,
                imported_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            ids, slugs, tickers, titles, descriptions, tags_list, volumes, liquidities,
            open_interests, volume_24hrs, comment_counts, neg_risks, actives, closeds,
            archiveds, created_ats, updated_ats, raw_jsons,
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def upsert_clarifications(rows: list) -> tuple:
    """批量 upsert market_clarifications 表，返回 (inserted, updated)（Rules 澄清）"""
    if not rows:
        return 0, 0

    ids, market_ids, clarifications = [], [], []
    created_ats, updated_ats, raw_jsons = [], [], []

    for r in rows:
        ids.append(str(r.get('id')) if r.get('id') is not None else None)
        market_ids.append(str(r.get('market_id') or r.get('marketId') or r.get('market')))
        clarifications.append(r.get('clarification') or r.get('text'))
        created_ats.append(_parse_ts(r.get('created_at') or r.get('createdAt')))
        updated_ats.append(_parse_ts(r.get('updated_at') or r.get('updatedAt')))
        raw_jsons.append(json.dumps(r, ensure_ascii=False))

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO market_clarifications (
                id, market_id, clarification, created_at, updated_at, raw_json
            )
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::text[], $4::timestamptz[],
                $5::timestamptz[], $6::jsonb[]
            )
            ON CONFLICT (id) DO UPDATE SET
                market_id = EXCLUDED.market_id, clarification = EXCLUDED.clarification,
                updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json,
                imported_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            ids, market_ids, clarifications, created_ats, updated_ats, raw_jsons,
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def upsert_holders(rows: list) -> tuple:
    """批量 upsert holders 表，返回 (inserted, updated)。快照采集，重复采集覆盖快照时间。"""
    if not rows:
        return 0, 0

    token_ids, wallets, names, pseu, bios = [], [], [], [], []
    amounts, outcome_idxs, verifieds, profile_imgs = [], [], [], []

    for r in rows:
        token_ids.append(r.get('token_id') or r.get('token') or r.get('market'))
        wallets.append(r.get('proxy_wallet') or r.get('proxyWallet'))
        names.append(r.get('name'))
        pseu.append(r.get('pseudonym'))
        bios.append(r.get('bio'))
        amounts.append(_to_num(r.get('amount')))
        oi = r.get('outcome_index') if r.get('outcome_index') is not None else r.get('outcomeIndex')
        outcome_idxs.append(int(oi) if oi is not None else None)
        verifieds.append(bool(r.get('verified')) if r.get('verified') is not None else None)
        profile_imgs.append(r.get('profile_image') or r.get('profileImage') or r.get('avatar'))

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO holders (
                token_id, proxy_wallet, name, pseudonym, bio, amount, outcome_index,
                verified, profile_image, snapshot_at
            )
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                $6::numeric[], $7::smallint[], $8::boolean[], $9::text[],
                $10::timestamptz[]
            )
            ON CONFLICT (token_id, proxy_wallet) DO UPDATE SET
                name = EXCLUDED.name, pseudonym = EXCLUDED.pseudonym,
                bio = EXCLUDED.bio, amount = EXCLUDED.amount,
                outcome_index = EXCLUDED.outcome_index, verified = EXCLUDED.verified,
                profile_image = EXCLUDED.profile_image, snapshot_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            token_ids, wallets, names, pseu, bios, amounts, outcome_idxs,
            verifieds, profile_imgs, [datetime.now()] * len(rows),
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def upsert_positions(rows: list) -> tuple:
    """批量 upsert positions 表，返回 (inserted, updated)。API 行字段为 camelCase。"""
    if not rows:
        return 0, 0

    token_ids, wallets, condition_ids, names = [], [], [], []
    avg_prices, sizes, curr_prices, current_values = [], [], [], []
    cash_pnls, realized_pnls, total_pnls, total_boughts = [], [], [], []
    outcomes, outcome_idxs = [], []

    for r in rows:
        token_ids.append(r.get('token_id') or r.get('asset') or r.get('token'))
        wallets.append(r.get('proxy_wallet') or r.get('proxyWallet'))
        condition_ids.append(r.get('condition_id') or r.get('conditionId'))
        names.append(r.get('name'))
        avg_prices.append(_to_num(r.get('avg_price') if r.get('avg_price') is not None else r.get('avgPrice')))
        sizes.append(_to_num(r.get('size')))
        curr_prices.append(_to_num(r.get('curr_price') if r.get('curr_price') is not None else r.get('currPrice')))
        current_values.append(_to_num(r.get('current_value') if r.get('current_value') is not None else r.get('currentValue')))
        cash_pnls.append(_to_num(r.get('cash_pnl') if r.get('cash_pnl') is not None else r.get('cashPnl')))
        realized_pnls.append(_to_num(r.get('realized_pnl') if r.get('realized_pnl') is not None else r.get('realizedPnl')))
        total_pnls.append(_to_num(r.get('total_pnl') if r.get('total_pnl') is not None else r.get('totalPnl')))
        total_boughts.append(_to_num(r.get('total_bought') if r.get('total_bought') is not None else r.get('totalBought')))
        outcomes.append(r.get('outcome'))
        oi = r.get('outcome_index') if r.get('outcome_index') is not None else r.get('outcomeIndex')
        outcome_idxs.append(int(oi) if oi is not None else None)

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO positions (
                token_id, proxy_wallet, condition_id, name, avg_price, size, curr_price,
                current_value, cash_pnl, realized_pnl, total_pnl, total_bought, outcome,
                outcome_index, snapshot_at
            )
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::numeric[],
                $6::numeric[], $7::numeric[], $8::numeric[], $9::numeric[], $10::numeric[],
                $11::numeric[], $12::numeric[], $13::text[], $14::smallint[],
                $15::timestamptz[]
            )
            ON CONFLICT (token_id, proxy_wallet) DO UPDATE SET
                condition_id = EXCLUDED.condition_id, name = EXCLUDED.name,
                avg_price = EXCLUDED.avg_price, size = EXCLUDED.size,
                curr_price = EXCLUDED.curr_price, current_value = EXCLUDED.current_value,
                cash_pnl = EXCLUDED.cash_pnl, realized_pnl = EXCLUDED.realized_pnl,
                total_pnl = EXCLUDED.total_pnl, total_bought = EXCLUDED.total_bought,
                outcome = EXCLUDED.outcome, outcome_index = EXCLUDED.outcome_index,
                snapshot_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            token_ids, wallets, condition_ids, names, avg_prices, sizes, curr_prices,
            current_values, cash_pnls, realized_pnls, total_pnls, total_boughts,
            outcomes, outcome_idxs, [datetime.now()] * len(rows),
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def upsert_price_history(rows: list, fidelity: int = 10) -> tuple:
    """批量 upsert price_history 表，返回 (inserted, updated)。行字段 token_id/t/p。"""
    if not rows:
        return 0, 0

    token_ids, ts, ps = [], [], []
    for r in rows:
        token_ids.append(r.get('token_id') or r.get('market'))
        ts.append(int(r.get('t') or 0))
        ps.append(_to_num(r.get('p')))

    async def _do(conn):
        res = await conn.fetch(
            """
            INSERT INTO price_history (token_id, t, p, fidelity)
            SELECT * FROM unnest(
                $1::text[], $2::bigint[], $3::numeric[], $4::integer[]
            )
            ON CONFLICT (token_id, t) DO UPDATE SET
                p = EXCLUDED.p, fidelity = EXCLUDED.fidelity, fetched_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            token_ids, ts, ps, [int(fidelity)] * len(rows),
        )
        inserted = sum(1 for r in res if r['inserted'])
        return inserted, len(res) - inserted

    return await execute_with_retry(_do)


async def upsert_orderbook(rows: list, snapshot_at: datetime = None) -> int:
    """批量写入 orderbook 快照行（token_id/side/price/size），返回写入条数。"""
    if not rows:
        return 0

    snap = snapshot_at or datetime.now()
    token_ids, sides, prices, sizes = [], [], [], []
    for r in rows:
        token_ids.append(r.get('token_id') or r.get('token'))
        sides.append(r.get('side'))
        prices.append(_to_num(r.get('price')))
        sizes.append(_to_num(r.get('size')))

    async def _do(conn):
        await conn.execute(
            """
            INSERT INTO orderbook (token_id, side, price, size, snapshot_at)
            SELECT * FROM unnest(
                $1::text[], $2::text[], $3::numeric[], $4::numeric[], $5::timestamptz[]
            )
            ON CONFLICT (token_id, side, price, snapshot_at) DO NOTHING
            """,
            token_ids, sides, prices, sizes, [snap] * len(rows),
        )
        return len(rows)

    return await execute_with_retry(_do)


async def get_price_history_latest() -> dict:
    """各 token 已入库的最大时间戳（增量采集起点），返回 {token_id: max_t}。"""
    async def _do(conn):
        res = await conn.fetch("SELECT token_id, max(t) AS t FROM price_history GROUP BY token_id")
        return {r['token_id']: r['t'] for r in res}

    return await execute_with_retry(_do)


async def get_scope_state(scope: str):
    """读取进度 (state, fetched_count)，无记录返回 None（断点续传用）"""
    async def _do(conn):
        return await conn.fetchrow(
            "SELECT state, fetched_count FROM scrape_progress WHERE scope = $1", scope
        )

    return await execute_with_retry(_do)


async def set_scope_state(scope: str, state: str, fetched_count: int = None) -> None:
    """写入/更新进度（state 可携带续传游标，fetched_count 记已处理数）

    fetched_count 为 None 时仅更新 state（保留旧计数），避免 NOT NULL 违规。
    """
    async def _do(conn):
        if fetched_count is None:
            await conn.execute(
                """
                INSERT INTO scrape_progress (scope, state)
                VALUES ($1, $2)
                ON CONFLICT (scope) DO UPDATE SET
                    state = EXCLUDED.state,
                    updated_at = now()
                """,
                scope, state,
            )
        else:
            await conn.execute(
                """
                INSERT INTO scrape_progress (scope, state, fetched_count)
                VALUES ($1, $2, $3)
                ON CONFLICT (scope) DO UPDATE SET
                    state = EXCLUDED.state,
                    fetched_count = EXCLUDED.fetched_count,
                    updated_at = now()
                """,
                scope, state, fetched_count,
            )

    await execute_with_retry(_do)


async def get_markets_for_detail(after_condition_id: str, limit: int) -> list:
    """取详情不完整（缺 clobTokenIds/description）的市场，按 condition_id 字典序翻页续传"""
    async def _do(conn):
        res = await conn.fetch(
            """
            SELECT condition_id, slug FROM markets
            WHERE condition_id > $1
              AND (raw_json->>'clobTokenIds' IS NULL OR raw_json->>'clobTokenIds' = 'null'
                   OR raw_json->>'description' IS NULL)
            ORDER BY condition_id
            LIMIT $2
            """,
            after_condition_id, limit,
        )
        return [dict(r) for r in res]

    return await execute_with_retry(_do)


async def get_market_ids_page(after_condition_id: str, limit: int) -> list:
    """全量 markets condition_id + 数字 market_id 分页（澄清采集驱动），按字典序续传。

    实测 /market-clarifications 只接受市场数字 id（raw_json.id），不接受 condition_id。
    """
    async def _do(conn):
        res = await conn.fetch(
            """
            SELECT condition_id, slug, raw_json->>'id' AS market_id FROM markets
            WHERE condition_id > $1 AND raw_json->>'id' IS NOT NULL
            ORDER BY condition_id
            LIMIT $2
            """,
            after_condition_id, limit,
        )
        return [dict(r) for r in res]

    return await execute_with_retry(_do)


async def get_events_for_comments(after_event_id: str, limit: int, min_comment_count: int = 0) -> list:
    """取评论采集事件队列（comment_count 过滤），按 id 字典序翻页续传"""
    async def _do(conn):
        res = await conn.fetch(
            """
            SELECT id, comment_count FROM events
            WHERE id > $1 AND comment_count >= $2
            ORDER BY id
            LIMIT $3
            """,
            after_event_id, min_comment_count, limit,
        )
        return [dict(r) for r in res]

    return await execute_with_retry(_do)


async def get_event_slugs(after_slug: str, limit: int) -> list:
    """按 slug 字典序分批取事件 slug（Stage B 批量补 tags 的驱动队列）"""
    async def _do(conn):
        rows = await conn.fetch(
            """
            SELECT slug FROM events
            WHERE slug IS NOT NULL AND slug > $1
            ORDER BY slug
            LIMIT $2
            """,
            after_slug, limit,
        )
        return [r['slug'] for r in rows]

    return await execute_with_retry(_do)


async def get_market_clob_tokens(after_condition_id: str, limit: int) -> list:
    """取市场 clobTokenIds（价格历史/盘口采集驱动），返回 [{condition_id, token_ids}]"""
    async def _do(conn):
        res = await conn.fetch(
            """
            SELECT condition_id, raw_json->>'clobTokenIds' AS tokens FROM markets
            WHERE condition_id > $1 AND raw_json->>'clobTokenIds' IS NOT NULL
            ORDER BY condition_id
            LIMIT $2
            """,
            after_condition_id, limit,
        )
        out = []
        for r in res:
            try:
                tokens = json.loads(r['tokens'])
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(tokens, list) and tokens:
                out.append({'condition_id': r['condition_id'], 'token_ids': tokens})
        return out

    return await execute_with_retry(_do)
