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
"""

UPSERT_TRADES_SQL = """
INSERT INTO trades (
    transaction_hash, condition_id, asset, outcome_index, size, price, side,
    timestamp, proxy_wallet, usdc_size, type, title, slug, event_slug, outcome,
    icon, name, pseudonym, bio, profile_image, profile_image_optimized, raw_json
)
SELECT * FROM (
    SELECT DISTINCT ON (transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side) *
    FROM unnest(
        $1::text[], $2::text[], $3::text[], $4::smallint[], $5::numeric[], $6::numeric[],
        $7::text[], $8::bigint[], $9::text[], $10::numeric[], $11::text[], $12::text[],
        $13::text[], $14::text[], $15::text[], $16::text[], $17::text[], $18::text[],
        $19::text[], $20::text[], $21::text[], $22::jsonb[]
    ) AS t(transaction_hash, condition_id, asset, outcome_index, size, price, side,
           timestamp, proxy_wallet, usdc_size, type, title, slug, event_slug, outcome,
           icon, name, pseudonym, bio, profile_image, profile_image_optimized, raw_json)
    ORDER BY transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side
) d
ON CONFLICT (transaction_hash, asset, outcome_index, size, timestamp, proxy_wallet, side)
DO UPDATE SET
    usdc_size = COALESCE(trades.usdc_size, EXCLUDED.usdc_size),
    type      = EXCLUDED.type,
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
    names, pseu, bios, pimgs, pimgs_o, raws = [], [], [], [], [], []

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
        raws.append(json.dumps(r, ensure_ascii=False))

    return [tx_hashes, cids, assets, oidxs, sizes, prices, sides, tss, wallets,
            usdcs, types_, titles, slugs, eslugs, outcomes, icons, names, pseu,
            bios, pimgs, pimgs_o, raws]


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
