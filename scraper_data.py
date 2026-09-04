# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Data API 全模块采集（Top Holders / Positions / 事件 Activity）

- holders:  对每个 market 拉 /holders（limit=100 翻页，按 token 分组展平）
- positions:对每个 market 拉 /v1/market-positions（limit=50 翻页，status=ALL）
- activity: 对每个 event 拉 /trades?eventId=（limit=1000 翻页，filterType=CASH，
            行补 event_id 后与 Phase A 自然键去重）

断点续传：scrape_progress scope='holders:{cid}' / 'positions:{cid}' / 'activity:{eid}'。
驱动队列（markets/events 表）按 id 字典序续传：scope='holders_resume' 等。
沿用 Data API 200 次/10s 令牌桶 + 并发 + 冷却重试。
"""

import asyncio
import logging

from tqdm import tqdm

import config
import db_pg
from data_api_client import DataAPIClient, TokenBucket

logger = logging.getLogger(__name__)

BATCH_ROWS = 5000  # 入库批量
GATHER_CHUNK = 5000  # 协程分批提交
RESUME_LIMIT = 2000  # 驱动队列批量


class Stats:
    """汇总统计（单线程 asyncio，无需锁）"""

    def __init__(self):
        self.inserted = 0
        self.updated = 0
        self.failures = 0


async def _collect_market_ids() -> list:
    """全量 market condition_id 队列（按字典序，断点续传）"""
    row = await db_pg.get_scope_state('holders_resume')
    after = row['state'] if row and row['state'] != 'done' else ''
    ids = []
    while True:
        page = await db_pg.get_market_ids_page(after, RESUME_LIMIT)
        if not page:
            break
        for m in page:
            ids.append(m['condition_id'])
        after = page[-1]['condition_id']
        if len(page) < RESUME_LIMIT:
            break
    return ids


async def _collect_event_ids() -> list:
    """全量 event id 队列（按字典序，断点续传）"""
    row = await db_pg.get_scope_state('activity_resume')
    after = row['state'] if row and row['state'] != 'done' else ''
    ids = []
    while True:
        page = await db_pg.get_events_for_comments(after, RESUME_LIMIT, min_comment_count=-1)
        if not page:
            break
        for ev in page:
            ids.append(ev['id'])
        after = page[-1]['id']
        if len(page) < RESUME_LIMIT:
            break
    return ids


async def _scrape_holders(api: DataAPIClient, condition_id: str, sem: asyncio.Semaphore,
                          pbar: tqdm, stats: Stats) -> None:
    async with sem:
        batch = []

        async def on_batch(rows):
            batch.extend(rows)
            if len(batch) >= BATCH_ROWS:
                ins, upd = await db_pg.upsert_holders(batch)
                stats.inserted += ins
                stats.updated += upd
                batch.clear()

        try:
            total = await api.fetch_holders(condition_id, on_batch=on_batch)
            if batch:
                ins, upd = await db_pg.upsert_holders(batch)
                stats.inserted += ins
                stats.updated += upd
            await db_pg.mark_scope_done(f'holders:{condition_id}', total)
        except Exception as exc:
            stats.failures += 1
            logger.warning('市场 %s holders 采集失败: %s', condition_id, exc)
        finally:
            pbar.update(1)
            pbar.set_postfix(inserted=stats.inserted, failed=stats.failures)


async def _scrape_positions(api: DataAPIClient, condition_id: str, sem: asyncio.Semaphore,
                            pbar: tqdm, stats: Stats) -> None:
    async with sem:
        batch = []

        async def on_batch(rows):
            batch.extend(rows)
            if len(batch) >= BATCH_ROWS:
                ins, upd = await db_pg.upsert_positions(batch)
                stats.inserted += ins
                stats.updated += upd
                batch.clear()

        try:
            total = await api.fetch_market_positions(condition_id, on_batch=on_batch)
            if batch:
                ins, upd = await db_pg.upsert_positions(batch)
                stats.inserted += ins
                stats.updated += upd
            await db_pg.mark_scope_done(f'positions:{condition_id}', total)
        except Exception as exc:
            stats.failures += 1
            logger.warning('市场 %s positions 采集失败: %s', condition_id, exc)
        finally:
            pbar.update(1)
            pbar.set_postfix(inserted=stats.inserted, failed=stats.failures)


async def collect_event_activity(api: DataAPIClient, event_id: str) -> tuple:
    """单事件全量 CASH 交易采集（worker 与单机模式共用）。

    fetch_event_trades 深翻页 + 攒批 upsert_trades，返回 (total, inserted, updated)，失败抛异常。
    """
    batch = []
    counters = {'ins': 0, 'upd': 0}

    async def on_batch(page):
        batch.extend(page)
        if len(batch) >= BATCH_ROWS:
            ins, upd = await db_pg.upsert_trades(batch)
            counters['ins'] += ins
            counters['upd'] += upd
            batch.clear()

    total = await api.fetch_event_trades(event_id, on_batch=on_batch)
    if batch:
        ins, upd = await db_pg.upsert_trades(batch)
        counters['ins'] += ins
        counters['upd'] += upd
        batch.clear()
    return total, counters['ins'], counters['upd']


async def _scrape_activity(api: DataAPIClient, event_id: str, sem: asyncio.Semaphore,
                           pbar: tqdm, stats: Stats) -> None:
    async with sem:
        try:
            total, ins, upd = await collect_event_activity(api, event_id)
            stats.inserted += ins
            stats.updated += upd
            await db_pg.mark_scope_done(f'activity:{event_id}', total)
        except Exception as exc:
            stats.failures += 1
            logger.warning('事件 %s activity 采集失败: %s', event_id, exc)
        finally:
            pbar.update(1)
            pbar.set_postfix(inserted=stats.inserted, failed=stats.failures)


async def _run_gathered(workers) -> None:
    for i in range(0, len(workers), GATHER_CHUNK):
        await asyncio.gather(*workers[i:i + GATHER_CHUNK])


async def scrape_holders(limit: int = None, concurrency: int = None,
                         only: list = None) -> dict:
    """全市场 Top Holders 采集（--limit 试跑前 N 个，only 定向市场列表）"""
    await db_pg.init_schema()
    ids = await _collect_market_ids()
    done = await db_pg.get_done_scopes('holders:')
    if only:
        todo = [only] if isinstance(only, str) else list(only)
    else:
        todo = [c for c in ids if f'holders:{c}' not in done]
        if limit:
            todo = todo[:limit]
    logger.info('holders: 共 %s 市场，已完成 %s，待采集 %s', len(ids), len(done), len(todo))
    if not todo:
        return {'markets': 0}

    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)
    stats = Stats()
    async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
        with tqdm(total=len(todo), desc='holders', unit='mkt') as pbar:
            await _run_gathered([
                _scrape_holders(api, cid, sem, pbar, stats) for cid in todo
            ])
    return {'markets': len(todo), **vars(stats)}


async def scrape_positions(limit: int = None, concurrency: int = None,
                           only: list = None) -> dict:
    """全市场 Positions 采集（--limit 试跑前 N 个，only 定向市场列表）"""
    await db_pg.init_schema()
    ids = await _collect_market_ids()
    done = await db_pg.get_done_scopes('positions:')
    if only:
        todo = [only] if isinstance(only, str) else list(only)
    else:
        todo = [c for c in ids if f'positions:{c}' not in done]
        if limit:
            todo = todo[:limit]
    logger.info('positions: 共 %s 市场，已完成 %s，待采集 %s', len(ids), len(done), len(todo))
    if not todo:
        return {'markets': 0}

    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)
    stats = Stats()
    async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
        with tqdm(total=len(todo), desc='positions', unit='mkt') as pbar:
            await _run_gathered([
                _scrape_positions(api, cid, sem, pbar, stats) for cid in todo
            ])
    return {'markets': len(todo), **vars(stats)}


async def scrape_activity(limit: int = None, concurrency: int = None,
                          only: list = None) -> dict:
    """全事件 CASH 交易采集（--limit 试跑前 N 个，only 定向事件列表）"""
    await db_pg.init_schema()
    ids = await _collect_event_ids()
    done = await db_pg.get_done_scopes('activity:')
    if only:
        todo = [only] if isinstance(only, str) else list(only)
    else:
        todo = [e for e in ids if f'activity:{e}' not in done]
        if limit:
            todo = todo[:limit]
    logger.info('activity: 共 %s 事件，已完成 %s，待采集 %s', len(ids), len(done), len(todo))
    if not todo:
        return {'events': 0}

    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)
    stats = Stats()
    async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
        with tqdm(total=len(todo), desc='activity', unit='evt') as pbar:
            await _run_gathered([
                _scrape_activity(api, eid, sem, pbar, stats) for eid in todo
            ])
    return {'events': len(todo), **vars(stats)}
