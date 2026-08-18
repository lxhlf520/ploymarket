# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""CLOB 全模块采集（价格历史 / 盘口深度快照）

- prices:   对每个 clobTokenId 拉 /prices-history（fidelity=720 日级近 1 月 +
            fidelity=10 近 24h），(token_id, t) 主键 upsert 幂等增量
- orderbook:对每个 clobTokenId 拉 GET /book 盘口深度，统一 snapshot_at 快照入库

断点续传：市场级 scope='prices:{condition_id}' / 'orderbook:{condition_id}'，
驱动队列按 condition_id 字典序：scope='prices_resume' / 'orderbook_resume'。
"""

import asyncio
import logging

from tqdm import tqdm

import config
import db_pg
from clob_client import CLOBClient

logger = logging.getLogger(__name__)

BATCH = 100     # 驱动队列批量（市场数）
GATHER_CHUNK = 2000  # 协程分批提交
PRICES_INTERVAL = '1m'   # fidelity=720 日级窗口（服务端 interval 上限 1m）
INTRADAY_INTERVAL = '1d'  # fidelity=10 近 24h


async def _collect_tokens(after: str, limit: int) -> list:
    """clobTokenIds 驱动队列（按 condition_id 字典序）"""
    return await db_pg.get_market_clob_tokens(after, limit)


async def scrape_prices(fidelity: int = None, limit: int = None,
                        concurrency: int = None) -> dict:
    """价格历史采集：fidelity=720 日级近 1 月；fidelity=10 近 24h。"""
    await db_pg.init_schema()
    fid = fidelity or config.PRICE_FIDELITY_DAILY
    interval = PRICES_INTERVAL if fid >= 720 else INTRADAY_INTERVAL

    row = await db_pg.get_scope_state('prices_resume')
    after = row['state'] if row and row['state'] != 'done' else ''
    done = await db_pg.get_done_scopes('prices:')
    stats = {'inserted': 0, 'updated': 0, 'tokens': 0, 'failed': 0}
    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)

    async with CLOBClient() as api:
        with tqdm(desc=f'prices f{fid}', unit='mkt') as pbar:
            truncated = False
            batch = min(BATCH, limit) if limit else BATCH
            while True:
                items = await _collect_tokens(after, batch)
                if not items:
                    break
                todo = [it for it in items if f'prices:{it["condition_id"]}' not in done]
                after = items[-1]['condition_id']

                async def _one(it):
                    async with sem:
                        try:
                            for tid in it['token_ids']:
                                history = await api.get_prices_history(tid, interval=interval, fidelity=fid)
                                stats['tokens'] += 1
                                if not history:
                                    continue
                                ins, upd = await db_pg.upsert_price_history(
                                    [{'token_id': tid, 't': p['t'], 'p': p['p']} for p in history],
                                    fidelity=fid,
                                )
                                stats['inserted'] += ins
                                stats['updated'] += upd
                            await db_pg.mark_scope_done(f'prices:{it["condition_id"]}', 1)
                        except Exception as exc:
                            stats['failed'] += 1
                            logger.warning('市场 %s 价格历史失败: %s', it['condition_id'], exc)

                for i in range(0, len(todo), GATHER_CHUNK):
                    await asyncio.gather(*[_one(it) for it in todo[i:i + GATHER_CHUNK]])
                await db_pg.set_scope_state('prices_resume', after)
                pbar.update(len(items))
                pbar.set_postfix(points=stats['inserted'], failed=stats['failed'])
                if limit and stats['tokens'] >= limit:
                    truncated = True
                    break
    if not truncated:
        await db_pg.mark_scope_done('prices_resume', stats['tokens'])
    return stats


async def scrape_orderbook(limit: int = None, concurrency: int = None) -> dict:
    """盘口深度快照：对每个 clobTokenId 拉 GET /book，bids/asks 统一时间戳入库。"""
    await db_pg.init_schema()
    row = await db_pg.get_scope_state('orderbook_resume')
    after = row['state'] if row and row['state'] != 'done' else ''
    done = await db_pg.get_done_scopes('orderbook:')
    stats = {'written': 0, 'markets': 0, 'failed': 0}
    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)

    async with CLOBClient() as api:
        with tqdm(desc='orderbook', unit='mkt') as pbar:
            truncated = False
            batch = min(BATCH, limit) if limit else BATCH
            while True:
                items = await _collect_tokens(after, batch)
                if not items:
                    break
                todo = [it for it in items if f'orderbook:{it["condition_id"]}' not in done]
                after = items[-1]['condition_id']

                async def _one(it):
                    async with sem:
                        try:
                            rows = []
                            for tid in it['token_ids']:
                                book = await api.get_book(tid)
                                if not book:
                                    continue
                                for side in ('bids', 'asks'):
                                    for level in book.get(side) or []:
                                        rows.append({
                                            'token_id': tid,
                                            'side': side.rstrip('s'),
                                            'price': level.get('price'),
                                            'size': level.get('size'),
                                        })
                            if rows:
                                stats['written'] += await db_pg.upsert_orderbook(rows)
                            stats['markets'] += 1
                            await db_pg.mark_scope_done(f'orderbook:{it["condition_id"]}', len(rows))
                        except Exception as exc:
                            stats['failed'] += 1
                            logger.warning('市场 %s 盘口失败: %s', it['condition_id'], exc)

                for i in range(0, len(todo), GATHER_CHUNK):
                    await asyncio.gather(*[_one(it) for it in todo[i:i + GATHER_CHUNK]])
                await db_pg.set_scope_state('orderbook_resume', after)
                pbar.update(len(items))
                pbar.set_postfix(levels=stats['written'], failed=stats['failed'])
                if limit and stats['markets'] >= limit:
                    truncated = True
                    break
    if not truncated:
        await db_pg.mark_scope_done('orderbook_resume', stats['markets'])
    return stats
