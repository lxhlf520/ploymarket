# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""交易批量采集 - Phase A 全市场交易 / Phase B 用户活动流

Phase A: 从 condition_ids.txt（58k+ conditionId 种子）逐市场拉 /trades 全量历史
Phase B: 从 PG trades 表取去重后的 proxyWallet，逐用户拉 /activity 全量
         （补 usdcSize/type 及 REDEEM/MERGE 等非 TRADE 活动，与 Phase A 自然键去重）

断点续传：scrape_progress 表按 scope 记录完成状态，中断后重跑自动跳过已完成 scope。
"""

import asyncio
import logging
import os
import time

from tqdm import tqdm

import config
import db_pg
from data_api_client import DataAPIClient, TokenBucket

logger = logging.getLogger(__name__)

BATCH_ROWS = 5000   # 入库批量大小（约 5 页 /trades 或 10 页 /activity）
GATHER_CHUNK = 10000  # 协程分批提交：避免一次性创建数十万协程的内存峰值


class Stats:
    """汇总统计（单线程 asyncio，无需锁）"""

    def __init__(self):
        self.inserted = 0
        self.updated = 0
        self.failures = 0

    def add(self, inserted: int, updated: int) -> None:
        self.inserted += inserted
        self.updated += updated


CONDITION_IDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'condition_ids.txt')


def load_condition_ids() -> list:
    """读取去重 conditionId 种子（condition_ids.txt 纯文本，Phase A 数据源）"""
    seen = set()
    ids = []
    with open(CONDITION_IDS_FILE, encoding='utf-8') as f:
        for line in f:
            cid = line.strip()
            if cid and cid not in seen:
                seen.add(cid)
                ids.append(cid)
    return ids


async def _batch_upsert(page: list, batch: list, stats: Stats) -> None:
    """按 BATCH_ROWS 累计分页数据并批量入库"""
    batch.extend(page)
    if len(batch) >= BATCH_ROWS:
        ins, upd = await db_pg.upsert_trades(batch)
        stats.add(ins, upd)
        batch.clear()


async def _flush_batch(batch: list, stats: Stats) -> None:
    if batch:
        ins, upd = await db_pg.upsert_trades(batch)
        stats.add(ins, upd)
        batch.clear()


async def _scrape_market(api: DataAPIClient, condition_id: str, sem: asyncio.Semaphore,
                         pbar: tqdm, stats: Stats) -> None:
    async with sem:
        batch = []

        async def on_batch(page):
            await _batch_upsert(page, batch, stats)

        try:
            total = await api.fetch_market_trades(condition_id, on_batch=on_batch)
            await _flush_batch(batch, stats)
            await db_pg.mark_scope_done(f'market:{condition_id}', total)
        except Exception as exc:
            stats.failures += 1
            logger.warning('市场 %s 采集失败: %s', condition_id, exc)
        finally:
            pbar.update(1)
            pbar.set_postfix(inserted=stats.inserted, updated=stats.updated, failed=stats.failures)


async def _scrape_user(api: DataAPIClient, wallet: str, sem: asyncio.Semaphore,
                       pbar: tqdm, stats: Stats) -> None:
    async with sem:
        batch = []

        async def on_batch(page):
            await _batch_upsert(page, batch, stats)

        try:
            total = await api.fetch_user_activity(wallet, on_batch=on_batch)
            await _flush_batch(batch, stats)
            await db_pg.mark_scope_done(f'user:{wallet}', total)
        except Exception as exc:
            stats.failures += 1
            logger.warning('用户 %s 采集失败: %s', wallet, exc)
        finally:
            pbar.update(1)
            pbar.set_postfix(inserted=stats.inserted, updated=stats.updated, failed=stats.failures)


async def _run_gathered(workers) -> None:
    """分批 gather 执行协程（控制内存峰值）"""
    for i in range(0, len(workers), GATHER_CHUNK):
        await asyncio.gather(*workers[i:i + GATHER_CHUNK])


async def scrape_market_trades(limit: int = None, concurrency: int = None,
                               only: list = None, refresh: bool = True) -> dict:
    """Phase A: 全市场交易采集（--limit 试跑前 N 个；only 定向指定市场列表）"""
    await db_pg.init_schema()
    cids = load_condition_ids()
    done = await db_pg.get_done_scopes('market:')
    if only:
        # 定向采集（如对账验证）：忽略完成状态，upsert 幂等可重跑
        todo = [only] if isinstance(only, str) else list(only)
    else:
        todo = [c for c in cids if f'market:{c}' not in done]
        if limit:
            todo = todo[:limit]
    logger.info('Phase A: 共 %s 个市场，已完成 %s，待采集 %s', len(cids), len(done), len(todo))
    if not todo:
        return {'markets': 0}

    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)
    stats = Stats()
    async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
        with tqdm(total=len(todo), desc='markets', unit='mkt') as pbar:
            await _run_gathered([
                _scrape_market(api, cid, sem, pbar, stats) for cid in todo
            ])
    if refresh:
        await db_pg.refresh_users()
    return {'markets': len(todo), **vars(stats)}


async def scrape_user_activity(limit: int = None, concurrency: int = None,
                               refresh: bool = True) -> dict:
    """Phase B: 用户活动流回补（钱包来自 PG trades 去重，可 --limit 试跑前 N 个）"""
    await db_pg.init_schema()
    wallets = await db_pg.get_pending_wallets(10_000_000)
    if limit:
        wallets = wallets[:limit]
    logger.info('Phase B: 待采集用户 %s 个', len(wallets))
    if not wallets:
        return {'users': 0}

    sem = asyncio.Semaphore(concurrency or config.TRADES_CONCURRENCY)
    stats = Stats()
    async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
        with tqdm(total=len(wallets), desc='users', unit='usr') as pbar:
            await _run_gathered([
                _scrape_user(api, w, sem, pbar, stats) for w in wallets
            ])
    if refresh:
        await db_pg.refresh_users()
    return {'users': len(wallets), **vars(stats)}


async def run_phases(market_limit: int = None, user_limit: int = None,
                     concurrency: int = None) -> None:
    """先市场后用户，串联执行"""
    t0 = time.time()
    r1 = await scrape_market_trades(limit=market_limit, concurrency=concurrency)
    logger.info('Phase A 完成: %s', r1)
    r2 = await scrape_user_activity(limit=user_limit, concurrency=concurrency)
    logger.info('Phase B 完成: %s', r2)
    logger.info('总耗时 %.1f 分钟', (time.time() - t0) / 60)
    logger.info('库汇总: %s', await db_pg.get_stats())
