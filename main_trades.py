# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""交易采集 CLI 编排入口

各阶段说明：
- initdb   建库建表（幂等）
- markets  Phase A：全市场 /trades 采集（--limit 试跑前 N 个，--market 定向单市场）
- users    Phase B：用户 /activity 回补（--limit 试跑前 N 个）
- enrich   Phase C：Polygon RPC 链上回填（--limit 限制处理哈希数，--batch 每批大小）
- all      依次执行 markets → users → enrich
- verify   链上回填覆盖度统计（不发起请求）
- stats    库汇总统计
--dry-run  仅统计预估请求数（抽样单页探测后外推下界），不采集不写库

用法示例：
    python main_trades.py --stage initdb
    python main_trades.py --stage markets --limit 5
    python main_trades.py --stage users --limit 2
    python main_trades.py --stage enrich --batch 500 --limit 5000
    python main_trades.py --stage all --dry-run
"""

import argparse
import asyncio
import json
import logging
import sys
import time

import config
import db_pg
import scraper_trades
import scraper_tx_enrich
from data_api_client import DataAPIClient, TokenBucket

logger = logging.getLogger('polymarket_trades')


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )
    # 第三方库日志降噪
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)


async def cmd_initdb() -> dict:
    """建库建表（幂等）"""
    db_pg.ensure_database()
    await db_pg.init_schema()
    return await db_pg.get_stats()


async def cmd_verify() -> dict:
    """链上回填覆盖度"""
    return await scraper_tx_enrich.verify()


async def _probe_pages(api: DataAPIClient, path: str, page_size: int,
                       params_fn, scopes: list) -> float:
    """对 scopes 抽样拉取首页，估算平均页数（下界：首页满页记 2 页起）"""
    pages = 0
    ok = 0
    for scope in scopes:
        try:
            page = await api.probe_page(path, params_fn(scope))
        except Exception as exc:
            logger.warning('探测 %s 失败: %s', scope, exc)
            continue
        ok += 1
        pages += 1 + (1 if len(page) >= page_size else 0)
    return (pages / ok) if ok else 1.0


async def dry_run_markets(probe: int) -> dict:
    await db_pg.init_schema()
    cids = scraper_trades.load_condition_ids()
    done = await db_pg.get_done_scopes('market:')
    todo = [c for c in cids if f'market:{c}' not in done]
    n = len(todo)
    if n == 0:
        return {'待采集市场': 0, '预估请求数': 0}
    avg_pages = 1.0
    probed = 0
    if probe > 0:
        async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
            avg_pages = await _probe_pages(
                api, '/trades', config.TRADES_PAGE_SIZE,
                lambda cid: {'market': cid, 'limit': config.TRADES_PAGE_SIZE, 'offset': 0},
                todo[:probe],
            )
        probed = min(probe, n)
    return {
        '待采集市场': n,
        '抽样数': probed,
        '样本平均页数(下界)': round(avg_pages, 2),
        '最低请求数': n,
        '预估请求数(下界)': int(n * avg_pages),
    }


async def dry_run_users(probe: int) -> dict:
    await db_pg.init_schema()
    n = await db_pg.count_pending_wallets()
    if n == 0:
        return {'待采集用户': 0, '预估请求数': 0}
    avg_pages = 1.0
    probed = 0
    if probe > 0:
        wallets = await db_pg.get_pending_wallets(probe)
        async with DataAPIClient(bucket=TokenBucket(capacity=config.TRADES_RATE_LIMIT)) as api:
            avg_pages = await _probe_pages(
                api, '/activity', config.ACTIVITY_PAGE_SIZE,
                lambda w: {'user': w, 'limit': config.ACTIVITY_PAGE_SIZE, 'offset': 0},
                wallets,
            )
        probed = len(wallets)
    return {
        '待采集用户': n,
        '抽样数': probed,
        '样本平均页数(下界)': round(avg_pages, 2),
        '最低请求数': n,
        '预估请求数(下界)': int(n * avg_pages),
    }


async def dry_run_enrich() -> dict:
    await db_pg.init_schema()
    pending = await db_pg.count_pending_tx_hashes()
    return {
        '未回填哈希数': pending,
        # 每个哈希 1 次收据 + 约 1 次区块查询（blocks 缓存命中后更低）
        '预估请求数(下界)': pending * 2,
    }


async def cmd_dry_run(stage: str, probe: int) -> None:
    parts = []
    if stage in ('markets', 'all'):
        parts.append(('Phase A 市场', await dry_run_markets(probe)))
    if stage in ('users', 'all'):
        parts.append(('Phase B 用户', await dry_run_users(probe)))
    if stage in ('enrich', 'all'):
        parts.append(('Phase C 回填', await dry_run_enrich()))
    for name, d in parts:
        print(f'{name}:')
        for k, v in d.items():
            print(f'  {k}: {v}')
    if stage == 'all' and parts:
        total = sum(d.get('预估请求数(下界)', 0) for _, d in parts)
        print(f'合计预估请求数(下界): {total}')


async def amain(args: argparse.Namespace) -> None:
    if args.stage == 'initdb':
        print(json.dumps(await cmd_initdb(), ensure_ascii=False, indent=2))
        return
    if args.dry_run:
        await cmd_dry_run(args.stage, args.probe)
        return
    if args.stage == 'verify':
        print(json.dumps(await cmd_verify(), ensure_ascii=False, indent=2))
        return
    if args.stage == 'stats':
        await db_pg.init_schema()
        print(json.dumps(await db_pg.get_stats(), ensure_ascii=False, indent=2))
        return

    t0 = time.time()
    if args.stage in ('markets', 'all'):
        r = await scraper_trades.scrape_market_trades(
            limit=args.limit, concurrency=args.concurrency, only=args.market,
            refresh=not args.skip_refresh_users)
        logger.info('Phase A 结果: %s', r)
    if args.stage in ('users', 'all'):
        r = await scraper_trades.scrape_user_activity(
            limit=args.limit, concurrency=args.concurrency,
            refresh=not args.skip_refresh_users)
        logger.info('Phase B 结果: %s', r)
    if args.stage in ('enrich', 'all'):
        r = await scraper_tx_enrich.enrich_pending_txs(limit=args.limit, batch_size=args.batch)
        logger.info('Phase C 结果: %s', r)
    logger.info('库汇总: %s', await db_pg.get_stats())
    logger.info('总耗时 %.1f 秒', time.time() - t0)


def main() -> None:
    parser = argparse.ArgumentParser(description='Polymarket 交易采集（Data API + Polygon RPC 回填）')
    parser.add_argument('--stage',
                        choices=['initdb', 'markets', 'users', 'enrich', 'all', 'verify', 'stats'],
                        default='all', help='执行阶段（默认 all）')
    parser.add_argument('--limit', type=int, default=None,
                        help='试跑限制：markets/users 前 N 个 scope，enrich 前 N 个哈希')
    parser.add_argument('--market', default=None,
                        help='仅采集指定 conditionId 的市场（忽略进度状态，幂等可重跑）')
    parser.add_argument('--concurrency', type=int, default=None,
                        help='采集并发数（默认 config.TRADES_CONCURRENCY）')
    parser.add_argument('--batch', type=int, default=None,
                        help='enrich 每批哈希数（默认 config.TX_ENRICH_BATCH）')
    parser.add_argument('--dry-run', action='store_true', help='仅统计预估请求数，不采集')
    parser.add_argument('--skip-refresh-users', action='store_true',
                        help='阶段结束后跳过 users 聚合刷新（大数据量时最后统一跑一次）')
    parser.add_argument('--probe', type=int, default=10,
                        help='--dry-run 抽样探测数（0 表示不探测）')
    parser.add_argument('-v', '--verbose', action='store_true', help='DEBUG 日志')
    args = parser.parse_args()

    setup_logging(args.verbose)
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        logger.warning('用户中断：已采集部分通过 scrape_progress 保留，可重跑续传')
        sys.exit(130)
    except Exception:
        logger.exception('执行失败')
        sys.exit(1)


if __name__ == '__main__':
    main()
