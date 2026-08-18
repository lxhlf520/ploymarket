# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Polymarket 全模块采集 CLI 统一入口（Gamma / Data API / CLOB）

各阶段说明：
- initdb         建库建表（幂等）
- tags           分类清单（不入库，驱动 keyset）
- events         事件 + 嵌套市场（tag_slug × closed 组合，offset 翻页）
- details        存量市场详情补拉（缺 clobTokenIds/description，按 slug 单查）
- clarifications Rules 澄清（按市场数字 id）
- comments       评论全量（需登录态，先 export_cookie.py；--event 定向单事件）
- holders        Top Holders（每市场）
- positions      持仓（每市场，status=ALL）
- activity       事件 CASH 交易（每事件，行补 event_id）
- prices         价格历史（--fidelity 720 日级近 1 月 / 10 近 24h）
- orderbook      盘口深度快照（GET /book）
- all            按依赖顺序依次执行：events → details → clarifications → comments
                 → holders → positions → activity → prices → orderbook
- stats          库汇总统计

用法示例：
    python main_collect.py --stage initdb
    python main_collect.py --stage events --limit 2
    python main_collect.py --stage comments --min-comments 1 --limit 10
    python main_collect.py --stage prices --fidelity 10 --limit 5
    python main_collect.py --stage all
"""

import argparse
import asyncio
import json
import logging
import sys
import time

import db_pg
import scraper_clob
import scraper_data
import scraper_gamma

logger = logging.getLogger('polymarket_collect')

ALL_STAGES = ['events', 'details', 'clarifications', 'comments',
              'holders', 'positions', 'activity', 'prices', 'orderbook']


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)


async def cmd_initdb() -> dict:
    db_pg.ensure_database()
    await db_pg.init_schema()
    return await db_pg.get_stats()


async def cmd_stats() -> dict:
    await db_pg.init_schema()
    async def _do(conn):
        return dict(await conn.fetchrow(
            """
            SELECT
                (SELECT count(*) FROM events) AS events,
                (SELECT count(*) FROM markets) AS markets,
                (SELECT count(*) FROM market_clarifications) AS clarifications,
                (SELECT count(*) FROM comments) AS comments,
                (SELECT count(*) FROM holders) AS holders,
                (SELECT count(*) FROM positions) AS positions,
                (SELECT count(*) FROM price_history) AS price_points,
                (SELECT count(*) FROM orderbook) AS orderbook_levels,
                (SELECT count(*) FROM trades) AS trades
            """
        ))
    return await db_pg.execute_with_retry(_do)


async def amain(args: argparse.Namespace) -> None:
    if args.stage == 'initdb':
        print(json.dumps(await cmd_initdb(), ensure_ascii=False, indent=2))
        return
    if args.stage == 'stats':
        print(json.dumps(await cmd_stats(), ensure_ascii=False, indent=2))
        return

    t0 = time.time()
    results = {}
    if args.stage in ('tags', 'events', 'details', 'clarifications', 'comments'):
        results[args.stage] = await scraper_gamma.run_stage(
            args.stage, limit=args.limit,
            tag_slug=args.tag, closed=args.closed, refresh_all=args.refresh_all,
            event_id=args.event, min_comments=args.min_comments)
    if args.stage in ('holders', 'positions', 'activity'):
        fn = {'holders': scraper_data.scrape_holders,
              'positions': scraper_data.scrape_positions,
              'activity': scraper_data.scrape_activity}[args.stage]
        results[args.stage] = await fn(limit=args.limit, concurrency=args.concurrency,
                                       only=args.only)
        if args.stage == 'activity':
            # activity 写入 trades 后聚合刷新 users 表
            results['users'] = await db_pg.refresh_users()
    if args.stage in ('prices', 'orderbook'):
        fn = {'prices': scraper_clob.scrape_prices,
              'orderbook': scraper_clob.scrape_orderbook}[args.stage]
        kw = {'limit': args.limit, 'concurrency': args.concurrency}
        if args.stage == 'prices':
            kw['fidelity'] = args.fidelity
        results[args.stage] = await fn(**kw)
    if args.stage == 'all':
        for stage in ALL_STAGES:
            logger.info('===== 阶段 %s =====', stage)
            if stage in ('events', 'details', 'clarifications', 'comments'):
                r = await scraper_gamma.run_stage(
                    stage, limit=args.limit,
                    tag_slug=args.tag, closed=args.closed, refresh_all=args.refresh_all,
                    event_id=args.event, min_comments=args.min_comments)
            elif stage in ('holders', 'positions', 'activity'):
                fn = {'holders': scraper_data.scrape_holders,
                      'positions': scraper_data.scrape_positions,
                      'activity': scraper_data.scrape_activity}[stage]
                r = await fn(limit=args.limit, concurrency=args.concurrency, only=args.only)
                if stage == 'activity':
                    # activity 写入 trades 后聚合刷新 users 表
                    results['users'] = await db_pg.refresh_users()
            else:
                fn = {'prices': scraper_clob.scrape_prices,
                      'orderbook': scraper_clob.scrape_orderbook}[stage]
                kw = {'limit': args.limit, 'concurrency': args.concurrency}
                if stage == 'prices':
                    kw['fidelity'] = args.fidelity
                r = await fn(**kw)
            results[stage] = r
            logger.info('阶段 %s 结果: %s', stage, r)

    for stage, r in results.items():
        logger.info('%s 结果: %s', stage, r)
    logger.info('库汇总: %s', await cmd_stats())
    logger.info('总耗时 %.1f 秒', time.time() - t0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Polymarket 全模块采集（Gamma / Data API / CLOB）')
    parser.add_argument('--stage',
                        choices=['initdb', 'stats', 'all'] + ALL_STAGES + ['tags'],
                        default='all', help='执行阶段（默认 all）')
    parser.add_argument('--limit', type=int, default=None,
                        help='试跑限制：各阶段处理前 N 个 scope')
    parser.add_argument('--only', default=None,
                        help='定向采集：holders/positions 传 conditionId，activity 传 eventId（可逗号分隔）')
    parser.add_argument('--event', default=None,
                        help='comments 定向单事件 id')
    parser.add_argument('--tag', default=None,
                        help='events 定向单分类 slug')
    parser.add_argument('--closed', action='store_true',
                        help='events 仅采集 closed=true 桶（默认两个桶都采）')
    parser.add_argument('--refresh-all', action='store_true',
                        help='details 遍历全量市场刷新（默认只补缺详情）')
    parser.add_argument('--min-comments', type=int, default=0,
                        help='comments 事件过滤：comment_count 下限')
    parser.add_argument('--fidelity', type=int, default=None,
                        help='prices 粒度：720 日级近 1 月 / 10 近 24h（默认 config.PRICE_FIDELITY_DAILY）')
    parser.add_argument('--concurrency', type=int, default=None,
                        help='采集并发数（默认 config.TRADES_CONCURRENCY）')
    parser.add_argument('-v', '--verbose', action='store_true', help='DEBUG 日志')
    args = parser.parse_args()

    if args.only:
        args.only = [s.strip() for s in args.only.split(',') if s.strip()]

    setup_logging(args.verbose)
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        logger.warning('用户中断：进度已写入 scrape_progress，可重跑续传')
        sys.exit(130)
    except Exception:
        logger.exception('执行失败')
        sys.exit(1)


if __name__ == '__main__':
    main()
