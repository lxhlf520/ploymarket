# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""activity 分布式 worker（单机多进程，每进程独立隧道代理出口）

- 任务队列：PG activity_tasks 表（event 级状态机 pending/running/done/failed）
- 领取：claim_activity_task 原子领取（FOR UPDATE SKIP LOCKED），多 worker 并发不重复
- 租约：running 超时自动回收——worker 被强杀后其任务由其他 worker 接管
- 出口：每进程经 --proxy 走独立隧道代理（独立出口 IP → 独立 200 req/10s 限流配额）

用法（多开 PowerShell 窗口，各带不同代理端口）：
    python worker_activity.py --proxy http://127.0.0.1:7890
    python worker_activity.py --proxy http://127.0.0.1:7891 --worker-id w2 --jobs 3
    python worker_activity.py --idle-wait 0          # 队列空即退出（一次性模式）

注意：与 main_collect.py --stage activity 单机模式不要混跑（进度表互相隔离，
worker 初始化时会把 scrape_progress 的 done 单向迁移进任务队列）。
"""

import argparse
import asyncio
import logging
import os
import socket
import time

from tqdm import tqdm

import config
import db_pg
import scraper_data
from data_api_client import DataAPIClient, TokenBucket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('polymarket_worker')

STATS_INTERVAL = 30  # 队列统计打印间隔（秒）


def parse_args():
    parser = argparse.ArgumentParser(description='activity 分布式 worker（event 级任务队列）')
    parser.add_argument('--proxy', default=None,
                        help='本进程出口代理（http://user:pass@host:port；不填=直连）')
    parser.add_argument('--worker-id', default=None,
                        help='worker 标识（默认 hostname:pid）')
    parser.add_argument('--jobs', type=int, default=3,
                        help='同时处理的 event 数（默认 3，打满单 IP 200 req/10s 配额）')
    parser.add_argument('--lease-min', type=int, default=15,
                        help='任务租约时长（分钟，默认 15；超时未完成自动回收）')
    parser.add_argument('--max-attempts', type=int, default=5,
                        help='单事件最大尝试次数，超过标 failed 死信（默认 5）')
    parser.add_argument('--idle-wait', type=int, default=30,
                        help='队列空时轮询间隔秒（默认 30；0=队列空即退出）')
    return parser.parse_args()


async def amain(args):
    # 幂等初始化任务队列（单向迁移 scrape_progress 的 activity done 断点）
    init = await db_pg.init_activity_tasks()
    logger.info('worker %s 启动 (proxy=%s, jobs=%s) 队列初始: %s',
                args.worker_id, args.proxy or '直连', args.jobs, init['stats'])

    bucket = TokenBucket(capacity=config.TRADES_RATE_LIMIT)
    worker_done = 0
    worker_rows = 0
    pbar = tqdm(desc=f'worker {args.worker_id}', unit='evt', dynamic_ncols=True)

    async with DataAPIClient(bucket=bucket, proxy=args.proxy) as api:

        async def job(eid: str, attempts: int) -> None:
            """领取后的事件采集任务：成功标 done，失败按 attempts 回 pending 或死信"""
            nonlocal worker_done, worker_rows
            try:
                total, ins, upd = await scraper_data.collect_event_activity(api, eid)
                await db_pg.complete_activity_task(eid, total)
                worker_done += 1
                worker_rows += total
                pbar.update(1)
                pbar.set_postfix(rows=worker_rows, done=worker_done)
                logger.info('事件 %s 完成: %s 行 (ins=%s upd=%s)', eid, total, ins, upd)
            except Exception as exc:
                status = await db_pg.fail_activity_task(eid, str(exc), args.max_attempts)
                pbar.update(1)
                logger.warning('事件 %s 失败 (第%s次→%s): %s', eid, attempts, status, exc)

        running = set()          # 进行中的采集协程
        next_stats = time.monotonic() + STATS_INTERVAL
        try:
            while True:
                # 收割已完成协程
                running = {t for t in running if not t.done()}
                # 补满协程池（领到空即停）
                while len(running) < args.jobs:
                    task = await db_pg.claim_activity_task(args.worker_id, args.lease_min)
                    if not task:
                        break
                    logger.info('领取事件 %s (第%s次尝试)', task['event_id'], task['attempts'])
                    running.add(asyncio.create_task(
                        job(task['event_id'], task['attempts'])))

                if not running:
                    if args.idle_wait <= 0:
                        logger.info('队列已空，worker 退出 (本进程完成 %s 事件 / %s 行)',
                                    worker_done, worker_rows)
                        break
                    await asyncio.sleep(min(args.idle_wait, 5))
                    continue

                # 等待任一协程结束（1s 粒度轮询，兼顾统计打印）
                done_set, running = await asyncio.wait(
                    running, timeout=1, return_when=asyncio.FIRST_COMPLETED)
                for t in done_set:
                    t.exception()          # 消费异常避免告警（job 内部已兜底）

                if time.monotonic() >= next_stats:
                    stats = await db_pg.activity_task_stats()
                    logger.info('队列: %s | 本 worker: %s 事件 / %s 行',
                                stats, worker_done, worker_rows)
                    next_stats = time.monotonic() + STATS_INTERVAL
        except asyncio.CancelledError:
            # Ctrl+C：停止领新任务，等在采事件完成后退出；
            # 强杀（二次中断/杀进程）场景由租约超时自动回收，其他 worker 接管
            logger.info('收到中断: 停止领取新任务，等待在采事件完成...')
            if running:
                await asyncio.gather(*running, return_exceptions=True)
            logger.info('在采事件已全部结束，worker 退出 (共 %s 事件 / %s 行)',
                        worker_done, worker_rows)
            raise


def main():
    args = parse_args()
    if not args.worker_id:
        args.worker_id = f'{socket.gethostname()}:{os.getpid()}'
    if args.jobs < 1:
        raise SystemExit('--jobs 至少为 1')
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        logger.info('worker 已退出')


if __name__ == '__main__':
    main()
