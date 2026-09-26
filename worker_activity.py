# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""activity 分布式 worker 核心（事件级任务队列，多实例可同进程并发）

- 任务队列：PG activity_tasks 表（event 级状态机 pending/running/done/failed）
- 领取：claim_activity_task 原子领取（FOR UPDATE SKIP LOCKED），多 worker 并发不重复
- 租约：running 超时自动回收——worker 被强杀后其任务由其他 worker 接管
- 出口：经 --proxy 走独立隧道代理（独立出口 IP → 独立 200 req/10s 限流配额）
- 限流自动切节点（可选）：--clash-base 指向 worker 专用 mihomo 实例的 controller，
  429 累计 3 次/403 → ban 当前节点与出口 IP → 自动切下一节点继续采集

统一入口（推荐，不用直接运行本文件）：
    python poly.py            # 一键：实例 + events + 多 worker（日志 [w1]/[w2] 前缀）
    python poly.py status     # 看状态
单实例调试仍可用：
    python worker_activity.py --proxy http://127.0.0.1:7901 --clash-base http://127.0.0.1:9101 --worker-id w1
    python worker_activity.py --idle-wait 0          # 队列空即退出（一次性模式）

注意：与 main_collect.py --stage activity 单机模式不要混跑（进度表互相隔离，
worker 初始化时会把 scrape_progress 的 done 单向迁移进任务队列）。
"""

import argparse
import asyncio
import contextlib
import contextvars
import logging
import os
import socket
import time
import urllib.parse

from tqdm import tqdm

import config
import clash_pool
import db_pg
import scraper_data
from data_api_client import DataAPIClient, TokenBucket

logger = logging.getLogger('polymarket_worker')

STATS_INTERVAL = 30       # 队列统计打印间隔（秒）
PROXY_CHECK_INTERVAL = 5  # 代理实例端口探测间隔（秒）
DB_BACKOFF_MAX = 30       # DB 瞬时故障退避上限（秒）
ROTATOR_INIT_RETRY = 3    # 轮换器初始化重试次数（实例刚重启时别急着降级为静态代理）

# 当前 worker 标识（poly.py 按它把日志分流到 logs/wN.log；单实例 CLI 下同样有值）
CURRENT_WORKER = contextvars.ContextVar('polymarket_worker_id', default=None)


class _Prefixed(logging.LoggerAdapter):
    """日志消息加 [wid] 前缀，同进程多 worker 时区分来源"""

    def process(self, msg, kwargs):
        return f'[{self.extra["wid"]}] {msg}', kwargs


def _proxy_up(proxy: str) -> bool:
    """本机代理端口探测（poly 启的 mihomo 实例）：实例掉线时暂停领任务，避免把事件烧成死信。

    远程/隧道代理无法端口探测，一律按可用处理（连接级失败交给请求重试与节点轮换）。
    """
    if not proxy:
        return True
    try:
        parts = urllib.parse.urlsplit(proxy)
        host, port = parts.hostname, parts.port
    except ValueError:
        return True
    if not port or host not in ('127.0.0.1', 'localhost', '::1'):
        return True
    with socket.socket() as s:
        s.settimeout(1.0)
        return s.connect_ex((host, port)) == 0


async def run_worker(worker_id: str = None, *, proxy: str = None, clash_base: str = None,
                     clash_secret: str = 'pm-worker', clash_group: str = 'PM',
                     jobs: int = 3, lease_min: int = 15, max_attempts: int = 5,
                     idle_wait: int = 30, rotate_after: int = 150,
                     max_delay: int = 10000, progress: bool = True) -> dict:
    """单 worker 采集循环（供 poly.py 同进程并发多实例，或 CLI 单实例）

    idle_wait=0 时队列空即返回；返回 {'done': 完成事件数, 'rows': 采集行数}
    """
    wid = worker_id or f'{socket.gethostname()}:{os.getpid()}'
    CURRENT_WORKER.set(wid)      # poly.py 按此把日志分流到 logs/wN.log
    log = _Prefixed(logging.getLogger('polymarket_worker'), {'wid': wid})

    # 幂等初始化任务队列（单向迁移 scrape_progress 的 activity done 断点）
    try:
        init = await db_pg.init_activity_tasks()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # DB 瞬时不可用不该拦住启动：poly 每 60s 会重试补队列
        log.warning('任务队列初始化失败（%s: %s），先启动；补队列会自动重试',
                    type(exc).__name__, exc)
        init = {'stats': '(查询失败)'}

    # 节点轮换器：--clash-base 指向 worker 专用 mihomo 实例时启用
    rotator = None
    if clash_base:
        api = clash_pool.ClashAPI(base=clash_base, secret=clash_secret,
                                  group=clash_group, mixed=proxy)
        rotator = clash_pool.AsyncNodeRotator(api, rotate_after=rotate_after,
                                              max_delay_ms=max_delay)
        ok = False
        for attempt in range(1, ROTATOR_INIT_RETRY + 1):
            ok = await rotator.load_nodes()
            if ok or attempt == ROTATOR_INIT_RETRY:
                break
            log.warning('节点轮换初始化失败（第 %d/%d 次），10 秒后重试',
                        attempt, ROTATOR_INIT_RETRY)
            await asyncio.sleep(10)
        if ok:
            await rotator.start_health_check()
        else:
            log.warning('节点轮换不可用（controller 不通/组无节点），退化为静态代理')
            with contextlib.suppress(BaseException):
                await api.close()
            rotator = None

    log.info('worker 启动 (proxy=%s, jobs=%s, rotate=%s) 队列初始: %s',
             proxy or (f'系统代理 {config.SYSTEM_PROXY}' if config.SYSTEM_PROXY else '直连'),
             jobs, f'{len(rotator.nodes)}节点' if rotator else '关', init['stats'])

    bucket = TokenBucket(capacity=config.TRADES_RATE_LIMIT)
    worker_done = 0
    worker_rows = 0
    pbar = tqdm(desc=f'worker {wid}', unit='evt', dynamic_ncols=True) if progress else None
    running = set()          # 进行中的采集协程

    try:
        async with DataAPIClient(
                bucket=bucket, proxy=proxy,
                on_request=rotator.on_request if rotator else None,
                on_rate_limited=rotator.on_rate_limited if rotator else None) as api:

            async def job(eid: str, attempts: int) -> None:
                """领取后的事件采集任务：成功标 done，失败按 attempts 回 pending 或死信"""
                nonlocal worker_done, worker_rows
                try:
                    total, ins, upd = await scraper_data.collect_event_activity(api, eid)
                    await db_pg.complete_activity_task(eid, total)
                    worker_done += 1
                    worker_rows += total
                    if pbar:
                        pbar.update(1)
                        pbar.set_postfix(rows=worker_rows, done=worker_done)
                    log.info('事件 %s 完成: %s 行 (ins=%s upd=%s)', eid, total, ins, upd)
                except Exception as exc:
                    status = await db_pg.fail_activity_task(eid, str(exc), max_attempts)
                    if pbar:
                        pbar.update(1)
                    log.warning('事件 %s 失败 (第%s次→%s): %s', eid, attempts, status, exc)

            next_stats = time.monotonic() + STATS_INTERVAL
            proxy_ok = True              # 代理实例存活门禁（掉线时暂停领新任务）
            proxy_warned = False
            next_proxy_check = 0.0
            db_retry_at = 0.0            # DB 抖动的下次领取时间（退避，不影响在采协程）
            db_retry_delay = 0.0
            while True:
                # 收割已完成协程
                running = {t for t in running if not t.done()}

                # 代理实例端口探测：实例挂了还在领任务，只是把事件白烧成死信
                if time.monotonic() >= next_proxy_check:
                    next_proxy_check = time.monotonic() + PROXY_CHECK_INTERVAL
                    proxy_ok = _proxy_up(proxy)
                    if not proxy_ok and not proxy_warned:
                        log.error('代理实例不可用（%s），暂停领新任务（实例恢复后自动继续）',
                                  proxy)
                        proxy_warned = True
                    elif proxy_ok and proxy_warned:
                        log.info('代理实例已恢复（%s），继续领任务', proxy)
                        proxy_warned = False

                # 补满协程池（领到空即停）；DB 瞬时故障只退避重试，不让 worker 循环退出
                if proxy_ok and time.monotonic() >= db_retry_at:
                    try:
                        while len(running) < jobs:
                            task = await db_pg.claim_activity_task(wid, lease_min)
                            if not task:
                                break
                            log.info('领取事件 %s (第%s次尝试)', task['event_id'], task['attempts'])
                            running.add(asyncio.create_task(
                                job(task['event_id'], task['attempts'])))
                        db_retry_delay = 0.0
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        db_retry_delay = min(max(db_retry_delay * 2, 2.0), DB_BACKOFF_MAX)
                        db_retry_at = time.monotonic() + db_retry_delay
                        log.warning('领取任务失败（DB 抖动？%s: %s），%.0fs 后重试',
                                    type(exc).__name__, exc, db_retry_delay)

                if not running:
                    if idle_wait <= 0:
                        log.info('队列已空，worker 退出 (完成 %s 事件 / %s 行)',
                                 worker_done, worker_rows)
                        break
                    await asyncio.sleep(min(idle_wait, 5))
                    continue

                # 等待任一协程结束（1s 粒度轮询，兼顾统计打印）
                done_set, running = await asyncio.wait(
                    running, timeout=1, return_when=asyncio.FIRST_COMPLETED)
                for t in done_set:
                    t.exception()          # 消费异常避免告警（job 内部已兜底）

                if time.monotonic() >= next_stats:
                    try:
                        stats = await db_pg.activity_task_stats()
                        log.info('队列: %s | 本 worker: %s 事件 / %s 行',
                                 stats, worker_done, worker_rows)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        log.warning('队列统计失败（DB 抖动？%s: %s）', type(exc).__name__, exc)
                    next_stats = time.monotonic() + STATS_INTERVAL
    except asyncio.CancelledError:
        # Ctrl+C：停止领新任务，等在采事件完成后退出；
        # 强杀（二次中断/杀进程）场景由租约超时自动回收，其他 worker 接管
        log.info('收到中断: 停止领取新任务，等待在采事件完成...')
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        log.info('在采事件已全部结束，worker 退出 (共 %s 事件 / %s 行)',
                 worker_done, worker_rows)
        raise
    finally:
        if pbar:
            pbar.close()
        if rotator:
            with contextlib.suppress(BaseException):
                await rotator.stop_health_check()
            with contextlib.suppress(BaseException):
                await rotator.api.close()
    return {'done': worker_done, 'rows': worker_rows}


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
    parser.add_argument('--clash-base', default=None,
                        help='mihomo controller 地址（如 http://127.0.0.1:9101）；'
                             '配置后启用限流自动切节点（需 --proxy 指向同一实例 mixed 端口）')
    parser.add_argument('--clash-secret', default='pm-worker',
                        help='mihomo external-controller secret（默认 pm-worker）')
    parser.add_argument('--clash-group', default='PM',
                        help='mihomo selector 组名（默认 PM）')
    parser.add_argument('--rotate-after', type=int, default=150,
                        help='每 N 次请求主动轮换节点（默认 150；0=仅限流时切换）')
    parser.add_argument('--max-delay', type=int, default=10000,
                        help='节点预筛延迟上限(ms)，超过则跳过（默认 10000）')
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )
    logging.getLogger('httpx').setLevel(logging.WARNING)   # 压掉每请求一行
    if args.jobs < 1:
        raise SystemExit('--jobs 至少为 1')
    try:
        asyncio.run(run_worker(
            args.worker_id, proxy=args.proxy, clash_base=args.clash_base,
            clash_secret=args.clash_secret, clash_group=args.clash_group,
            jobs=args.jobs, lease_min=args.lease_min,
            max_attempts=args.max_attempts, idle_wait=args.idle_wait,
            rotate_after=args.rotate_after, max_delay=args.max_delay))
    except KeyboardInterrupt:
        logger.info('worker 已退出')


if __name__ == '__main__':
    main()
