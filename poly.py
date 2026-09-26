# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Polymarket 采集系统统一入口（实例 + 数据 + worker 一条命令）

    python poly.py                 # run（默认）：一键跑起来，幂等可反复跑
    python poly.py status          # 状态：DB 统计 / 实例端口 / worker 进程
    python poly.py stop            # 停止全部 mihomo 实例

run 流程（每步先检查状态再动作，重复跑不会重复起）：
    [1/5] 建库建表      main_collect.py --stage initdb
    [2/5] 代理池配置    clash_worker/w1..wN.yaml（缺失/无节点/内核未就位则自动生成）
    [3/5] 实例          后台无窗口启动 mihomo（日志 clash_worker/mihomo-wN.log），已运行的复用
    [4/5] 事件数据      events 表为空则自动采集（经 w1 实例代理；--collect-events 强制）
    [5/5] worker        同进程并发 N 个（日志 [w1]/[w2] 前缀），后台每 60s 自动补任务队列

运行后 Ctrl+C 一次全停（worker + 本次启动的 mihomo 实例）；被强杀时用
python poly.py stop 清理残留实例。

常用参数（run）：
    --n 3                   实例数 = worker 数（默认 3）
    --jobs 3                每 worker 并发事件数（默认 3）
    --collect-events [N]    强制采集 events（可跟数量先小量试跑；不带数字=全量补采）
    --no-events             跳过事件检查（确认队列有数据时）
    --dry-run               只检查并打印计划，不执行任何动作
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
import socket
import subprocess
import sys
import time

import asyncpg
import yaml

import config
import db_pg
import make_worker_clash as mwc
import worker_activity

BASE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
OUT_DIR = os.path.join(BASE, 'clash_worker')
PID_FILE = os.path.join(OUT_DIR, '.poly_pids.json')

MIXED_START = mwc.DEFAULT_MIXED_START   # 7901
CTL_START = mwc.DEFAULT_CTL_START       # 9101
SECRET = mwc.DEFAULT_SECRET
GROUP = mwc.PM_GROUP
EVENTS_PROXY = f'http://127.0.0.1:{MIXED_START}'
WORKERS = 3
JOBS = 3
REFILL_INTERVAL = 60    # 自动补任务队列间隔（秒）

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S')
logging.getLogger('httpx').setLevel(logging.WARNING)   # 压掉每请求一行（节点延迟预筛会产生数百行）
logger = logging.getLogger('poly')

_STARTED_PROCS: list = []   # 本次 poly 启动的 mihomo 进程（退出时停止）


def log(msg: str) -> None:
    logger.info('[poly] %s', msg)


# ==================== 通用工具 ====================

def port_open(port: int, host: str = '127.0.0.1') -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def _pid_alive(pid: int) -> bool:
    """tasklist 校验 pid 是否存活（Python 无跨平台 API，Windows 用 tasklist）"""
    try:
        out = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'],
                             capture_output=True, text=True, errors='replace',
                             creationflags=subprocess.CREATE_NO_WINDOW)
        return str(pid) in (out.stdout or '')
    except Exception:
        return False


def _kill_pid(pid: int) -> None:
    subprocess.call(['taskkill', '/PID', str(pid), '/T', '/F'],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW)


def _load_pids() -> dict:
    try:
        with open(PID_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_pids(data: dict) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(PID_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _clear_pid_file_if_own() -> None:
    """本次 poly 正常退出（实例已停止）时移除记录；被强杀则保留供 stop 清理"""
    data = _load_pids()
    if data.get('poly_pid') == os.getpid() and os.path.isfile(PID_FILE):
        os.remove(PID_FILE)


def _run(cmd: list, env: dict = None) -> int:
    """同步子进程（输出直接继承到当前控制台）"""
    return subprocess.call(cmd, cwd=BASE, env=env)


def _stop_started() -> None:
    """停止本次 poly 启动的 mihomo 实例（复用的不属于本次，不动）"""
    for proc in _STARTED_PROCS:
        if proc.poll() is None:
            proc.terminate()
    for proc in _STARTED_PROCS:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


# ==================== run 各步骤 ====================

def _check_not_running() -> None:
    data = _load_pids()
    pid = data.get('poly_pid')
    if pid and pid != os.getpid() and _pid_alive(pid):
        raise SystemExit(f'已有 poly 在运行（pid {pid}）。若确认已退出，先执行: python poly.py stop')


def step_initdb(dry: bool) -> None:
    log('[1/5] 建库建表（initdb，幂等）')
    if dry:
        log('      → python main_collect.py --stage initdb')
        return
    if _run([PY, 'main_collect.py', '--stage', 'initdb']) != 0:
        raise SystemExit('initdb 失败：检查 .env 数据库配置（PG_HOST / PG_USER / PG_PASSWORD）')


def step_configs(n: int, dry: bool):
    """检查/生成 w1..wN.yaml；返回 (core, 是否就绪)"""
    log(f'[2/5] 代理池配置检查（clash_worker/w1..w{n}.yaml）')
    core = mwc.find_core()
    why = ''
    if not core:
        why = '未探测到 mihomo 内核（可放 mihomo*.zip 到项目目录自动解压）'
    else:
        for i in range(1, n + 1):
            if not os.path.isfile(os.path.join(OUT_DIR, f'w{i}.yaml')):
                why = f'w{i}.yaml 缺失'
                break
    if not why:
        try:
            with open(os.path.join(OUT_DIR, 'w1.yaml'), encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
            if not cfg.get('proxies'):
                why = 'w1.yaml 无节点'
            elif cfg.get('mixed-port') != MIXED_START:
                why = f'w1.yaml mixed 端口变化（{cfg.get("mixed-port")} → {MIXED_START}）'
        except Exception as exc:
            why = f'w1.yaml 读取失败（{exc}）'
    if not why:
        log(f'      配置有效（{n} 个实例，内核 {core}），跳过生成')
        return core, True
    log(f'      {why} → 重新生成')
    if dry:
        log(f'      → python make_worker_clash.py --n {n}')
        return core, False
    info = mwc.build_instances(n, log=lambda m: log(f'      {m}'))
    if not info['core']:
        raise SystemExit('未找到 mihomo 内核：把 mihomo.exe 放到 clash_worker/ 或 mihomo_core/，'
                         '或把 mihomo*.zip 放到项目目录（自动解压），详见 README')
    return info['core'], True


def start_instances(n: int, core: str, dry: bool) -> list:
    """确保 n 个实例运行：已监听的复用，缺的后台启动；返回实例信息列表"""
    log(f'[3/5] 实例检查（mixed {MIXED_START}-{MIXED_START + n - 1}）')
    old_pids = {x.get('i'): x.get('pid') for x in (_load_pids().get('instances') or [])}
    inst = []
    for i in range(1, n + 1):
        mixed = MIXED_START + i - 1
        ctl = CTL_START + i - 1
        if port_open(mixed):
            inst.append({'i': i, 'mixed': mixed, 'ctl': ctl,
                         'pid': old_pids.get(i), 'state': 'running'})
            continue
        if dry:
            inst.append({'i': i, 'mixed': mixed, 'ctl': ctl, 'pid': None, 'state': 'planned'})
            continue
        logf = open(os.path.join(OUT_DIR, f'mihomo-w{i}.log'), 'ab')
        proc = subprocess.Popen([core, '-f', f'w{i}.yaml'], cwd=OUT_DIR,
                                stdout=logf, stderr=subprocess.STDOUT,
                                creationflags=subprocess.CREATE_NO_WINDOW)
        _STARTED_PROCS.append(proc)
        inst.append({'i': i, 'mixed': mixed, 'ctl': ctl, 'pid': proc.pid, 'state': 'starting'})
    if dry:
        for x in inst:
            log(f'      w{x["i"]}: mixed {x["mixed"]} {"运行中（复用）" if x["state"] == "running" else "将启动"}')
        return inst
    # 等待就绪（最多 20s）
    deadline = time.time() + 20
    while time.time() < deadline and any(not port_open(x['mixed']) for x in inst):
        time.sleep(0.5)
    for x in inst:
        x['state'] = 'running' if port_open(x['mixed']) else 'failed'
    ready = [x for x in inst if x['state'] == 'running']
    log(f'      就绪 {len(ready)}/{n}' + (f'：{[x["mixed"] for x in ready]}' if ready else ''))
    for x in inst:
        if x['state'] == 'failed':
            log(f'      警告: w{x["i"]} (mixed {x["mixed"]}) 启动失败，'
                f'查看 clash_worker/mihomo-w{x["i"]}.log')
    _save_pids({'poly_pid': os.getpid(), 'updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'instances': [{'i': x['i'], 'mixed': x['mixed'], 'ctl': x['ctl'],
                               'pid': x['pid']} for x in inst if x['state'] == 'running']})
    return inst


def _planned(n: int) -> list:
    """dry-run 用的假实例列表（只打印计划）"""
    return [{'i': i, 'mixed': MIXED_START + i - 1, 'ctl': CTL_START + i - 1,
             'pid': None, 'state': 'planned'} for i in range(1, n + 1)]


async def _count_events() -> int:
    """独立连接查询（不碰 worker 的全局连接池：asyncpg 池绑定事件循环，
    在 asyncio.run 的临时循环里创建会污染后续 worker 的循环）"""
    conn = await asyncpg.connect(config.PG_DSN)
    try:
        return await conn.fetchval('SELECT count(*) FROM events')
    finally:
        await conn.close()


def step_events(dry: bool, no_events: bool, force: int = None) -> None:
    log('[4/5] 事件数据检查（worker 任务队列的数据源）')
    if no_events:
        log('      --no-events，跳过')
        return
    try:
        cnt = asyncio.run(_count_events())
    except Exception as exc:
        log(f'      查询 events 失败（{type(exc).__name__}: {exc}）→ 请先完成 [1/5] 建库')
        return
    log(f'      events 表 {cnt} 行')
    if cnt and force is None:
        log('      已非空，跳过（如需补全/更新: python poly.py --collect-events）')
        return
    lim = force or 0        # None/0 → 全量；N>0 → 只采 N 个
    cmd = [PY, 'main_collect.py', '--stage', 'events'] + (['--limit', str(lim)] if lim else [])
    shown = ' '.join(cmd[1:])
    if dry:
        log(f'      → {shown}')
        return
    if cnt:
        log(f'      --collect-events 指定 → 强制采集（{shown}）')
    else:
        log(f'      events 为空 → 自动采集（{shown}；可能较久，Ctrl+C 可中断、重跑续采）')
    env = dict(os.environ)
    env.setdefault('HTTPS_PROXY', EVENTS_PROXY)
    env.setdefault('HTTP_PROXY', EVENTS_PROXY)
    if _run(cmd, env=env) != 0:
        log('      警告: events 采集未正常结束（重跑 python poly.py 会续采）')


async def _refill_loop() -> None:
    """周期把 events 表新增事件灌入任务队列（幂等）——events 补采后 worker 无需重启"""
    while True:
        await asyncio.sleep(REFILL_INTERVAL)
        try:
            res = await db_pg.init_activity_tasks()
            if res.get('queued'):
                log(f'自动补队列: 新增 {res["queued"]} 个任务')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning('[poly] 自动补队列失败（稍后重试）: %s', exc)


async def _duration_stop(duration: int, tasks: list) -> None:
    """--duration：到点后取消全部 worker（走与 Ctrl+C 相同的优雅停止路径）"""
    await asyncio.sleep(duration)
    log(f'--duration {duration}s 到，停止 worker...')
    for t in tasks:
        t.cancel()


async def _workers_main(ports: list, jobs: int, duration: int = 0) -> None:
    tasks = [asyncio.create_task(worker_activity.run_worker(
                 f'w{i}', proxy=f'http://127.0.0.1:{mixed}',
                 clash_base=f'http://127.0.0.1:{ctl}',
                 clash_secret=SECRET, clash_group=GROUP,
                 jobs=jobs, progress=False))
             for i, mixed, ctl in ports]
    refill = asyncio.create_task(_refill_loop())
    timer = asyncio.create_task(_duration_stop(duration, tasks)) if duration > 0 else None
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (i, _m, _c), r in zip(ports, results):
            if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                log(f'      w{i} 异常退出: {type(r).__name__}: {r}')
    finally:
        for task in (refill, timer):
            if task is not None:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task


def step_workers(inst: list, jobs: int, dry: bool, duration: int = 0) -> None:
    ports = [(x['i'], x['mixed'], x['ctl']) for x in inst if x['state'] in ('running', 'planned')]
    log(f'[5/5] 启动 {len(ports)} 个 worker（同进程并发，日志 [w1]/[w2] 前缀）')
    for i, mixed, ctl in ports:
        log(f'      → w{i}: proxy 127.0.0.1:{mixed} controller 127.0.0.1:{ctl} jobs={jobs}')
    log(f'      每 {REFILL_INTERVAL}s 自动补任务队列（events 采完无需重启 worker）')
    if duration > 0:
        log(f'      --duration {duration}s：到点自动优雅停止')
    log('      Ctrl+C 一次全停（worker + 本次启动的 mihomo 实例）')
    if dry:
        return
    if not ports:
        log('      无可用实例，worker 未启动')
        return
    log('      提示: 若之前在别的窗口手动起过 worker，先关掉它们（避免重复领任务）')
    asyncio.run(_workers_main(ports, jobs, duration))


def cmd_run(args) -> None:
    t0 = time.time()
    _check_not_running()
    step_initdb(args.dry_run)
    core, ready = step_configs(args.n, args.dry_run)
    if ready:
        inst = start_instances(args.n, core, args.dry_run)
    else:
        inst = _planned(args.n) if args.dry_run else []
    step_events(args.dry_run, args.no_events, args.collect_events)
    if not args.dry_run:
        log(f'准备就绪（{time.time() - t0:.1f}s）')
    step_workers(inst, args.jobs, args.dry_run, args.duration)
    if args.dry_run:
        log('（dry-run：以上为将执行的动作）')


# ==================== status / stop ====================

async def _db_status() -> dict:
    """独立连接查询（status 是独立进程，同样不用全局池，避免残留连接）"""
    conn = await asyncpg.connect(config.PG_DSN)
    try:
        tasks = {'pending': 0, 'running': 0, 'done': 0, 'failed': 0}
        for r in await conn.fetch('SELECT status, count(*) AS n FROM activity_tasks GROUP BY status'):
            tasks[r['status']] = r['n']
        return {
            'events': await conn.fetchval('SELECT count(*) FROM events'),
            'trades': await conn.fetchval('SELECT count(*) FROM trades'),
            'users': await conn.fetchval('SELECT count(*) FROM users'),
            'tasks': tasks,
        }
    finally:
        await conn.close()


def cmd_status(args) -> None:
    log('数据库:')
    try:
        st = asyncio.run(_db_status())
        t = st['tasks']
        log(f'      events {st["events"]} 行 | 队列 pending={t["pending"]} running={t["running"]} '
            f'done={t["done"]} failed={t["failed"]}')
        log(f'      trades {st["trades"]} 行 | users {st["users"]} 行')
    except Exception as exc:
        log(f'      查询失败（{type(exc).__name__}: {exc}）→ 先执行 python poly.py 完成建库')
    log('实例:')
    for i in range(1, args.n + 1):
        mixed, ctl = MIXED_START + i - 1, CTL_START + i - 1
        state = '运行中' if port_open(mixed) else '未运行'
        log(f'      w{i}: mixed {mixed} ctl {ctl} {state}')
    owner = _load_pids().get('poly_pid')
    if owner and _pid_alive(owner):
        log(f'poly 主进程: 运行中（pid {owner}，worker 在它的窗口里；停止=到该窗口 Ctrl+C）')
    else:
        log('poly 主进程: 未运行')


def cmd_stop() -> None:
    data = _load_pids()
    insts = data.get('instances') or []
    if not insts:
        log('没有 poly 管理的实例记录（手动/bat 启动的实例不在此列）')
    killed = 0
    for x in insts:
        pid = x.get('pid')
        if pid and _pid_alive(pid):
            _kill_pid(pid)
            killed += 1
            log(f'      已停止 w{x.get("i")} (pid {pid})')
    if os.path.isfile(PID_FILE):
        os.remove(PID_FILE)
    log(f'完成（停止 {killed} 个实例）')
    owner = data.get('poly_pid')
    if owner and _pid_alive(owner):
        log(f'注意: poly 主进程仍在运行（pid {owner}），请到它的窗口 Ctrl+C 停止 worker')


# ==================== 入口 ====================

def main():
    argv = sys.argv[1:]
    if not argv or argv[0].startswith('-'):
        argv = ['run'] + argv            # python poly.py 等价 python poly.py run
    parser = argparse.ArgumentParser(
        prog='poly.py', description='Polymarket 采集系统统一入口（run/status/stop）')
    sub = parser.add_subparsers(dest='cmd')
    p_run = sub.add_parser('run', help='一键跑起来（默认）')
    p_run.add_argument('--n', type=int, default=WORKERS, help=f'实例数=worker 数（默认 {WORKERS}）')
    p_run.add_argument('--jobs', type=int, default=JOBS, help=f'每 worker 并发事件数（默认 {JOBS}）')
    p_run.add_argument('--collect-events', nargs='?', type=int, const=0, default=None,
                       metavar='N',
                       help='强制采集 events（可跟数量如 --collect-events 2 先小量试跑；不带数字=全量补采）')
    p_run.add_argument('--no-events', action='store_true', help='跳过事件检查/采集')
    p_run.add_argument('--duration', type=int, default=0,
                       help='运行 N 秒后自动优雅停止（0=一直运行；试跑验证用）')
    p_run.add_argument('--dry-run', action='store_true', help='只检查并打印计划，不执行任何动作')
    p_status = sub.add_parser('status', help='查看状态（DB 统计 / 实例端口 / 主进程）')
    p_status.add_argument('--n', type=int, default=WORKERS, help=f'探测的实例数（默认 {WORKERS}）')
    sub.add_parser('stop', help='停止全部 mihomo 实例')
    args = parser.parse_args(argv)

    if getattr(args, 'n', 1) < 1:
        raise SystemExit('--n 至少为 1')
    try:
        if args.cmd == 'stop':
            cmd_stop()
        elif args.cmd == 'status':
            cmd_status(args)
        else:
            cmd_run(args)
    except KeyboardInterrupt:
        log('收到中断')
    finally:
        if getattr(args, 'cmd', 'run') == 'run':
            _stop_started()
            _clear_pid_file_if_own()


if __name__ == '__main__':
    main()
