# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Polymarket 一键启动：建库 → 代理池 → 事件数据 → worker（幂等，可反复跑）

用法（ploymarket 目录下）：
    python start_all.py                 # 全流程（首次部署 / 日常重启都用它）
    python start_all.py --n 3 --jobs 3  # 实例数 / 每 worker 并发（默认 3 / 3）
    python start_all.py --dry-run       # 只检查并打印将执行的动作，不实际执行
    python start_all.py --collect-events 2   # 试跑：强制先小量采 2 个事件
    python start_all.py --collect-events     # 非空后补全：强制重采 events（全量）
    python start_all.py --skip-events   # 跳过事件检查（确认队列已有数据时）
    python start_all.py --workers-only  # 只起 worker（代理池与数据已就绪）

流程（每步幂等，已完成的自动跳过）：
    1. initdb    建库建表
    2. clash     代理池配置缺失或内核无效 → 自动生成（自动探测订阅与 mihomo 内核）
    3. mihomo    mixed 7901+ 未监听 → 启动 N 个实例（controller 9101+）
    4. events    events 表为空 → 自动跑 Gamma 采集（走代理池；任务队列的数据源）
    5. workers   起 N 个 worker 窗口，各绑定独立实例
                 （worker 启动时自动把 events 灌入任务队列）

停止：worker 窗口 Ctrl+C；代理实例 clash_worker\\stop_mihomo.bat
"""

import argparse
import asyncio
import os
import socket
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
MIXED_START = 7901
CTL_START = 9101
EVENTS_PROXY = f'http://127.0.0.1:{MIXED_START}'


def log(msg: str) -> None:
    print(f'[start_all] {msg}', flush=True)


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(('127.0.0.1', port)) == 0


def run(cmd: list, env: dict = None) -> int:
    shown = ' '.join(cmd[1:] if cmd[0] == PY else cmd)
    log(f'执行: {shown}')
    return subprocess.call(cmd, cwd=BASE, env=env)


# ---------- 1) 建库 ----------

def step_initdb(dry: bool) -> None:
    log('[1/5] 建库建表（initdb，幂等）')
    if dry:
        log('      → python main_collect.py --stage initdb')
        return
    if run([PY, 'main_collect.py', '--stage', 'initdb']) != 0:
        raise SystemExit('initdb 失败：检查 .env / PostgreSQL 连接后重试')


# ---------- 2) 代理池配置 ----------

def _read_bat() -> str:
    """读已生成的 start_mihomo.bat（GBK 编码）"""
    bat = os.path.join(BASE, 'clash_worker', 'start_mihomo.bat')
    if not os.path.isfile(bat):
        return ''
    with open(bat, encoding='gbk', errors='replace') as f:
        return f.read()


def _bat_core(text: str) -> str:
    """从 bat 内容里提取 CORE 路径"""
    for line in text.splitlines():
        line = line.strip()
        if line.lower().startswith('set core='):
            return line.split('=', 1)[1].strip()
    return ''


def step_clash(n: int, dry: bool) -> None:
    log('[2/5] 代理池配置检查（clash_worker）')
    text = _read_bat()
    core = _bat_core(text)
    inst = text.count('start "pm-mihomo-w')
    ok = (inst >= n
          and os.path.isfile(os.path.join(BASE, 'clash_worker', 'w1.yaml'))
          and core and 'CHANGE_ME' not in core and os.path.isfile(core))
    if ok:
        log(f'      配置有效（{inst} 个实例，内核 {core}），跳过生成')
        return
    reason = f'start_mihomo.bat 仅 {inst} 个实例（需 {n}）' if inst and inst < n \
        else '配置缺失或内核无效'
    log(f'      {reason} → 重新生成（自动探测订阅与内核）')
    if dry:
        log(f'      → python make_worker_clash.py --n {n}')
        return
    if run([PY, 'make_worker_clash.py', '--n', str(n)]) != 0:
        raise SystemExit('生成失败：确认 clash_config.yaml（订阅）与 mihomo 内核就位')


# ---------- 3) mihomo 实例 ----------

def step_mihomo(n: int, dry: bool) -> None:
    ports = [MIXED_START + i for i in range(n)]
    alive = [p for p in ports if port_open(p)]
    log(f'[3/5] mihomo 实例检查（mixed {ports[0]}-{ports[-1]}，已运行 {len(alive)}/{n}）')
    if len(alive) == n:
        log('      全部在运行，跳过')
        return
    if alive:
        log(f'      部分在运行（{alive}）→ 执行 start_mihomo.bat 补齐'
            '（已占端口的实例会自动失败退出，无影响）')
    if dry:
        log('      → clash_worker\\start_mihomo.bat')
        return
    if not alive:
        log('      启动实例 ...')
    subprocess.Popen(['cmd', '/c', r'clash_worker\start_mihomo.bat'], cwd=BASE)
    for _ in range(30):
        if all(port_open(p) for p in ports):
            log(f'      实例就绪: {ports}')
            return
        time.sleep(0.5)
    miss = [p for p in ports if not port_open(p)]
    log(f'      警告: 端口未就绪 {miss}（worker 可能连不上代理；查看运行时窗口排查）')


# ---------- 4) 事件数据 ----------

async def _count_events() -> int:
    import db_pg

    async def _do(conn):
        return await conn.fetchval('SELECT count(*) FROM events')

    return await db_pg.execute_with_retry(_do)


def step_events(dry: bool, skip: bool, force: int = None) -> None:
    log('[4/5] 事件数据检查（worker 任务队列的数据源）')
    if skip:
        log('      --skip-events，跳过')
        return
    try:
        cnt = asyncio.run(_count_events())
    except Exception as exc:
        log(f'      查询 events 失败（{type(exc).__name__}: {exc}）→ 请先完成 initdb')
        return
    log(f'      events 表 {cnt} 行')
    if cnt and force is None:
        log('      已非空，跳过（如需补全/更新: python start_all.py --collect-events）')
        return
    lim = force or 0       # None/0 → 全量；N>0 → 只采 N 个
    cmd = [PY, 'main_collect.py', '--stage', 'events'] \
        + (['--limit', str(lim)] if lim else [])
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
    if run(cmd, env=env) != 0:
        log('      警告: events 采集未正常结束（可重跑本脚本续采）')


# ---------- 5) worker ----------

def step_workers(n: int, jobs: int, dry: bool) -> None:
    log(f'[5/5] 启动 {n} 个 worker（每个绑定独立实例）')
    log('      注意：如已有同 worker-id 的 worker 在跑，先到其窗口 Ctrl+C 关闭')
    for i in range(1, n + 1):
        mixed, ctl = MIXED_START + i - 1, CTL_START + i - 1
        if dry:
            log(f'      → pm-worker-w{i}: --proxy http://127.0.0.1:{mixed} '
                f'--clash-base http://127.0.0.1:{ctl} --jobs {jobs}')
            continue
        # 注意：start 的标题必须带引号——无引号时会被 cmd 当成"要运行的程序"，
        # 找不到就弹 "Windows 找不到文件" 对话框并阻塞（实测验证）
        cmdline = (
            f'cmd /c start "pm-worker-w{i}" cmd /k python worker_activity.py '
            f'--proxy http://127.0.0.1:{mixed} --clash-base http://127.0.0.1:{ctl} '
            f'--worker-id w{i} --jobs {jobs}')
        subprocess.Popen(cmdline, cwd=BASE)
        time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description='Polymarket 一键启动（幂等，可反复跑）')
    parser.add_argument('--n', type=int, default=3, help='mihomo 实例/worker 数量（默认 3）')
    parser.add_argument('--jobs', type=int, default=3, help='每 worker 并发事件数（默认 3）')
    parser.add_argument('--dry-run', action='store_true', help='只检查并打印将执行的动作')
    parser.add_argument('--skip-events', action='store_true', help='跳过事件数据检查')
    parser.add_argument('--collect-events', nargs='?', type=int, const=0, default=None,
                        metavar='N',
                        help='强制采集 events（可跟数量如 --collect-events 2 试跑；不带数量=全量补全）')
    parser.add_argument('--workers-only', action='store_true', help='只起 worker（跳过 1-4 步）')
    args = parser.parse_args()

    t0 = time.time()
    if not args.workers_only:
        step_initdb(args.dry_run)
        step_clash(args.n, args.dry_run)
        step_mihomo(args.n, args.dry_run)
        step_events(args.dry_run, args.skip_events, args.collect_events)
    step_workers(args.n, args.jobs, args.dry_run)
    log('完成（%.1fs）。停止: worker 窗口 Ctrl+C；实例 clash_worker\\stop_mihomo.bat'
        % (time.time() - t0))


if __name__ == '__main__':
    main()
