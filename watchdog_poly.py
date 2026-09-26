# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""poly.py 看门狗：poly 意外退出（被强杀 / OOM / 机器重启）后自动拉起

    python watchdog_poly.py                # 常驻前台运行（检查间隔 60s）
    python watchdog_poly.py --once         # 只检查一次：该拉起就拉起，然后退出
    python watchdog_poly.py --n 3 --jobs 3 # 自动拉起时透传给 poly.py 的参数

判定规则（不瞎拉起、也不漏拉起）：
    - 在运行（PID 记录里的 poly_pid 活着）            → 不动
    - clash_worker/.poly_stopped 存在（人工停止标记） → 不动（python poly.py stop / 正常 Ctrl+C 都会写）
    - 其余情况（无标记且无进程 = 意外退出）           → 拉起 python poly.py

拉起后的 poly 输出重定向到 logs/poly-stdout.log；看门狗自身日志 logs/watchdog.log。
连续拉起失败会指数退避（最长 30 分钟一次），避免配置错误时刷屏空转。

说明：本脚本只负责“把 poly 拉起来”。实例巡检（mihomo 掉线重启）、worker 异常重启、
死信告警都由 poly.py 自己承担；看门狗只兜“整个进程没了”这一层。
长期无人值守可把它注册为计划任务（登录时运行 + --once）或放进启动目录。
"""

import argparse
import json
import os
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE, 'clash_worker')
PID_FILE = os.path.join(OUT_DIR, '.poly_pids.json')
STOP_FLAG = os.path.join(OUT_DIR, '.poly_stopped')
LOG_DIR = os.path.join(BASE, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'watchdog.log')
PY = sys.executable

CHECK_INTERVAL = 60     # 检查间隔（秒）
SETTLE_WAIT = 60        # 拉起后观察期（秒）：过了观察期还在跑就算拉起成功
BACKOFF_MAX = 1800      # 连续拉起失败的最大退避（秒）


def log(msg: str) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    line = f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}'
    print(line, flush=True)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except OSError:
        pass


def _pid_alive(pid: int) -> bool:
    """tasklist 校验 pid 是否存活（与 poly.py 同款判定）"""
    try:
        out = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'],
                            capture_output=True, text=True, errors='replace',
                            creationflags=subprocess.CREATE_NO_WINDOW)
        return str(pid) in (out.stdout or '')
    except Exception:
        return False


def _recorded_pid() -> int | None:
    try:
        with open(PID_FILE, encoding='utf-8') as f:
            return (json.load(f) or {}).get('poly_pid')
    except Exception:
        return None


def _launch(extra: list) -> subprocess.Popen:
    os.makedirs(LOG_DIR, exist_ok=True)
    logf = open(os.path.join(LOG_DIR, 'poly-stdout.log'), 'ab')
    return subprocess.Popen([PY, 'poly.py'] + extra, cwd=BASE,
                            stdout=logf, stderr=subprocess.STDOUT,
                            creationflags=subprocess.CREATE_NO_WINDOW)


def watch(once: bool, extra: list, interval: int) -> None:
    fails = 0
    while True:
        pid = _recorded_pid()
        if pid and _pid_alive(pid):
            fails = 0
            if once:
                log(f'poly 在运行（pid {pid}），无需动作')
                return
            time.sleep(interval)
            continue
        if os.path.isfile(STOP_FLAG):
            if once:
                log('存在人工停止标记（clash_worker/.poly_stopped），不拉起')
                return
            time.sleep(interval)
            continue
        log(f'poly 未运行（记录 pid={pid}），无停止标记 → 拉起: python poly.py {" ".join(extra)}'.rstrip())
        try:
            proc = _launch(extra)
        except Exception as exc:
            log(f'拉起失败（{type(exc).__name__}: {exc}）')
            proc = None
        if once:
            return
        time.sleep(SETTLE_WAIT if proc is not None else 0)
        if proc is not None and proc.poll() is None:
            log(f'拉起成功（pid {proc.pid}），继续监控')
            fails = 0
            continue
        fails += 1
        delay = min(interval * 2 ** fails, BACKOFF_MAX)
        if proc is not None:
            log(f'拉起的 poly 已退出（exit {proc.returncode}），{delay}s 后重试')
        else:
            log(f'{delay}s 后重试')
        time.sleep(delay)


def main() -> None:
    ap = argparse.ArgumentParser(
        prog='watchdog_poly.py', description='poly.py 看门狗（意外退出后自动拉起）')
    ap.add_argument('--once', action='store_true',
                    help='只检查一次：该拉起就拉起，然后退出（适合注册计划任务/启动项）')
    ap.add_argument('--n', type=int, default=None, help='拉起时透传给 poly.py 的 --n（实例数）')
    ap.add_argument('--jobs', type=int, default=None, help='拉起时透传给 poly.py 的 --jobs')
    ap.add_argument('--interval', type=int, default=CHECK_INTERVAL,
                    help=f'检查间隔秒数（默认 {CHECK_INTERVAL}）')
    args = ap.parse_args()

    extra = []
    if args.n:
        extra += ['--n', str(args.n)]
    if args.jobs:
        extra += ['--jobs', str(args.jobs)]
    log(f'看门狗启动（间隔 {args.interval}s' + (f'，参数 {" ".join(extra)}' if extra else '') + '）')
    try:
        watch(args.once, extra, args.interval)
    except KeyboardInterrupt:
        log('看门狗退出（poly 不受影响，仍在运行；如需停止 poly: python poly.py stop 或到它的窗口 Ctrl+C）')


if __name__ == '__main__':
    main()
