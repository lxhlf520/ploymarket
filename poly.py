# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Polymarket 采集系统统一入口（实例 + 数据 + worker 一条命令）

    python poly.py                 # run（默认）：一键跑起来，幂等可反复跑
    python poly.py status          # 状态：DB 统计 / 实例与 controller / 主进程 / 日志位置
    python poly.py stop            # 停止全部 mihomo 实例（pid 记录 + 端口兜底）
    python poly.py retry-failed    # 死信重新入队（failed → pending）

run 流程（每步先检查状态再动作，重复跑不会重复起）：
    [1/5] 建库建表      main_collect.py --stage initdb
    [2/5] 代理池配置    clash_worker/w1..wN.yaml（缺失/无节点/内核未就位则自动生成）
    [3/5] 实例          校验 controller/组/节点后复用，不可用则清掉重建（后台无窗口）
    [4/5] 事件数据      events 表为空则自动采集（经 w1 实例代理；--collect-events 强制）
    [5/5] worker        同进程并发 N 个（日志 [w1]/[w2] 前缀），每 60s 自动补任务队列

长跑稳定性（无人值守时自愈，不用盯着）：
    - worker 异常退出 → 指数退避后自动重启（最多 60s 一次），不静默减员
    - 领任务时 DB 抖动 → 退避重试，worker 循环不退出（PG 重启/网络抖动由 db_pg 兜底）
    - 代理实例掉线 → worker 暂停领新任务（不把事件白烧成死信），poly 每 30s 巡检自动重启实例
    - controller 不可用 → 节点轮换不拉黑节点，实例恢复后立即可切换
    - 死信（failed）不自动重试但会醒目提醒，人工确认后用 retry-failed 重新入队
    - 心跳：每 5 分钟一条“实例 N/N | 队列 …”日志，日志落盘 logs/poly.log + logs/wN.log

Ctrl+C 一次全停（worker + 本次启动的 mihomo 实例）；被强杀时用 python poly.py stop 清残留。

常用参数（run）：
    --n 3                   实例数 = worker 数（默认 3）
    --jobs 3                每 worker 并发事件数（默认 3）
    --collect-events [N]    强制采集 events（可跟数量先小量试跑；不带数字=全量补采）
    --no-events             跳过事件检查（确认队列有数据时）
    --duration N            跑 N 秒后自动优雅停止（0=一直跑；试跑验证用）
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
import urllib.parse
from logging.handlers import RotatingFileHandler

import asyncpg
import httpx
import yaml

import config
import db_pg
import make_worker_clash as mwc
import worker_activity

BASE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
OUT_DIR = os.path.join(BASE, 'clash_worker')
PID_FILE = os.path.join(OUT_DIR, '.poly_pids.json')
STOP_FLAG = os.path.join(OUT_DIR, '.poly_stopped')   # 人工停止标记（watchdog 据此不自动拉起）
LOG_DIR = os.path.join(BASE, 'logs')

MIXED_START = mwc.DEFAULT_MIXED_START   # 7901
CTL_START = mwc.DEFAULT_CTL_START       # 9101
SECRET = mwc.DEFAULT_SECRET
GROUP = mwc.PM_GROUP
EVENTS_PROXY = f'http://127.0.0.1:{MIXED_START}'
WORKERS = 3
JOBS = 3
REFILL_INTERVAL = 60       # 自动补任务队列间隔（秒）
EVENTS_RETRY = 3           # events 采集失败后的自动重试次数（断点续采，重跑幂等）
EVENTS_RETRY_DELAY = 30    # events 采集失败重试的首次等待（秒，之后翻倍）
MONITOR_INTERVAL = 30      # 实例健康巡检间隔（秒）
HEARTBEAT_INTERVAL = 300   # 运行心跳日志间隔（秒）
WORKER_RESTART_MIN = 5     # worker 异常重启最小间隔（秒）
WORKER_RESTART_MAX = 60    # worker 异常重启最大间隔（秒）
CTL_TIMEOUT = 5            # controller 探测超时（秒）

os.makedirs(LOG_DIR, exist_ok=True)
_CONSOLE_FMT = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S')
_FILE_FMT = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

_console = logging.StreamHandler()
_console.setFormatter(_CONSOLE_FMT)
_poly_log = RotatingFileHandler(os.path.join(LOG_DIR, 'poly.log'),
                                maxBytes=10 * 1024 * 1024, backupCount=5,
                                encoding='utf-8')
_poly_log.setFormatter(_FILE_FMT)
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_root.addHandler(_console)
_root.addHandler(_poly_log)
logging.getLogger('httpx').setLevel(logging.WARNING)   # 压掉每请求一行（节点延迟预筛会产生数百行）
logger = logging.getLogger('poly')

_STARTED_PROCS: list = []    # 本次 poly 启动的 mihomo 进程（退出时停止）
_WORKER_HANDLERS: dict = {}  # wid -> 日志文件 handler（worker 退出时摘掉）


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


def _port_pid(port: int) -> int | None:
    """查监听指定端口的进程 pid（Windows netstat）"""
    try:
        out = subprocess.run(['netstat', '-ano'], capture_output=True, text=True,
                             errors='replace',
                             creationflags=subprocess.CREATE_NO_WINDOW).stdout or ''
    except Exception:
        return None
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[0].upper() == 'TCP' and parts[3].upper() == 'LISTENING':
            if parts[1].rsplit(':', 1)[-1] == str(port):
                try:
                    return int(parts[4])
                except ValueError:
                    return None
    return None


def _proc_name(pid: int) -> str:
    """取进程名（tasklist），小写；取不到返回空串"""
    try:
        out = subprocess.run(['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV', '/NH'],
                             capture_output=True, text=True, errors='replace',
                             creationflags=subprocess.CREATE_NO_WINDOW).stdout or ''
    except Exception:
        return ''
    return out.split('","')[0].strip('"\r\n ').lower()


# 只认这些内核进程名，避免误杀占用同端口的其它程序
_CORE_HINTS = ('mihomo', 'clash', 'meta', 'kuai', 'verge')


def _kill_port_owner(port: int) -> int | None:
    """清理占用端口的 mihomo 系残留进程，返回被清理的 pid；非本套进程只提醒不动手"""
    pid = _port_pid(port)
    if not pid:
        return None
    name = _proc_name(pid)
    if not any(h in name for h in _CORE_HINTS):
        log(f'      端口 {port} 被 {name or "?"} (pid {pid}) 占用，非 mihomo 实例，未动')
        return None
    log(f'      清理端口 {port} 残留实例 {name} (pid {pid})')
    _kill_pid(pid)
    return pid


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


def _mark_stopped() -> None:
    """记下“人工已停止”（watchdog_poly.py 见到标记不自动拉起 poly）"""
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(STOP_FLAG, 'w', encoding='utf-8') as f:
            f.write(time.strftime('%Y-%m-%d %H:%M:%S'))
    except OSError:
        pass


def _clear_stopped() -> None:
    with contextlib.suppress(OSError):
        os.remove(STOP_FLAG)


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


# ==================== 日志分流 / 实例控制层探测 ====================

class _WorkerFilter(logging.Filter):
    """只放行属于指定 worker 的日志（靠 worker_activity.CURRENT_WORKER 区分：协程上下文隔离）"""

    def __init__(self, wid: str):
        super().__init__()
        self.wid = wid

    def filter(self, record) -> bool:
        return worker_activity.CURRENT_WORKER.get() == self.wid


def _attach_worker_log(wid: str) -> None:
    """给 worker 挂专属日志文件 logs/wN.log（退出时 _detach_worker_log 摘掉）"""
    if wid in _WORKER_HANDLERS:
        return
    handler = RotatingFileHandler(os.path.join(LOG_DIR, f'{wid}.log'),
                                  maxBytes=5 * 1024 * 1024, backupCount=3,
                                  encoding='utf-8')
    handler.setFormatter(_FILE_FMT)
    handler.addFilter(_WorkerFilter(wid))
    _root.addHandler(handler)
    _WORKER_HANDLERS[wid] = handler


def _detach_worker_log(wid: str) -> None:
    handler = _WORKER_HANDLERS.pop(wid, None)
    if handler is not None:
        _root.removeHandler(handler)
        with contextlib.suppress(Exception):
            handler.close()


def _ctl_ok(ctl: int, secret: str, group: str = None) -> bool:
    """探测 mihomo controller 是否真可用（只看端口会被僵尸/旧实例骗过）

    group 给定时额外校验节点组存在且组内有节点。
    """
    try:
        with httpx.Client(timeout=CTL_TIMEOUT,
                          headers={'Authorization': f'Bearer {secret}'}) as cli:
            if cli.get(f'http://127.0.0.1:{ctl}/version').status_code != 200:
                return False
            if not group:
                return True
            url = f'http://127.0.0.1:{ctl}/proxies/{urllib.parse.quote(group, safe="")}'
            resp = cli.get(url)
            if resp.status_code != 200:
                return False
            return bool((resp.json() or {}).get('all'))
    except Exception:
        return False


def _spawn_instance(i: int, core: str) -> subprocess.Popen:
    """后台无窗口启动一个 mihomo 实例（日志追加到 clash_worker/mihomo-wN.log）"""
    os.makedirs(OUT_DIR, exist_ok=True)
    logf = open(os.path.join(OUT_DIR, f'mihomo-w{i}.log'), 'ab')
    proc = subprocess.Popen([core, '-f', f'w{i}.yaml'], cwd=OUT_DIR,
                            stdout=logf, stderr=subprocess.STDOUT,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    _STARTED_PROCS.append(proc)
    return proc


def _save_instances(inst: list) -> None:
    """记录实例 pid（供 stop 清理 / 重复启动检测）"""
    _save_pids({'poly_pid': os.getpid(),
                'updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'instances': [{'i': x['i'], 'mixed': x['mixed'], 'ctl': x['ctl'],
                               'pid': x['pid']} for x in inst if x.get('pid')]})


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
    """确保 n 个实例可用：controller/组校验通过才复用，否则清掉残留重建；返回实例信息列表"""
    log(f'[3/5] 实例检查（mixed {MIXED_START}-{MIXED_START + n - 1}）')
    old_pids = {x.get('i'): x.get('pid') for x in (_load_pids().get('instances') or [])}
    inst = []
    for i in range(1, n + 1):
        mixed = MIXED_START + i - 1
        ctl = CTL_START + i - 1
        live = port_open(mixed)
        if live and _ctl_ok(ctl, SECRET, GROUP):
            inst.append({'i': i, 'mixed': mixed, 'ctl': ctl, 'core': core,
                         'pid': old_pids.get(i), 'state': 'running'})
            continue
        why = '端口未监听' if not live else 'controller 不可用（旧实例 / secret 不匹配）'
        if dry:
            log(f'      w{i}: {why} → 将启动')
            inst.append({'i': i, 'mixed': mixed, 'ctl': ctl, 'core': core,
                         'pid': None, 'state': 'planned'})
            continue
        if live:
            log(f'      w{i}: {why} → 清理残留并重建')
            _kill_port_owner(mixed)
            _kill_port_owner(ctl)
            time.sleep(0.5)
        else:
            log(f'      w{i}: {why} → 启动')
        inst.append({'i': i, 'mixed': mixed, 'ctl': ctl, 'core': core,
                     'pid': _spawn_instance(i, core).pid, 'state': 'starting'})
    if dry:
        return inst
    # 等待就绪（最多 20s，以 controller 可用为判定：端口开着不代表配置对）
    deadline = time.time() + 20
    while time.time() < deadline:
        if all(x['state'] != 'starting' or _ctl_ok(x['ctl'], SECRET, GROUP) for x in inst):
            break
        time.sleep(0.5)
    for x in inst:
        if x['state'] == 'starting':
            x['state'] = 'running' if _ctl_ok(x['ctl'], SECRET, GROUP) else 'failed'
    ready = [x for x in inst if x['state'] == 'running']
    log(f'      就绪 {len(ready)}/{n}' + (f'：{[x["mixed"] for x in ready]}' if ready else ''))
    for x in inst:
        if x['state'] == 'failed':
            log(f'      警告: w{x["i"]} (mixed {x["mixed"]}) 启动失败，'
                f'查看 clash_worker/mihomo-w{x["i"]}.log')
    _save_instances(inst)
    return inst


def _planned(n: int) -> list:
    """dry-run 用的假实例列表（只打印计划）"""
    return [{'i': i, 'mixed': MIXED_START + i - 1, 'ctl': CTL_START + i - 1,
             'core': None, 'pid': None, 'state': 'planned'} for i in range(1, n + 1)]


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
    # 采集失败自动重试：Stage A/B 均有 set_scope_state 断点，重跑只续采不重复；
    # DB/PG 集群代理抖动导致的异常退出靠这里兜住，避免人工重跑 poly.py
    delay = EVENTS_RETRY_DELAY
    for attempt in range(EVENTS_RETRY + 1):
        rc = _run(cmd, env=env)
        if rc == 0:
            break
        if rc in (130, 3221225786):    # Ctrl+C（POSIX 130 / Windows 0xC000013A）
            log('      采集被中断，退出（进度已入库，重跑 python poly.py 续采）')
            break
        if attempt < EVENTS_RETRY:
            log(f'      采集异常退出（rc={rc}），{delay}s 后自动重试'
                f'（第 {attempt + 1}/{EVENTS_RETRY} 次；断点续采，不会重复采）')
            time.sleep(delay)
            delay = min(delay * 2, 120)
        else:
            log(f'      采集重试 {EVENTS_RETRY} 次仍未正常结束；'
                '重跑 python poly.py 会继续续采（进度已入库）')


async def _task_stats() -> dict:
    """独立连接查任务队列统计（不碰 worker 的全局连接池：池绑定事件循环）"""
    conn = await asyncpg.connect(config.PG_DSN)
    try:
        stats = {'pending': 0, 'running': 0, 'done': 0, 'failed': 0}
        for r in await conn.fetch('SELECT status, count(*) AS n FROM activity_tasks GROUP BY status'):
            stats[r['status']] = r['n']
        return stats
    finally:
        await conn.close()


async def _refill_loop() -> None:
    """周期把 events 表新增事件灌入任务队列（幂等）——events 补采后 worker 无需重启；
    死信（failed）数量变化时告警（不自动重试：需人工确认原因后 retry-failed）"""
    last_failed = None
    while True:
        await asyncio.sleep(REFILL_INTERVAL)
        try:
            res = await db_pg.init_activity_tasks()
            if res.get('queued'):
                log(f'自动补队列: 新增 {res["queued"]} 个任务')
            stats = res.get('stats')
            failed = stats.get('failed', 0) if isinstance(stats, dict) else 0
            if failed and failed != last_failed:
                log(f'注意: 队列有 {failed} 个死信（failed），确认原因后: python poly.py retry-failed')
            last_failed = failed
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning('[poly] 自动补队列失败（稍后重试）: %s', exc)


async def _duration_stop(duration: int, tasks: list) -> None:
    """--duration：到点后取消全部任务（走与 Ctrl+C 相同的优雅停止路径）"""
    await asyncio.sleep(duration)
    log(f'--duration {duration}s 到，停止 worker...')
    for t in tasks:
        t.cancel()


async def _monitor_loop(inst: list) -> None:
    """实例健康巡检：掉线自动重启（不只看端口在听，controller 要真可用）；
    另打周期心跳日志（长跑可见性：一眼看出实例/队列是否正常）"""
    last_beat = 0.0
    while True:
        await asyncio.sleep(MONITOR_INTERVAL)
        for x in inst:
            try:
                ok = port_open(x['mixed']) and await asyncio.to_thread(
                    _ctl_ok, x['ctl'], SECRET, GROUP)
            except Exception:
                ok = False
            if ok:
                x['state'] = 'running'
                continue
            log(f'实例 w{x["i"]} 掉线（mixed {x["mixed"]} / ctl {x["ctl"]}）→ 自动重启')
            x['state'] = 'failed'
            await asyncio.to_thread(_kill_port_owner, x['mixed'])
            await asyncio.to_thread(_kill_port_owner, x['ctl'])
            await asyncio.sleep(1)
            try:
                x['pid'] = _spawn_instance(x['i'], x['core']).pid
            except Exception as exc:
                log(f'实例 w{x["i"]} 重启失败（{type(exc).__name__}: {exc}），下轮巡检再试')
                continue
            _save_instances(inst)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if await asyncio.to_thread(_ctl_ok, x['ctl'], SECRET, GROUP):
                    break
                await asyncio.sleep(1)
            if await asyncio.to_thread(_ctl_ok, x['ctl'], SECRET, GROUP):
                x['state'] = 'running'
                log(f'实例 w{x["i"]} 已恢复（pid {x["pid"]}）')
            else:
                log(f'实例 w{x["i"]} 重启后仍不可用，下轮巡检再试')
        now = time.monotonic()
        if now >= last_beat:
            last_beat = now + HEARTBEAT_INTERVAL
            up = sum(1 for x in inst if x['state'] == 'running')
            try:
                stats = await _task_stats()
                log(f'心跳: 实例 {up}/{len(inst)} | 队列 pending={stats["pending"]} '
                    f'running={stats["running"]} done={stats["done"]} failed={stats["failed"]}')
            except Exception as exc:
                log(f'心跳: 实例 {up}/{len(inst)} | 队列查询失败（{type(exc).__name__}: {exc}）')


async def _supervise(worker_id: str, mixed: int, ctl: int, jobs: int) -> None:
    """worker 监督：异常退出后指数退避自动重启（正常取消不重启：Ctrl+C/--duration）"""
    fails = 0
    _attach_worker_log(worker_id)
    try:
        while True:
            try:
                res = await worker_activity.run_worker(
                    worker_id, proxy=f'http://127.0.0.1:{mixed}',
                    clash_base=f'http://127.0.0.1:{ctl}',
                    clash_secret=SECRET, clash_group=GROUP,
                    jobs=jobs, progress=False)
                log(f'      {worker_id} 正常退出: {res}')
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fails += 1
                delay = min(WORKER_RESTART_MIN * 2 ** (fails - 1), WORKER_RESTART_MAX)
                log(f'      {worker_id} 异常退出（第 {fails} 次: {type(exc).__name__}: {exc}），'
                    f'{delay}s 后自动重启')
                await asyncio.sleep(delay)
    finally:
        _detach_worker_log(worker_id)


async def _workers_main(inst: list, jobs: int, duration: int = 0) -> None:
    ports = [(x['i'], x['mixed'], x['ctl']) for x in inst if x['state'] in ('running', 'planned')]
    tasks = [asyncio.create_task(_supervise(f'w{i}', mixed, ctl, jobs))
             for i, mixed, ctl in ports]
    refill = asyncio.create_task(_refill_loop())
    monitor = asyncio.create_task(_monitor_loop(inst))
    timer = (asyncio.create_task(_duration_stop(duration, tasks + [monitor]))
             if duration > 0 else None)
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (i, _m, _c), r in zip(ports, results):
            if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                log(f'      w{i} 异常退出: {type(r).__name__}: {r}')
    finally:
        for task in (refill, monitor, timer):
            if task is not None:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task


def step_workers(inst: list, jobs: int, dry: bool, duration: int = 0) -> None:
    ports = [(x['i'], x['mixed'], x['ctl']) for x in inst if x['state'] in ('running', 'planned')]
    log(f'[5/5] 启动 {len(ports)} 个 worker（同进程并发，日志 [w1]/[w2] 前缀）')
    for i, mixed, ctl in ports:
        log(f'      → w{i}: proxy 127.0.0.1:{mixed} controller 127.0.0.1:{ctl} jobs={jobs}')
    log(f'      每 {REFILL_INTERVAL}s 自动补队列；每 {MONITOR_INTERVAL}s 实例巡检（掉线自动重启）；'
        f'{HEARTBEAT_INTERVAL // 60} 分钟一次心跳')
    log('      日志落盘: logs/poly.log + logs/wN.log（异常退出/掉线/死信都会记在这里）')
    if duration > 0:
        log(f'      --duration {duration}s：到点自动优雅停止')
    log('      Ctrl+C 一次全停（worker + 本次启动的 mihomo 实例）')
    if dry:
        return
    if not ports:
        log('      无可用实例，worker 未启动')
        return
    log('      提示: 若之前在别的窗口手动起过 worker，先关掉它们（避免重复领任务）')
    asyncio.run(_workers_main(inst, jobs, duration))


def cmd_run(args) -> None:
    t0 = time.time()
    _check_not_running()
    if not args.dry_run:
        _clear_stopped()                  # 人工启动：清掉停止标记
    try:
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
    finally:
        if not args.dry_run:
            _mark_stopped()               # 退出（含 Ctrl+C/启动失败）：记停止标记，watchdog 不自动拉起
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
        if t['failed']:
            log(f'      注意: {t["failed"]} 个死信（failed）—— 确认原因后: python poly.py retry-failed')
    except Exception as exc:
        log(f'      查询失败（{type(exc).__name__}: {exc}）→ 先执行 python poly.py 完成建库')
    log('实例:')
    up = 0
    for i in range(1, args.n + 1):
        mixed, ctl = MIXED_START + i - 1, CTL_START + i - 1
        if not port_open(mixed):
            state = '未运行'
        elif _ctl_ok(ctl, SECRET, GROUP):
            state = '运行中'
            up += 1
        else:
            state = '端口在听但 controller 不可用（旧实例/secret 不匹配，run 时会自动重建）'
        log(f'      w{i}: mixed {mixed} ctl {ctl} {state}')
    log(f'      实例可用 {up}/{args.n}')
    owner = _load_pids().get('poly_pid')
    if owner and _pid_alive(owner):
        log(f'poly 主进程: 运行中（pid {owner}，worker 在它的窗口里；停止=到该窗口 Ctrl+C）')
    else:
        log('poly 主进程: 未运行')
    log(f'日志: {os.path.join(LOG_DIR, "poly.log")}（worker 分文件 logs/wN.log）')


def cmd_stop() -> None:
    data = _load_pids()
    insts = data.get('instances') or []
    if not insts:
        log('没有 poly 管理的实例记录，按端口约定兜底清理（非 mihomo 进程不动）')
    killed = set()
    for x in insts:
        pid = x.get('pid')
        if pid and _pid_alive(pid):
            _kill_pid(pid)
            killed.add(pid)
            log(f'      已停止 w{x.get("i")} (pid {pid})')
    # 端口兜底：pid 记录丢失/被强杀时按端口约定清理 mihomo 残留
    n = max([x.get('i') or 0 for x in insts] + [WORKERS])
    for i in range(1, n + 1):
        for port in (MIXED_START + i - 1, CTL_START + i - 1):
            pid = _port_pid(port)
            if pid and pid not in killed:
                got = _kill_port_owner(port)
                if got:
                    killed.add(got)
    if os.path.isfile(PID_FILE):
        os.remove(PID_FILE)
    _mark_stopped()
    log(f'完成（停止 {len(killed)} 个实例；已写停止标记，watchdog_poly.py 不会自动拉起）')
    owner = data.get('poly_pid')
    if owner and _pid_alive(owner):
        log(f'注意: poly 主进程仍在运行（pid {owner}），请到它的窗口 Ctrl+C 停止 worker')


def cmd_retry_failed(args) -> None:
    """死信重新入队：failed → pending（人工确认原因后执行）"""
    async def _do() -> int:
        try:
            return await db_pg.retry_failed_activity_tasks(args.limit)
        finally:
            with contextlib.suppress(Exception):
                await db_pg.reset_pool()   # 一次性命令：用完即关，不留连接

    try:
        n = asyncio.run(_do())
    except Exception as exc:
        raise SystemExit(f'重新入队失败（{type(exc).__name__}: {exc}）')
    if n:
        log(f'已将 {n} 个死信重置为 pending（worker 会在下一轮领取）')
    else:
        log('没有死信（failed）需要处理')


# ==================== 入口 ====================

def main():
    argv = sys.argv[1:]
    if not argv or argv[0].startswith('-'):
        argv = ['run'] + argv            # python poly.py 等价 python poly.py run
    parser = argparse.ArgumentParser(
        prog='poly.py', description='Polymarket 采集系统统一入口（run/status/stop/retry-failed）')
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
    p_status = sub.add_parser('status', help='查看状态（DB 统计 / 实例与 controller / 主进程）')
    p_status.add_argument('--n', type=int, default=WORKERS, help=f'探测的实例数（默认 {WORKERS}）')
    sub.add_parser('stop', help='停止全部 mihomo 实例（并记停止标记）')
    p_retry = sub.add_parser('retry-failed', help='死信重新入队（failed → pending）')
    p_retry.add_argument('--limit', type=int, default=None,
                         help='最多处理多少条（默认全部）')
    args = parser.parse_args(argv)

    if getattr(args, 'n', 1) < 1:
        raise SystemExit('--n 至少为 1')
    try:
        if args.cmd == 'stop':
            cmd_stop()
        elif args.cmd == 'status':
            cmd_status(args)
        elif args.cmd == 'retry-failed':
            cmd_retry_failed(args)
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
