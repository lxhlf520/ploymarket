# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""从机场订阅配置生成 N 个 worker 专用 mihomo 实例配置与启停脚本

原理：polymarket Data API 限流按出口 IP 计（200 req/10s），每个 worker 独占一个
mihomo 实例（独立 mixed 端口 + 独立 selector 组），worker 通过 controller API
在 429/403 时自动切换本实例节点换 IP（clash_pool.AsyncNodeRotator）。

用法（polymarket 目录下）：
    python make_worker_clash.py --n 3
    python make_worker_clash.py --n 3 --src ../trading/clash_config.yaml

输出 clash_worker/：
    w1.yaml w2.yaml ...        每实例配置（mixed 7901+i / controller 9101+i）
    start_mihomo.bat           启动全部实例（默认借用快安内置 mihomo 内核）
    stop_mihomo.bat            停止全部实例（按窗口标题杀进程树）

之后按 README 启动 worker：--proxy http://127.0.0.1:7901 --clash-base http://127.0.0.1:9101
"""
import argparse
import ctypes
import os

import yaml

# polymarket 相关域名与出口 IP 探测站都走 PM 组；其余直连
PM_GROUP = 'PM'
RULES = [
    f'DOMAIN-SUFFIX,polymarket.com,{PM_GROUP}',
    f'DOMAIN-SUFFIX,ip-api.com,{PM_GROUP}',   # egress_ip 探测必须走代理才反映节点出口
    'MATCH,DIRECT',
]

DEFAULT_CORE_CANDIDATES = [
    r'C:\Program Files\快安\core\KuaiAnCore.exe',   # 快安客户端内置 Mihomo Meta
    r'C:\Program Files\Clash Verge\mihomo.exe',
]


def find_core() -> str:
    for p in DEFAULT_CORE_CANDIDATES:
        if os.path.isfile(p):
            return p
    return ''


def short_path(p: str) -> str:
    """转 8.3 短路径（纯 ASCII），避免 bat 中文路径编码问题；失败返回原路径"""
    try:
        buf = ctypes.create_unicode_buffer(300)
        if ctypes.windll.kernel32.GetShortPathNameW(p, buf, 300):
            return buf.value
    except Exception:
        pass
    return p


def gen_config(src: dict, mixed_port: int, ctl_port: int, secret: str) -> dict:
    return {
        'mixed-port': mixed_port,
        'allow-lan': False,
        'mode': 'rule',
        'log-level': 'warning',
        'external-controller': f'127.0.0.1:{ctl_port}',
        'secret': secret,
        # 不配置 dns 劫持（避免多实例/系统冲突；代理域名由出口侧解析）
        'proxies': src.get('proxies') or [],
        'proxy-groups': [{
            'name': PM_GROUP,
            'type': 'select',   # 手动切换组（url-test 无法 PUT 指定节点）
            'proxies': [p['name'] for p in src.get('proxies') or []],
        }],
        'rules': RULES,
    }


def main():
    parser = argparse.ArgumentParser(description='生成 worker 专用 mihomo 配置')
    parser.add_argument('--n', type=int, default=3, help='实例数（默认 3）')
    parser.add_argument('--src', default='../trading/clash_config.yaml',
                        help='机场订阅配置路径')
    parser.add_argument('--mixed-start', type=int, default=7901,
                        help='mixed 起始端口（默认 7901，避开常用 7890）')
    parser.add_argument('--ctl-start', type=int, default=9101,
                        help='controller 起始端口（默认 9101）')
    parser.add_argument('--secret', default='pm-worker', help='controller secret')
    parser.add_argument('--core', default=None, help='mihomo 内核路径（默认自动探测）')
    parser.add_argument('--out', default='clash_worker', help='输出目录')
    args = parser.parse_args()

    src_path = os.path.normpath(os.path.join(os.path.dirname(__file__), args.src)) \
        if not os.path.isabs(args.src) else args.src
    with open(src_path, encoding='utf-8') as f:
        src = yaml.safe_load(f)
    proxies = src.get('proxies') or []
    if not proxies:
        raise SystemExit(f'订阅配置无节点: {src_path}')
    print(f'读取订阅: {src_path} ({len(proxies)} 节点)')

    core = args.core or find_core()
    if not core:
        print('警告: 未找到 mihomo 内核，请把 start_mihomo.bat 中 CORE 改为实际路径')
    else:
        core = short_path(core)
        print(f'使用内核: {core}')

    out_dir = os.path.join(os.path.dirname(__file__), args.out)
    os.makedirs(out_dir, exist_ok=True)

    starts = []
    for i in range(1, args.n + 1):
        cfg = gen_config(src, args.mixed_start + i - 1, args.ctl_start + i - 1,
                         args.secret)
        path = os.path.join(out_dir, f'w{i}.yaml')
        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
        mixed = args.mixed_start + i - 1
        ctl = args.ctl_start + i - 1
        print(f'  生成 w{i}.yaml: mixed={mixed} controller={ctl}')
        starts.append((i, mixed, ctl))

    # start_mihomo.bat：每实例一个最小化窗口
    lines = ['@echo off',
             'REM Polymarket worker 专用 mihomo 实例（由 make_worker_clash.py 生成）']
    if core:
        lines.append(f'set CORE={core}')
    else:
        lines.append('set CORE=CHANGE_ME_to_mihomo.exe')
    lines.append('cd /d %~dp0')
    for i, _mixed, _ctl in starts:
        lines.append(f'start "pm-mihomo-w{i}" /min cmd /c "%CORE% -f w{i}.yaml"')
    lines += ['echo.', 'echo mihomo instances started:',
              *[f'echo   w{i}: mixed={m} controller={c}' for i, m, c in starts]]
    with open(os.path.join(out_dir, 'start_mihomo.bat'), 'w', encoding='gbk',
              errors='replace') as f:
        f.write('\r\n'.join(lines) + '\r\n')

    # stop_mihomo.bat：按窗口标题杀进程树（不影响用户自己的代理客户端）
    stop = ['@echo off']
    stop += [f'taskkill /f /t /fi "WINDOWTITLE eq pm-mihomo-w{i}*" >nul 2>&1'
             for i, _m, _c in starts]
    stop += ['echo mihomo worker instances stopped']
    with open(os.path.join(out_dir, 'stop_mihomo.bat'), 'w', encoding='gbk',
              errors='replace') as f:
        f.write('\r\n'.join(stop) + '\r\n')

    print(f'输出目录: {out_dir}')
    print('下一步: 运行 start_mihomo.bat，然后按 README 启动 worker'
          '（--proxy http://127.0.0.1:7901 --clash-base http://127.0.0.1:9101 ...）')


if __name__ == '__main__':
    main()
