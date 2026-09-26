# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""从机场订阅配置生成 N 个 worker 专用 mihomo 实例配置（clash_worker/w1..wN.yaml）

原理：polymarket Data API 限流按出口 IP 计（200 req/10s），每个 worker 独占一个
mihomo 实例（独立 mixed 端口 + 独立 selector 组），worker 通过 controller API
在 429/403 时自动切换本实例节点换 IP（clash_pool.AsyncNodeRotator）。

本模块是 poly.py 的配置生成库（也可单独 CLI 调用）：
    python make_worker_clash.py --n 3      # 显式生成（日常不用，poly.py 自动处理）
    python make_worker_clash.py --n 3 --src ../trading/clash_config.yaml

自动探测规则：
    订阅：polymarket/clash_config.yaml → ../trading/clash_config.yaml
    内核：clash_worker/mihomo*.exe → mihomo_core/mihomo*.exe
          → mihomo*.zip 自动解压到 mihomo_core/ → 本机快安 / Clash Verge

实例的起停由 poly.py 统一管理（python poly.py / python poly.py stop），不需要 bat。
"""
import argparse
import glob
import os
import zipfile

import yaml

BASE = os.path.dirname(os.path.abspath(__file__))

# 订阅配置自动探测顺序（相对脚本目录）
DEFAULT_SRC_CANDIDATES = ['clash_config.yaml', '../trading/clash_config.yaml']

# 项目内自带的 mihomo 内核目录（跨机器部署时随包携带）
CORE_SUBDIRS = ['clash_worker', 'mihomo_core']

# mihomo*.zip 自动解压的搜索位置（相对脚本目录）
ZIP_SEARCH_DIRS = ['.', 'mihomo_core', 'clash_worker', '..']

# 本机代理客户端内置内核（兜底）
DEFAULT_CORE_CANDIDATES = [
    r'C:\Program Files\快安\core\KuaiAnCore.exe',   # 快安客户端内置 Mihomo Meta
    r'C:\Program Files\Clash Verge\mihomo.exe',
]

# polymarket 相关域名与出口 IP 探测站都走 PM 组；其余直连
PM_GROUP = 'PM'
RULES = [
    f'DOMAIN-SUFFIX,polymarket.com,{PM_GROUP}',
    f'DOMAIN-SUFFIX,ip-api.com,{PM_GROUP}',   # egress_ip 探测必须走代理才反映节点出口
    'MATCH,DIRECT',
]

DEFAULT_MIXED_START = 7901   # 避开常用 7890
DEFAULT_CTL_START = 9101
DEFAULT_SECRET = 'pm-worker'


def find_src() -> str:
    """自动探测订阅配置：脚本目录 clash_config.yaml → ../trading/clash_config.yaml"""
    for rel in DEFAULT_SRC_CANDIDATES:
        p = os.path.normpath(os.path.join(BASE, rel))
        if os.path.isfile(p):
            return p
    return ''


def _extract_core_zip() -> str:
    """mihomo*.zip 自动解压到 mihomo_core/，返回解出的内核路径（无则空）"""
    for rel in ZIP_SEARCH_DIRS:
        for zp in sorted(glob.glob(os.path.join(BASE, rel, 'mihomo*.zip'))):
            target = os.path.join(BASE, 'mihomo_core')
            os.makedirs(target, exist_ok=True)
            try:
                with zipfile.ZipFile(zp) as z:
                    z.extractall(target)
            except Exception:
                continue
            hits = sorted(glob.glob(os.path.join(target, '**', 'mihomo*.exe'),
                                    recursive=True))
            if hits:
                return hits[0]
    return ''


def find_core() -> str:
    """探测内核：clash_worker/ → mihomo_core/ → mihomo*.zip 解压 → 本机客户端"""
    for sub in CORE_SUBDIRS:
        hits = sorted(glob.glob(os.path.join(BASE, sub, 'mihomo*.exe')))
        if hits:
            return hits[0]
    extracted = _extract_core_zip()
    if extracted:
        return extracted
    for p in DEFAULT_CORE_CANDIDATES:
        if os.path.isfile(p):
            return p
    return ''


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


def build_instances(n: int = 3, *, src_path: str = None, core_path: str = None,
                    out_dir: str = None, log=print) -> dict:
    """生成 w1..wN.yaml，返回 {'src', 'core', 'instances': [{i, mixed, ctl, yaml}]}

    core 探测失败不抛错（返回空串），由调用方决定是否阻断。
    """
    if src_path:
        src_file = src_path if os.path.isabs(src_path) \
            else os.path.normpath(os.path.join(BASE, src_path))
    else:
        src_file = find_src()
        if not src_file:
            raise SystemExit('未找到订阅配置：请把机场订阅 yaml 放到 polymarket/clash_config.yaml'
                             '（或用 --src 指定路径）')
    with open(src_file, encoding='utf-8') as f:
        src = yaml.safe_load(f) or {}
    proxies = src.get('proxies') or []
    if not proxies:
        raise SystemExit(f'订阅配置无节点: {src_file}')
    log(f'读取订阅: {src_file} ({len(proxies)} 节点)')

    core = core_path if core_path is not None else find_core()
    if core:
        log(f'使用内核: {core}')
    else:
        log('警告: 未找到 mihomo 内核（可放 mihomo*.zip 到项目目录，自动解压）')

    target_dir = out_dir or os.path.join(BASE, 'clash_worker')
    os.makedirs(target_dir, exist_ok=True)

    instances = []
    for i in range(1, n + 1):
        mixed = DEFAULT_MIXED_START + i - 1
        ctl = DEFAULT_CTL_START + i - 1
        cfg = gen_config(src, mixed, ctl, DEFAULT_SECRET)
        path = os.path.join(target_dir, f'w{i}.yaml')
        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
        log(f'生成 w{i}.yaml: mixed={mixed} controller={ctl}')
        instances.append({'i': i, 'mixed': mixed, 'ctl': ctl, 'yaml': path})

    return {'src': src_file, 'core': core, 'instances': instances}


def main():
    parser = argparse.ArgumentParser(description='生成 worker 专用 mihomo 配置')
    parser.add_argument('--n', type=int, default=3, help='实例数（默认 3）')
    parser.add_argument('--src', default=None,
                        help='机场订阅配置路径（默认自动探测：clash_config.yaml / '
                             '../trading/clash_config.yaml）')
    parser.add_argument('--core', default=None, help='mihomo 内核路径（默认自动探测）')
    parser.add_argument('--out', default=None, help='输出目录（默认 clash_worker）')
    args = parser.parse_args()

    info = build_instances(args.n, src_path=args.src, core_path=args.core, out_dir=args.out)
    print(f'输出目录: {os.path.dirname(info["instances"][0]["yaml"])}')
    print('下一步: python poly.py    （统一入口：实例 + 数据 + worker 一条命令）')


if __name__ == '__main__':
    main()
