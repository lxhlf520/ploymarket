# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""浏览器登录态 Cookie 导出（评论全量采集前置步骤）

流程：
1. 浏览器（Chrome/Edge）打开 https://polymarket.com 并完成登录
2. 在 DevTools Console（F12）执行下方 JS，复制输出
3. 运行本脚本粘贴输出，或直接 python export_cookie.py --json "{...}"
   → 写入 cookies.json（gamma_client 采集时自动注入）

JS（控制台执行，返回 JSON 字符串）：
    JSON.stringify({...document.cookie.split('; ').reduce((o, kv) => {
        const [k, ...v] = kv.split('='); o[k] = v.join('='); return o;
    }, {}), ...Object.entries(localStorage).reduce((o, [k, v]) => {
        try { const j = JSON.parse(v);
            if (j && typeof j === 'object' && (j.accessToken || j.refreshToken || j.token)) o[k] = v;
        } catch (e) {} return o;
    }, {})})
"""

import argparse
import json
import os
import sys

import config


def save_cookies(data, cookie_file: str = None) -> str:
    """把 dict/list 写入 cookies.json，返回文件路径"""
    path = cookie_file or config.COOKIE_FILE
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description='导出 polymarket 登录 Cookie 到 cookies.json')
    parser.add_argument('--json', default=None,
                        help='浏览器控制台导出的 JSON（dict 或 [{name,value}]）')
    parser.add_argument('--file', default=None,
                        help='从文件读取 JSON（含粘贴到文本文件的场景）')
    parser.add_argument('--out', default=None, help='输出文件路径（默认 config.COOKIE_FILE）')
    parser.add_argument('--check', action='store_true',
                        help='仅检查 cookies.json 是否存在且可注入，不写入')
    args = parser.parse_args()

    if args.check:
        if not os.path.exists(config.COOKIE_FILE):
            print('cookies.json 不存在，请先执行导出')
            sys.exit(1)
        from gamma_client import load_cookie_header
        header = load_cookie_header()
        print(f'cookies.json 存在，Cookie header 长度 {len(header)}')
        print('字段示例:', header[:120] + ('...' if len(header) > 120 else ''))
        return

    data = None
    if args.file:
        with open(args.file, encoding='utf-8') as f:
            data = json.load(f)
    elif args.json:
        data = json.loads(args.json)
    else:
        # 交互模式：打印 JS 让用户控制台执行后粘贴
        print('=' * 70)
        print('步骤 1：浏览器打开 https://polymarket.com 并登录')
        print('步骤 2：DevTools Console 执行：')
        print('=' * 70)
        print('''JSON.stringify({...document.cookie.split('; ').reduce((o, kv) => {
    const [k, ...v] = kv.split('='); o[k] = v.join('='); return o;
}, {}), ...Object.entries(localStorage).reduce((o, [k, v]) => {
    try { const j = JSON.parse(v);
        if (j && typeof j === 'object' && (j.accessToken || j.refreshToken || j.token)) o[k] = v;
    } catch (e) {} return o;
}, {})})''')
        print('=' * 70)
        print('步骤 3：将输出的 JSON 粘贴到下面（Ctrl+Z / Ctrl+D 或空行结束）：')
        lines = []
        try:
            while True:
                line = input()
                if not line.strip():
                    break
                lines.append(line.strip())
        except EOFError:
            pass
        raw = ''.join(lines)
        if not raw:
            print('未输入内容，退出')
            sys.exit(1)
        data = json.loads(raw)

    path = save_cookies(data, args.out)
    n = len(data) if isinstance(data, (dict, list)) else 0
    print(f'已写入 {path}（{n} 个字段）。可运行 python export_cookie.py --check 验证')


if __name__ == '__main__':
    main()
