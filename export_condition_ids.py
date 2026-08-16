# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""从本地 SQLite polymarket.db 导出 conditionId 种子文件 condition_ids.txt。

部署机不带 618MB 的 SQLite 库，仓库以纯文本种子文件代替 Phase A 数据源。
本脚本只需在持有 SQLite 库的机器上执行一次。
"""

import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config


def main() -> None:
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'condition_ids.txt')
    if not os.path.exists(config.DB_PATH):
        print(f'SQLite 库不存在: {config.DB_PATH}')
        sys.exit(1)

    conn = sqlite3.connect(f'file:{config.DB_PATH}?mode=ro', uri=True)
    cur = conn.cursor()
    cur.execute('SELECT raw_json FROM markets')
    seen = set()
    count = 0
    with open(out_path, 'w', encoding='utf-8') as f:
        for (raw,) in cur.fetchall():
            try:
                obj = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            cid = obj.get('conditionId')
            if cid and cid not in seen:
                seen.add(cid)
                f.write(cid + '\n')
                count += 1
    conn.close()
    print(f'已导出 {count} 个 conditionId -> {out_path}')


if __name__ == '__main__':
    main()
