# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""从 PG 导出 Polymarket 官网盘口链接（验证用）

数据库 markets.slug / events.slug 直接拼官网 URL：
    市场盘口页: https://polymarket.com/market/{slug}   （切 Orderbook 标签核对深度）
    事件页:     https://polymarket.com/event/{slug}

用法:
  python market_links.py                # 列出前 100 个市场（含官网链接）
  python market_links.py --limit 500
  python market_links.py --keyword 特朗普 --orderbook-only
  python market_links.py --event-slug will-samuel-alito-announce-his-retirement-by  # 按事件查全部选项
  python market_links.py --event-id 212877
  python market_links.py --no-html      # 只打印文本链接，不生成 HTML
"""

import argparse
import json
import sys
from html import escape

import psycopg2
import psycopg2.extras

import config

MARKET_URL = 'https://polymarket.com/market/{}'
EVENT_URL = 'https://polymarket.com/event/{}'


def db_query(sql: str, params: tuple = None) -> list:
    conn = psycopg2.connect(config.PG_DSN)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def load_markets(limit: int, keyword: str, event_id: str = None, event_slug: str = None) -> list:
    conds, params = [], []
    if event_id:
        conds.append('m.event_id = %s')
        params.append(str(event_id))
    if event_slug:
        conds.append('e.slug = %s')
        params.append(event_slug)
    where = f'WHERE {" AND ".join(conds)}' if conds else ''
    # 按事件过滤时尽量全列（事件选项通常 < 1000），其余场景按 limit
    eff_limit = 1000 if (event_id or event_slug) else limit
    params.append(eff_limit)
    sql = f"""
        SELECT m.condition_id, m.slug, m.question, m.volume, m.closed,
               m.raw_json->>'clobTokenIds' AS tokens_json,
               e.title AS event_title, e.slug AS event_slug, e.id AS event_id
        FROM markets m
        LEFT JOIN events e ON m.event_id = e.id::text
        {where}
        ORDER BY e.title NULLS LAST, m.volume DESC NULLS LAST
        LIMIT %s
    """
    rows = db_query(sql, tuple(params))
    if keyword:
        kw = keyword.lower()
        rows = [r for r in rows if kw in (r.get('question') or '').lower()
                or kw in (r.get('slug') or '').lower()
                or kw in (r.get('event_title') or '').lower()]
    return rows


def load_orderbook_tokens() -> dict:
    """orderbook 有数据的 token_id -> 最新快照时间"""
    rows = db_query(
        """
        SELECT DISTINCT ON (token_id) token_id, snapshot_at
        FROM orderbook ORDER BY token_id, snapshot_at DESC
        """
    )
    return {r['token_id']: r['snapshot_at'] for r in rows}


def load_latest_prices() -> dict:
    """token_id -> 最新价格（price_history 各 token 最新点）"""
    rows = db_query(
        """
        SELECT DISTINCT ON (token_id) token_id, p, t
        FROM price_history ORDER BY token_id, t DESC
        """
    )
    return {r['token_id']: {'p': float(r['p']), 't': r['t']} for r in rows}


def parse_tokens(tokens_json: str) -> list:
    if not tokens_json:
        return []
    try:
        v = json.loads(tokens_json)
        return v if isinstance(v, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def fmt_ts(ts) -> str:
    if not ts:
        return '-'
    return str(ts).split('.')[0].replace('T', ' ')


def render_html(markets: list, has_snap: dict, prices: dict) -> str:
    rows = []
    for m in markets:
        tokens = parse_tokens(m.get('tokens_json'))
        yes_tok = tokens[0] if tokens else None
        yes_price = prices.get(yes_tok, {}).get('p') if yes_tok else None
        snap = None
        for t in tokens:
            ts = has_snap.get(t)
            if ts and (snap is None or ts > snap):
                snap = ts
        mkt_url = MARKET_URL.format(m['slug']) if m.get('slug') else ''
        evt_url = EVENT_URL.format(m['event_slug']) if m.get('event_slug') else ''
        question = escape(m.get('question') or '-')
        mkt_link = (f'<a href="{mkt_url}" target="_blank">打开盘口 ↗</a>'
                    if mkt_url else '-')
        evt_link = (f'<a href="{evt_url}" target="_blank">事件页 ↗</a>'
                    if evt_url else '-')
        price_str = f'{yes_price:.4f}' if yes_price is not None else '-'
        evt_cell = (f'<td class="evt">{escape(m.get("event_title") or "-")}<br>{evt_link}</td>'
                    if m.get('event_title') else f'<td class="evt">-</td>')
        rows.append(
            f'<tr>'
            f'<td class="q">{question}</td>'
            f'{evt_cell}'
            f'<td>{mkt_link}</td>'
            f'<td class="num">{float(m["volume"] or 0):,.0f}</td>'
            f'<td class="num">{price_str}</td>'
            f'<td class="num">{fmt_ts(snap)}</td>'
            f'<td class="cid">{m.get("condition_id", "")[:16]}…</td>'
            f'</tr>'
        )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>Polymarket 盘口链接（数据库 → 官网）</title>
<style>
  body {{ font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; margin: 24px; background: #f7f8fa; }}
  h1 {{ font-size: 20px; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  th, td {{ padding: 8px 12px; border-bottom: 1px solid #eee; text-align: left; font-size: 13px; }}
  th {{ background: #2d3748; color: #fff; position: sticky; top: 0; }}
  tr:hover {{ background: #f0f6ff; }}
  td.q {{ min-width: 320px; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.cid {{ color: #999; font-size: 12px; }}
  td.evt {{ color: #555; font-size: 12px; min-width: 180px; }}
  a {{ color: #2563eb; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .meta {{ color: #666; margin-bottom: 12px; }}
</style>
</head>
<body>
<h1>Polymarket 盘口链接（点击直达官网验证）</h1>
<div class="meta">共 {len(markets)} 个市场 · 官网"一个盘口多个选项"= 事件下多个市场（已按事件分组排列）· 市场页切 <b>Orderbook</b> 标签核对深度 · Yes 价为 price_history 最新点</div>
<table>
<tr><th>市场问题</th><th>所属事件（官网多选项盘口）</th><th>官网盘口</th><th>成交量</th><th>Yes 最新价</th><th>盘口快照</th><th>condition_id</th></tr>
{''.join(rows)}
</table>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser(description='从 PG 导出 Polymarket 官网盘口链接')
    ap.add_argument('--limit', type=int, default=100, help='最多列出 N 个市场（默认 100）')
    ap.add_argument('--keyword', default='', help='按市场问题/事件标题关键词过滤')
    ap.add_argument('--event-id', default='', help='只列指定事件 id 下的所有选项市场')
    ap.add_argument('--event-slug', default='', help='只列指定事件 slug 下的所有选项市场')
    ap.add_argument('--orderbook-only', action='store_true', help='只列出有盘口快照数据的市场')
    ap.add_argument('--no-html', action='store_true', help='不生成 HTML，只打印文本链接')
    args = ap.parse_args()

    markets = load_markets(args.limit, args.keyword,
                           event_id=args.event_id or None,
                           event_slug=args.event_slug or None)
    has_snap = load_orderbook_tokens()
    prices = load_latest_prices()

    if args.orderbook_only:
        markets = [m for m in markets
                   if any(t in has_snap for t in parse_tokens(m.get('tokens_json')))]

    if not markets:
        print('无匹配市场')
        sys.exit(0)

    print(f'共 {len(markets)} 个市场，官网链接规则: https://polymarket.com/market/{{slug}}')
    if not args.no_html:
        path = 'market_links.html'
        with open(path, 'w', encoding='utf-8') as f:
            f.write(render_html(markets, has_snap, prices))
        print(f'已生成可点击链接页: {path}（浏览器打开，点击"打开盘口"直达官网）')
    for m in markets[:20]:
        if m.get('slug'):
            print(f'  {MARKET_URL.format(m["slug"])}  # {m.get("question", "")[:60]}')


if __name__ == '__main__':
    main()
