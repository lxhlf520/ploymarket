# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gamma API 全模块采集（分类 / 事件列表 / 盘口详情 / Rules 澄清 / 评论）

各阶段断点续传：
- 事件+市场:  scope='markets:{closed}'，state 存最后 offset；
              tags 补全 scope='event_tags'，state 存最后 slug 游标
- 详情补拉:  scope='market_details'，state 存最后 condition_id
- 澄清:      scope='clarif'，state 存最后 condition_id
- 评论:      scope='comments:{event_id}'，完成标记 done；事件队列游标 scope='comments_resume'

实测接口约束（2026-08 实测，以此为准）：
- /events/keyset 的 offset 参数一律 422（offset not allowed），next_cursor 失效（返回同页）
- /events 的 offset 上限 2000，超限 422（提示改用 keyset）——两接口均无法全量翻页
- /markets 的 offset 上限 2000（任何参数组合），超限 422；
  全量市场覆盖主路径：/events/keyset?slug= 批量（≤100）遍历全部事件，
  展开事件嵌套 markets（含已结算选项）入库，另补 tags/最新元数据
- /markets 的 condition_id 参数被忽略，详情只能按 slug 单查
- /market-clarifications 只接受市场数字 id（raw_json.id）

评论登录态：未登录时每页仅返回 10 条且翻页受限，
需先 export_cookie.py 导出浏览器 Cookie 到 cookies.json 后采集。
"""

import asyncio
import logging
import time

import httpx
from tqdm import tqdm

import config
import db_pg
from gamma_client import GammaAPIClient

logger = logging.getLogger(__name__)

DETAIL_BATCH = 100    # 详情并发批量
CLARIF_BATCH = 100    # 澄清并发批量
COMMENTS_BATCH = 200  # 评论入库批量（页）


async def _flatten_market_events(markets: list) -> tuple:
    """从 /markets 列表提取嵌套 events（反推事件）并补齐 eventId，返回 (markets, events)。
    嵌套 events 无 tags/嵌套 markets 字段，tags 由 Stage B 批量 slug 补全。"""
    events = []
    seen = set()
    for m in markets:
        evs = m.get('events') or []
        if not m.get('eventId') and evs:
            m['eventId'] = evs[0].get('id')
        for ev in evs:
            eid = ev.get('id')
            if eid and eid not in seen:
                seen.add(eid)
                events.append(ev)
    return markets, events


async def scrape_tags() -> list:
    """拉全部分类清单（驱动 keyset 采集，不入库）"""
    tags = []
    offset = 0
    async with GammaAPIClient() as api:
        while True:
            page = await api.get_tags(limit=config.TAGS_PAGE_SIZE, offset=offset)
            if not page:
                break
            tags.extend(page)
            if len(page) < config.TAGS_PAGE_SIZE:
                break
            offset += len(page)
    logger.info('分类清单共 %s 个', len(tags))
    return tags


async def scrape_events(closed: bool = None, limit: int = None) -> dict:
    """全量事件 + 市场采集（实测接口约束下的可行方案）：

    Stage A: /markets 全量双桶（offset 翻页，上限 60k > 存量 65,893 市场）
             市场入库，嵌套 events 反推事件入库（无 tags）
    Stage B: /events/keyset?slug= 批量（≤100 个/次）拉完整事件，补全 tags
             与最新元数据（/events 的 offset 上限 2000、keyset 游标失效，
             唯此路径可 100% 覆盖事件）
    """
    await db_pg.init_schema()
    stats = {'inserted': 0, 'updated': 0, 'events': 0, 'markets': 0, 'failed': 0}
    async with GammaAPIClient() as api:
        # ---------- Stage A: /markets 全量双桶 ----------
        flags = [closed] if closed is not None else [False, True]
        for closed_flag in flags:
            scope = f'markets:{1 if closed_flag else 0}'
            row = await db_pg.get_scope_state(scope)
            offset = int(row['state']) if row and str(row['state']).isdigit() else 0
            if row and row['state'] == 'done':
                logger.info('跳过已完成: %s', scope)
                continue
            truncated = False
            with tqdm(desc=f'markets closed={closed_flag}', unit='mkt') as pbar:
                while True:
                    try:
                        page = await api.get_markets_page({
                            'limit': config.MARKETS_PAGE_SIZE,
                            'offset': offset,
                            'closed': closed_flag,
                            'order': 'volume',
                            'ascending': False,
                        })
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 422:
                            # offset 超接口上限 2000：该桶热门前段已采完，
                            # 剩余（含已结算选项）由 Stage B keyset 全量补齐
                            logger.warning('%s offset=%s 超 /markets 上限 2000，'
                                           '该桶结束，全量由 Stage B keyset 补齐', scope, offset)
                            break
                        raise
                    if not page:
                        break
                    mkt_rows, ev_rows = await _flatten_market_events(page)
                    ins_m, upd_m = await db_pg.upsert_markets(mkt_rows)
                    ins_e, upd_e = await db_pg.upsert_events(ev_rows)
                    stats['inserted'] += ins_e + ins_m
                    stats['updated'] += upd_e + upd_m
                    stats['events'] += len(ev_rows)
                    stats['markets'] += len(mkt_rows)
                    offset += len(page)
                    await db_pg.set_scope_state(scope, str(offset))
                    pbar.update(len(page))
                    pbar.set_postfix(events=stats['events'], markets=stats['markets'])
                    if len(page) < config.MARKETS_PAGE_SIZE:
                        break
                    if limit and offset >= limit:
                        truncated = True
                        break
            if not truncated:
                await db_pg.mark_scope_done(scope, offset)
            logger.info('%s 完成: %s 市场%s', scope, offset,
                        '（limit 截断，未标记完成）' if truncated else '')

        # ---------- Stage B: 事件 slug 批量补 tags ----------
        row = await db_pg.get_scope_state('event_tags')
        after = row['state'] if row and row['state'] != 'done' else ''
        if row and row['state'] == 'done':
            logger.info('跳过已完成: event_tags')
        else:
            truncated = False
            sb_done = 0
            with tqdm(desc='event tags', unit='evt') as pbar:
                while True:
                    slugs = await db_pg.get_event_slugs(after, config.CLARIFICATIONS_BATCH)
                    if not slugs:
                        break
                    evs, _ = await api.get_events_keyset({'limit': len(slugs), 'slug': slugs})
                    # 展开嵌套 markets（keyset 事件含完整市场列表，含已结算选项）——
                    # 全量市场覆盖主路径（/markets offset 仅 2000）；
                    # 按 conditionId 去重，避免 ON CONFLICT 批内重复；
                    # 跳过 conditionId 为空的影子市场（negRisk 体育子市场，
                    # 无独立盘口/volume，/markets 亦查不到，无可入库唯一键）
                    mkt_rows = []
                    seen_m = set()
                    skip_shadow = 0
                    for ev in evs:
                        for m in ev.get('markets') or []:
                            if not isinstance(m, dict):
                                continue
                            m = dict(m)
                            if not m.get('conditionId'):
                                skip_shadow += 1
                                continue
                            if m['conditionId'] in seen_m:
                                continue
                            seen_m.add(m['conditionId'])
                            if not m.get('eventId'):
                                m['eventId'] = ev.get('id')
                            mkt_rows.append(m)
                    if skip_shadow:
                        logger.info('本次跳过无 conditionId 影子市场 %s 个', skip_shadow)
                    if mkt_rows:
                        ins_m, upd_m = await db_pg.upsert_markets(mkt_rows)
                        stats['inserted'] += ins_m
                        stats['updated'] += upd_m
                        stats['markets'] += len(mkt_rows)
                    ins, upd = await db_pg.upsert_events(evs)
                    stats['inserted'] += ins
                    stats['updated'] += upd
                    stats['events'] += len(evs)
                    sb_done += len(evs)
                    after = slugs[-1]
                    await db_pg.set_scope_state('event_tags', after)
                    pbar.update(len(slugs))
                    pbar.set_postfix(updated=upd)
                    if limit and sb_done >= limit:
                        truncated = True
                        break
            if not truncated:
                await db_pg.mark_scope_done('event_tags', stats['events'])
    return stats


async def scrape_market_details(limit: int = None, refresh_all: bool = False) -> dict:
    """补拉缺失详情的存量市场（缺 clobTokenIds/description），按 slug 单查 /markets。
    实测 /markets 的 condition_id 参数被忽略，只能按 slug 查询。
    refresh_all=True 时遍历全量市场刷新最新状态。"""
    await db_pg.init_schema()
    row = await db_pg.get_scope_state('market_details')
    after = row['state'] if row and row['state'] != 'done' else ''
    processed = int(row['fetched_count'] or 0) if row else 0

    stats = {'inserted': 0, 'updated': 0, 'checked': 0, 'failed': 0, 'skipped': 0}
    async with GammaAPIClient() as api:
        with tqdm(desc='market details', unit='mkt') as pbar:
            truncated = False
            while True:
                if refresh_all:
                    items = await db_pg.get_market_ids_page(after, DETAIL_BATCH)
                else:
                    items = await db_pg.get_markets_for_detail(after, DETAIL_BATCH)
                if not items:
                    break
                # 无 slug 的无法单查，跳过并续传游标
                todo = [m for m in items if m.get('slug')]
                skipped = len(items) - len(todo)
                results = await asyncio.gather(
                    *[api.get_market(slug=m['slug']) for m in todo],
                    return_exceptions=True,
                )
                batch = []
                for m, res in zip(todo, results):
                    if isinstance(res, Exception):
                        stats['failed'] += 1
                        logger.warning('市场 %s 详情失败: %s', m['slug'], res)
                        continue
                    if isinstance(res, dict):
                        batch.append(res)
                    elif isinstance(res, list) and res:
                        batch.append(res[0])
                for m in items:
                    after = m['condition_id']
                if batch:
                    ins, upd = await db_pg.upsert_markets(batch)
                    stats['inserted'] += ins
                    stats['updated'] += upd
                processed += len(items)
                stats['checked'] += len(items)
                stats['skipped'] += skipped
                pbar.update(len(items))
                pbar.set_postfix(updated=stats['updated'], failed=stats['failed'])
                await db_pg.set_scope_state('market_details', after, processed)
                if limit and stats['checked'] >= limit:
                    truncated = True
                    break
    if not truncated:
        await db_pg.mark_scope_done('market_details', processed)
    return stats


async def scrape_clarifications(limit: int = None) -> dict:
    """对每个 market 拉 /market-clarifications（Rules），空数组也标记已处理。
    实测接口只接受市场数字 id（raw_json.id），不用 condition_id。"""
    await db_pg.init_schema()
    row = await db_pg.get_scope_state('clarif')
    after = row['state'] if row and row['state'] != 'done' else ''
    processed = int(row['fetched_count'] or 0) if row else 0

    stats = {'inserted': 0, 'updated': 0, 'checked': 0, 'failed': 0}
    async with GammaAPIClient() as api:
        with tqdm(desc='clarifications', unit='mkt') as pbar:
            truncated = False
            while True:
                items = await db_pg.get_market_ids_page(after, CLARIF_BATCH)
                if not items:
                    break
                results = await asyncio.gather(
                    *[api.get_clarifications(m['market_id']) for m in items],
                    return_exceptions=True,
                )
                batch = []
                for m, res in zip(items, results):
                    if isinstance(res, Exception):
                        stats['failed'] += 1
                        logger.warning('市场 %s 澄清失败: %s', m['market_id'], res)
                        continue
                    for cl in res:
                        cl.setdefault('market_id', m['market_id'])
                        batch.append(cl)
                    after = m['condition_id']
                if batch:
                    ins, upd = await db_pg.upsert_clarifications(batch)
                    stats['inserted'] += ins
                    stats['updated'] += upd
                processed += len(items)
                stats['checked'] += len(items)
                pbar.update(len(items))
                pbar.set_postfix(clarif=stats['inserted'], failed=stats['failed'])
                await db_pg.set_scope_state('clarif', after, processed)
                if limit and stats['checked'] >= limit:
                    truncated = True
                    break
    if not truncated:
        await db_pg.mark_scope_done('clarif', processed)
    return stats


async def _scrape_event_comments(api: GammaAPIClient, event_id: str) -> tuple:
    """单事件评论全量翻页入库，返回 (inserted, updated)。"""
    batch = []
    total = 0

    async def _flush():
        nonlocal batch
        if batch:
            ins, upd = await db_pg.upsert_comments(batch)
            batch = []
            return ins, upd
        return 0, 0

    inserted = updated = 0
    offset = 0
    while True:
        page = await api.get_comments(
            'Event', event_id, limit=config.COMMENTS_LIMIT, offset=offset)
        if not page:
            break
        for c in page:
            c.setdefault('event_id', str(event_id))
            c.setdefault('parent_entity_type', 'Event')
            batch.append(c)
        total += len(page)
        if len(batch) >= config.COMMENTS_LIMIT * COMMENTS_BATCH:
            ins, upd = await _flush()
            inserted += ins
            updated += upd
        if len(page) < config.COMMENTS_LIMIT:
            break
        offset += len(page)
        # 防死循环：同一 offset 重复返回时中止
        if offset > 100_000:
            logger.warning('事件 %s 评论 offset 超限，提前中止', event_id)
            break
    ins, upd = await _flush()
    inserted += ins
    updated += upd
    return inserted, updated, total


async def scrape_comments(event_id: str = None, limit: int = None,
                          min_comments: int = config.MIN_COMMENT_COUNT) -> dict:
    """全量事件评论采集（需登录态翻页）。event_id 定向单事件；
    否则按 events 表 comment_count>=min_comments 队列续传。"""
    await db_pg.init_schema()
    stats = {'inserted': 0, 'updated': 0, 'events': 0, 'comments': 0, 'failed': 0}

    async with GammaAPIClient() as api:
        logged_in = await api.is_logged_in()
        logger.info('登录态探测: %s%s', logged_in,
                    '' if logged_in else '（未登录，评论每页仅 10 条，建议先 export_cookie.py）')
        if not logged_in and not event_id:
            logger.warning('未登录时评论翻页受限，继续尝试采集（可能不完整）')

        if event_id:
            event_ids = [str(event_id)]
            done_scopes = set()
        else:
            done_scopes = await db_pg.get_done_scopes('comments:')
            row = await db_pg.get_scope_state('comments_resume')
            after = row['state'] if row and row['state'] != 'done' else ''
            event_ids = []
            while True:
                page = await db_pg.get_events_for_comments(after, 1000, min_comments)
                if not page:
                    break
                for ev in page:
                    if f'comments:{ev["id"]}' not in done_scopes:
                        event_ids.append(ev['id'])
                after = page[-1]['id']
                if len(page) < 1000:
                    break
            logger.info('评论待采集事件 %s 个', len(event_ids))
            if limit:
                event_ids = event_ids[:limit]

        with tqdm(total=len(event_ids), desc='comments', unit='evt') as pbar:
            for eid in event_ids:
                scope = f'comments:{eid}'
                try:
                    ins, upd, total = await _scrape_event_comments(api, eid)
                    stats['inserted'] += ins
                    stats['updated'] += upd
                    stats['comments'] += total
                    stats['events'] += 1
                    await db_pg.mark_scope_done(scope, total)
                except Exception as exc:
                    stats['failed'] += 1
                    logger.warning('事件 %s 评论采集失败: %s', eid, exc)
                finally:
                    pbar.update(1)
                    pbar.set_postfix(comments=stats['comments'], failed=stats['failed'])
                    await db_pg.set_scope_state('comments_resume', eid, stats['events'])
    return stats


async def run_stage(stage: str, limit: int = None, **kw) -> dict:
    """main_collect 调用入口：stage ∈ tags|events|details|clarifications|comments"""
    t0 = time.time()
    if stage == 'tags':
        tags = await scrape_tags()
        return {'tags': len(tags), '耗时秒': round(time.time() - t0, 1)}
    if stage == 'events':
        return {**await scrape_events(closed=kw.get('closed'),
                                      limit=limit),
                '耗时秒': round(time.time() - t0, 1)}
    if stage == 'details':
        return {**await scrape_market_details(limit=limit,
                                              refresh_all=kw.get('refresh_all', False)),
                '耗时秒': round(time.time() - t0, 1)}
    if stage == 'clarifications':
        return {**await scrape_clarifications(limit=limit), '耗时秒': round(time.time() - t0, 1)}
    if stage == 'comments':
        return {**await scrape_comments(event_id=kw.get('event_id'),
                                        limit=limit,
                                        min_comments=kw.get('min_comments',
                                                            config.MIN_COMMENT_COUNT)),
                '耗时秒': round(time.time() - t0, 1)}
    raise ValueError(f'未知 stage: {stage}')
