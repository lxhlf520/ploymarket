# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gamma API 客户端（gamma-api.polymarket.com）

覆盖全模块采集所需端点（侦察实测约束）：
- /tags                        分类清单（limit/offset 翻页）
- /events/keyset               事件列表（tag_slug/closed/order/ascending + offset 翻页，
                               next_cursor 实测服务端失效，不依赖游标）
- /markets                     单盘口详情（slug= / condition_id=）与全量列表（limit<=100）
- /events/{id}、/events/slug/{slug}  事件详情（含嵌套 markets）
- /market-clarifications       Rules 澄清（按 market_id）
- /comments                    评论（parent_entity_type=Event/Market，登录态才可全量翻页）
- /is-logged-in                登录状态探测

限流：与 Data API 同为 Cloudflare 保护，沿用 200 次/10s 令牌桶 + 指数退避；
登录 Cookie 从 cookies.json 读取注入（export_cookie.py 导出）。
"""

import asyncio
import json
import logging
import os

import httpx

import config
from data_api_client import TokenBucket

logger = logging.getLogger(__name__)


def load_cookie_header(cookie_file: str = None) -> str:
    """从 cookies.json 读取登录态并拼 Cookie header，支持 dict / [{name,value}] 两种格式"""
    path = cookie_file or config.COOKIE_FILE
    if not os.path.exists(path):
        return ''
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning('Cookie 文件读取失败: %s', exc)
        return ''
    if isinstance(data, dict):
        return '; '.join(f'{k}={v}' for k, v in data.items())
    if isinstance(data, list):
        pairs = []
        for item in data:
            if isinstance(item, dict) and item.get('name') is not None:
                pairs.append(f"{item['name']}={item.get('value', '')}")
        return '; '.join(pairs)
    return ''


class GammaAPIClient:
    """封装 Gamma API 各端点，带限流/重试/登录态注入"""

    def __init__(self, base_url: str = None, cookie_file: str = None):
        self.base_url = base_url or config.GAMMA_API_BASE
        self.bucket = TokenBucket(capacity=config.TRADES_RATE_LIMIT)
        headers = dict(config.HEADERS)
        cookie = load_cookie_header(cookie_file)
        if cookie:
            headers['Cookie'] = cookie
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.DATA_TIMEOUT, connect=15),
            headers=headers,
            limits=httpx.Limits(
                max_connections=config.TRADES_CONCURRENCY + 8,
                max_keepalive_connections=config.TRADES_CONCURRENCY,
            ),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> 'GammaAPIClient':
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def _get(self, path: str, params: dict = None) -> object:
        """带限流与退避重试的单次 GET，返回解析后的 JSON"""
        backoff = config.RETRY_BACKOFF
        for attempt in range(config.MAX_RETRIES + 1):
            await self.bucket.acquire()
            try:
                resp = await self._client.get(f'{self.base_url}{path}', params=params)
            except httpx.HTTPError as exc:
                logger.warning('%s 请求异常(第%s次): %s', path, attempt + 1, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code in (401, 403):
                raise PermissionError(f'{path} 鉴权失败 HTTP {resp.status_code}: {resp.text[:200]}')
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                logger.warning('%s HTTP %s(第%s次)，退避 %.1fs', path, resp.status_code, attempt + 1, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f'{path} 重试 {config.MAX_RETRIES} 次后仍失败: {params}')

    # ---------- 分类 / 事件列表 ----------

    async def get_tags(self, limit: int = 100, offset: int = 0) -> list:
        """分类清单单页"""
        return await self._get('/tags', {'limit': limit, 'offset': offset})

    async def get_events_keyset(self, params: dict) -> tuple:
        """/events/keyset 单页，返回 (events, next_cursor)。

        实测约束：offset 参数一律 422（offset not allowed on keyset endpoints），
        next_cursor 失效（返回同页）；仅批量 slug 查询（params 传 slug 列表）可靠。
        """
        data = await self._get('/events/keyset', params)
        if isinstance(data, list):
            return data, None
        return data.get('events', []), data.get('next_cursor')

    async def get_event(self, event_id) -> dict:
        """事件详情（含嵌套 markets）"""
        return await self._get(f'/events/{event_id}')

    async def get_event_by_slug(self, slug: str) -> dict:
        """按 slug 取事件详情"""
        return await self._get(f'/events/slug/{slug}')

    # ---------- 盘口 ----------

    async def get_market(self, slug: str = None, condition_id: str = None) -> dict:
        """单盘口详情，slug 或 condition_id 二选一（缺失时返回 None）"""
        if slug:
            return await self._get('/markets', {'slug': slug})
        if condition_id:
            return await self._get('/markets', {'condition_id': condition_id})
        return None

    async def get_markets_page(self, params: dict) -> list:
        """/markets 列表单页（limit<=100）"""
        return await self._get('/markets', params)

    async def get_clarifications(self, market_id: str) -> list:
        """Rules 澄清（可为空数组）"""
        data = await self._get('/market-clarifications', {'market_id': market_id})
        return data if isinstance(data, list) else []

    # ---------- 评论 / 登录态 ----------

    async def get_comments(self, parent_entity_type: str, parent_entity_id,
                           limit: int = 50, offset: int = 0) -> list:
        """评论单页（parent_entity_type: Event/Market）"""
        data = await self._get('/comments', {
            'parent_entity_type': parent_entity_type,
            'parent_entity_id': parent_entity_id,
            'limit': limit,
            'offset': offset,
            'order': 'createdAt',
        })
        return data if isinstance(data, list) else []

    async def is_logged_in(self) -> bool:
        """登录态探测（401 未登录 / 200 已登录）"""
        try:
            resp = await self._client.get(f'{self.base_url}/is-logged-in')
            return resp.status_code == 200
        except httpx.HTTPError:
            return False
