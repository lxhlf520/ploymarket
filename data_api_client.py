# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Data API 客户端 - 交易与活动流采集（data-api.polymarket.com）

实测接口约束（以此为准）：
- /trades:   limit<=10000, offset<=10000, 支持 market/user/end 过滤
- /activity: limit<=500,   offset<=5000,  支持 user/end 过滤（含 usdcSize、type）
- 两接口默认按时间戳倒序；offset 超出上限返回 400
- 深翻页策略：end 时间窗口 + 窗口内 offset，边界时间戳行数超过上限时前移 end

限流：Cloudflare 先减速后 429（无 Retry-After），慢响应视为成功不重试；
Data API /trades+/activity 共享预算 200 次/10 秒，用全局令牌桶对齐。
"""

import asyncio
import logging
import time

import httpx

import config

logger = logging.getLogger(__name__)


class TokenBucket:
    """全局令牌桶限流：容量 200，补充速率 20/秒（对齐 200 req/10s）"""

    def __init__(self, capacity: int = 200, rate_per_sec: float = 20.0):
        self.capacity = float(capacity)
        self.rate = rate_per_sec
        self._tokens = float(capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait)


class DataAPIClient:
    """封装 Data API 的 /trades 与 /activity 全量拉取"""

    # 各端点的 offset 上限（窗口内翻页保护）
    MAX_OFFSETS = {'/trades': 10000, '/activity': 5000}

    def __init__(self, base_url: str = None, bucket: TokenBucket = None,
                 proxy: str = None, on_request=None, on_rate_limited=None):
        self.base_url = base_url or config.DATA_API_BASE
        self.bucket = bucket or TokenBucket()
        # 可选回调（async fn）：on_request 每次请求前调用（节点轮换计数/暂停等待）；
        # on_rate_limited 遇 429/403 时调用（限流自动切节点），不影响既有重试逻辑
        self.on_request = on_request
        self.on_rate_limited = on_rate_limited
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.DATA_TIMEOUT, connect=15),
            headers=config.HEADERS,
            proxy=proxy,
            limits=httpx.Limits(
                max_connections=config.TRADES_CONCURRENCY + 8,
                max_keepalive_connections=config.TRADES_CONCURRENCY,
            ),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> 'DataAPIClient':
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def _get_page(self, path: str, params: dict) -> list:
        """带限流与退避重试的单页 GET，返回 JSON 列表"""
        backoff = config.RETRY_BACKOFF
        for attempt in range(config.MAX_RETRIES + 1):
            await self.bucket.acquire()
            if self.on_request:
                await self.on_request()
            try:
                resp = await self._client.get(f'{self.base_url}{path}', params=params)
            except httpx.HTTPError as exc:
                # 连接级错误通常是出口节点不可用：也通知轮换器（累计达阈值自动切换）
                if self.on_rate_limited:
                    await self.on_rate_limited(0)
                logger.warning('%s 请求异常(第%s次): %s', path, attempt + 1, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code == 400:
                raise RuntimeError(f'{path} 参数被拒: {resp.text[:200]}')
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if resp.status_code == 429 and self.on_rate_limited:
                    await self.on_rate_limited(429)
                logger.warning('%s HTTP %s(第%s次)，退避 %.1fs', path, resp.status_code, attempt + 1, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code == 403:
                # CF 按 IP 拦截：通知轮换器切节点后照常抛异常，任务回 pending 换 IP 重试
                if self.on_rate_limited:
                    await self.on_rate_limited(403)
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f'{path} 重试 {config.MAX_RETRIES} 次后仍失败: {params}')

    async def fetch_all(self, path: str, base_params: dict, page_size: int,
                        on_batch=None) -> int:
        """end 时间窗口 + 窗口内 offset 的全量翻页拉取，返回总行数。

        on_batch: 可选回调 async fn(list[dict])，每页拉取后调用（用于分批入库）。
        """
        max_offset = self.MAX_OFFSETS.get(path, 10000)
        total = 0
        end = None
        offset = 0
        guard = 0
        while True:
            params = dict(base_params)
            params['limit'] = page_size
            params['offset'] = offset
            if end is not None:
                params['end'] = end

            page = await self._get_page(path, params)
            if not page:
                break
            total += len(page)
            if on_batch is not None:
                await on_batch(page)

            if len(page) < page_size:
                break

            oldest = min(int(r['timestamp']) for r in page)
            if end is not None and oldest >= end:
                # 边界时间戳还有更多行：窗口内用 offset 继续翻
                offset += page_size
                guard += 1
                if offset > max_offset or guard > 50:
                    # 极端情况（同一秒行数超过上限）：前移 end 放弃该时间戳剩余行
                    end = oldest - 1
                    offset = 0
                    guard = 0
            else:
                end = oldest
                offset = 0
                guard = 0
        return total

    async def fetch_market_trades(self, condition_id: str, on_batch=None) -> int:
        """拉取单个市场全部历史交易"""
        return await self.fetch_all(
            '/trades', {'market': condition_id},
            config.TRADES_PAGE_SIZE, on_batch=on_batch,
        )

    async def fetch_user_activity(self, wallet: str, on_batch=None) -> int:
        """拉取单个用户全部活动流（含 TRADE/REDEEM/MERGE/SPLIT/REWARD/CONVERSION）"""
        return await self.fetch_all(
            '/activity', {'user': wallet},
            config.ACTIVITY_PAGE_SIZE, on_batch=on_batch,
        )

    async def fetch_holders(self, market: str, on_batch=None) -> int:
        """拉取单个市场全部 Top Holders。

        实测响应为列表 [{token, holders: [...]}]（按 token 分组），
        展平为每行带 token_id 的 holder 记录后回调 on_batch。
        """
        offset = 0
        total = 0
        guard = 0
        while True:
            data = await self._get_page('/holders', {
                'market': market,
                'limit': config.HOLDERS_PAGE_SIZE,
                'offset': offset,
            })
            if not data:
                break
            rows = []
            for group in data:
                tid = group.get('token')
                for h in group.get('holders') or []:
                    h['token_id'] = tid
                    rows.append(h)
            if on_batch and rows:
                await on_batch(rows)
            total += len(rows)
            guard += 1
            if len(rows) < config.HOLDERS_PAGE_SIZE or guard > 2000:
                break
            offset += config.HOLDERS_PAGE_SIZE
        return total

    async def fetch_market_positions(self, market: str, on_batch=None) -> int:
        """拉取单个市场持仓快照（每 token 组 Top limit 条，offset 实测无效无法翻页）。
    
        实测响应 [{token, positions: [...]}]（按 token 分组），offset 参数被忽略
        （多次请求返回相同数据），故只拉 1 页即可，展平后补 condition_id/token_id。
        """
        data = await self._get_page('/v1/market-positions', {
            'market': market,
            'limit': config.POSITIONS_PAGE_SIZE,
            'offset': 0,
            'status': 'ALL',
        })
        if isinstance(data, dict):
            data = data.get('data') or []
        if not data:
            return 0
        rows = []
        for group in data:
            token_id = group.get('token')
            for pos in group.get('positions') or []:
                pos['condition_id'] = market
                if not pos.get('token_id'):
                    pos['token_id'] = token_id
                rows.append(pos)
        if on_batch and rows:
            await on_batch(rows)
        return len(rows)

    async def fetch_event_trades(self, event_id, on_batch=None) -> int:
        """拉取单个事件全部 CASH 交易（Activity 维度）。

        实测 /trades?eventId= 响应行无 eventId 字段（仅 eventSlug），
        入库前统一补充 eventId。沿用 end 窗口翻页突破 offset 上限。
        """
        async def _wrap(page):
            for r in page:
                if r.get('eventId') is None:
                    r['eventId'] = str(event_id)
            if on_batch:
                await on_batch(page)

        return await self.fetch_all(
            '/trades', {'eventId': event_id, 'filterType': 'CASH'},
            config.TRADES_PAGE_SIZE, on_batch=_wrap,
        )

    async def probe_page(self, path: str, params: dict) -> list:
        """拉取单个页面（--dry-run 估算用），返回行列表"""
        return await self._get_page(path, params)
