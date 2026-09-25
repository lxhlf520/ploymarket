# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Data API 客户端 - 交易与活动流采集（data-api.polymarket.com）

v1 于 2026-10-24 退役，当前默认走 v2（设 DATA_API_V2=0 临时回退 v1）：
- v2: /v2/trades（condition/event_id 批量 ≤20）、/v2/activity（user 锚定）、
      /v2/holders、/v2/positions；cursor 翻页（opaque，翻页请求必须重复携带
      过滤参数）；snake_case 行 + {data, pagination} envelope
- v1: /trades（market/eventId）、/activity、/holders、/v1/market-positions；
      end 窗口 + offset 翻页，行字段 camelCase，offset 超上限 400

v2 口径实测：默认参数（taker_only=true、filter_type=TOKENS、filter_amount=0.01）
与 v1 逐行一致（行键 overlap 100%）；单页上限 trades/activity 均 1000。
行统一归一化为 v1 风格 camelCase（token_id → asset），下游零改动。

限流：Cloudflare 先减速后 429（无 Retry-After），慢响应视为成功不重试；
Data API /trades+/activity 共享预算 200 次/10 秒，用全局令牌桶对齐。
"""

import asyncio
import logging
import time

import httpx

import config

logger = logging.getLogger(__name__)

# v2 行字段映射（snake_case → camelCase，与 v1 行对齐，下游 db_pg 零改动）
# 特例：trades/activity 行的 token_id ↔ v1 asset；holders/positions 保持 token_id
V2_FIELD_MAP = {
    'transaction_hash': 'transactionHash',
    'condition_id': 'conditionId',
    'proxy_wallet': 'proxyWallet',
    'outcome_index': 'outcomeIndex',
    'profile_image': 'profileImage',
    'profile_image_optimized': 'profileImageOptimized',
    'event_slug': 'eventSlug',
    'usdc_size': 'usdcSize',
}


def norm_v2_row(row: dict, token_as_asset: bool = False) -> dict:
    """v2 行归一化：snake_case → camelCase（保持与 v1 行结构一致）。

    token_as_asset: trades/activity 行的 token_id 对应 v1 asset 字段时置 True。
    """
    out = {}
    for k, v in row.items():
        if k == 'token_id':
            out['asset' if token_as_asset else 'token_id'] = v
        else:
            out[V2_FIELD_MAP.get(k, k)] = v
    return out


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
        last_err = '未知'   # 记录最后一次失败根因，重试耗尽后带入异常方便定位
        for attempt in range(config.MAX_RETRIES + 1):
            await self.bucket.acquire()
            if self.on_request:
                await self.on_request()
            try:
                resp = await self._client.get(f'{self.base_url}{path}', params=params)
            except httpx.HTTPError as exc:
                # 连接级错误通常是出口节点不可用：也通知轮换器（累计达阈值自动切换）
                last_err = f'{type(exc).__name__}: {str(exc)[:150]}'
                if self.on_rate_limited:
                    await self.on_rate_limited(0)
                logger.warning('%s 请求异常(第%s次): %s', path, attempt + 1, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code == 400:
                raise RuntimeError(f'{path} 参数被拒: {resp.text[:200]}')
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                last_err = f'HTTP {resp.status_code}'
                if resp.status_code == 429 and self.on_rate_limited:
                    await self.on_rate_limited(429)
                logger.warning('%s HTTP %s(第%s次)，退避 %.1fs', path, resp.status_code, attempt + 1, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code == 403:
                # CF 按 IP 拦截：打印响应体片段（CF 挑战页有特征），通知轮换器后抛异常
                last_err = f'HTTP 403 body[:150]={resp.text[:150]!r}'
                logger.error('%s HTTP 403 (CF封禁/挑战) url=%s body[:150]=%s',
                             path, resp.request.url, resp.text[:150])
                if self.on_rate_limited:
                    await self.on_rate_limited(403)
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f'{path} 重试 {config.MAX_RETRIES} 次后仍失败 [{last_err}]: {params}')

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

    async def fetch_all_v2(self, path: str, base_params: dict, page_size: int,
                           on_batch=None, max_pages: int = 50000) -> int:
        """v2 cursor 翻页全量拉取（envelope: {data, pagination}），返回总行数。

        关键约束（实测）：cursor 仅承载 keyset 位置，不承载过滤条件——
        翻页请求必须重复携带原始过滤参数（condition/user/...），否则退化为全局 feed。
        on_batch: 可选回调 async fn(list[dict])，行已做 snake_case→camelCase 归一化。
        """
        total = 0
        cursor = None
        pages = 0
        while pages < max_pages:
            params = dict(base_params)
            params['limit'] = page_size
            if cursor:
                params['cursor'] = cursor
            env = await self._get_page(path, params)
            data = (env or {}).get('data') or []
            if data:
                rows = [norm_v2_row(r, token_as_asset=True) for r in data]
                total += len(rows)
                if on_batch is not None:
                    await on_batch(rows)
            pages += 1
            pag = (env or {}).get('pagination') or {}
            nxt = pag.get('next_cursor')
            # next_cursor 与上一页相同视为死循环保护
            if not pag.get('has_more') or not nxt or nxt == cursor:
                break
            cursor = nxt
        return total

    async def fetch_trades_batch(self, condition_ids: list, on_batch=None,
                                 page_size: int = None) -> dict:
        """v2 批量拉取一组市场（≤TRADES_BATCH_SIZE 个）全部交易，返回 {cid: 行数}。

        批量上限 20 个 distinct condition；响应行按行内 conditionId 分桶统计。
        仅 v2 支持；调用方负责把失败组降级为逐市场重试。
        """
        cids = [c for c in dict.fromkeys(condition_ids) if c]
        if not cids or len(cids) > config.TRADES_BATCH_SIZE:
            raise ValueError(f'批量 condition 数量非法: {len(cids)}')
        counts = {c: 0 for c in cids}

        async def _wrap(rows):
            for r in rows:
                cid = r.get('conditionId')
                if cid in counts:
                    counts[cid] += 1
            if on_batch:
                await on_batch(rows)

        await self.fetch_all_v2('/v2/trades', {'condition': ','.join(cids)},
                                page_size or config.TRADES_PAGE_SIZE, on_batch=_wrap)
        return counts

    async def fetch_market_trades(self, condition_id: str, on_batch=None) -> int:
        """拉取单个市场全部历史交易（v2 cursor / v1 end 窗口）"""
        if config.DATA_API_V2:
            return await self.fetch_all_v2(
                '/v2/trades', {'condition': condition_id},
                config.TRADES_PAGE_SIZE, on_batch=on_batch)
        return await self.fetch_all(
            '/trades', {'market': condition_id},
            config.TRADES_PAGE_SIZE, on_batch=on_batch)

    async def fetch_user_activity(self, wallet: str, on_batch=None) -> int:
        """拉取单个用户全部活动流（含 TRADE/REDEEM/MERGE/SPLIT/REWARD/CONVERSION）"""
        if config.DATA_API_V2:
            return await self.fetch_all_v2(
                '/v2/activity', {'user': wallet},
                config.ACTIVITY_PAGE_SIZE, on_batch=on_batch)
        return await self.fetch_all(
            '/activity', {'user': wallet},
            config.ACTIVITY_PAGE_SIZE, on_batch=on_batch)

    async def fetch_holders(self, market: str, on_batch=None) -> int:
        """拉取单个市场全部 Top Holders。

        v2 响应 {data: [{token_id, holders: [...]}]}（cursor 翻页）；
        v1 响应 [{token, holders: [...]}]（offset 翻页）。
        展平为每行带 token_id 的 holder 记录后回调 on_batch。
        """
        if config.DATA_API_V2:
            return await self._fetch_holders_v2(market, on_batch)
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

    async def _fetch_holders_v2(self, market: str, on_batch=None) -> int:
        """v2 /v2/holders（condition 错定，cursor 翻页，分组结构与 v1 一致）"""
        total = 0
        cursor = None
        guard = 0
        while guard < 2000:
            params = {'condition': market, 'limit': config.HOLDERS_PAGE_SIZE}
            if cursor:
                params['cursor'] = cursor
            env = await self._get_page('/v2/holders', params)
            data = (env or {}).get('data') or []
            rows = []
            for group in data:
                tid = group.get('token_id') or group.get('token')
                for h in group.get('holders') or []:
                    h = norm_v2_row(h)
                    h['token_id'] = tid
                    rows.append(h)
            if on_batch and rows:
                await on_batch(rows)
            total += len(rows)
            guard += 1
            pag = (env or {}).get('pagination') or {}
            nxt = pag.get('next_cursor')
            if not pag.get('has_more') or not nxt or nxt == cursor:
                break
            cursor = nxt
        return total

    async def fetch_market_positions(self, market: str, on_batch=None) -> int:
        """拉取单个市场持仓快照（每 token 组 Top limit 条）。

        v2: /v2/positions（扁平行 + cursor 翻页，默认 status=OPEN）；
        v1: /v1/market-positions（[{token, positions: [...]}] 分组，offset 实测无效只拉 1 页）。
        """
        if config.DATA_API_V2:
            return await self._fetch_positions_v2(market, on_batch)
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

    async def _fetch_positions_v2(self, market: str, on_batch=None) -> int:
        """v2 /v2/positions（condition 错定，cursor 翻页，扁平行）。

        字段归一化：current_size→size、current_price→curr_price（对齐 v1/db 字段名）；
        cash_pnl / total_bought 在 v2 无对应字段（保持留空）。
        """
        total = 0
        cursor = None
        guard = 0
        while guard < 2000:
            params = {'condition': market, 'limit': config.POSITIONS_PAGE_SIZE}
            if cursor:
                params['cursor'] = cursor
            env = await self._get_page('/v2/positions', params)
            data = (env or {}).get('data') or []
            rows = []
            for pos in data:
                pos = norm_v2_row(pos)
                pos.setdefault('size', pos.get('current_size'))
                pos.setdefault('curr_price', pos.get('current_price'))
                if not pos.get('condition_id'):
                    pos['condition_id'] = market
                rows.append(pos)
            if on_batch and rows:
                await on_batch(rows)
            total += len(rows)
            guard += 1
            pag = (env or {}).get('pagination') or {}
            nxt = pag.get('next_cursor')
            if not pag.get('has_more') or not nxt or nxt == cursor:
                break
            cursor = nxt
        return total

    async def fetch_event_trades(self, event_id, on_batch=None) -> int:
        """拉取单个事件全部 CASH 交易（Activity 维度）。

        v1 /trades?eventId= 与 v2 /v2/trades?event_id=&filter_type=CASH。
        实测响应行无 eventId 字段（仅 eventSlug），入库前统一补充 eventId。
        """
        async def _wrap(page):
            for r in page:
                if r.get('eventId') is None:
                    r['eventId'] = str(event_id)
            if on_batch:
                await on_batch(page)

        if config.DATA_API_V2:
            return await self.fetch_all_v2(
                '/v2/trades', {'event_id': str(event_id), 'filter_type': 'CASH'},
                config.TRADES_PAGE_SIZE, on_batch=_wrap)
        return await self.fetch_all(
            '/trades', {'eventId': event_id, 'filterType': 'CASH'},
            config.TRADES_PAGE_SIZE, on_batch=_wrap,
        )

    async def probe_page(self, path: str, params: dict) -> list:
        """拉取单个页面（--dry-run 估算用），返回行列表（v2 envelope 自动解包）"""
        data = await self._get_page(path, params)
        if isinstance(data, dict):
            data = data.get('data') or []
        return data
