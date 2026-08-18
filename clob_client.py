# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""CLOB API 客户端（clob.polymarket.com）

实测接口约束（以此为准）：
- /prices-history     GET，market=clobTokenId，interval ∈ {1m,1w,1d,6h,1h}，
                      fidelity ∈ {10,60,720...}；startTs/endTs 窗口过长返回 400
- /book               GET，token_id= 单 token 盘口深度 {bids:[{price,size}], asks:[...]}
                      （批量 POST /books 已失效，实测一律 400 Invalid payload）
- /last-trades-prices POST 批量已失效（400），保留容错封装返回 None
- /time               GET，服务器时间戳（秒）

限流：独立令牌桶（与 Gamma/Data API 不同源），沿用 200 次/10s 预算 + 退避重试。
"""

import asyncio
import logging

import httpx

import config
from data_api_client import TokenBucket

logger = logging.getLogger(__name__)


class CLOBClient:
    """封装 CLOB API，带限流/重试"""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or config.CLOB_API_BASE
        self.bucket = TokenBucket(capacity=config.TRADES_RATE_LIMIT)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.DATA_TIMEOUT, connect=15),
            headers=dict(config.HEADERS),
            limits=httpx.Limits(
                max_connections=config.TRADES_CONCURRENCY + 8,
                max_keepalive_connections=config.TRADES_CONCURRENCY,
            ),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> 'CLOBClient':
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def _get(self, path: str, params: dict = None) -> object:
        """带限流与退避重试的单次 GET"""
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
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                logger.warning('%s HTTP %s(第%s次)，退避 %.1fs', path, resp.status_code, attempt + 1, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f'{path} 重试 {config.MAX_RETRIES} 次后仍失败: {params}')

    async def get_time(self) -> int:
        """服务器时间戳（秒，时钟校准）"""
        return int(await self._get('/time'))

    async def get_prices_history(self, market: str, interval: str = '1d',
                                 fidelity: int = 10) -> list:
        """价格历史，返回 [{t, p}]（interval 窗口内的点，服务端限流窗口上限）"""
        data = await self._get('/prices-history', {
            'market': market,
            'interval': interval,
            'fidelity': fidelity,
        })
        return data.get('history') or []

    async def get_book(self, token_id: str) -> dict:
        """单 token 盘口深度，返回 {bids, asks}（每档 {price, size}）"""
        return await self._get('/book', {'token_id': token_id})

    async def get_last_trades_prices(self, token_ids: list) -> dict:
        """批量最新成交价（实测 POST 已失效返回 None，保留接口兼容）"""
        try:
            resp = await self._client.post(
                f'{self.base_url}/last-trades-prices',
                params={'token_ids': ','.join(token_ids)},
            )
            if resp.status_code == 200:
                return resp.json()
        except httpx.HTTPError as exc:
            logger.warning('last-trades-prices 请求异常: %s', exc)
        logger.warning('last-trades-prices 失效（HTTP 400），返回 None')
        return None
