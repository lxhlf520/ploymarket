# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""链上交易回填 - Polygon RPC 收据与区块时间戳

对 trades 表中每个去重的 transaction_hash：
- eth_getTransactionReceipt 取区块号/gas/status/from/to 等链上收据
- eth_getBlockByNumber 取区块时间戳（blocks 表缓存，避免重复查询）

节点池轮换：config.POLYGON_RPC_URLS 按序使用，失败/限流自动切换下一节点；
查询结果为 null（节点未同步到该交易）经重试后标记 failed，不再无限重试。
"""

import asyncio
import logging

import httpx
from tqdm import tqdm

import config
import db_pg

logger = logging.getLogger(__name__)


class PolygonRPC:
    """Polygon 公共 RPC 客户端（多节点轮换 + 并发信号量）"""

    def __init__(self, urls: list = None, concurrency: int = None):
        self.urls = list(urls or config.POLYGON_RPC_URLS)
        self.idx = 0
        self.sem = asyncio.Semaphore(concurrency or config.ENRICH_CONCURRENCY)
        self._client = httpx.AsyncClient(timeout=config.RPC_TIMEOUT)
        self._switch_lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    async def _switch_node(self) -> None:
        async with self._switch_lock:
            self.idx = (self.idx + 1) % len(self.urls)

    async def _call(self, method: str, params: list):
        """调用 RPC，失败自动切换节点重试，全部失败返回 None"""
        for _ in range(len(self.urls) * config.RPC_RETRIES):
            url = self.urls[self.idx % len(self.urls)]
            try:
                async with self.sem:
                    resp = await self._client.post(
                        url, json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
                    )
                if resp.status_code == 429:
                    await asyncio.sleep(2)
                    await self._switch_node()
                    continue
                resp.raise_for_status()
                data = resp.json()
                if 'result' in data:
                    return data['result']
                # RPC 内部错误（含 result: null 由上层判定）
                await asyncio.sleep(0.5)
                await self._switch_node()
                continue
            except (httpx.HTTPError, ValueError) as exc:
                logger.debug('RPC 节点 %s 调用失败: %s', url, exc)
                await asyncio.sleep(0.5)
                await self._switch_node()
                continue
        return None

    async def batch_get_receipts(self, hashes: list) -> list:
        """并发拉取交易收据，返回 [(hash, receipt_or_None)]"""
        results = await asyncio.gather(*[self._call('eth_getTransactionReceipt', [h]) for h in hashes])
        return list(zip(hashes, results))

    async def batch_get_block_timestamps(self, block_numbers: list) -> list:
        """并发拉取区块时间戳，返回 [(block_number, ts)]"""
        results = await asyncio.gather(
            *[self._call('eth_getBlockByNumber', [hex(bn), False]) for bn in block_numbers]
        )
        out = []
        for bn, blk in zip(block_numbers, results):
            if blk and blk.get('timestamp'):
                out.append((bn, int(blk['timestamp'], 16)))
        return out


def _receipt_to_row(tx_hash: str, receipt: dict, block_ts: dict) -> dict:
    bn = int(receipt['blockNumber'], 16)
    return {
        'transaction_hash': tx_hash,
        'block_number': bn,
        'block_timestamp': block_ts.get(bn),
        'tx_from': receipt.get('from'),
        'tx_to': receipt.get('to'),
        'gas_used': int(receipt['gasUsed'], 16),
        'effective_gas_price': int(receipt['effectiveGasPrice'], 16),
        'status': int(receipt['status'], 16),
        'tx_type': int(receipt.get('type', '0x0'), 16),
        'tx_index': int(receipt['transactionIndex'], 16),
        'cumulative_gas_used': int(receipt['cumulativeGasUsed'], 16),
    }


async def enrich_pending_txs(limit: int = None, batch_size: int = None) -> dict:
    """扫描 trades 未回填哈希并批量回填，返回统计"""
    await db_pg.init_schema()
    batch_size = batch_size or config.TX_ENRICH_BATCH
    rpc = PolygonRPC()

    after_hash = ''
    enriched = 0
    failed = 0
    processed = 0
    try:
        while True:
            hashes = await db_pg.get_pending_tx_hashes(after_hash, batch_size)
            if not hashes:
                break

            pairs = await rpc.batch_get_receipts(hashes)
            good = [(h, r) for h, r in pairs if r is not None]
            bad = [h for h, r in pairs if r is None]

            # 区块时间戳：先查缓存，缺的再走 RPC
            block_numbers = sorted({int(r['blockNumber'], 16) for _, r in good})
            cached = await db_pg.get_block_timestamps(block_numbers)
            missing = [bn for bn in block_numbers if bn not in cached]
            fetched = await rpc.batch_get_block_timestamps(missing)
            await db_pg.upsert_blocks(fetched)
            block_ts = {**cached, **dict(fetched)}

            receipts = [_receipt_to_row(h, r, block_ts) for h, r in good]
            await db_pg.upsert_tx_receipts(receipts)
            if bad:
                await db_pg.mark_tx_failed(bad)

            enriched += len(receipts)
            failed += len(bad)
            processed += len(hashes)
            after_hash = hashes[-1]

            if limit and processed >= limit:
                break
            if processed % (batch_size * 10) < batch_size:
                stats = await db_pg.get_tx_stats()
                logger.info('回填进度: 已处理 %s 哈希, 累计成功 %s, 本次失败 %s, 覆盖率 %s/%s',
                            processed, stats['enriched'], failed,
                            stats['enriched'], stats['total_hashes'])
    finally:
        await rpc.close()

    stats = await db_pg.get_tx_stats()
    return {'processed': processed, 'enriched_this_run': enriched, 'failed_this_run': failed, **stats}


async def verify() -> dict:
    """覆盖度统计（不发起 RPC）"""
    await db_pg.init_schema()
    return await db_pg.get_tx_stats()
