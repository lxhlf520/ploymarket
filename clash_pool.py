# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""mihomo(Clash.Meta) controller API 封装 + 限流自动节点轮换

复用 glassdoor 项目 ClashAPI/NodeRotator 的实战经验（IP 被限流/拉黑 → ban 节点
与出口 IP → 切换 selector 组下一节点 → 验证新出口 IP），asyncio 化适配 worker。

前置条件（由 make_worker_clash.py 生成的 worker 专用 mihomo 实例）：
- mixed 端口承载采集流量（worker --proxy 指向它）
- external-controller 提供节点列表/切换 API（worker --clash-base 指向它）
- PM select 组可手动切换；polymarket.com 与 ip-api.com 域名走 PM 组
  （ip-api 走代理才能反映节点真实出口 IP）

切换不需要重建 DataAPIClient：所有采集连接都经 mixed 端口，selector 组切换后
新请求自动走新节点出口。
"""
import asyncio
import logging
import random
import time
import urllib.parse

import httpx

logger = logging.getLogger(__name__)

EGRESS_URL = 'http://ip-api.com/json'


class ClashAPI:
    """mihomo external-controller RESTful API（异步）"""

    def __init__(self, base: str = 'http://127.0.0.1:9090', secret: str = '',
                 group: str = 'PM', mixed: str = 'http://127.0.0.1:7890'):
        self.base = base.rstrip('/')
        self.group = group
        self.mixed = mixed
        headers = {'Authorization': f'Bearer {secret}'} if secret else {}
        self._ctl = httpx.AsyncClient(base_url=self.base, headers=headers, timeout=5)
        # 出口 IP 探测：必须经 mixed 端口走代理，返回的才是节点出口 IP
        self._eg = httpx.AsyncClient(proxy=self.mixed, timeout=10)

    async def close(self) -> None:
        await self._ctl.aclose()
        await self._eg.aclose()

    async def alive(self) -> bool:
        try:
            return (await self._ctl.get('/version')).status_code == 200
        except Exception:
            return False

    def _group_url(self) -> str:
        return f'/proxies/{urllib.parse.quote(self.group, safe="")}'

    async def group_info(self) -> dict:
        resp = await self._ctl.get(self._group_url())
        resp.raise_for_status()
        return resp.json()

    async def current(self) -> str | None:
        return (await self.group_info()).get('now')

    async def nodes(self) -> list:
        return list((await self.group_info()).get('all') or [])

    async def switch(self, node: str) -> bool:
        """把 selector 组切到指定节点"""
        try:
            resp = await self._ctl.put(self._group_url(), json={'name': node})
            return resp.status_code in (200, 204)
        except Exception as exc:
            logger.warning('切换 %s 失败: %s', node, exc)
            return False

    async def egress_ip(self, timeout: float = 8):
        """经 mixed 端口查出口 IP，返回 (ip, 'country isp')，失败返回 None"""
        try:
            resp = await self._eg.get(EGRESS_URL, timeout=timeout)
            resp.raise_for_status()
            d = resp.json()
            return d.get('query'), f"{d.get('country')} {d.get('isp')}"
        except Exception:
            return None

    async def switch_and_wait(self, node: str, settle: float = 1.0):
        """切换节点并等其生效，返回新出口 (ip, desc)；切换失败或探测失败返回 None"""
        if not await self.switch(node):
            return None
        await asyncio.sleep(settle)
        return await self.egress_ip()


class AsyncNodeRotator:
    """限流驱动的节点轮换器（glassdoor NodeRotator 的 asyncio 版）

    - 429：累计 3 次（10 秒防抖）才切换，偶发限流不折腾
    - 403：立即切换（CF 按 IP 拦截）
    - 切换：ban 当前节点 + 出口 IP（默认 15 分钟冷却）→ 轮询下一可用节点
      （跳过冷却中节点，切换后验证出口 IP 不在 ban 列表）→ 全局暂停 5 秒等生效
    - 主动轮换：每 rotate_after 次请求换一个 IP，摊薄单 IP 请求量
    """

    def __init__(self, api: ClashAPI, rotate_after: int = 150,
                 ban_cooldown: float = 900, switch_pause: float = 5.0,
                 node_cooldown: float = 300):
        self.api = api
        self.rotate_after = rotate_after
        self.ban_cooldown = ban_cooldown
        self.switch_pause = switch_pause
        self.node_cooldown = node_cooldown
        self.lock = asyncio.Lock()
        self.nodes: list = []
        self.idx = -1
        self.current: str | None = None
        self.req_count = 0
        self.banned_nodes: dict = {}
        self.banned_ips: dict = {}
        self.last_rotate = 0.0
        self.pending_429 = 0
        self.pause_until = 0.0
        self.enabled = False

    async def load_nodes(self) -> bool:
        """启动时拉取 selector 组节点列表；成功后 rotator 生效"""
        if not await self.api.alive():
            logger.warning('clash controller 不可用: %s', self.api.base)
            return False
        try:
            nodes = await self.api.nodes()
        except Exception as exc:
            logger.warning('拉取节点列表失败: %s', exc)
            return False
        if not nodes:
            logger.warning('selector 组 %s 无可用节点', self.api.group)
            return False
        random.shuffle(nodes)
        self.nodes = nodes
        try:
            self.current = await self.api.current()
        except Exception:
            self.current = None
        if self.current in nodes:
            self.idx = nodes.index(self.current)
        self.enabled = True
        logger.info('clash 轮换启用: 组 %s @ %s, %s 个节点, 当前 %s',
                    self.api.group, self.api.base, len(nodes), self.current)
        return True

    async def on_request(self):
        """每次 API 请求前回调：切换后暂停期等待 + 主动轮换计数"""
        if not self.enabled:
            return
        wait = self.pause_until - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        async with self.lock:
            self.req_count += 1
            if self.rotate_after and self.req_count >= self.rotate_after:
                await self._rotate_locked('proactive')

    async def on_rate_limited(self, status: int = 429):
        """DataAPIClient 遇 429/403 回调"""
        if not self.enabled:
            return
        if status == 403:
            await self._ban_and_rotate('403')
            return
        async with self.lock:
            if time.time() - self.last_rotate < 10:
                return
            self.pending_429 += 1
            if self.pending_429 < 3:
                logger.warning('429 (%d/3)，暂不切换', self.pending_429)
                return
            self.pending_429 = 0
        await self._ban_and_rotate('429')

    async def on_ok(self):
        async with self.lock:
            self.pending_429 = 0

    async def _ban_and_rotate(self, reason: str):
        async with self.lock:
            now = time.time()
            if now - self.last_rotate < 10:
                return
            if self.current:
                unban = now + self.ban_cooldown
                self.banned_nodes[self.current] = unban
                eg = await self.api.egress_ip(timeout=4)
                if eg and eg[0]:
                    self.banned_ips[eg[0]] = unban
                    logger.warning('ban 节点 %s（出口 %s），冷却 %.0f 分钟',
                                   self.current, eg[0], self.ban_cooldown / 60)
            await self._rotate_locked(reason)

    async def _rotate_locked(self, reason: str):
        """轮询选下一个可用节点（调用方须已持锁）"""
        now = time.time()
        n = len(self.nodes)
        for _ in range(n):
            self.idx = (self.idx + 1) % n
            node = self.nodes[self.idx]
            if now < self.banned_nodes.get(node, 0):
                continue
            eg = await self.api.switch_and_wait(node, settle=1.0)
            if not eg:
                # 节点切不动或探测不出出口：短冷却后跳过
                self.banned_nodes[node] = now + self.node_cooldown
                continue
            if now < self.banned_ips.get(eg[0], 0):
                # 新出口 IP 仍在冷却（同 IP 多节点）：跳过
                self.banned_nodes[node] = max(self.banned_ips[eg[0]],
                                              now + self.node_cooldown)
                continue
            logger.warning('rotate[%s]: req=%s %s -> %s (egress %s %s)',
                           reason, self.req_count, self.current, node, eg[0], eg[1])
            self.current = node
            self.req_count = 0
            self.last_rotate = time.time()
            self.pause_until = time.time() + self.switch_pause
            return
        # 全部节点冷却中：全局暂停到最早解禁
        wake = min(list(self.banned_nodes.values()) + [time.time() + 120])
        self.pause_until = wake + self.switch_pause
        logger.warning('所有节点冷却中，%.0f 秒后重试', self.pause_until - time.time())
