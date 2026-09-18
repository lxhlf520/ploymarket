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

    async def delay(self, node: str, timeout_ms: int = 5000) -> int | None:
        """测试节点延迟（ms），失败返回 None。走 controller /delay 端点，不切节点"""
        url = f'/proxies/{urllib.parse.quote(node, safe="")}/delay'
        try:
            resp = await self._ctl.get(url, params={
                'url': 'http://www.gstatic.com/generate_204',
                'timeout': timeout_ms,
            }, timeout=timeout_ms / 1000 + 3)
            if resp.status_code == 200:
                return resp.json().get('delay')
        except Exception:
            pass
        return None

    async def batch_delay(self, nodes: list, timeout_ms: int = 5000,
                          concurrency: int = 20) -> dict:
        """并发批量测延迟，返回 {node: delay_ms}（仅含成功的）"""
        sem = asyncio.Semaphore(concurrency)
        results = {}

        async def _one(n):
            async with sem:
                d = await self.delay(n, timeout_ms)
                if d is not None:
                    results[n] = d

        await asyncio.gather(*[_one(n) for n in nodes])
        return results

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
                 node_cooldown: float = 300,
                 max_delay_ms: int = 3000,
                 health_interval: float = 600):
        self.api = api
        self.rotate_after = rotate_after
        self.ban_cooldown = ban_cooldown
        self.switch_pause = switch_pause
        self.node_cooldown = node_cooldown
        self.max_delay_ms = max_delay_ms
        self.health_interval = health_interval
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
        self._health_task: asyncio.Task | None = None
        self._node_delays: dict = {}  # {node: last_delay_ms}

    async def load_nodes(self) -> bool:
        """启动时拉取 selector 组节点列表 → 并发预筛延迟 → 按延迟排序；成功后 rotator 生效"""
        if not await self.api.alive():
            logger.warning('clash controller 不可用: %s', self.api.base)
            return False
        try:
            all_nodes = await self.api.nodes()
        except Exception as exc:
            logger.warning('拉取节点列表失败: %s', exc)
            return False
        if not all_nodes:
            logger.warning('selector 组 %s 无可用节点', self.api.group)
            return False
        # ── 并发预筛：批量测延迟，只保留延迟 < max_delay_ms 的节点 ──
        logger.info('预筛 %d 个节点（延迟上限 %d ms）...', len(all_nodes), self.max_delay_ms)
        delays = await self.api.batch_delay(all_nodes, timeout_ms=self.max_delay_ms)
        # 按延迟升序排列（快的优先）
        good = sorted(delays, key=lambda n: delays[n])
        skipped = len(all_nodes) - len(good)
        if skipped:
            logger.info('预筛结果: %d 可用, %d 超时/不可达（已跳过）', len(good), skipped)
        if not good:
            logger.warning('预筛后无可用节点！全部超时或不可达')
            return False
        random.shuffle(good)  # 同延迟档位内随机，避免总是先打最快那几个
        self.nodes = good
        self._node_delays = delays
        try:
            self.current = await self.api.current()
        except Exception:
            self.current = None
        if self.current in self.nodes:
            self.idx = self.nodes.index(self.current)
        self.enabled = True
        logger.info('clash 轮换启用: 组 %s @ %s, %d/%d 节点通过预筛, 当前 %s',
                    self.api.group, self.api.base, len(good), len(all_nodes), self.current)
        if good:
            top5 = good[:5]
            logger.info('延迟最低 5 节点: %s',
                        ', '.join(f'{n}({delays[n]}ms)' for n in top5))
        return True

    async def start_health_check(self):
        """启动后台周期性健康检查（每 health_interval 秒重测一次延迟）"""
        if self._health_task is None or self._health_task.done():
            self._health_task = asyncio.create_task(self._health_loop())

    async def stop_health_check(self):
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

    async def _health_loop(self):
        """后台循环：定期重测所有节点延迟，剔除变差节点，发现恢复节点"""
        while self.enabled:
            await asyncio.sleep(self.health_interval)
            try:
                await self._do_health_check()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning('健康检查异常: %s', exc)

    async def _do_health_check(self):
        """单次健康检查：重测延迟 → 更新节点池"""
        all_nodes = await self.api.nodes()
        if not all_nodes:
            return
        delays = await self.api.batch_delay(all_nodes, timeout_ms=self.max_delay_ms)
        good_set = set(delays)
        old_nodes = set(self.nodes)
        # 新增的可达节点（之前不可达，现在恢复了）
        new_good = good_set - old_nodes
        # 变差的节点（之前可达，现在超时了）
        gone_bad = old_nodes - good_set
        async with self.lock:
            now = time.time()
            # 剔除变差节点（如果在冷却中则不重复 ban）
            for n in gone_bad:
                if n not in self.banned_nodes or self.banned_nodes[n] < now:
                    self.banned_nodes[n] = now + self.node_cooldown
            # 恢复节点加入池
            for n in new_good:
                self.banned_nodes.pop(n, None)
                if n not in self.nodes:
                    self.nodes.append(n)
            # 重建节点列表：保留仍在 good_set 中的，按延迟排序
            alive_nodes = [n for n in self.nodes if n in good_set]
            # 加上新恢复的
            for n in new_good:
                if n not in alive_nodes:
                    alive_nodes.append(n)
            if alive_nodes:
                random.shuffle(alive_nodes)
                self.nodes = alive_nodes
                # 修正 idx
                if self.current in self.nodes:
                    self.idx = self.nodes.index(self.current)
                else:
                    self.idx = -1
            self._node_delays = delays
        if gone_bad:
            logger.info('健康检查: %d 节点变差已剔除: %s',
                        len(gone_bad), ', '.join(gone_bad))
        if new_good:
            logger.info('健康检查: %d 节点恢复: %s',
                        len(new_good), ', '.join(new_good))
        logger.info('健康检查完成: %d/%d 节点可用', len(self.nodes), len(all_nodes))

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
