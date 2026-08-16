# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Polymarket 采集配置"""

import os


def _load_env(path: str) -> None:
    """加载 .env（部署机数据库配置入口；已存在的环境变量优先，不覆盖）"""
    if not os.path.exists(path):
        return
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip())


_load_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# ==================== 原有配置（Gamma API / SQLite 事件市场评论采集） ====================

# SQLite 数据库路径（原有事件/市场/评论库，交易采集仅只读引用其 conditionId）
DB_PATH = os.path.join(os.path.dirname(__file__), 'polymarket.db')

GAMMA_API_BASE = 'https://gamma-api.polymarket.com'
POLYMARKET_BASE = 'https://polymarket.com'

EVENTS_PAGE_SIZE = 100
COMMENTS_PAGE_SIZE = 50
CONCURRENT_REQUESTS = 8
REQUEST_DELAY = 0.2
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Origin': 'https://polymarket.com',
    'Referer': 'https://polymarket.com/',
}

FETCH_CLOSED_EVENTS = False
MIN_COMMENT_COUNT = 0

# ==================== 交易采集扩展配置（Data API / PostgreSQL / Polygon RPC） ====================

# Data API（交易与活动流，无需鉴权）
DATA_API_BASE = 'https://data-api.polymarket.com'

# 本地 PostgreSQL（新建库 polymarket，与 yelp/dailymail 等库同实例）
# 连接信息从 .env / 环境变量读取（参考 .env.example），密码不硬编码
PG_HOST = os.environ.get('PG_HOST', 'localhost')
PG_PORT = int(os.environ.get('PG_PORT', '5432'))
PG_USER = os.environ.get('PG_USER', 'postgres')
PG_PASSWORD = os.environ.get('PG_PASSWORD', '')
PG_DB = os.environ.get('PG_DB', 'polymarket')
PG_DSN = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}'
PG_ADMIN_DSN = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/postgres'
PG_POOL_MIN = 4
PG_POOL_MAX = 16

# Polygon RPC 节点池（按优先级使用，故障自动切换下一个）
POLYGON_RPC_URLS = [
    'https://polygon.drpc.org',
    'https://polygon-bor-rpc.publicnode.com',
    'https://1rpc.io/matic',
]

# Data API 采集参数
# /trades: limit<=10000, offset<=10000；/activity: limit<=500, offset<=5000
# 两接口均支持 end 时间戳过滤，统一用 end 窗口翻页突破 offset 上限
TRADES_PAGE_SIZE = 1000          # /trades 单页条数
ACTIVITY_PAGE_SIZE = 500         # /activity 单页条数（接口上限 500）
TRADES_RATE_LIMIT = 200          # Data API /trades+/activity 共享预算：次/10 秒
TRADES_CONCURRENCY = 16          # 并发市场/用户采集数
DATA_TIMEOUT = 60                # Data API 超时秒数（Cloudflare 先减速后 429，慢响应视为成功）

# Polygon RPC 链上回填参数
TX_ENRICH_BATCH = 500            # 每次从 PG 取未回填哈希的批量大小
ENRICH_CONCURRENCY = 10          # RPC 回填并发数
RPC_TIMEOUT = 20                 # 单个 RPC 调用超时秒数
RPC_RETRIES = 3                  # 单个节点失败重试次数（超过后切换下一节点）
TX_RETRY_COOLDOWN_HOURS = 6      # failed 哈希冷却窗口（小时后自动重试，防止风暴）
