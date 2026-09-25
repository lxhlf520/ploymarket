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


def _parse_proxy_server(server: str) -> str:
    """解析注册表 ProxyServer 字段为 http:// 代理 URL。

    支持两种格式：
    - 统一:   "127.0.0.1:7890"
    - 分协议: "http=127.0.0.1:7890;https=127.0.0.1:7890;socks=..."
    """
    server = (server or '').strip()
    if not server:
        return ''
    if '=' in server:
        parts = dict(kv.split('=', 1) for kv in server.split(';') if '=' in kv)
        addr = (parts.get('https') or parts.get('http') or '').strip()
    else:
        addr = server
    if not addr:
        return ''
    if not addr.startswith('http://') and not addr.startswith('https://'):
        addr = 'http://' + addr
    return addr


def apply_system_proxy() -> str:
    """自动跟随本机系统代理（“本机网络做那个就是那个”）。

    httpx 的 trust_env 只读环境变量、不读 Windows 注册表系统代理，
    这里把注册表里的系统代理回填到 HTTP(S)_PROXY 环境变量，
    使所有 httpx 客户端（data_api/gamma/clob/tx_enrich）自动走本机代理。

    规则：
    - 非 Windows 直接返回（交由环境变量/直连）
    - 已显式设置 HTTP(S)_PROXY 环境变量则尊重，不覆盖
    - 系统代理关闭（或用 TUN 模式）时不设置，保持直连（TUN 在网络层接管）
    - 显式传 --proxy 的 mihomo worker 不受影响（httpx 中显式代理优先于环境变量）
    返回最终生效的代理 URL（无则空串）。
    """
    # 已显式设置代理环境变量：尊重现有配置
    existing = (os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
                or os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
                or os.environ.get('ALL_PROXY') or os.environ.get('all_proxy'))
    if existing:
        return existing
    if os.name != 'nt':
        return ''
    try:
        import winreg
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r'Software\Microsoft\Windows\CurrentVersion\Internet Settings')
        try:
            enable, _ = winreg.QueryValueEx(key, 'ProxyEnable')
            if not enable:
                return ''
            server, _ = winreg.QueryValueEx(key, 'ProxyServer')
            try:
                override, _ = winreg.QueryValueEx(key, 'ProxyOverride')
            except OSError:
                override = ''
        finally:
            winreg.CloseKey(key)
    except OSError:
        return ''
    proxy_url = _parse_proxy_server(server)
    if not proxy_url:
        return ''
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url
    # 本地回环不走代理（mihomo controller/mixed 等 127.0.0.1 端点必须直连）
    no_proxy = 'localhost,127.0.0.1'
    if override:
        # 注册表 ProxyOverride 用 ; 分隔，<local> 表示本地地址
        extra = ','.join(o.strip() for o in override.split(';')
                         if o.strip() and o.strip() != '<local>')
        if extra:
            no_proxy += ',' + extra
    os.environ['NO_PROXY'] = no_proxy
    os.environ['no_proxy'] = no_proxy
    return proxy_url


# 模块加载时自动应用（早于任何 httpx 客户端创建）
SYSTEM_PROXY = apply_system_proxy()

# ==================== 原有配置（Gamma API / SQLite 事件市场评论采集） ====================

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

# CLOB API（价格历史/盘口深度/时间）
CLOB_API_BASE = 'https://clob.polymarket.com'

# 登录态 Cookie 文件（浏览器登录后 export_cookie.py 导出，评论全量采集用）
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.json')

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

# Gamma API 全模块采集参数
TAGS_PAGE_SIZE = 100             # /tags 单页条数
KEYSET_PAGE_SIZE = 100           # /events/keyset 单页条数
MARKETS_PAGE_SIZE = 100          # /markets 单页条数
CLARIFICATIONS_BATCH = 100       # 规则澄清并发批量

# Data API 全模块采集参数
HOLDERS_PAGE_SIZE = 100          # /holders 单页条数
POSITIONS_PAGE_SIZE = 50         # /v1/market-positions 单页条数

# 评论采集参数（登录态下才能翻页看全）
COMMENTS_LIMIT = 50              # /comments 单页条数（未登录服务端可能只回 10）

# CLOB 价格历史参数
PRICE_FIDELITY_DAILY = 720       # 日级/跨天：fidelity=720（5 分钟粒度）
PRICE_FIDELITY_INTRADAY = 10     # 近 24h：fidelity=10（10 秒粒度）

# Data API 采集参数
# v1 于 2026-10-24 退役：v2 用 cursor 翻页（不再依赖 end 窗口）+ 批量 condition（≤20/请求）
# + snake_case 行 + {data, pagination} envelope；设 DATA_API_V2=0 可临时回退 v1
DATA_API_V2 = os.environ.get('DATA_API_V2', '1') != '0'
TRADES_PAGE_SIZE = 1000          # /trades 单页条数（v2 上限 1000）
ACTIVITY_PAGE_SIZE = 1000        # /activity 单页条数（v2 上限 1000，v1 上限 500）
TRADES_BATCH_SIZE = 20           # Phase A 批量 condition 数（v2 上限 20/请求）
TRADES_RATE_LIMIT = 200          # Data API /trades+/activity 共享预算：次/10 秒
TRADES_CONCURRENCY = 16          # 并发市场/用户采集数
DATA_TIMEOUT = 60                # Data API 超时秒数（Cloudflare 先减速后 429，慢响应视为成功）

# Polygon RPC 链上回填参数
TX_ENRICH_BATCH = 500            # 每次从 PG 取未回填哈希的批量大小
ENRICH_CONCURRENCY = 10          # RPC 回填并发数
RPC_TIMEOUT = 20                 # 单个 RPC 调用超时秒数
RPC_RETRIES = 3                  # 单个节点失败重试次数（超过后切换下一节点）
TX_RETRY_COOLDOWN_HOURS = 6      # failed 哈希冷却窗口（小时后自动重试，防止风暴）
