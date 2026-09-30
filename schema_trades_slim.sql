-- ============================================================================
-- trades_slim: Polymarket 全量回填瘦身分区表（纯 PG15 内建，无需扩展）
--
-- 依据 12 万行同样本重建实测（_slim_schema_feasibility.py）：
--   原结构干净重灌 2,218 B/行 vs 本表 547 B/行，压缩率 4.06x；
--   12 亿行外推：原实况 3.5-3.7TB（含索引膨胀）-> 本表 0.66TB
--   （考虑全量小市场占比上调诚实区间 0.7-0.9TB）。
--
-- 压缩来源：
--   1. 去 raw_json（845B，归一化后的冗余副本）与 title/slug/event_slug/icon/
--      name/pseudonym/bio/profile_image/profile_image_optimized/event_id
--      共 11 个展示/画像列（~223B，同一市场抄写在其每笔成交上）
--   2. hex 文本 -> bytea：transaction_hash 66B->32B、condition_id 66B->32B、
--      proxy_wallet 42B->20B
--   3. price 定点化 bigint（原 numeric 带 float 垃圾精度位，如
--      0.41999999999999998445687...，round 到微分后干净）
--   4. 索引 6 -> 4：pkey 从 7 列复合（278B/条目）改为 (tx_hash, asset, ts)
--      三列（132B/条目）；timestamp 的 btree 由 BRIN 替代（近零开销）
--   5. 按月 RANGE 分区：按市场+时间窗查询时分区裁剪 + BRIN 命中
--
-- 前置实测验证：(transaction_hash, asset) 在样本中零重复，可作自然键；
-- 分区表唯一索引必须包含分区键，故唯一键为 (tx_hash, asset, ts)。
--
-- 写入端字段映射（COPY / INSERT 时转换）：
--   tx_hash      = decode(substr(transaction_hash, 3), 'hex')  -- 去掉 0x 前缀
--   condition_id = decode(substr(condition_id, 3), 'hex')
--   wallet       = decode(substr(proxy_wallet, 3), 'hex')
--   price_micro  = round(price * 1000000)::bigint
--   asset / outcome_index / size / side / ts / usdc_size / type / outcome /
--   created_at 原样照搬（ts 即原 timestamp 列，unix 秒）
-- ============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS trades_slim (
    tx_hash       bytea        NOT NULL,
    condition_id  bytea        NOT NULL,
    asset         text         NOT NULL,
    outcome_index smallint     NOT NULL,
    size          numeric      NOT NULL,
    price_micro   bigint       NOT NULL,
    side          text         NOT NULL,
    ts            bigint       NOT NULL,
    wallet        bytea        NOT NULL,
    usdc_size     numeric,
    type          text         NOT NULL,
    outcome       text,
    created_at    timestamptz  NOT NULL DEFAULT now()
) PARTITION BY RANGE (ts);

COMMENT ON TABLE trades_slim IS
    'Polymarket trades 瘦身分区表：547 B/行实测（原 2,218 B/行），12 亿行约 0.7-0.9 TB';

-- 月分区 2021-01 .. 2027-12 动态生成（覆盖全量回填历史 + 未来增量缓冲）
DO $$
DECLARE
    d date := date '2021-01-01';
    e int;
    s int;
BEGIN
    WHILE d < date '2028-01-01' LOOP
        e := extract(epoch FROM d)::int;
        s := extract(epoch FROM (d + interval '1 month'))::int;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS trades_slim_%s PARTITION OF trades_slim '
            'FOR VALUES FROM (%s) TO (%s)',
            to_char(d, 'YYYYMM'), e, s);
        d := (d + interval '1 month')::date;
    END LOOP;
END $$;

-- 越界兜底分区（脏数据/时钟偏移不让写入直接报错；定期巡检把它清空）
CREATE TABLE IF NOT EXISTS trades_slim_default PARTITION OF trades_slim DEFAULT;

-- 索引（分区表级创建自动级联到所有月分区）
CREATE UNIQUE INDEX IF NOT EXISTS trades_slim_tx_asset_ts_idx
    ON trades_slim (tx_hash, asset, ts);
CREATE INDEX IF NOT EXISTS trades_slim_condition_ts_idx
    ON trades_slim (condition_id, ts);
CREATE INDEX IF NOT EXISTS trades_slim_wallet_ts_idx
    ON trades_slim (wallet, ts DESC);
CREATE INDEX IF NOT EXISTS trades_slim_ts_brin
    ON trades_slim USING brin (ts);

COMMIT;

-- ----------------------------------------------------------------------------
-- 兼容视图：旧查询按原列名平滑过渡（展示列不再存于 trades，需要时 join markets）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW trades_compat AS
SELECT
    '0x' || encode(tx_hash, 'hex')       AS transaction_hash,
    '0x' || encode(condition_id, 'hex')  AS condition_id,
    asset,
    outcome_index,
    size,
    price_micro / 1000000.0              AS price,
    side,
    ts                                   AS "timestamp",
    '0x' || encode(wallet, 'hex')        AS proxy_wallet,
    usdc_size,
    type,
    outcome,
    created_at
FROM trades_slim;

-- ----------------------------------------------------------------------------
-- 灌数示例（从旧表迁移/双写期间同步）：
--
--   INSERT INTO trades_slim (tx_hash, condition_id, asset, outcome_index, size,
--                            price_micro, side, ts, wallet, usdc_size, type,
--                            outcome, created_at)
--   SELECT decode(substr(transaction_hash, 3), 'hex'),
--          decode(substr(condition_id, 3), 'hex'),
--          asset, outcome_index, size,
--          round(price * 1000000)::bigint,
--          side, "timestamp",
--          decode(substr(proxy_wallet, 3), 'hex'),
--          usdc_size, type, outcome, created_at
--   FROM trades
--   ON CONFLICT DO NOTHING;
--
-- 可选叠加 TimescaleDB 列压老分区（约 150-300 GB @ 12 亿行）：
--   需服务器安装 TSDB <= 2.28（2.29.0 起移除 PG15 支持），装后对已灌完的
--   历史月份 chunk 启用列压；热分区（当月）保持普通写入，写入端零适配。
-- ----------------------------------------------------------------------------
