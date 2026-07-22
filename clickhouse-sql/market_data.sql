-- ClickHouse 表结构定义：Append-Only 行情明细表

CREATE DATABASE IF NOT EXISTS market;

-- 1. 原始交易流明细表 (Append-Only)
CREATE TABLE IF NOT EXISTS market.uniswap_ticks (
    tx_hash String,
    block_number UInt64,
    `timestamp` DateTime,
    pool_address String,
    sender String,
    recipient String,
    amount0 Float64,
    amount1 Float64,
    sqrt_price_x96 String,
    liquidity String,
    tick Int32
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (pool_address, timestamp, block_number)
SETTINGS index_granularity = 8192;

-- 2. 轻度聚合指标表 (Append-Only)
CREATE TABLE IF NOT EXISTS market.market_signals (
    window_start DateTime,
    window_end DateTime,
    pool_address String,
    price_volatility Float64,
    volume_5m Float64,
    trade_count UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (pool_address, window_start)
SETTINGS index_granularity = 8192;
