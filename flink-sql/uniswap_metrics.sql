-- 1. 创建源表：从 Redpanda 消费原始 Tick 数据
CREATE TABLE uniswap_raw_ticks (
    tx_hash STRING,
    block_number BIGINT,
    `timestamp` BIGINT,
    pool_address STRING,
    sender STRING,
    recipient STRING,
    amount0 DOUBLE,
    amount1 DOUBLE,
    sqrt_price_x96 STRING,
    liquidity STRING,
    tick INT,
    -- 提取事件时间并定义 Watermark
    ts AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'uniswap-raw-ticks',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-sql-metrics-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- 2. 创建结果表：输出到 Redpanda
CREATE TABLE market_signals (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    pool_address STRING,
    price_volatility DOUBLE,
    volume_5m DOUBLE,
    trade_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'market-signals',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'format' = 'json'
);

-- 3. 实时计算逻辑：利用滑动窗口计算 5 分钟的波动率和交易量，每 1 分钟滑动一次
INSERT INTO market_signals
SELECT
    HOP_START(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
    HOP_END(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_end,
    pool_address,
    -- 简化波动率计算：(最高 tick - 最低 tick) 或者使用价格差异 (这里以 tick 极差近似价格波动率)
    CAST((MAX(tick) - MIN(tick)) AS DOUBLE) / NULLIF(ABS(MIN(tick)), 0) AS price_volatility,
    -- 过去 5 分钟交易总量 (绝对值相加近似)
    SUM(ABS(amount0) + ABS(amount1)) AS volume_5m,
    COUNT(*) AS trade_count
FROM uniswap_raw_ticks
GROUP BY
    HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE),
    pool_address;
