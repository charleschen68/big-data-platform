-- 将 Redpanda 数据写入 ClickHouse 的 Flink SQL 任务

-- 1. Redpanda 源表 (复用 uniswap_raw_ticks 定义)
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
    tick INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'uniswap-raw-ticks',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-ch-sink-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- 2. ClickHouse 目标表
CREATE TABLE clickhouse_uniswap_ticks (
    tx_hash STRING,
    block_number BIGINT,
    `timestamp` TIMESTAMP(3),
    pool_address STRING,
    sender STRING,
    recipient STRING,
    amount0 DOUBLE,
    amount1 DOUBLE,
    sqrt_price_x96 STRING,
    liquidity STRING,
    tick INT
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://clickhouse:8123/market',
    'table-name' = 'uniswap_ticks',
    'username' = 'default',
    'password' = '',
    'sink.batch-size' = '500',
    'sink.flush-interval' = '1000',
    'sink.max-retries' = '3'
);

-- 3. 写入 ClickHouse (Append-Only)
INSERT INTO clickhouse_uniswap_ticks
SELECT
    tx_hash,
    block_number,
    TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
    pool_address,
    sender,
    recipient,
    amount0,
    amount1,
    sqrt_price_x96,
    liquidity,
    tick
FROM uniswap_raw_ticks;
