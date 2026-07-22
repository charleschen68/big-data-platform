import clickhouse_connect
import pandas as pd
import time
from typing import List, Dict

class BacktestEngine:
    def __init__(self, host: str = 'localhost', port: int = 8123):
        print(f"Connecting to ClickHouse at {host}:{port}...")
        self.client = clickhouse_connect.get_client(host=host, port=port, username='default', password='')
        print("✅ Connected to ClickHouse.")

    def run_trend_strategy_backtest(self, pool_address: str, start_time: str, end_time: str, moving_average_window: int = 20):
        """
        验证特定趋势指标：当短期均线突破长期均线时买入，跌破时卖出
        通过极速 SQL 将大量计算下推至 ClickHouse。
        """
        print(f"Starting backtest for pool {pool_address} from {start_time} to {end_time}...")
        start_ts = time.time()

        # 使用 ClickHouse 向量化引擎计算 K 线和移动平均
        query = f"""
        WITH k_lines AS (
            SELECT
                toStartOfMinute(timestamp) as minute_ts,
                argMin(tick, timestamp) as open_tick,
                max(tick) as high_tick,
                min(tick) as low_tick,
                argMax(tick, timestamp) as close_tick,
                sum(abs(amount0) + abs(amount1)) as volume
            FROM market.uniswap_ticks
            WHERE pool_address = '{pool_address}'
              AND timestamp >= '{start_time}'
              AND timestamp <= '{end_time}'
            GROUP BY minute_ts
            ORDER BY minute_ts
        ),
        k_lines_with_ma AS (
            SELECT
                minute_ts,
                close_tick,
                avg(close_tick) OVER (ORDER BY minute_ts ROWS BETWEEN {moving_average_window - 1} PRECEDING AND CURRENT ROW) as ma_close
            FROM k_lines
        )
        SELECT * FROM k_lines_with_ma
        """

        result = self.client.query_df(query)
        query_time = time.time() - start_ts
        print(f"✅ ClickHouse query executed in {query_time:.3f} seconds, fetched {len(result)} rows.")

        if result.empty:
            print("No data found for the given range.")
            return

        # 本地 Pandas 继续做轻量级的信号回测 (向量化操作)
        result['signal'] = 0
        # 如果 close_tick > ma_close，产生买入信号 1
        result.loc[result['close_tick'] > result['ma_close'], 'signal'] = 1
        # 如果 close_tick < ma_close，产生卖出信号 -1
        result.loc[result['close_tick'] < result['ma_close'], 'signal'] = -1

        # 计算策略收益 (简化，使用 tick 差值代表收益)
        result['returns'] = result['close_tick'].diff()
        result['strategy_returns'] = result['signal'].shift(1) * result['returns']

        cumulative_return = result['strategy_returns'].sum()
        win_rate = (result['strategy_returns'] > 0).mean() * 100

        print("--- Backtest Results ---")
        print(f"Total Rows Analyzed: {len(result)}")
        print(f"Cumulative Tick Return: {cumulative_return}")
        print(f"Win Rate: {win_rate:.2f}%")
        print("------------------------")

if __name__ == '__main__':
    engine = BacktestEngine()
    # 示例调用
    engine.run_trend_strategy_backtest(
        pool_address='0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8', # USDC/WETH pool
        start_time='2023-01-01 00:00:00',
        end_time='2024-01-01 00:00:00',
        moving_average_window=50
    )
