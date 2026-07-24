import logging
import time
from datetime import datetime

import ccxt
import mysql.connector
import pandas as pd
import pytz
from dotenv import load_dotenv
from collector_runtime.config import env_int, env_str
from collector_runtime.health import WorkloadHealth, start_health_server

# 设置上海时区
TZ_SHANGHAI = pytz.timezone('Asia/Shanghai')

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MarketData")

load_dotenv()


def load_market_settings() -> dict[str, object]:
    return {
        "mysql_host": env_str("MYSQL_HOST", "mysql"),
        "mysql_user": env_str("MYSQL_USER", "root"),
        "mysql_password": env_str("MYSQL_PASSWORD", ""),
        "mysql_database": env_str("MYSQL_DATABASE", "trade"),
        "interval": env_int("MARKET_INTERVAL_SECONDS", 60),
        "health_port": env_int("HEALTH_PORT", 8080),
        "stale_after": env_int("HEALTH_STALE_AFTER_SECONDS", 180),
    }


def calculate_indicators(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    delta = df["close"].diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    average_gain = gains.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    average_loss = losses.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    relative_strength = average_gain / average_loss
    rsi = 100 - (100 / (1 + relative_strength))
    rsi = rsi.mask((average_loss == 0) & (average_gain > 0), 100.0)
    rsi = rsi.mask((average_gain == 0) & (average_loss > 0), 0.0)
    rsi = rsi.mask((average_gain == 0) & (average_loss == 0), 50.0)
    df["rsi_14"] = rsi

    previous_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr_14"] = true_range.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    return df


class MarketDataCollector:
    def __init__(self, settings: dict[str, object], health: WorkloadHealth):
        self.settings = settings
        self.health = health

        # 1. 初始化交易所 (使用币安公共接口)
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        # 2. 初始化数据库连接
        self.db = mysql.connector.connect(
            host=settings["mysql_host"],
            user=settings["mysql_user"],
            password=settings["mysql_password"],
            database=settings["mysql_database"],
        )
        self.cursor = self.db.cursor()
        self.health.mark_ready()

    def fetch_and_calculate(self, symbol="ETH/USDT", limit=100):
        try:
            # 1. 获取 OHLCV 数据 (1分钟K线)
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # 2. 计算技术指标
            df = calculate_indicators(df)

            # 3. 准备写入
            # 过滤掉指标计算初期的 NaN 值
            df = df.dropna()

            sql = """
                  REPLACE INTO eth_kline_features
                      (timestamp, datetime_sh, price_at_t, rsi_14, atr_14, high, low, close)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                  """

            records = []
            for _, row in df.iterrows():
                # 注意：ccxt 返回的时间戳已经是 UTC 毫秒，我们需要将其转为上海时区 datetime 存储以供直观核查
                dt_object = datetime.fromtimestamp(row['timestamp'] / 1000, TZ_SHANGHAI)

                records.append((
                    int(row['timestamp']),
                    dt_object.strftime('%Y-%m-%d %H:%M:%S'),
                    float(row['close']),
                    float(row['rsi_14']),
                    float(row['atr_14']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close'])
                ))

            # 4. 执行批量写入
            self.cursor.executemany(sql, records)
            self.db.commit()

            logger.info(f"Successfully processed {len(records)} records for {symbol}")
            return True

        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            self.db.rollback()
            return False

    def run_forever(self):
        logger.info(f"Market data collector started (Interval: {self.settings['interval']}s)...")
        while True:
            if self.fetch_and_calculate():
                self.health.heartbeat()
            # 每分钟运行一次
            time.sleep(self.settings["interval"])

if __name__ == "__main__":
    settings = load_market_settings()
    health = WorkloadHealth(settings["stale_after"])
    start_health_server(health, settings["health_port"])
    collector = MarketDataCollector(settings, health)
    # 首次运行先获取 500 条以确保 RSI 计算准确性
    collector.fetch_and_calculate(limit=500)
    collector.run_forever()
