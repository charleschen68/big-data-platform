import importlib.util
from pathlib import Path

import pytest


def load_dataflow_module(relative_path: str, name: str):
    path = Path(__file__).parents[1] / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_market_settings_require_password(monkeypatch):
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="MYSQL_PASSWORD must not be empty"):
        module.load_market_settings()


def test_settlement_settings_use_cluster_service_names(monkeypatch):
    module = load_dataflow_module("eth_trade_dataflow/eth_trade_settlement.py", "settlement")
    monkeypatch.delenv("MILVUS_HOST", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    settings = module.load_settlement_settings()
    assert settings["milvus_host"] == "milvus"
    assert settings["kafka_bootstrap"] == "kafka:29092"
    assert settings["topic"] == "topic_trade_signals"


def test_market_indicators_do_not_require_pandas_ta():
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")
    frame = module.pd.DataFrame(
        {
            "high": list(range(101, 121)),
            "low": list(range(99, 119)),
            "close": list(range(100, 120)),
        }
    )
    result = module.calculate_indicators(frame)
    assert {"rsi_14", "atr_14"}.issubset(result.columns)
    assert result[["rsi_14", "atr_14"]].dropna().empty is False


@pytest.mark.parametrize(
    ("closes", "expected_rsi"),
    [
        (list(range(100, 120)), 100.0),
        (list(range(120, 100, -1)), 0.0),
    ],
)
def test_market_indicators_handle_monotonic_prices(closes, expected_rsi):
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")
    frame = module.pd.DataFrame(
        {
            "high": [close + 1 for close in closes],
            "low": [close - 1 for close in closes],
            "close": closes,
        }
    )

    result = module.calculate_indicators(frame)

    assert result["rsi_14"].dropna().iloc[-1] == expected_rsi


class RecordingHealth:
    def __init__(self, events):
        self.events = events

    def mark_ready(self):
        self.events.append("ready")

    def heartbeat(self):
        self.events.append("heartbeat")


def market_settings():
    return {
        "mysql_host": "mysql",
        "mysql_user": "root",
        "mysql_password": "password",
        "mysql_database": "configured_database",
        "interval": 1,
    }


def test_market_initialization_error_escapes_without_marking_ready(monkeypatch):
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")
    events = []

    def fail_connect(**kwargs):
        events.append("connect")
        raise RuntimeError("MySQL unavailable")

    monkeypatch.setattr(module.ccxt, "binance", lambda options: object())
    monkeypatch.setattr(module.mysql.connector, "connect", fail_connect)

    with pytest.raises(RuntimeError, match="MySQL unavailable"):
        module.MarketDataCollector(market_settings(), RecordingHealth(events))

    assert events == ["connect"]


def test_market_marks_ready_after_database_connection_and_cursor(monkeypatch):
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")
    events = []
    cursor = object()

    class Database:
        def cursor(self):
            events.append("cursor")
            return cursor

    def connect(**kwargs):
        events.append("connect")
        return Database()

    monkeypatch.setattr(module.ccxt, "binance", lambda options: object())
    monkeypatch.setattr(module.mysql.connector, "connect", connect)

    collector = module.MarketDataCollector(market_settings(), RecordingHealth(events))

    assert collector.cursor is cursor
    assert events == ["connect", "cursor", "ready"]


def test_market_failed_iteration_does_not_heartbeat(monkeypatch):
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")
    events = []
    collector = object.__new__(module.MarketDataCollector)
    collector.settings = {"interval": 1}
    collector.health = RecordingHealth(events)
    collector.fetch_and_calculate = lambda: False

    def stop_after_sleep(interval):
        assert interval == 1
        raise RuntimeError("stop test loop")

    monkeypatch.setattr(module.time, "sleep", stop_after_sleep)

    with pytest.raises(RuntimeError, match="stop test loop"):
        collector.run_forever()

    assert events == []


def test_market_writes_to_the_selected_database_without_hard_coded_schema():
    module = load_dataflow_module("eth_trade_dataflow/market_data_collector.py", "market_collector")

    class Cursor:
        def executemany(self, sql, records):
            self.sql = sql
            self.records = list(records)

    class Database:
        def commit(self):
            return None

        def rollback(self):
            raise AssertionError("market write should not fail")

    class Exchange:
        def fetch_ohlcv(self, symbol, timeframe, limit):
            return [
                [minute * 60_000, 100 + minute, 101 + minute, 99 + minute, 100 + minute, 1]
                for minute in range(20)
            ]

    collector = object.__new__(module.MarketDataCollector)
    collector.exchange = Exchange()
    collector.cursor = Cursor()
    collector.db = Database()

    assert collector.fetch_and_calculate() is True
    assert "REPLACE INTO eth_kline_features" in collector.cursor.sql
    assert "trade.eth_kline_features" not in collector.cursor.sql


def test_settlement_marks_ready_after_milvus_and_kafka_initialization(monkeypatch):
    module = load_dataflow_module("eth_trade_dataflow/eth_trade_settlement.py", "settlement")
    events = []
    consumer_options = {}

    class FakeCollection:
        def __init__(self, name):
            events.append("collection")

        def load(self):
            events.append("collection_loaded")

    class FakeConsumer:
        def close(self):
            return None

    def connect(alias, host, port):
        events.append("milvus_connected")

    def build_consumer(*args, **kwargs):
        events.append("consumer_initialized")
        consumer_options.update(kwargs)
        return FakeConsumer()

    monkeypatch.setattr(module.connections, "connect", connect)
    monkeypatch.setattr(module, "Collection", FakeCollection)
    monkeypatch.setattr(module, "KafkaConsumer", build_consumer)

    worker = module.MilvusSettlementWorker(module.load_settlement_settings(), RecordingHealth(events))

    assert worker.topic == "topic_trade_signals"
    assert events == ["milvus_connected", "collection", "collection_loaded", "consumer_initialized", "ready"]
    assert consumer_options["group_id"] == "milvus_settlement_group"


def test_settlement_idle_poll_heartbeats_with_bounded_polling():
    module = load_dataflow_module("eth_trade_dataflow/eth_trade_settlement.py", "settlement")
    events = []
    poll_calls = []

    class FakeConsumer:
        def poll(self, **kwargs):
            poll_calls.append(kwargs)
            if len(poll_calls) == 1:
                return {}
            raise KeyboardInterrupt

        def close(self):
            events.append("closed")

    worker = object.__new__(module.MilvusSettlementWorker)
    worker.consumer = FakeConsumer()
    worker.health = RecordingHealth(events)

    worker.run_forever()

    assert poll_calls == [
        {"timeout_ms": 1000, "max_records": 100},
        {"timeout_ms": 1000, "max_records": 100},
    ]
    assert events == ["heartbeat", "closed"]
