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
