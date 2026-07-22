import importlib.util
from datetime import datetime, timezone
from pathlib import Path


def load_module():
    path = Path(__file__).parents[1] / "eth_info_dataflow" / "eth_model_retrain.py"
    spec = importlib.util.spec_from_file_location("model_retrain", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_training_filter_uses_current_trading_schema():
    module = load_module()
    expression = module.build_training_filter(datetime(2026, 7, 22, tzinfo=timezone.utc))
    assert expression == "is_settled == true and pub_date >= 1769126400000 and pub_date < 1776902400000"


def test_delete_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("RETRAIN_DELETE_AFTER_BACKUP", raising=False)
    module = load_module()
    assert module.load_retrain_settings()["delete_after_backup"] is False
