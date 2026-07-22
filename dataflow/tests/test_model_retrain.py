import importlib.util
import json
import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest


def load_module():
    path = Path(__file__).parents[1] / "eth_info_dataflow" / "eth_model_retrain.py"
    spec = importlib.util.spec_from_file_location("model_retrain", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCollection:
    def __init__(self, records):
        self.records = records
        self.delete_calls = []
        self.flush_calls = 0
        self.on_delete = None

    def load(self):
        pass

    def query(self, **_kwargs):
        return self.records

    def delete(self, expression):
        if self.on_delete:
            self.on_delete()
        self.delete_calls.append(expression)

    def flush(self):
        self.flush_calls += 1


def install_fake_runtime(monkeypatch, records, *, parquet_error=None, model_error=None):
    collection = FakeCollection(records)

    class FakeDataFrame:
        def __init__(self, _records):
            pass

        def to_parquet(self, path, **_kwargs):
            if parquet_error:
                raise parquet_error
            Path(path).write_bytes(b"parquet backup")

    class FakeRegressor:
        def __init__(self, **_kwargs):
            pass

        def fit(self, _features, _labels):
            pass

    def dump(_model, path):
        Path(path).write_bytes(b"new model")
        if model_error:
            raise model_error

    fake_joblib = types.ModuleType("joblib")
    fake_joblib.dump = dump
    fake_numpy = types.ModuleType("numpy")
    fake_numpy.array = lambda values: values
    fake_pandas = types.ModuleType("pandas")
    fake_pandas.DataFrame = FakeDataFrame
    fake_pymilvus = types.ModuleType("pymilvus")
    fake_pymilvus.connections = types.SimpleNamespace(connect=lambda *_args, **_kwargs: None)
    fake_pymilvus.Collection = lambda _name: collection
    fake_xgboost = types.ModuleType("xgboost")
    fake_xgboost.XGBRegressor = FakeRegressor

    for name, module in {
        "joblib": fake_joblib,
        "numpy": fake_numpy,
        "pandas": fake_pandas,
        "pymilvus": fake_pymilvus,
        "xgboost": fake_xgboost,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return collection


def configure_artifacts(monkeypatch, tmp_path, *, delete_after_backup):
    monkeypatch.setenv("RETRAIN_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("RETRAIN_MINIMUM_SAMPLES", "1")
    monkeypatch.setenv("RETRAIN_DELETE_AFTER_BACKUP", str(delete_after_backup).lower())


def test_training_filter_uses_current_trading_schema():
    module = load_module()
    expression = module.build_training_filter(datetime(2026, 7, 22, tzinfo=timezone.utc))
    assert expression == "is_settled == true and pub_date >= 1769126400000 and pub_date < 1776902400000"


def test_training_filter_rejects_naive_datetime():
    module = load_module()
    with pytest.raises(ValueError, match="timezone-aware"):
        module.build_training_filter(datetime(2026, 7, 22))


def test_delete_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("RETRAIN_DELETE_AFTER_BACKUP", raising=False)
    module = load_module()
    assert module.load_retrain_settings()["delete_after_backup"] is False


def test_failed_parquet_write_never_deletes_records(monkeypatch, tmp_path):
    configure_artifacts(monkeypatch, tmp_path, delete_after_backup=True)
    collection = install_fake_runtime(
        monkeypatch,
        [{"event_id": "event-1", "vector": [1.0], "sentiment_score": 0.1, "return": 0.2}],
        parquet_error=OSError("disk full"),
    )

    with pytest.raises(OSError, match="disk full"):
        load_module().train_and_cleanup()

    assert collection.delete_calls == []
    assert not list((tmp_path / "artifacts" / "backups").glob("*.parquet"))


def test_default_retention_setting_never_deletes_records(monkeypatch, tmp_path):
    monkeypatch.delenv("RETRAIN_DELETE_AFTER_BACKUP", raising=False)
    monkeypatch.setenv("RETRAIN_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("RETRAIN_MINIMUM_SAMPLES", "1")
    collection = install_fake_runtime(
        monkeypatch,
        [{"event_id": "event-1", "vector": [1.0], "sentiment_score": 0.1, "return": 0.2}],
    )

    load_module().train_and_cleanup()

    assert collection.delete_calls == []
    assert collection.flush_calls == 0


def test_enabled_deletion_follows_atomic_backup_publication_and_json_escapes_ids(
    monkeypatch, tmp_path
):
    configure_artifacts(monkeypatch, tmp_path, delete_after_backup=True)
    records = [
        {"event_id": 'quote"id', "vector": [1.0], "sentiment_score": 0.1, "return": 0.2},
        {"event_id": "slash\\id", "vector": [2.0], "sentiment_score": 0.2, "return": 0.3},
        *[
            {"event_id": f"event-{index}", "vector": [1.0], "sentiment_score": 0.1, "return": 0.2}
            for index in range(2, 501)
        ],
    ]
    collection = install_fake_runtime(monkeypatch, records)
    module = load_module()
    real_replace = os.replace
    replace_calls = []
    operations = []

    def record_replace(source, destination):
        replace_calls.append((Path(source), Path(destination)))
        if Path(destination).suffix == ".parquet":
            operations.append("backup_published")
        real_replace(source, destination)

    monkeypatch.setattr(module.os, "replace", record_replace)
    collection.on_delete = lambda: operations.append("delete")
    module.train_and_cleanup()

    backup_dir = tmp_path / "artifacts" / "backups"
    assert len(list(backup_dir.glob("*.parquet"))) == 1
    assert any(destination.suffix == ".parquet" for _, destination in replace_calls)
    assert operations.index("backup_published") < operations.index("delete")
    event_ids = [row["event_id"] for row in records]
    assert collection.delete_calls == [
        f"event_id in {json.dumps(event_ids[:500])}",
        f"event_id in {json.dumps(event_ids[500:])}",
    ]
    assert collection.flush_calls == 1


def test_failed_model_publish_preserves_previous_live_model(monkeypatch, tmp_path):
    configure_artifacts(monkeypatch, tmp_path, delete_after_backup=True)
    model_path = tmp_path / "artifacts" / "models" / "eth_sentiment_xgb.joblib"
    model_path.parent.mkdir(parents=True)
    model_path.write_bytes(b"previous model")
    collection = install_fake_runtime(
        monkeypatch,
        [{"event_id": "event-1", "vector": [1.0], "sentiment_score": 0.1, "return": 0.2}],
        model_error=OSError("model write failed"),
    )

    with pytest.raises(OSError, match="model write failed"):
        load_module().train_and_cleanup()

    assert model_path.read_bytes() == b"previous model"
    assert collection.delete_calls == []
    assert not list(model_path.parent.glob("*.tmp"))


def test_backup_paths_are_unique_and_do_not_replace_prior_backups(tmp_path):
    module = load_module()
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()

    class FakeDataFrame:
        def to_parquet(self, path, **_kwargs):
            Path(path).write_bytes(b"parquet backup")

    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    first = module._write_backup_atomically(FakeDataFrame(), backup_dir, now)
    second = module._write_backup_atomically(FakeDataFrame(), backup_dir, now)

    assert first != second
    assert first.read_bytes() == b"parquet backup"
    assert second.read_bytes() == b"parquet backup"
