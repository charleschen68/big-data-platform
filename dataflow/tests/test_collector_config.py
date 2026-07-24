import pytest

from collector_runtime.config import env_bool, env_int, env_str


def test_env_str_uses_default(monkeypatch):
    monkeypatch.delenv("COLLECTOR_NAME", raising=False)
    assert env_str("COLLECTOR_NAME", "rss") == "rss"


def test_env_int_rejects_value_below_minimum(monkeypatch):
    monkeypatch.setenv("INTERVAL", "0")
    with pytest.raises(ValueError, match="INTERVAL must be >= 1"):
        env_int("INTERVAL", 60, minimum=1)


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("1", True), ("false", False), ("0", False)])
def test_env_bool(monkeypatch, raw, expected):
    monkeypatch.setenv("FEATURE_FLAG", raw)
    assert env_bool("FEATURE_FLAG") is expected


def test_env_bool_rejects_unknown_value(monkeypatch):
    monkeypatch.setenv("FEATURE_FLAG", "sometimes")
    with pytest.raises(ValueError, match="FEATURE_FLAG must be a boolean"):
        env_bool("FEATURE_FLAG")
