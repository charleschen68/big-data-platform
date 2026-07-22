import importlib.util
from pathlib import Path


def load_module():
    path = Path(__file__).parents[1] / "eth_info_dataflow" / "rss_to_eth_social_stream.py"
    spec = importlib.util.spec_from_file_location("rss_collector", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_rss_settings_from_environment(monkeypatch):
    monkeypatch.setenv("RSS_FEEDS", "https://one.example/rss,https://two.example/rss")
    monkeypatch.setenv("RSS_TOPIC", "news")
    monkeypatch.setenv("RSS_CHECK_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    module = load_module()
    settings = module.load_rss_settings()
    assert settings["feeds"] == ["https://one.example/rss", "https://two.example/rss"]
    assert settings["topic"] == "news"
    assert settings["check_interval"] == 30
    assert settings["bootstrap_servers"] == "kafka:29092"
