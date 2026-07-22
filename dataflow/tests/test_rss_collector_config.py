import asyncio
import importlib.util
from pathlib import Path

import pytest


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


class StopCollector(Exception):
    pass


class RecordingHealth:
    def __init__(self, events):
        self.events = events

    def mark_ready(self):
        self.events.append("ready")

    def heartbeat(self):
        self.events.append("heartbeat")


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False


def build_collector(module, health):
    return module.RSSCollector(
        urls=[],
        bootstrap_servers="kafka:29092",
        check_interval=0,
        topic="news",
        health=health,
    )


def test_start_marks_ready_only_after_producer_starts(monkeypatch):
    module = load_module()
    events = []

    class Producer:
        async def start(self):
            events.append("producer_started")

        async def stop(self):
            events.append("producer_stopped")

    monkeypatch.setattr(module, "AIOKafkaProducer", lambda **kwargs: Producer())
    monkeypatch.setattr(module.aiohttp, "ClientSession", FakeSession)
    collector = build_collector(module, RecordingHealth(events))

    async def stop_after_startup():
        events.append("run_once")
        raise StopCollector

    monkeypatch.setattr(collector, "run_once", stop_after_startup)

    with pytest.raises(StopCollector):
        asyncio.run(collector.start())

    assert events == ["producer_started", "ready", "run_once", "producer_stopped"]


def test_start_does_not_mark_ready_when_producer_startup_fails(monkeypatch):
    module = load_module()
    events = []

    class StartupError(Exception):
        pass

    class Producer:
        async def start(self):
            events.append("producer_start_failed")
            raise StartupError("Kafka unavailable")

        async def stop(self):
            events.append("producer_stopped")

    monkeypatch.setattr(module, "AIOKafkaProducer", lambda **kwargs: Producer())
    collector = build_collector(module, RecordingHealth(events))

    with pytest.raises(StartupError, match="Kafka unavailable"):
        asyncio.run(collector.start())

    assert events == ["producer_start_failed"]


def test_start_heartbeats_after_each_completed_run_once(monkeypatch):
    module = load_module()
    events = []

    class Producer:
        async def start(self):
            events.append("producer_started")

        async def stop(self):
            events.append("producer_stopped")

    monkeypatch.setattr(module, "AIOKafkaProducer", lambda **kwargs: Producer())
    monkeypatch.setattr(module.aiohttp, "ClientSession", FakeSession)

    async def no_sleep(seconds):
        return None

    monkeypatch.setattr(module.asyncio, "sleep", no_sleep)
    collector = build_collector(module, RecordingHealth(events))
    run_count = 0

    async def run_three_times():
        nonlocal run_count
        run_count += 1
        events.append(f"run_once_{run_count}")
        if run_count == 3:
            raise StopCollector

    monkeypatch.setattr(collector, "run_once", run_three_times)

    with pytest.raises(StopCollector):
        asyncio.run(collector.start())

    assert events == [
        "producer_started",
        "ready",
        "run_once_1",
        "heartbeat",
        "run_once_2",
        "heartbeat",
        "run_once_3",
        "producer_stopped",
    ]
