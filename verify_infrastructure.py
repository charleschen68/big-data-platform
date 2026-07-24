import json
import subprocess
import time

import requests
from kafka import KafkaProducer

from infrastructure_checks import collector_errors


def kubectl_json(*args: str) -> dict:
    completed = subprocess.run(
        ["kubectl", "--context", "k3s-node", "-n", "collectors", *args, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def test_collectors() -> bool:
    try:
        errors = collector_errors(
            kubectl_json("get", "deployments"),
            kubectl_json("get", "cronjob", "model-retrain"),
            kubectl_json("get", "secret", "collector-secrets"),
        )
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"❌ Collectors: kubectl check failed: {exc}")
        return False
    if errors:
        for error in errors:
            print(f"❌ Collectors: {error}")
        return False
    print("✅ Collectors: deployments, CronJob and Secret are healthy.")
    return True


def test_redpanda() -> bool:
    print("Testing Kafka connection (localhost:9092)...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        producer.send("test-topic", {"test": "message", "timestamp": time.time()})
        producer.flush()
        print("✅ Kafka: Connection successful, message sent.")
        return True
    except Exception as exc:
        print(f"❌ Kafka: Failed to connect or send message: {exc}")
        return False


def test_clickhouse() -> bool:
    print("Testing ClickHouse connection (localhost:8123)...")
    try:
        response = requests.get(
            "http://localhost:8123/ping",
            timeout=5,
        )
        response.raise_for_status()
        result = response.text.strip()
        print(f"✅ ClickHouse: Connection successful, ping returned: {result}")
        return True
    except Exception as exc:
        print(f"❌ ClickHouse: Failed to connect: {exc}")
        return False


def test_flink() -> bool:
    print("Testing Flink REST API connection (localhost:8081)...")
    try:
        response = requests.get("http://localhost:8081/config", timeout=5)
        if response.status_code == 200:
            print("✅ Flink: JobManager is accessible.")
            return True
        else:
            print(f"❌ Flink: HTTP {response.status_code}")
            return False
    except Exception as exc:
        print(f"❌ Flink: Failed to connect to REST API: {exc}")
        return False


def main() -> int:
    checks = [test_redpanda(), test_clickhouse(), test_flink(), test_collectors()]
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
