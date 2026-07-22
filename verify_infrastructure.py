import json
import time
from kafka import KafkaProducer
import clickhouse_connect
import requests

def test_redpanda():
    print("Testing Redpanda connection (localhost:9093)...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send('test-topic', {'test': 'message', 'timestamp': time.time()})
        producer.flush()
        print("✅ Redpanda: Connection successful, message sent.")
    except Exception as e:
        print(f"❌ Redpanda: Failed to connect or send message: {e}")

def test_clickhouse():
    print("Testing ClickHouse connection (localhost:8123)...")
    try:
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
        result = client.command('SELECT 1')
        print(f"✅ ClickHouse: Connection successful, ping returned: {result}")
    except Exception as e:
        print(f"❌ ClickHouse: Failed to connect: {e}")

def test_flink():
    print("Testing Flink REST API connection (localhost:8081)...")
    try:
        response = requests.get('http://localhost:8081/config', timeout=5)
        if response.status_code == 200:
            print("✅ Flink: JobManager is accessible.")
        else:
            print(f"❌ Flink: HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ Flink: Failed to connect to REST API: {e}")

if __name__ == '__main__':
    test_redpanda()
    test_clickhouse()
    test_flink()
