# Phase 2：Python 采集器迁入 k3s Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将当前由 Compose `dev-runner` 托管的三个常驻 Python 工作负载迁入 k3s `collectors` namespace，并把 `eth_model_retrain.py` 部署为 CronJob；完成探针、Secret、资源限额、跨栈服务发现和无重复切流后下线 `dev-runner`。

**Architecture:** 每个工作负载使用独立的 ARM64 Python 3.11 镜像和 Kubernetes workload。公共配置与 HTTP 健康端点由轻量模块提供；Kafka、MySQL、Milvus 继续留在 Compose，通过 `ExternalName` Service 暴露为 namespace 内稳定 DNS 名。常驻进程使用 Deployment，重训练使用 `concurrencyPolicy: Forbid` 的 CronJob，并将模型/备份写入 PVC。

**Tech Stack:** Python 3.11、pytest 8.2、Docker/OCI、k3s v1.36.2+k3s1、Kubernetes Deployment/CronJob/ConfigMap/Secret/ExternalName Service/PVC、Kafka 3.7、MySQL 8、Milvus 2.4.15。

## Global Constraints

- Phase 2 只迁移 `rss_to_eth_social_stream.py`、`market_data_collector.py`、`eth_trade_settlement.py` 和 `eth_model_retrain.py`；其他 `dataflow/` 实验脚本不进入集群。
- Kafka、MySQL、Milvus 仍留在 Compose；集群内业务只使用 `kafka:29092`、`mysql:3306`、`milvus:19530`，不得硬编码宿主机 IP 或 `*.orb.local` 域名。
- Compose Kafka 的内部 advertised listener 仍为 `kafka:29092`；因此 Kubernetes ExternalName Service 必须命名为 `kafka`，否则客户端拿到 broker metadata 后无法解析。
- 密码只通过 `collector-secrets` Secret 注入；真实密码不得写入 Git、ConfigMap、镜像、测试或计划文件。
- 所有常驻进程必须提供 `/live` 和 `/ready` HTTP 探针；启动失败必须让容器非零退出，不允许靠无限异常循环伪装健康。
- 三个常驻 Deployment 均为单副本；资源 limit 合计不得超过 960Mi，符合总体设计中“采集器 1G”的预算。
- 所有镜像固定 Python 3.11，使用非 root 用户 `collector`，`imagePullPolicy: Never`；镜像通过 k3s containerd import 进入单节点，不引入临时 registry。
- 重训练默认不得删除 Milvus 历史数据；只有 `RETRAIN_DELETE_AFTER_BACKUP=true` 且 Parquet 备份成功时才允许删除。
- 迁移期间 Compose 业务栈持续运行；切流只停止并移除 `dev-runner`，不得重建 Kafka、Flink、MySQL、Milvus 或 ClickHouse 容器。
- Phase 2 验收未通过不得开始 Phase 3。
- 提交信息使用中文短句；所有 Git 命令在仓库根目录执行。

---

### Task 1: 公共环境配置与健康端点

**Files:**
- Create: `dataflow/collector_runtime/__init__.py`
- Create: `dataflow/collector_runtime/config.py`
- Create: `dataflow/collector_runtime/health.py`
- Create: `dataflow/tests/conftest.py`
- Create: `dataflow/tests/test_collector_config.py`
- Create: `dataflow/tests/test_health.py`

**Interfaces:**
- Produces: `env_str(name, default) -> str`、`env_int(name, default, minimum=1) -> int`、`env_bool(name, default=False) -> bool`
- Produces: `WorkloadHealth(stale_after_seconds)`，方法 `mark_ready()`、`heartbeat()`、`status(path) -> tuple[int, bytes]`
- Produces: `start_health_server(health, port) -> ThreadingHTTPServer`

- [ ] **Step 1: 创建测试导入路径**

创建 `dataflow/tests/conftest.py`：

```python
import sys
from pathlib import Path

DATAFLOW_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DATAFLOW_ROOT))
```

- [ ] **Step 2: 写配置解析失败测试**

创建 `dataflow/tests/test_collector_config.py`：

```python
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
```

- [ ] **Step 3: 写健康状态测试**

创建 `dataflow/tests/test_health.py`：

```python
from collector_runtime.health import WorkloadHealth


def test_health_is_not_ready_before_initialization():
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: 10.0)
    assert health.status("/live") == (200, b"live\n")
    assert health.status("/ready") == (503, b"not ready\n")


def test_health_becomes_ready_after_heartbeat():
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: 10.0)
    health.mark_ready()
    health.heartbeat()
    assert health.status("/ready") == (200, b"ready\n")


def test_stale_heartbeat_fails_both_probes_after_startup():
    now = [10.0]
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: now[0])
    health.mark_ready()
    health.heartbeat()
    now[0] = 71.0
    assert health.status("/ready") == (503, b"stale\n")
    assert health.status("/live") == (503, b"stale\n")


def test_unknown_health_path_returns_not_found():
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: 10.0)
    assert health.status("/unknown") == (404, b"not found\n")
```

- [ ] **Step 4: 运行测试确认失败**

Run:

```bash
pytest -q dataflow/tests/test_collector_config.py dataflow/tests/test_health.py
```

Expected: collection fails with `ModuleNotFoundError: No module named 'collector_runtime'`.

- [ ] **Step 5: 实现配置模块**

创建空文件 `dataflow/collector_runtime/__init__.py`，并创建 `dataflow/collector_runtime/config.py`：

```python
import os


def env_str(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} must not be empty")
    return value


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, str(default)).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")
```

- [ ] **Step 6: 实现健康端点**

创建 `dataflow/collector_runtime/health.py`：

```python
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Callable


class WorkloadHealth:
    def __init__(self, stale_after_seconds: int, clock: Callable[[], float] = time.monotonic):
        self._stale_after_seconds = stale_after_seconds
        self._clock = clock
        self._ready = False
        self._last_heartbeat = clock()
        self._lock = Lock()

    def mark_ready(self) -> None:
        with self._lock:
            self._ready = True

    def heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat = self._clock()

    def status(self, path: str) -> tuple[int, bytes]:
        if path not in {"/live", "/ready"}:
            return 404, b"not found\n"
        with self._lock:
            ready = self._ready
            stale = self._clock() - self._last_heartbeat > self._stale_after_seconds
        if stale:
            return 503, b"stale\n"
        if path == "/ready" and not ready:
            return 503, b"not ready\n"
        return (200, b"ready\n") if path == "/ready" else (200, b"live\n")


def start_health_server(health: WorkloadHealth, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            status, body = health.status(self.path)
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    Thread(target=server.serve_forever, name="health-server", daemon=True).start()
    return server
```

- [ ] **Step 7: 运行公共模块测试**

Run:

```bash
pytest -q dataflow/tests/test_collector_config.py dataflow/tests/test_health.py
```

Expected: `11 passed`.

- [ ] **Step 8: 提交**

```bash
git add dataflow/collector_runtime dataflow/tests
git commit -m "添加采集器配置与健康检查基础模块"
```

---

### Task 2: RSS 采集器参数化并接入探针

**Files:**
- Modify: `dataflow/eth_info_dataflow/rss_to_eth_social_stream.py`
- Create: `dataflow/tests/test_rss_collector_config.py`

**Interfaces:**
- Consumes: Task 1 的 `env_int`、`env_str`、`WorkloadHealth`、`start_health_server`
- Produces: `load_rss_settings() -> dict[str, object]`
- Produces: 环境变量 `RSS_FEEDS`、`RSS_TOPIC`、`RSS_CHECK_INTERVAL_SECONDS`、`KAFKA_BOOTSTRAP_SERVERS`、`HEALTH_PORT`、`HEALTH_STALE_AFTER_SECONDS`

- [ ] **Step 1: 写 RSS 配置测试**

创建 `dataflow/tests/test_rss_collector_config.py`：

```python
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest -q dataflow/tests/test_rss_collector_config.py`

Expected: FAIL with `AttributeError: module 'rss_collector' has no attribute 'load_rss_settings'`.

- [ ] **Step 3: 增加参数与健康状态**

在 RSS 文件 imports 后增加：

```python
from collector_runtime.config import env_int, env_str
from collector_runtime.health import WorkloadHealth, start_health_server

DEFAULT_RSS_FEEDS = (
    "https://cointelegraph.com/rss,"
    "https://www.coindesk.com/arc/outboundfeeds/rss/,"
    "https://decrypt.co/feed"
)


def load_rss_settings() -> dict[str, object]:
    feeds = [value.strip() for value in env_str("RSS_FEEDS", DEFAULT_RSS_FEEDS).split(",") if value.strip()]
    if not feeds:
        raise ValueError("RSS_FEEDS must contain at least one URL")
    return {
        "feeds": feeds,
        "topic": env_str("RSS_TOPIC", "eth_social_stream"),
        "check_interval": env_int("RSS_CHECK_INTERVAL_SECONDS", 60),
        "bootstrap_servers": env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "health_port": env_int("HEALTH_PORT", 8080),
        "stale_after": env_int("HEALTH_STALE_AFTER_SECONDS", 180),
    }
```

将 `RSSCollector.__init__` 改为接收 `topic: str` 与 `health: WorkloadHealth` 并保存；将 `process_item()` 中硬编码 topic 改为 `self.topic`。在 `await self.producer.start()` 后调用 `self.health.mark_ready()`；每次 `await self.run_once()` 返回后调用 `self.health.heartbeat()`。

将主入口替换为：

```python
if __name__ == "__main__":
    settings = load_rss_settings()
    health = WorkloadHealth(settings["stale_after"])
    start_health_server(health, settings["health_port"])
    collector = RSSCollector(
        urls=settings["feeds"],
        bootstrap_servers=settings["bootstrap_servers"],
        check_interval=settings["check_interval"],
        topic=settings["topic"],
        health=health,
    )
    try:
        asyncio.run(collector.start())
    except KeyboardInterrupt:
        logger.info("用户停止采集任务")
```

删除原来的 `start_http_server(8000)`、`prometheus_client` import、`NEWS_SENT` 定义以及 `NEWS_SENT.labels(...).inc()` 调用；Phase 2 的统一探针固定使用 8080，业务计数器在 Phase 3 统一接入 Prometheus。

- [ ] **Step 4: 运行 RSS 与公共测试**

Run:

```bash
pytest -q dataflow/tests/test_rss_collector_config.py dataflow/tests/test_collector_config.py dataflow/tests/test_health.py
```

Expected: all tests pass.

- [ ] **Step 5: 提交**

```bash
git add dataflow/eth_info_dataflow/rss_to_eth_social_stream.py dataflow/tests/test_rss_collector_config.py
git commit -m "RSS 采集器改用环境配置并接入探针"
```

---

### Task 3: 市场采集与交易结算进程接入探针

**Files:**
- Modify: `dataflow/eth_trade_dataflow/market_data_collector.py`
- Modify: `dataflow/eth_trade_dataflow/eth_trade_settlement.py`
- Create: `dataflow/tests/test_trade_worker_config.py`

**Interfaces:**
- Consumes: Task 1 公共运行模块
- Produces: 市场采集环境变量 `MYSQL_HOST`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DATABASE`、`MARKET_INTERVAL_SECONDS`、`HEALTH_PORT`
- Produces: 结算环境变量 `MILVUS_HOST`、`MILVUS_PORT`、`MILVUS_COLLECTION`、`KAFKA_BOOTSTRAP_SERVERS`、`TRADE_SIGNALS_TOPIC`、`HEALTH_PORT`
- Produces: Kafka settlement consumer 固定 `group_id=milvus_settlement_group`，空闲时也通过 `poll(timeout_ms=1000)` 更新心跳

- [ ] **Step 1: 写配置测试**

创建 `dataflow/tests/test_trade_worker_config.py`，使用与 Task 2 相同的 `importlib.util.spec_from_file_location` 方法加载两个脚本，并断言：

```python
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
    frame = module.pd.DataFrame({
        "high": list(range(101, 121)),
        "low": list(range(99, 119)),
        "close": list(range(100, 120)),
    })
    result = module.calculate_indicators(frame)
    assert {"rsi_14", "atr_14"}.issubset(result.columns)
    assert result[["rsi_14", "atr_14"]].dropna().empty is False
```

文件顶部必须包含：

```python
import importlib.util
from pathlib import Path
import pytest


def load_dataflow_module(relative_path: str, name: str):
    path = Path(__file__).parents[1] / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest -q dataflow/tests/test_trade_worker_config.py`

Expected: both tests fail because the two `load_*_settings` functions do not exist.

- [ ] **Step 3: 参数化市场采集器**

新增 `load_market_settings()`，默认值固定为：

```python
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
```

删除 `pandas_ta` import，新增不依赖第三方 TA 扩展的指标函数：

```python
def calculate_indicators(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    delta = df["close"].diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    average_gain = gains.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    average_loss = losses.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    relative_strength = average_gain / average_loss
    df["rsi_14"] = 100 - (100 / (1 + relative_strength))

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
```

让 `fetch_and_calculate()` 调用 `calculate_indicators(df)`。让 `MarketDataCollector.__init__(settings, health)` 使用 settings 建立数据库连接并在成功后 `mark_ready()`。让 `fetch_and_calculate()` 成功提交时返回 `True`，异常回滚后返回 `False`。`run_forever()` 每次成功时调用 `heartbeat()`，sleep 使用 `settings["interval"]`。主入口启动健康服务器；构造失败不得捕获，交给容器非零退出。

- [ ] **Step 4: 参数化结算进程并改为 poll 循环**

新增：

```python
def load_settlement_settings() -> dict[str, object]:
    return {
        "milvus_host": env_str("MILVUS_HOST", "milvus"),
        "milvus_port": env_int("MILVUS_PORT", 19530),
        "collection": env_str("MILVUS_COLLECTION", "eth_sentiment_analysis"),
        "kafka_bootstrap": env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "topic": env_str("TRADE_SIGNALS_TOPIC", "topic_trade_signals"),
        "health_port": env_int("HEALTH_PORT", 8080),
        "stale_after": env_int("HEALTH_STALE_AFTER_SECONDS", 30),
    }
```

让构造函数使用 settings；Milvus collection load 和 KafkaConsumer 初始化成功后调用 `health.mark_ready()`。将 `for message in self.consumer` 替换为：

```python
while True:
    batches = self.consumer.poll(timeout_ms=1000, max_records=100)
    self.health.heartbeat()
    for messages in batches.values():
        for message in messages:
            signal = message.value
            if signal.get("action") == "SELL":
                self.process_sell_signal(signal)
```

主入口启动健康服务器。保留固定 group id，禁止用随机 group id，否则切流时会重复消费全部信号。

- [ ] **Step 5: 运行测试和语法检查**

Run:

```bash
pytest -q dataflow/tests/test_trade_worker_config.py dataflow/tests/test_collector_config.py dataflow/tests/test_health.py
python3 -m py_compile dataflow/eth_trade_dataflow/market_data_collector.py dataflow/eth_trade_dataflow/eth_trade_settlement.py
```

Expected: tests and compilation pass.

- [ ] **Step 6: 提交**

```bash
git add dataflow/eth_trade_dataflow/market_data_collector.py dataflow/eth_trade_dataflow/eth_trade_settlement.py dataflow/tests/test_trade_worker_config.py
git commit -m "市场采集与交易结算进程接入 Kubernetes 探针"
```

---

### Task 4: 修正并保护模型重训练任务

**Files:**
- Modify: `dataflow/eth_info_dataflow/eth_model_retrain.py`
- Create: `dataflow/tests/test_model_retrain.py`

**Interfaces:**
- Consumes: 当前交易集合字段 `event_id`、`vector`、`sentiment_score`、`return`、`pub_date`、`is_settled`
- Produces: `build_training_filter(now) -> str`
- Produces: `RETRAIN_DELETE_AFTER_BACKUP=false` 安全开关
- Produces: 模型 `/artifacts/models/eth_sentiment_xgb.joblib` 与备份 `/artifacts/backups/*.parquet`

- [ ] **Step 1: 写 schema 与删除保护测试**

创建 `dataflow/tests/test_model_retrain.py`：

```python
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest -q dataflow/tests/test_model_retrain.py`

Expected: FAIL because `build_training_filter` and `load_retrain_settings` do not exist.

- [ ] **Step 3: 参数化并对齐当前 Milvus schema**

新增 settings，使用：

```python
def load_retrain_settings() -> dict[str, object]:
    artifact_root = Path(env_str("RETRAIN_ARTIFACT_ROOT", "/artifacts"))
    return {
        "milvus_host": env_str("MILVUS_HOST", "milvus"),
        "milvus_port": env_int("MILVUS_PORT", 19530),
        "collection": env_str("MILVUS_COLLECTION", "eth_sentiment_analysis"),
        "minimum_samples": env_int("RETRAIN_MINIMUM_SAMPLES", 100),
        "delete_after_backup": env_bool("RETRAIN_DELETE_AFTER_BACKUP", False),
        "model_path": artifact_root / "models" / "eth_sentiment_xgb.joblib",
        "backup_dir": artifact_root / "backups",
    }


def build_training_filter(now: datetime) -> str:
    six_months_ago = int((now - timedelta(days=180)).timestamp() * 1000)
    three_months_ago = int((now - timedelta(days=90)).timestamp() * 1000)
    return f"is_settled == true and pub_date >= {six_months_ago} and pub_date < {three_months_ago}"
```

新增标准库 imports `json`、`timezone` 和 `Path`；把 `joblib`、`numpy`、`pandas`、`pymilvus`、`xgboost` imports 移入 `train_and_cleanup()`，使配置与过滤表达式的单元测试不依赖重型运行包。

查询字段改为：

```python
output_fields=["event_id", "vector", "sentiment_score", "return", "pub_date"]
```

标签改为 `y = np.array([row["return"] for row in res])`，删除键改为 `event_id`。Parquet 使用 `engine="pyarrow"`。先创建 artifacts 目录并成功写入备份；只有 `delete_after_backup` 为 true 时，才用 `collection.delete(f"event_id in {json.dumps(batch_ids)}")` 分批删除。异常必须记录后 `raise`，让 CronJob 标为 Failed。

- [ ] **Step 4: 运行单元测试与全量 Python 测试**

Run:

```bash
pytest -q dataflow/tests
python3 -m py_compile dataflow/eth_info_dataflow/eth_model_retrain.py
```

Expected: all tests and compilation pass.

- [ ] **Step 5: 提交**

```bash
git add dataflow/eth_info_dataflow/eth_model_retrain.py dataflow/tests/test_model_retrain.py
git commit -m "模型重训练对齐交易集合并增加删除保护"
```

---

### Task 5: 为四个工作负载构建最小独立镜像

**Files:**
- Create: `dataflow/docker/rss.Dockerfile`
- Create: `dataflow/docker/market.Dockerfile`
- Create: `dataflow/docker/settlement.Dockerfile`
- Create: `dataflow/docker/retrain.Dockerfile`
- Create: `dataflow/requirements/rss.txt`
- Create: `dataflow/requirements/market.txt`
- Create: `dataflow/requirements/settlement.txt`
- Create: `dataflow/requirements/retrain.txt`
- Create: `infra/scripts/build-and-import-collectors.sh`

**Interfaces:**
- Consumes: Tasks 1–4 的 Python 模块
- Produces: `big-data/rss-collector:phase2`、`big-data/market-collector:phase2`、`big-data/settlement-worker:phase2`、`big-data/model-retrain:phase2`

- [ ] **Step 1: 写锁定的运行依赖**

创建四个 requirements 文件：

```text
# dataflow/requirements/rss.txt
aiohttp==3.13.5
aiokafka==0.13.0
beautifulsoup4==4.14.3
feedparser==6.0.12
```

```text
# dataflow/requirements/market.txt
ccxt==4.5.67
mysql-connector-python==9.7.0
pandas==2.3.3
python-dotenv==1.2.2
pytz==2026.2
```

```text
# dataflow/requirements/settlement.txt
kafka-python==2.3.2
pymilvus==2.4.15
python-dotenv==1.2.2
```

```text
# dataflow/requirements/retrain.txt
joblib==1.5.3
numpy==2.3.5
pandas==2.3.3
pyarrow==22.0.0
pymilvus==2.4.15
xgboost==3.1.2
```

以上版本已在 2026-07-22 通过 PyPI index 核对存在；构建必须使用这些精确版本，不得改成无版本约束依赖。

- [ ] **Step 2: 写四个非 root Dockerfile**

创建 `dataflow/docker/rss.Dockerfile`：

```dockerfile
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/dataflow \
    HOME=/tmp

RUN groupadd --system --gid 10001 collector \
    && useradd --system --uid 10001 --gid collector --home /app collector
WORKDIR /app

COPY dataflow/requirements/rss.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --requirement /tmp/requirements.txt

COPY --chown=collector:collector dataflow/collector_runtime /app/dataflow/collector_runtime
COPY --chown=collector:collector dataflow/eth_info_dataflow/rss_to_eth_social_stream.py /app/dataflow/eth_info_dataflow/rss_to_eth_social_stream.py

USER collector
EXPOSE 8080
CMD ["python", "/app/dataflow/eth_info_dataflow/rss_to_eth_social_stream.py"]
```

创建 `dataflow/docker/market.Dockerfile`：

```dockerfile
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/dataflow \
    HOME=/tmp

RUN groupadd --system --gid 10001 collector \
    && useradd --system --uid 10001 --gid collector --home /app collector
WORKDIR /app

COPY dataflow/requirements/market.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --requirement /tmp/requirements.txt

COPY --chown=collector:collector dataflow/collector_runtime /app/dataflow/collector_runtime
COPY --chown=collector:collector dataflow/eth_trade_dataflow/market_data_collector.py /app/dataflow/eth_trade_dataflow/market_data_collector.py

USER collector
EXPOSE 8080
CMD ["python", "/app/dataflow/eth_trade_dataflow/market_data_collector.py"]
```

创建 `dataflow/docker/settlement.Dockerfile`：

```dockerfile
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/dataflow \
    HOME=/tmp

RUN groupadd --system --gid 10001 collector \
    && useradd --system --uid 10001 --gid collector --home /app collector
WORKDIR /app

COPY dataflow/requirements/settlement.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --requirement /tmp/requirements.txt

COPY --chown=collector:collector dataflow/collector_runtime /app/dataflow/collector_runtime
COPY --chown=collector:collector dataflow/eth_trade_dataflow/eth_trade_settlement.py /app/dataflow/eth_trade_dataflow/eth_trade_settlement.py

USER collector
EXPOSE 8080
CMD ["python", "/app/dataflow/eth_trade_dataflow/eth_trade_settlement.py"]
```

创建 `dataflow/docker/retrain.Dockerfile`：

```dockerfile
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/dataflow \
    HOME=/tmp

RUN groupadd --system --gid 10001 collector \
    && useradd --system --uid 10001 --gid collector --home /app collector \
    && mkdir -p /artifacts/models /artifacts/backups \
    && chown -R collector:collector /artifacts
WORKDIR /app

COPY dataflow/requirements/retrain.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --requirement /tmp/requirements.txt

COPY --chown=collector:collector dataflow/collector_runtime /app/dataflow/collector_runtime
COPY --chown=collector:collector dataflow/eth_info_dataflow/eth_model_retrain.py /app/dataflow/eth_info_dataflow/eth_model_retrain.py

USER collector
CMD ["python", "/app/dataflow/eth_info_dataflow/eth_model_retrain.py"]
```

- [ ] **Step 3: 写可重复构建与导入脚本**

创建 `infra/scripts/build-and-import-collectors.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

VM_NAME="${VM_NAME:-k3s-node}"
PLATFORM="linux/arm64"

images=(rss market settlement retrain)
for name in "${images[@]}"; do
  case "${name}" in
    rss) image="big-data/rss-collector:phase2" ;;
    market) image="big-data/market-collector:phase2" ;;
    settlement) image="big-data/settlement-worker:phase2" ;;
    retrain) image="big-data/model-retrain:phase2" ;;
  esac
  docker build --platform "${PLATFORM}" -f "dataflow/docker/${name}.Dockerfile" -t "${image}" .
  docker save "${image}" | orb -m "${VM_NAME}" -u root k3s ctr images import -
done

orb -m "${VM_NAME}" -u root k3s ctr images list | grep 'big-data/'
```

执行 `chmod +x infra/scripts/build-and-import-collectors.sh`。

- [ ] **Step 4: 构建镜像并做非 root 冒烟检查**

Run:

```bash
infra/scripts/build-and-import-collectors.sh
for image in big-data/rss-collector:phase2 big-data/market-collector:phase2 big-data/settlement-worker:phase2 big-data/model-retrain:phase2; do
  docker run --rm --entrypoint id "$image"
done
```

Expected: 四个镜像构建和导入成功；每个 `id` 输出的 uid 非 0。

- [ ] **Step 5: 提交**

```bash
git add dataflow/docker dataflow/requirements infra/scripts/build-and-import-collectors.sh
git commit -m "为 Phase 2 工作负载添加独立容器镜像"
```

---

### Task 6: 建立 Compose 与 collectors namespace 的稳定服务发现

**Files:**
- Create: `infra/k8s/collectors/external-services.yaml`
- Create: `infra/k8s/collectors/configmap.yaml`
- Create: `infra/k8s/collectors/kustomization.yaml`

**Interfaces:**
- Consumes: OrbStack 容器域名 `kafka.orb.local`、`mysql.orb.local`、`milvus-standalone.orb.local`
- Produces: namespace 内 DNS `kafka`、`mysql`、`milvus`
- Produces: ConfigMap `collector-config`

- [ ] **Step 1: 写 ExternalName Service 清单**

创建 `infra/k8s/collectors/external-services.yaml`，包含三个 `type: ExternalName` Service：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: collectors
spec:
  type: ExternalName
  externalName: kafka.orb.local
  ports:
    - name: kafka
      port: 29092
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: collectors
spec:
  type: ExternalName
  externalName: mysql.orb.local
  ports:
    - name: mysql
      port: 3306
---
apiVersion: v1
kind: Service
metadata:
  name: milvus
  namespace: collectors
spec:
  type: ExternalName
  externalName: milvus-standalone.orb.local
  ports:
    - name: milvus
      port: 19530
```

- [ ] **Step 2: 写非敏感 ConfigMap**

创建 `infra/k8s/collectors/configmap.yaml`：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: collector-config
  namespace: collectors
data:
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  RSS_TOPIC: eth_social_stream
  RSS_CHECK_INTERVAL_SECONDS: "60"
  RSS_FEEDS: https://cointelegraph.com/rss,https://www.coindesk.com/arc/outboundfeeds/rss/,https://decrypt.co/feed
  MYSQL_HOST: mysql
  MYSQL_USER: root
  MYSQL_DATABASE: trade
  MARKET_INTERVAL_SECONDS: "60"
  MILVUS_HOST: milvus
  MILVUS_PORT: "19530"
  MILVUS_COLLECTION: eth_sentiment_analysis
  TRADE_SIGNALS_TOPIC: topic_trade_signals
  HEALTH_PORT: "8080"
  HEALTH_STALE_AFTER_SECONDS: "180"
  RETRAIN_ARTIFACT_ROOT: /artifacts
  RETRAIN_MINIMUM_SAMPLES: "100"
  RETRAIN_DELETE_AFTER_BACKUP: "false"
```

- [ ] **Step 3: 创建 Secret（不入 Git）**

Run:

```bash
MYSQL_PASSWORD="$(awk -F= '$1=="MYSQL_ROOT_PASSWORD" {print substr($0, index($0, "=") + 1)}' infra/compose/.env)"
test -n "$MYSQL_PASSWORD"
kubectl --context k3s-node -n collectors create secret generic collector-secrets \
  --from-literal=MYSQL_PASSWORD="$MYSQL_PASSWORD" \
  --dry-run=client -o yaml | kubectl --context k3s-node apply -f -
unset MYSQL_PASSWORD
kubectl --context k3s-node -n collectors get secret collector-secrets -o jsonpath='{.metadata.name}{" data="}{.data.MYSQL_PASSWORD}{"\n"}' | sed 's/data=.*/data=<redacted>/'
```

Expected: `collector-secrets data=<redacted>`；终端不得打印密码明文。

- [ ] **Step 4: 写初始 kustomization 并验证 DNS/TCP**

创建：

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - external-services.yaml
  - configmap.yaml
```

Run:

```bash
kubectl --context k3s-node apply -k infra/k8s/collectors
kubectl --context k3s-node -n collectors run network-check --rm -i --restart=Never --image=busybox:1.36 -- \
  sh -c 'nslookup kafka && nc -zvw5 kafka 29092 && nc -zvw5 mysql 3306 && nc -zvw5 milvus 19530'
```

Expected: 三个 DNS/端口检查全部成功。任一失败必须修复 OrbStack 容器域名访问，禁止改成硬编码 IP。

- [ ] **Step 5: 提交**

```bash
git add infra/k8s/collectors
git commit -m "为采集器建立 Compose 跨栈服务发现"
```

---

### Task 7: 部署三个 Deployment 与重训练 CronJob

**Files:**
- Create: `infra/k8s/collectors/rss-deployment.yaml`
- Create: `infra/k8s/collectors/market-deployment.yaml`
- Create: `infra/k8s/collectors/settlement-deployment.yaml`
- Create: `infra/k8s/collectors/retrain-cronjob.yaml`
- Create: `infra/k8s/collectors/artifacts-pvc.yaml`
- Modify: `infra/k8s/collectors/kustomization.yaml`

**Interfaces:**
- Consumes: Tasks 5–6 的四个镜像、ConfigMap、Secret 和 Service
- Produces: Deployments `rss-collector`、`market-collector`、`settlement-worker`
- Produces: CronJob `model-retrain`，schedule `0 3 * * 0`，时区 `Asia/Shanghai`

- [ ] **Step 1: 写三个 Deployment**

三个文件都先使用 `replicas: 0`，Task 8 切流时再改为 1。创建 `rss-deployment.yaml`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rss-collector
  namespace: collectors
spec:
  replicas: 0
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: rss-collector
  template:
    metadata:
      labels:
        app: rss-collector
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: rss-collector
          image: big-data/rss-collector:phase2
          imagePullPolicy: Never
          envFrom:
            - configMapRef: {name: collector-config}
          ports:
            - {name: health, containerPort: 8080}
          livenessProbe:
            httpGet: {path: /live, port: health}
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          readinessProbe:
            httpGet: {path: /ready, port: health}
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 3
          resources:
            requests: {cpu: 50m, memory: 64Mi}
            limits: {cpu: 200m, memory: 192Mi}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: ["ALL"]}
          volumeMounts:
            - {name: tmp, mountPath: /tmp}
      volumes:
        - name: tmp
          emptyDir: {}
```

创建 `market-deployment.yaml`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: market-collector
  namespace: collectors
spec:
  replicas: 0
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: market-collector
  template:
    metadata:
      labels:
        app: market-collector
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: market-collector
          image: big-data/market-collector:phase2
          imagePullPolicy: Never
          envFrom:
            - configMapRef: {name: collector-config}
          env:
            - name: MYSQL_PASSWORD
              valueFrom:
                secretKeyRef: {name: collector-secrets, key: MYSQL_PASSWORD}
          ports:
            - {name: health, containerPort: 8080}
          livenessProbe:
            httpGet: {path: /live, port: health}
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          readinessProbe:
            httpGet: {path: /ready, port: health}
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 3
          resources:
            requests: {cpu: 100m, memory: 128Mi}
            limits: {cpu: 500m, memory: 384Mi}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: ["ALL"]}
          volumeMounts:
            - {name: tmp, mountPath: /tmp}
      volumes:
        - name: tmp
          emptyDir: {}
```

创建 `settlement-deployment.yaml`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: settlement-worker
  namespace: collectors
spec:
  replicas: 0
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: settlement-worker
  template:
    metadata:
      labels:
        app: settlement-worker
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: settlement-worker
          image: big-data/settlement-worker:phase2
          imagePullPolicy: Never
          envFrom:
            - configMapRef: {name: collector-config}
          ports:
            - {name: health, containerPort: 8080}
          livenessProbe:
            httpGet: {path: /live, port: health}
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 3
          readinessProbe:
            httpGet: {path: /ready, port: health}
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 2
            failureThreshold: 3
          resources:
            requests: {cpu: 100m, memory: 128Mi}
            limits: {cpu: 500m, memory: 384Mi}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: ["ALL"]}
          volumeMounts:
            - {name: tmp, mountPath: /tmp}
      volumes:
        - name: tmp
          emptyDir: {}
```

- [ ] **Step 2: 写 artifacts PVC 与 CronJob**

创建 `artifacts-pvc.yaml`：

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: collector-artifacts
  namespace: collectors
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: local-path
  resources:
    requests:
      storage: 5Gi
```

创建 `retrain-cronjob.yaml`：

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: model-retrain
  namespace: collectors
spec:
  schedule: "0 3 * * 0"
  timeZone: Asia/Shanghai
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 2
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 1
      activeDeadlineSeconds: 3600
      template:
        spec:
          restartPolicy: Never
          securityContext:
            runAsNonRoot: true
            runAsUser: 10001
            seccompProfile:
              type: RuntimeDefault
          containers:
            - name: model-retrain
              image: big-data/model-retrain:phase2
              imagePullPolicy: Never
              envFrom:
                - configMapRef:
                    name: collector-config
              resources:
                requests:
                  cpu: 200m
                  memory: 256Mi
                limits:
                  cpu: "2"
                  memory: 1Gi
              securityContext:
                allowPrivilegeEscalation: false
                capabilities:
                  drop: ["ALL"]
              volumeMounts:
                - name: artifacts
                  mountPath: /artifacts
                - name: tmp
                  mountPath: /tmp
          volumes:
            - name: artifacts
              persistentVolumeClaim:
                claimName: collector-artifacts
            - name: tmp
              emptyDir: {}
```

- [ ] **Step 3: 将五个资源加入 kustomization 并做静态检查**

将 `kustomization.yaml` 改为：

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - external-services.yaml
  - configmap.yaml
  - artifacts-pvc.yaml
  - rss-deployment.yaml
  - market-deployment.yaml
  - settlement-deployment.yaml
  - retrain-cronjob.yaml
```

Run:

```bash
kubectl kustomize infra/k8s/collectors > /tmp/phase2-rendered.yaml
kubectl --context k3s-node apply --dry-run=server -f /tmp/phase2-rendered.yaml
```

Expected: server-side dry run succeeds.

- [ ] **Step 4: 初始以零副本部署并验证 CronJob 模板**

为避免在切流前双写，三个 Deployment 清单初始 `replicas: 0`。Run:

```bash
kubectl --context k3s-node apply -k infra/k8s/collectors
kubectl --context k3s-node -n collectors create job --from=cronjob/model-retrain model-retrain-smoke
kubectl --context k3s-node -n collectors wait --for=condition=complete job/model-retrain-smoke --timeout=3600s || \
  kubectl --context k3s-node -n collectors logs job/model-retrain-smoke
```

Expected: 样本不足时 Job 仍以 0 退出并打印样本量不足；连接、schema 或依赖错误必须非零失败并修复。

- [ ] **Step 5: 提交**

```bash
git add infra/k8s/collectors
git commit -m "添加采集器 Deployment 与模型重训练 CronJob"
```

---

### Task 8: 无重复切流、故障恢复验收并下线 dev-runner

**Files:**
- Modify: `infra/k8s/collectors/rss-deployment.yaml`
- Modify: `infra/k8s/collectors/market-deployment.yaml`
- Modify: `infra/k8s/collectors/settlement-deployment.yaml`
- Modify: `infra/compose/docker-compose.yml`
- Modify: `verify_infrastructure.py`
- Create: `infrastructure_checks.py`
- Modify: `docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md`
- Create: `docs/runbooks/phase2-collectors-cutover.md`

**Interfaces:**
- Consumes: Task 7 的零副本资源
- Produces: 三个 `replicas: 1` 且 Ready 的 Deployment；Compose 不再包含 `dev-runner`
- Produces: Phase 2 验收证据与回滚步骤

- [ ] **Step 1: 记录切流前基线**

Run:

```bash
docker compose -f infra/compose/docker-compose.yml ps -q | sort > /tmp/phase2-compose-before.txt
docker inspect -f '{{.Id}} {{.Name}}' kafka mysql milvus-standalone flink-jobmanager flink-taskmanager clickhouse > /tmp/phase2-critical-container-ids.txt
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic eth_social_stream > /tmp/phase2-offset-before.txt
docker compose -f infra/compose/docker-compose.yml logs --tail=50 dev-runner
```

Expected: 三个旧 Python 进程仍由 `dev-runner` 运行；关键容器 ID 已记录。

- [ ] **Step 2: 在一个 60 秒采集周期内完成切流**

先把三个 Deployment 清单的 `replicas` 从 0 改为 1，但尚不 apply。然后执行：

```bash
docker compose -f infra/compose/docker-compose.yml stop dev-runner
kubectl --context k3s-node apply -k infra/k8s/collectors
kubectl --context k3s-node -n collectors rollout status deployment/rss-collector --timeout=180s
kubectl --context k3s-node -n collectors rollout status deployment/market-collector --timeout=180s
kubectl --context k3s-node -n collectors rollout status deployment/settlement-worker --timeout=180s
```

Expected: 三个 Deployment 在 180 秒内 Ready。任一失败时立即执行回滚：

```bash
kubectl --context k3s-node -n collectors scale deployment/rss-collector deployment/market-collector deployment/settlement-worker --replicas=0
docker compose -f infra/compose/docker-compose.yml start dev-runner
```

- [ ] **Step 3: 验证数据继续流动且关键 Compose 容器未重建**

等待 120 秒后：

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic eth_social_stream
kubectl --context k3s-node -n collectors logs deployment/rss-collector --tail=100
kubectl --context k3s-node -n collectors logs deployment/market-collector --tail=100
kubectl --context k3s-node -n collectors logs deployment/settlement-worker --tail=100
docker inspect -f '{{.Id}} {{.Name}}' kafka mysql milvus-standalone flink-jobmanager flink-taskmanager clickhouse | diff /tmp/phase2-critical-container-ids.txt -
```

Expected: Kafka topic offset 相比基线增长或 RSS 明确记录“本轮无新数据”；market 有成功写入日志；settlement 维持 Ready；关键容器 ID diff 无输出。

- [ ] **Step 4: 执行 Pod 删除自愈验收**

逐个执行，不能并行删除：

```bash
for deployment in rss-collector market-collector settlement-worker; do
  old_pod="$(kubectl --context k3s-node -n collectors get pod -l app="$deployment" -o jsonpath='{.items[0].metadata.name}')"
  kubectl --context k3s-node -n collectors delete pod "$old_pod"
  kubectl --context k3s-node -n collectors rollout status deployment/$deployment --timeout=180s
  new_pod="$(kubectl --context k3s-node -n collectors get pod -l app="$deployment" -o jsonpath='{.items[0].metadata.name}')"
  test "$old_pod" != "$new_pod"
done
kubectl --context k3s-node -n collectors get pods
```

Expected: 每个被删 pod 都由新 pod 替代并 Ready；RSS 在两个采集周期内恢复写入；settlement 使用相同 consumer group，不从历史重新消费。

- [ ] **Step 5: 从 Compose 清单移除 dev-runner**

删除 `infra/compose/docker-compose.yml` 中完整的 `dev-runner:` service。验证删除清单不会重建其他服务：

```bash
docker compose -f infra/compose/docker-compose.yml config --quiet
docker compose -f infra/compose/docker-compose.yml up -d --remove-orphans
docker inspect -f '{{.Id}} {{.Name}}' kafka mysql milvus-standalone flink-jobmanager flink-taskmanager clickhouse | diff /tmp/phase2-critical-container-ids.txt -
docker ps --format '{{.Names}}' | grep -x dev-box && exit 1 || true
```

Expected: compose config valid；关键容器 ID 不变；`dev-box` 不存在。

- [ ] **Step 6: 扩展基础设施验证脚本**

创建 `infrastructure_checks.py`：

```python
EXPECTED_COLLECTOR_DEPLOYMENTS = {"rss-collector", "market-collector", "settlement-worker"}


def collector_errors(deployments: dict, cronjob: dict, secret: dict) -> list[str]:
    errors = []
    available = {
        item["metadata"]["name"]: item.get("status", {}).get("availableReplicas", 0)
        for item in deployments.get("items", [])
    }
    for name in sorted(EXPECTED_COLLECTOR_DEPLOYMENTS):
        if available.get(name) != 1:
            errors.append(f"deployment {name} availableReplicas={available.get(name, 0)}")
    if cronjob.get("spec", {}).get("suspend", False):
        errors.append("cronjob model-retrain is suspended")
    if "MYSQL_PASSWORD" not in secret.get("data", {}):
        errors.append("collector-secrets is missing MYSQL_PASSWORD")
    return errors
```

创建 `dataflow/tests/test_verify_infrastructure.py`：

```python
from infrastructure_checks import collector_errors


def healthy_payloads():
    deployments = {
        "items": [
            {"metadata": {"name": name}, "status": {"availableReplicas": 1}}
            for name in ("rss-collector", "market-collector", "settlement-worker")
        ]
    }
    cronjob = {"spec": {"suspend": False}}
    secret = {"data": {"MYSQL_PASSWORD": "cmVkYWN0ZWQ="}}
    return deployments, cronjob, secret


def test_collectors_healthy():
    assert collector_errors(*healthy_payloads()) == []


def test_missing_replica_is_an_error():
    deployments, cronjob, secret = healthy_payloads()
    deployments["items"][0]["status"]["availableReplicas"] = 0
    assert collector_errors(deployments, cronjob, secret) == [
        "deployment rss-collector availableReplicas=0"
    ]


def test_suspended_cronjob_is_an_error():
    deployments, cronjob, secret = healthy_payloads()
    cronjob["spec"]["suspend"] = True
    assert collector_errors(deployments, cronjob, secret) == [
        "cronjob model-retrain is suspended"
    ]
```

在 `verify_infrastructure.py` 新增：

```python
import subprocess
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
```

把现有 `test_redpanda()`、`test_clickhouse()`、`test_flink()` 改为成功时 `return True`、所有失败分支 `return False`。主入口替换为：

```python
if __name__ == "__main__":
    checks = [test_redpanda(), test_clickhouse(), test_flink(), test_collectors()]
    raise SystemExit(0 if all(checks) else 1)
```

Run:

```bash
pytest -q dataflow/tests/test_verify_infrastructure.py
python3 verify_infrastructure.py
```

Expected: tests pass；实时冒烟脚本全部通过。

- [ ] **Step 7: 写切流 runbook 和完成标注**

`docs/runbooks/phase2-collectors-cutover.md` 必须记录：镜像构建/import 命令、Secret 重建命令、部署命令、零副本到单副本切流、三个回滚命令、Pod 删除验收、CronJob 手工触发、PVC 备份位置。将总体设计 Phase 2 段落改为删除线并附实际完成 commit；只有本 Task 所有实时验收通过后才能标记完成。

- [ ] **Step 8: 全量验证并提交**

Run:

```bash
pytest -q dataflow/tests
python3 -m py_compile \
  dataflow/eth_info_dataflow/rss_to_eth_social_stream.py \
  dataflow/eth_trade_dataflow/market_data_collector.py \
  dataflow/eth_trade_dataflow/eth_trade_settlement.py \
  dataflow/eth_info_dataflow/eth_model_retrain.py \
  verify_infrastructure.py
kubectl --context k3s-node apply --dry-run=server -k infra/k8s/collectors
python3 verify_infrastructure.py
git diff --check
```

Expected: all commands exit 0.

```bash
git add infra/compose/docker-compose.yml infra/k8s/collectors verify_infrastructure.py infrastructure_checks.py dataflow/tests/test_verify_infrastructure.py docs/runbooks/phase2-collectors-cutover.md docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md
git commit -m "完成 Phase 2 采集器迁移与故障恢复验收"
```

---

## Phase 2 Completion Gate

开始 Phase 3 前必须同时满足：

1. `rss-collector`、`market-collector`、`settlement-worker` 均为 `1/1 Available`，探针生效且资源限制符合预算。
2. 删除任一 pod 后 180 秒内自动恢复 Ready，RSS 在两个采集周期内恢复工作。
3. `model-retrain` 手工 Job 成功或以“样本不足”正常退出；任何连接/schema 错误表现为 Failed。
4. `collector-secrets` 存在但仓库扫描无密码明文。
5. Kafka、MySQL、Milvus 通过稳定 Service DNS 访问，业务代码不包含 `*.orb.local` 或宿主机 IP。
6. `dev-runner` 已从 Compose 清单和运行容器中移除，其他关键 Compose 容器 ID 与切流前一致。
7. `pytest -q dataflow/tests`、Kubernetes server-side dry run、`python3 verify_infrastructure.py` 全部退出 0。
8. 总体设计 Phase 2 已回填实际完成 commit；未满足前不得编写或执行 Phase 3 实施计划。
