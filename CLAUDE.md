# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A real-time big-data platform for crypto (ETH) sentiment trading and risk control. It combines:

- **Java/Flink streaming jobs** (`datastream/`) — Maven multi-module project, Java 17, Flink 1.18.1
- **Python data collectors** (`dataflow/`) — scrapers/collectors that produce into Kafka, plus Milvus schema scripts
- **Rust Substreams** (`substreams/uniswap-ticks/`) — extracts Uniswap swap ticks from Ethereum blocks (WASM)
- **Flink SQL + ClickHouse** (`flink-sql/`, `clickhouse-sql/`) — Uniswap tick analytics pipeline
- All infrastructure runs via `docker-compose.yml`

Comments and docs are largely in Chinese.

## Build commands

```bash
# Build all Java modules (no tests exist in the repo; builds skip them)
mvn clean package -DskipTests          # or ./build.sh

# Build one job module plus its dependencies (common)
mvn clean package -DskipTests -pl datastream/eth-sentiment-trading-job -am
```

Python dependencies: `pip install -r requirements.txt` (pyspark, aiokafka, pymilvus, ccxt, clickhouse_connect, etc.).

Substreams module: standard Rust/cargo build targeting `wasm32-unknown-unknown` (see `substreams/uniswap-ticks/substreams.yaml`).

## Infrastructure

`docker-compose up -d` starts: Kafka (KRaft, 9092), Flink JobManager/TaskManager (UI 8081), MySQL (3306, db `streampark`, root/streampark), StreamPark (10000, job deployment platform), Milvus + etcd + MinIO (19530, Attu UI 8000), ClickHouse (8123/9000), Prometheus (9090), Grafana (3000), cAdvisor (8080), and a `dev-runner` container that auto-runs the Python collectors in `dataflow/` and writes `*.log` files next to them.

`verify_infrastructure.py` smoke-tests Kafka/Redpanda and ClickHouse connectivity from the host.

## Architecture

### Sentiment trading pipeline (the core flow)

1. Python collectors (`dataflow/eth_info_dataflow/rss_to_eth_social_stream.py`, `cryptopanic_to_eth_social_stream.py`) scrape news/social feeds → Kafka topic `eth_social_stream`; `dataflow/eth_trade_dataflow/market_data_collector.py` writes ETH market features (rsi_14, atr_14, price) to MySQL.
2. Flink job `datastream/eth-sentiment-trading-job` (`EthSentimentTradingJob`) consumes `eth_social_stream` and chains async operators from `func/`:
   - `EthSentimentOllamaFunction` — sentiment scoring/summary via a local Ollama LLM (`host.docker.internal:11434` in prod)
   - `EthPriceFeatureAsyncFunction` — joins MySQL price features by timestamp
   - `EthEmbeddingFunction` — embeds text via Ollama embeddings API
   - `EthBacktestDecisionFunction` — similarity-searches Milvus for historical analogues and emits trade signals → Kafka topic `topic_trade_signals`
3. `dataflow/eth_trade_dataflow/eth_trade_settlement.py` consumes signals and settles trades; `eth_model_retrain.py` handles retraining.

Other Flink jobs in `datastream/` follow the same shape: `kafka2milvus` (generic Kafka→embedding→Milvus), `realtime-riskcontrol-embedding-job`, `eth-sentiment-analysis-job`, `employee-message-processor`.

### Uniswap tick pipeline

Substreams (Ethereum → `uniswap-raw-ticks` Kafka/Redpanda topic) → Flink SQL scripts in `flink-sql/` compute metrics and sink to ClickHouse tables defined in `clickhouse-sql/market_data.sql` (`market.uniswap_ticks`) → `backtest_engine.py` runs strategy backtests by pushing computation down into ClickHouse SQL.

## Conventions and gotchas

- **Flink jobs get config via CLI args**, parsed by `common`'s `com.bigdata.common.utils.MyParameter` (wraps `ParameterTool` with localhost defaults). Each job's main class carries Javadoc comments listing the exact local vs. production (`docker`) start arguments — keep these in sync when adding parameters. Production uses docker-network hostnames (`kafka:29092`, `milvus-standalone`, `mysql:3306`, `host.docker.internal` for Ollama).
- **All Flink/connector dependencies are `provided` scope** (supplied by the Flink cluster). Job jars are built with maven-shade-plugin, which **relocates protobuf, grpc, guava, and the Milvus SDK** under `com.expert.bigdata.shaded.*` to avoid classpath conflicts with Flink. When adding a dependency that must ship in the jar, use compile scope and consider whether it needs relocation.
- Jobs are deployed through StreamPark (or `flink run -c <mainClass> <jar>` against the compose cluster).
- `global_lib/`, `dependency-reduced-pom.xml`, and `target/` directories are build artifacts — don't edit them.
- Job modules depend on the `common` module (`com.expert.bigdata:common`); build with `-am` so it's rebuilt too.
