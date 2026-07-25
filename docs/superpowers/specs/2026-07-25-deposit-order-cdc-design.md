# Deposit Order CDC Pipeline — Java Design

## Overview

Convert the existing Flink SQL CDC pipeline (`source_cw_dws_deposit_order` → `sink_cw_dws_deposit_order_1`) from pure SQL DDL + `INSERT INTO` to Java code using the Flink Table API.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  DepositOrderCdcJob.java                 │
│                                                          │
│  Source DDL (OceanBase CDC) ──► TableEnvironment         │
│  Sink DDL (JDBC/MySQL)     ──► ───► INSERT INTO ...     │
│  POJO schema (DwsDepositOrder)                           │
└─────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
  OceanBase DB              Sink DB (bigdata_cms_prod)
  bigdata_cms              (template vars resolved)
  cw_dws_deposit_order
```

## Module Structure

```
datastream/deposit-order-cdc/
├── pom.xml                          # Inherits from datastream parent
└── src/main/java/com/expert/bigdata/
    ├── app/
    │   └── DepositOrderCdcJob.java  # Main entry point, Table API pipeline
    └── pojo/
        └── DwsDepositOrder.java     # POJO with all 55+ columns
```

## Component Details

### 1. DwsDepositOrder.java (POJO)

Maps the source table `cw_dws_deposit_order` schema to a Java POJO.

- **55 fields** (deduplicated from SQL which has 4 duplicate columns)
- **Type mapping:**
  - `BIGINT` → `Long`
  - `STRING` → `String`
  - `DECIMAL(22, 8)` → `BigDecimal`
  - `TIMESTAMP` → `LocalDateTime`
  - `INT` → `Integer`
  - `BOOLEAN` → `Boolean`
- **Duplicate column handling:** Source SQL defines `bind_bank_address`, `created_at`, `updated_at`, `pay_amount` twice. POJO declares each field once.
- **Field naming:** SQL snake_case → Java camelCase (e.g., `order_deposit_name` → `orderDepositName`)

### 2. DepositOrderCdcJob.java (Main Job)

Uses Flink Table API with SQL DDL strings:

**Source DDL:**
- `connector = 'oceanbase-cdc'`
- `scan.startup.mode = 'initial'`
- Connection params: hostname, port, username, password, tenant-name, database-name, table-name, rootserver-list, logproxy.host, logproxy.port, working-mode
- All parameters passed as CLI args (e.g., `--hostname`, `--port`)

**Sink DDL:**
- `connector = 'jdbc'`
- `url` with `${sink_bigdata_ob_ip}:${sink_bigdata_ob_port}` template variables
- `${sink_bigdata_ob_account}` for credentials
- `sink.buffer-flush.max-rows = '1000'`
- `sink.buffer-flush.interval = '5s'`
- `driver = 'com.mysql.cj.jdbc.Driver'`
- `LIKE source ... EXCLUDING ALL INCLUDING GENERATED`

**Pipeline:**
1. Create `TableEnvironment` with streaming settings
2. Execute source DDL → creates `source_cw_dws_deposit_order` view
3. Execute sink DDL → creates `sink_cw_dws_deposit_order_1` view
4. `INSERT INTO sink_cw_dws_deposit_order_1 SELECT * FROM source_cw_dws_deposit_order`
5. Enable checkpointing (5s interval)
6. `tableEnv.execute("DepositOrderCdcJob")`

### 3. pom.xml

- Inherits from `datastream` parent pom
- Dependencies:
  - `flink-table-api-java` (core Table API)
  - `flink-table-api-java-bridge`
  - `flink-table-runtime`
  - `flink-connector-base`
  - OceanBase CDC connector (or MySQL CDC with OB config)
  - JDBC connector
  - MySQL JDBC driver
- Maven shade plugin with existing relocation patterns (protobuf, grpc, guava)

## Deployment

- Produces fat JAR via `mvn package`
- Deployed to Flink cluster (k8s or standalone)
- Checkpoint config: 5s interval, matching existing job conventions
- CLI args for all configurable parameters (hostname, port, username, password, etc.)

## Success Criteria

1. Java code reproduces the exact behavior of the original SQL pipeline
2. Follows existing project conventions (package structure, naming, pom patterns)
3. All 55+ fields correctly mapped with proper types
4. Template variables in sink DDL resolved at runtime
5. Duplicate columns handled correctly
6. Builds and packages with `mvn package`
7. Deployable to existing Flink cluster
