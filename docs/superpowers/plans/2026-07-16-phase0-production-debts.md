# Phase 0：生产化还债 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在上 k8s 之前，于现有 docker-compose 环境内解决五项生产化欠账：Flink 状态入 MinIO、savepoint 恢复演练、settlement 幂等、fastjson 漏洞替换、硬编码密码清除、决策核心单测。

**Architecture:** 不改变现有部署形态（compose + StreamPark/flink CLI），只改配置与代码。信号幂等采用"决策函数补发 `event_id`/`signal_id` → settlement 用 MySQL 去重表检查"的两端设计。决策逻辑抽取为纯函数类 `DecisionLogic` 以便单测。

**Tech Stack:** Flink 1.18.1 / Java 17 / Maven（JUnit 5 + Surefire 3）、Python 3 + pytest、MinIO（S3 协议）、MySQL 8。

## Global Constraints

- Java 17；Flink 1.18.1；Flink 及连接器依赖保持 `provided` scope（见根 `pom.xml`）
- maven-shade-plugin 对 protobuf/grpc/guava/milvus 的 relocation 配置不得改动
- docker-compose.yml 暂留仓库根目录（目录重构属 Phase 1，本计划不做）
- MinIO 凭证 minioadmin/minioadmin 仅限本地环境，写入 compose 无妨；MySQL 密码必须走 `.env`
- 提交信息用中文短句，风格同 `git log`（如"设置 pipline name"）
- 所有 Maven 命令在仓库根目录执行；`docker compose` 命令同样在根目录执行

---

### Task 1: Flink checkpoint/savepoint 指向 MinIO

**Files:**
- Modify: `docker-compose.yml`（flink-jobmanager 与 flink-taskmanager 两个服务的 environment）

**Interfaces:**
- Produces: S3 桶 `flink-state`；集群配置 `state.checkpoints.dir=s3://flink-state/checkpoints`、`state.savepoints.dir=s3://flink-state/savepoints`（Task 2 的演练依赖它们）

- [ ] **Step 1: 在 MinIO 创建 bucket**

MinIO 用文件系统后端，`/data` 下建目录即建桶：

```bash
docker exec milvus-minio mkdir -p /data/flink-state
docker exec milvus-minio ls /data
```

预期输出包含 `flink-state`。

- [ ] **Step 2: 修改 jobmanager 的 environment**

在 `docker-compose.yml` 的 `flink-jobmanager` 服务 `environment` 中：
(a) 新增一行环境变量（与 FLINK_PROPERTIES 平级）：

```yaml
      - ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-1.18.1.jar
```

注意：值必须与镜像内的 jar 文件名完全一致。先确认真实文件名再填：

```bash
docker exec flink-jobmanager ls /opt/flink/opt | grep s3-fs-presto
```

用输出的确切文件名（如 `flink-s3-fs-presto-1.18.1.jar`）作为该变量的值。

(b) 将 FLINK_PROPERTIES 中这两行：

```yaml
        state.checkpoints.dir: file:///opt/flink/checkpoints
        state.savepoints.dir: file:///opt/flink/savepoints
```

替换为：

```yaml
        state.checkpoints.dir: s3://flink-state/checkpoints
        state.savepoints.dir: s3://flink-state/savepoints
        s3.endpoint: http://minio:9000
        s3.path.style.access: true
        s3.access-key: minioadmin
        s3.secret-key: minioadmin
```

- [ ] **Step 3: 对 flink-taskmanager 做同样修改**

taskmanager 的 FLINK_PROPERTIES 目前没有 state.* 行，直接追加上面 6 行 s3/state 配置，并同样加 `ENABLE_BUILT_IN_PLUGINS` 环境变量。TaskManager 也需要 S3 插件（checkpoint 数据由 TM 直接写 S3）。

- [ ] **Step 4: 重建两个容器并验证插件加载**

```bash
docker compose up -d flink-jobmanager flink-taskmanager
docker logs flink-jobmanager 2>&1 | grep -i "s3-fs-presto"
```

预期：日志出现插件启用信息（如 `Linking flink-s3-fs-presto ... to plugin directory`）。

- [ ] **Step 5: 提交一个作业验证 checkpoint 落到 MinIO**

用 realtime-riskcontrol-embedding-job（代码里已 `env.enableCheckpointing(5000)`，无 Ollama 依赖）：

```bash
mvn -q clean package -DskipTests -pl datastream/realtime-riskcontrol-embedding-job -am
ls datastream/realtime-riskcontrol-embedding-job/target/*.jar   # 找 shaded jar（体积大的那个）
docker cp datastream/realtime-riskcontrol-embedding-job/target/<shaded-jar 文件名> flink-jobmanager:/tmp/rc-job.jar
docker exec flink-jobmanager flink run -d -c com.expert.bigdata.app.RealtimeRiskControlEmbeddingJob /tmp/rc-job.jar \
  --kafkaUrl kafka:29092 --milvusHost milvus-standalone --ollamaHost host.docker.internal
sleep 30
docker exec milvus-minio find /data/flink-state/checkpoints -name "_metadata" | head -3
```

预期：至少一条 `_metadata` 路径（形如 `/data/flink-state/checkpoints/<job-id>/chk-N/_metadata`）。若为空，查 `docker logs flink-jobmanager | grep -i checkpoint` 排错，不得跳过。

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml
git commit -m "Flink checkpoint/savepoint 改存 MinIO(S3)"
```

---

### Task 2: savepoint 升级演练 + 运维手册

**Files:**
- Create: `docs/runbooks/flink-savepoint-upgrade.md`

**Interfaces:**
- Consumes: Task 1 的 `s3://flink-state/savepoints` 配置与正在运行的 rc-job

- [ ] **Step 1: 写运维手册**

创建 `docs/runbooks/flink-savepoint-upgrade.md`，内容：

````markdown
# Flink 作业无损升级手册（savepoint → 停止 → 部署 → 恢复）

前提：集群 state.savepoints.dir 已指向 s3://flink-state/savepoints（见 docker-compose.yml）。

## 1. 找到作业 ID

```bash
docker exec flink-jobmanager flink list
```

## 2. 带 savepoint 停止作业

```bash
docker exec flink-jobmanager flink stop --savepointPath s3://flink-state/savepoints <JOB_ID>
```

记下输出的 savepoint 完整路径（形如 `s3://flink-state/savepoints/savepoint-xxxxxx-yyyyyyyyyyyy`）。

## 3. 部署新版本并从 savepoint 恢复

```bash
docker cp <新构建的 shaded jar> flink-jobmanager:/tmp/job.jar
docker exec flink-jobmanager flink run -d -s <savepoint 完整路径> -c <主类> /tmp/job.jar <原启动参数>
```

## 4. 验证恢复成功

```bash
docker exec flink-jobmanager flink list          # 状态 RUNNING
docker logs flink-jobmanager 2>&1 | grep -i "restor"  # 出现 Restoring job ... from Savepoint
```

## 注意事项

- 改了算子拓扑（增删算子）时，恢复需给算子设 uid 并在必要时加 `--allowNonRestoredState`。
- StreamPark 界面上的"停止时触发 savepoint"等价于第 2 步。
````

- [ ] **Step 2: 按手册完整演练一遍**

对 Task 1 启动的 rc-job 逐条执行手册命令（第 3 步用同一个 jar 模拟"新版本"）。

预期：`flink stop` 输出 savepoint 路径；重新 run 后 `flink list` 显示 RUNNING；jobmanager 日志有 "Restoring job … from Savepoint"。任何一步不符，修正手册直到演练通过——手册必须是验证过的。

- [ ] **Step 3: 演练后清理并提交**

```bash
docker exec flink-jobmanager flink list   # 确认只有预期作业
git add docs/runbooks/flink-savepoint-upgrade.md
git commit -m "添加 Flink savepoint 升级手册(已演练)"
```

---

### Task 3: fastjson 1.2.83 → fastjson v2（漏洞修复）

**Files:**
- Modify: `pom.xml:139-144`（根 pom 的 fastjson 依赖）

**Interfaces:**
- Produces: `com.alibaba:fastjson:2.0.57`、scope=compile（后续任务的 Java 代码继续 `import com.alibaba.fastjson.*`，API 不变）

说明：`com.alibaba:fastjson` 的 2.0.x 版本是基于 fastjson2 内核的兼容包，包名和 API 与 1.x 相同，代码零改动。同时把 scope 从 `provided` 改为 `compile`，让 fastjson 打进 shaded jar，作业自带依赖、不再依赖集群 lib 目录。

- [ ] **Step 1: 确认集群 lib 里是否有旧版 fastjson**

```bash
docker exec flink-jobmanager sh -c 'ls /opt/flink/lib | grep -i fastjson' || echo "lib 中无 fastjson"
```

记录结果，Step 4 用。

- [ ] **Step 2: 修改根 pom.xml**

将：

```xml
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.83</version>
            <scope>provided</scope>
        </dependency>
```

改为：

```xml
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>2.0.57</version>
        </dependency>
```

（若 2.0.57 无法解析，到 Maven Central 查 `com.alibaba:fastjson` 最新 2.0.x 版本替换。）

- [ ] **Step 3: 全量构建验证**

```bash
mvn clean package -DskipTests
```

预期：BUILD SUCCESS。若出现编译错误（个别 1.x API 在兼容包中缺失），按报错逐个改为 fastjson2 等价 API，但 11 个使用文件都只用 `JSON.parseObject / getJSONArray / getBigDecimal / toJSONString` 等核心 API，预期无需改动。

- [ ] **Step 4: 若集群 lib 有旧 jar，移除并重启**

仅当 Step 1 发现旧 jar 时执行（避免新旧双版本同在 classpath）：

```bash
docker exec flink-jobmanager sh -c 'rm /opt/flink/lib/fastjson-1.2.83.jar'   # 文件名以 Step 1 输出为准
docker exec flink-taskmanager sh -c 'rm /opt/flink/lib/fastjson-1.2.83.jar'
docker compose restart flink-jobmanager flink-taskmanager
```

注意：若 lib 是宿主机 volume 挂载（`docker compose config` 查 volumes），直接删宿主机文件。

- [ ] **Step 5: 冒烟验证**

重新部署 Task 1 的 rc-job（新 jar），确认 RUNNING 且日志无 ClassNotFoundException/NoClassDefFoundError：

```bash
docker exec flink-jobmanager flink list
docker logs --tail 100 flink-taskmanager 2>&1 | grep -iE "classnotfound|noclassdef" || echo "无类加载错误"
```

- [ ] **Step 6: Commit**

```bash
git add pom.xml
git commit -m "fastjson 升级到 2.0.x 兼容包并改为 compile scope"
```

---

### Task 4: 决策逻辑抽取为纯函数 + 信号补 event_id + JUnit 单测

**Files:**
- Create: `datastream/eth-sentiment-trading-job/src/main/java/com/expert/bigdata/func/DecisionLogic.java`
- Test: `datastream/eth-sentiment-trading-job/src/test/java/com/expert/bigdata/func/DecisionLogicTest.java`
- Modify: `datastream/eth-sentiment-trading-job/src/main/java/com/expert/bigdata/func/EthBacktestDecisionFunction.java`
- Modify: `datastream/eth-sentiment-trading-job/pom.xml`（加 JUnit 5 + Surefire 3）

**Interfaces:**
- Consumes: 上游 embeddingStream 的 JSON 里含 `id` 字段（`MilvusSink.java:118` 已用 `node.getString("id")` 证实存在）
- Produces:
  - `DecisionLogic.decideAction(long sentimentScore) -> String`（"BUY"/"SELL"/"HOLD"）
  - `DecisionLogic.computeStats(List<DecisionLogic.BacktestMatch>) -> DecisionLogic.BacktestStats(int validMatches, double winRate, double maxSimilarity)`
  - `DecisionLogic.buildFilterExpr(long sentimentScore, double rsi14) -> String`
  - `DecisionLogic.signalId(String eventId, String action) -> String`（格式 `<eventId>:<action>`）
  - 交易信号 JSON 新增字段 `event_id`、`signal_id`（Task 5 的 settlement 依赖）

- [ ] **Step 1: 给 trading-job 的 pom.xml 加测试依赖与 Surefire**

在 `datastream/eth-sentiment-trading-job/pom.xml` 的 `<dependencies>` 中追加：

```xml
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.2</version>
            <scope>test</scope>
        </dependency>
```

在该 pom 的 `<build><plugins>` 中追加（若无 build/plugins 节则创建）：

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.5</version>
            </plugin>
```

（Maven 默认的 Surefire 2.x 不识别 JUnit 5，必须显式声明 3.x。）

- [ ] **Step 2: 写失败测试**

创建 `datastream/eth-sentiment-trading-job/src/test/java/com/expert/bigdata/func/DecisionLogicTest.java`：

```java
package com.expert.bigdata.func;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DecisionLogicTest {

    @Test
    void decideAction_boundaries() {
        assertEquals("BUY", DecisionLogic.decideAction(9));
        assertEquals("HOLD", DecisionLogic.decideAction(8));   // 边界：>8 才 BUY
        assertEquals("HOLD", DecisionLogic.decideAction(2));   // 边界：<2 才 SELL
        assertEquals("SELL", DecisionLogic.decideAction(1));
    }

    @Test
    void computeStats_emptyMatches() {
        DecisionLogic.BacktestStats stats = DecisionLogic.computeStats(List.of());
        assertEquals(0, stats.validMatches());
        assertEquals(0.0, stats.winRate());
        assertEquals(0.0, stats.maxSimilarity());
    }

    @Test
    void computeStats_filtersBySimilarityThreshold() {
        // 0.9 不算（阈值为严格大于），0.95 和 0.92 算；其中一条 histReturn>0
        DecisionLogic.BacktestStats stats = DecisionLogic.computeStats(List.of(
                new DecisionLogic.BacktestMatch(0.90f, 5.0f),
                new DecisionLogic.BacktestMatch(0.95f, 0.02f),
                new DecisionLogic.BacktestMatch(0.92f, -0.01f)));
        assertEquals(2, stats.validMatches());
        assertEquals(0.5, stats.winRate());
        assertEquals(0.95, stats.maxSimilarity(), 1e-6);
    }

    @Test
    void buildFilterExpr_format() {
        assertEquals("sentiment_score == 9 && rsi_14 >= 55.00 && is_settled == true",
                DecisionLogic.buildFilterExpr(9, 60.0));
    }

    @Test
    void signalId_format() {
        assertEquals("evt-123:SELL", DecisionLogic.signalId("evt-123", "SELL"));
    }
}
```

- [ ] **Step 3: 运行确认失败**

```bash
mvn -pl datastream/eth-sentiment-trading-job -am test
```

预期：编译失败，`DecisionLogic` 不存在（cannot find symbol）。

- [ ] **Step 4: 实现 DecisionLogic**

创建 `datastream/eth-sentiment-trading-job/src/main/java/com/expert/bigdata/func/DecisionLogic.java`：

```java
package com.expert.bigdata.func;

import java.util.List;

/**
 * 交易决策的纯逻辑，与 Flink/Milvus 运行时解耦以便单元测试。
 * 阈值与 EthBacktestDecisionFunction 原实现保持一致。
 */
public final class DecisionLogic {

    public static final double SIMILARITY_THRESHOLD = 0.9;

    public record BacktestMatch(float similarity, float histReturn) {}

    public record BacktestStats(int validMatches, double winRate, double maxSimilarity) {}

    private DecisionLogic() {}

    public static String decideAction(long sentimentScore) {
        if (sentimentScore > 8) {
            return "BUY";
        }
        if (sentimentScore < 2) {
            return "SELL";
        }
        return "HOLD";
    }

    public static BacktestStats computeStats(List<BacktestMatch> matches) {
        int valid = 0;
        double winCount = 0;
        double maxSim = 0;
        for (BacktestMatch m : matches) {
            if (m.similarity() > SIMILARITY_THRESHOLD) {
                valid++;
                maxSim = Math.max(maxSim, m.similarity());
                if (m.histReturn() > 0) {
                    winCount++;
                }
            }
        }
        return new BacktestStats(valid, valid > 0 ? winCount / valid : 0, maxSim);
    }

    public static String buildFilterExpr(long sentimentScore, double rsi14) {
        return String.format("sentiment_score == %d && rsi_14 >= %.2f && is_settled == true",
                sentimentScore, rsi14 - 5);
    }

    public static String signalId(String eventId, String action) {
        return eventId + ":" + action;
    }
}
```

- [ ] **Step 5: 运行确认通过**

```bash
mvn -pl datastream/eth-sentiment-trading-job -am test
```

预期：`Tests run: 5, Failures: 0, Errors: 0`，BUILD SUCCESS。

- [ ] **Step 6: 改造 EthBacktestDecisionFunction 调用纯逻辑并补 event_id**

修改 `EthBacktestDecisionFunction.java` 的 `asyncInvoke`：

(a) 标量过滤表达式改用纯函数——将：

```java
                String expr = String.format("sentiment_score == %d && rsi_14 >= %.2f && is_settled == true",
                        node.getLong("sentiment_score"),
                        node.getDouble("rsi_14") - 5);
```

替换为：

```java
                String expr = DecisionLogic.buildFilterExpr(
                        node.getLong("sentiment_score"), node.getDouble("rsi_14"));
```

(b) 回测统计循环改用纯函数——将从 `double winCount = 0;` 到 `double avgWinRate = ...;` 的整段（原 113-141 行）替换为：

```java
                List<DecisionLogic.BacktestMatch> matches = new ArrayList<>();
                if (searchResp.getStatus() == io.milvus.param.R.Status.Success.getCode()
                        && searchResp.getData() != null) {
                    SearchResultsWrapper wrapper = new SearchResultsWrapper(searchResp.getData().getResults());
                    for (SearchResultsWrapper.IDScore res : wrapper.getIDScore(0)) {
                        Object histReturnObj = res.get("return");
                        float histReturn = (histReturnObj instanceof Number)
                                ? ((Number) histReturnObj).floatValue() : 0f;
                        matches.add(new DecisionLogic.BacktestMatch(res.getScore(), histReturn));
                    }
                }
                DecisionLogic.BacktestStats stats = DecisionLogic.computeStats(matches);
                double avgWinRate = stats.winRate();
                double maxSimilarity = stats.maxSimilarity();
```

(c) 信号构建改用纯函数并补 `event_id`/`signal_id`——将：

```java
                    JSONObject signal = new JSONObject();
                    if (node.getLong("sentiment_score") > 8) {
                        signal.put("action", "BUY");
                    } else if (node.getLong("sentiment_score") < 2) {
                        signal.put("action", "SELL");
                    } else {
                        signal.put("action", "HOLD");
                    }
```

替换为：

```java
                    JSONObject signal = new JSONObject();
                    String action = DecisionLogic.decideAction(node.getLong("sentiment_score"));
                    String eventId = node.getString("id");
                    signal.put("action", action);
                    signal.put("event_id", eventId);
                    signal.put("signal_id", DecisionLogic.signalId(eventId, action));
```

(d) 删除 `asyncInvoke` 中已被 (b) 取代的局部变量声明（`double winCount`、`int validMatches`、`double maxSimilarity`），避免重复定义编译错误。

- [ ] **Step 7: 构建 + 测试全部通过**

```bash
mvn -pl datastream/eth-sentiment-trading-job -am clean package
```

预期：测试 5 通过，BUILD SUCCESS（package 阶段自动跑 surefire）。

- [ ] **Step 8: Commit**

```bash
git add datastream/eth-sentiment-trading-job
git commit -m "决策逻辑抽取为纯函数并补发信号 event_id/signal_id, 添加单元测试"
```

---

### Task 5: settlement 幂等（MySQL 去重表）

**Files:**
- Create: `dataflow/eth_trade_dataflow/settlement_logic.py`
- Test: `dataflow/tests/test_settlement_logic.py`
- Modify: `dataflow/eth_trade_dataflow/eth_trade_settlement.py`
- Modify: `schema.sql`（追加去重表 DDL）
- Modify: `requirements.txt`（追加 pytest）

**Interfaces:**
- Consumes: Task 4 信号中的 `event_id`、`signal_id`、`action` 字段
- Produces:
  - `settlement_logic.make_signal_id(signal: dict) -> str`（优先取 signal["signal_id"]，缺失时用 f"{event_id}:{action}" 兜底；两者都拼不出则抛 ValueError）
  - `settlement_logic.compute_settlement(buy_price, sell_price) -> tuple[float, float]`（返回 (return_rate, win_rate)，非法输入抛 ValueError）
  - MySQL 表 `streampark.processed_signals(signal_id VARCHAR(255) PK, processed_at TIMESTAMP)`

已知范围边界（不在本任务内修）：当前信号尚未携带 `buy_price`/`sell_price`（策略侧未实现），settlement 对缺价信号维持现状——告警并跳过。本任务交付的是幂等基础设施，价格字段补齐属策略开发。

- [ ] **Step 1: requirements.txt 追加 pytest**

在 `requirements.txt` 末尾追加：

```
pytest==8.2.0
```

安装：`pip install pytest==8.2.0`

- [ ] **Step 2: 写失败测试**

创建 `dataflow/tests/__init__.py`（空文件）和 `dataflow/tests/test_settlement_logic.py`：

```python
import pytest

from dataflow.eth_trade_dataflow.settlement_logic import make_signal_id, compute_settlement


class TestMakeSignalId:
    def test_prefers_explicit_signal_id(self):
        assert make_signal_id({"signal_id": "evt-1:SELL", "event_id": "evt-1", "action": "SELL"}) == "evt-1:SELL"

    def test_falls_back_to_event_id_and_action(self):
        assert make_signal_id({"event_id": "evt-2", "action": "SELL"}) == "evt-2:SELL"

    def test_raises_when_underivable(self):
        with pytest.raises(ValueError):
            make_signal_id({"action": "SELL"})


class TestComputeSettlement:
    def test_profit(self):
        return_rate, win = compute_settlement(buy_price=100.0, sell_price=110.0)
        assert return_rate == pytest.approx(0.1)
        assert win == 1.0

    def test_loss(self):
        return_rate, win = compute_settlement(buy_price=100.0, sell_price=90.0)
        assert return_rate == pytest.approx(-0.1)
        assert win == 0.0

    def test_invalid_buy_price(self):
        with pytest.raises(ValueError):
            compute_settlement(buy_price=0.0, sell_price=90.0)

    def test_missing_prices(self):
        with pytest.raises(ValueError):
            compute_settlement(buy_price=None, sell_price=90.0)
```

- [ ] **Step 3: 运行确认失败**

```bash
cd /Users/ad/big-data-platform && python -m pytest dataflow/tests -v
```

预期：FAIL，`ModuleNotFoundError: No module named 'dataflow.eth_trade_dataflow.settlement_logic'`。
（若报 dataflow 不是包：在 `dataflow/` 和 `dataflow/eth_trade_dataflow/` 各建空 `__init__.py`。）

- [ ] **Step 4: 实现 settlement_logic.py**

创建 `dataflow/eth_trade_dataflow/settlement_logic.py`：

```python
"""settlement 的纯逻辑：信号幂等键与收益计算。与 Kafka/Milvus/MySQL IO 解耦以便单测。"""


def make_signal_id(signal: dict) -> str:
    signal_id = signal.get("signal_id")
    if signal_id:
        return signal_id
    event_id = signal.get("event_id")
    action = signal.get("action")
    if event_id and action:
        return f"{event_id}:{action}"
    raise ValueError(f"signal 缺少 signal_id 且无法由 event_id+action 推导: {signal}")


def compute_settlement(buy_price, sell_price) -> tuple:
    if buy_price is None or sell_price is None:
        raise ValueError("buy_price/sell_price 缺失")
    if buy_price <= 0:
        raise ValueError(f"非法 buy_price: {buy_price}")
    return_rate = (sell_price - buy_price) / buy_price
    win_rate = 1.0 if return_rate > 0 else 0.0
    return return_rate, win_rate
```

- [ ] **Step 5: 运行确认通过**

```bash
python -m pytest dataflow/tests -v
```

预期：7 passed。

- [ ] **Step 6: 建去重表**

在 `schema.sql` 末尾追加：

```sql
-- 交易信号幂等去重表：settlement 处理过的 signal_id 记录于此，重复消费时跳过
CREATE TABLE IF NOT EXISTS processed_signals (
    signal_id VARCHAR(255) NOT NULL PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

应用到运行中的 MySQL：

```bash
docker compose exec -T mysql mysql -uroot -p"${MYSQL_PASSWORD}" streampark -e "CREATE TABLE IF NOT EXISTS processed_signals (signal_id VARCHAR(255) NOT NULL PRIMARY KEY, processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP); SHOW TABLES LIKE 'processed_signals';"
```

预期输出含 `processed_signals`。

- [ ] **Step 7: settlement worker 接入去重表**

修改 `dataflow/eth_trade_dataflow/eth_trade_settlement.py`：

(a) 顶部 import 追加：

```python
import mysql.connector

from settlement_logic import make_signal_id, compute_settlement
```

（worker 在容器内以脚本方式直跑，用同目录直接导入，不走包路径。）

(b) `__init__` 末尾追加 MySQL 连接：

```python
        # 3. 幂等去重存储
        self.db = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "mysql"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=os.getenv("MYSQL_DATABASE", "streampark"),
            autocommit=True,
        )

    def _already_processed(self, signal_id: str) -> bool:
        with self.db.cursor() as cur:
            cur.execute("SELECT 1 FROM processed_signals WHERE signal_id = %s", (signal_id,))
            return cur.fetchone() is not None

    def _mark_processed(self, signal_id: str):
        with self.db.cursor() as cur:
            cur.execute("INSERT IGNORE INTO processed_signals (signal_id) VALUES (%s)", (signal_id,))
```

(c) `process_sell_signal` 开头（`event_id = signal.get("event_id")` 之前）插入幂等检查：

```python
        try:
            signal_id = make_signal_id(signal)
        except ValueError as e:
            logger.warning(f"无法生成幂等键，跳过: {e}")
            return
        if self._already_processed(signal_id):
            logger.info(f"信号 {signal_id} 已处理过，幂等跳过。")
            return
```

(d) 收益计算改用纯函数——将原来的：

```python
        if buy_price <= 0:
            logger.warning(f"Invalid buy_price in signal for event_id {event_id}. Skipping.")
            return

        return_rate = (sell_price - buy_price) / buy_price

        # 胜负判定：收益率 > 0 记为胜 (1.0)，否则为负 (0.0)
        win_rate_val = 1.0 if return_rate > 0 else 0.0
```

替换为：

```python
        try:
            return_rate, win_rate_val = compute_settlement(buy_price, sell_price)
        except ValueError as e:
            logger.warning(f"结算价格非法，跳过 event_id {event_id}: {e}")
            return
```

(e) upsert 成功后标记已处理——在 `logger.info(f"✅ Successfully updated ...")` 之后追加一行：

```python
            self._mark_processed(signal_id)
```

（先处理、成功后再标记：崩溃在两步之间会导致重放，但 Milvus upsert 写的是相同值，幂等无害——at-least-once + 幂等效果。）

- [ ] **Step 8: 验证 worker 语法与测试**

```bash
python -m py_compile dataflow/eth_trade_dataflow/eth_trade_settlement.py
python -m pytest dataflow/tests -v
```

预期：py_compile 无输出（成功）；7 passed。

- [ ] **Step 9: Commit**

```bash
git add dataflow/eth_trade_dataflow/settlement_logic.py dataflow/eth_trade_dataflow/eth_trade_settlement.py dataflow/tests requirements.txt schema.sql
git commit -m "settlement 增加信号幂等去重(MySQL processed_signals 表)"
```

---

### Task 6: 清除硬编码密码

**Files:**
- Modify: `common/src/main/java/com/bigdata/common/utils/MyParameter.java:25`
- Modify: `docker-compose.yml`（mysql 及引用该密码的服务）
- Create: `.env.example`
- Modify: `datastream/*/src/main/java/**` 中含 `--dbPassword "streampark"` 的 Javadoc（用 grep 定位）

**Interfaces:**
- Produces: 环境变量约定 `DB_PASSWORD`（Java 作业）、`MYSQL_ROOT_PASSWORD`（compose）；`.env` 为唯一真实密码存放处（已在 .gitignore）

- [ ] **Step 1: MyParameter 移除密码默认值**

将 `MyParameter.java` 中：

```java
        this.dbPassword = parameterTool.get("dbPassword", "streampark");
```

改为：

```java
        // 密码不设硬编码默认值：优先 --dbPassword 参数，其次 DB_PASSWORD 环境变量
        this.dbPassword = parameterTool.get("dbPassword", System.getenv().getOrDefault("DB_PASSWORD", ""));
```

- [ ] **Step 2: 清洗 Javadoc 中的真实密码**

定位所有出现处：

```bash
grep -rn 'dbPassword "streampark"\|dbPassword \x27streampark\x27' datastream --include='*.java'
```

对每个命中文件，把 `--dbPassword "streampark"` 替换为 `--dbPassword "${DB_PASSWORD}"`。

- [ ] **Step 3: compose 密码参数化**

创建 `.env.example`（可提交的模板）：

```
# 复制为 .env 并填入真实值（.env 已被 gitignore）
MYSQL_ROOT_PASSWORD=changeme
```

创建本地 `.env`（不提交），当前值保持不变以免影响已初始化的 MySQL 数据卷：

```
MYSQL_ROOT_PASSWORD=streampark
```

在 `docker-compose.yml` 中定位所有密码字面量：

```bash
grep -n 'streampark' docker-compose.yml
```

将 mysql 服务的 `MYSQL_ROOT_PASSWORD: streampark` 改为 `MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD:?请在 .env 中设置}`；其他服务（如 streampark）引用同一密码的环境变量同样改为 `${MYSQL_ROOT_PASSWORD}`。镜像名/用户名里的 "streampark" 字样不要动。

注意：MySQL 数据卷已初始化，`MYSQL_ROOT_PASSWORD` 只在首次初始化生效，因此本任务只是把密码移出 git，不做轮换；轮换留待需要时用 `ALTER USER` 执行。

- [ ] **Step 4: 验证**

```bash
docker compose config > /dev/null && echo "compose 配置有效"
set -a && . ./.env && set +a && docker compose up -d mysql && docker compose exec -T mysql mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -e "SELECT 1"
mvn -q clean package -DskipTests -pl common -am
grep -rn '"streampark"' common/src datastream --include='*.java' || echo "Java 源码已无硬编码密码"
```

预期：四条命令依次成功，最后一条输出"已无硬编码密码"（若 grep 仍有命中且属密码语义，回到 Step 1/2 处理）。

- [ ] **Step 5: Commit**

```bash
git add common docker-compose.yml .env.example datastream
git commit -m "清除硬编码数据库密码, 统一走 .env 与 DB_PASSWORD 环境变量"
```

---

## 收尾

- [ ] **全量回归**：`mvn clean package`（含单测）BUILD SUCCESS；`python -m pytest dataflow/tests -v` 全过；`docker exec flink-jobmanager flink list` 作业 RUNNING。
- [ ] 在 spec（`docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md`）Phase 0 各项后标注完成状态并提交。
