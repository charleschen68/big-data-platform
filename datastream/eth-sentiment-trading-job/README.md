# 实时以太坊情感量化交易与风控引擎

## 一、模块简介

`eth-sentiment-trading-job` 是基于 Apache Flink 构建的大数据流式处理项目，它结合了最前沿的本地大语言模型（LLM）与向量数据库（Milvus）技术，提供从实时社交流识别、情感计算及量价复合回测到下单交易与记录的长效闭环。

它不仅负责接收信息，同时也负责“记忆”——将具有行情标志性和情绪代表性的事件作为向量基石沉淀至 Milvus 中，未来新事件再发生时，程序会查询库中相似环境的“历史胜率”，实现自动化、高确信度的辅助量化交易。

## 二、核心处理链路 (Pipeline)

程序的执行主要包含以下 `5` 个核心处理阶段：

1. **实时数据拉取 (`KafkaSource`)**
   从上游消息队列 Kafka 的 `eth_social_stream` 实时读取包含新闻、大V推文或者其他结构化资讯数据。
2. **第一层认知：情感打分 (`EthSentimentOllamaFunction`)**
   使用本地部署的 Ollama 环境（例如 `gemma-31b` 模型）。通过引入思维链（CoT）及结构化 JSON Schema (Structured Outputs)，约束模型根据当前新闻做一步步推演计算后，精确输出 `1 ~ 10` 分的情感极性分数与推理原因。
3. **第二层提炼：历史特征缝合 (`EthPriceFeatureAsyncFunction`)**
   通过 JDBC 异步查询 MySQL 内部的 `trade.eth_kline_features` 宽表，获取信息发生时 ETH 的 RSI、ATR、以及当时现货价格等标量指标，并同上面的模型推理汇总到同一条 JSON 记录中。
4. **第三层转化：高维语义向量化 (`EthEmbeddingFunction`)**
   重新发起一次调用，利用 Ollama 侧的专用 Embedding 模型（例如 `nomic-embed-text`），把源始业务逻辑信息编码转换为密集型浮点向量。
5. **最终决策及持久化记忆 (`EthBacktestDecisionFunction` & `MilvusSink`)**
   - **决策体系**：将前文计算所得的向量和行情指标（RSI等标量）丢入 Milvus 进行**相似度混合检索**。如果搜索召回的近似历史（如Cosine距离 > 0.9）且这些相似历史带来的平均回报胜率较高（> 65%），则风控过关，构建一条 `BUY`（做多）触发交易信号到下游 Kafka `topic_trade_signals` 中。
   - **记忆下沉**：经过全流程处理流转的数据，由 `MilvusSink` 将该向量和结构化附带数据持久化入 Milvus，等待外围批处理节点后续回填真实盈亏（`win_rate`, `is_settled`），变成未来的寻根判断素材。

## 三、部署与环境依赖

运行此 Flink Job 需要以下组件环境配合：

- **Kafka Cluster**：确保 `eth_social_stream` 有数据流入，并存在下游 `topic_trade_signals` (或自动创建)。
- **MySQL Database**：存储 Kline 数据及提取如 RSI 和 ATR 指标。表为 `trade.eth_kline_features`。
- **Ollama 本地 LLM 环境**：需准备生成分析模型与 Embedding 模型 (如 `gemma-31b-crack` 与 `nomic-embed-text`)，并在 `application.properties` 中调整 `ollama.api.url`。
- **Milvus 向量数据库**：需提前建立 Collection `eth_sentiment_analysis` 并构建相关 Schema。

## 四、本地编译与运行指南

### 1. 编译打包

项目在父级模块或当前目录下执行标准的 Maven 构建指令：

```bash
mvn clean compile
# 生成可提交到 Flink 集群的包
mvn clean package -DskipTests
```

### 2. 运行

如果要在 IDE 或本地直接运行 `main` 函数，请确保在运行配置 (Run Configuration) 中：
- 将 Flink 的依赖设为 **Include dependencies with 'Provided' scope** （在 IntelliJ IDEA 等工具中提供此类勾选项）。
- 环境必须配置相关组件的服务端口不被占用并将 `application.properties` 设置正确。

> [!NOTE]
> 该模块已高度优化为异步 IO 调用 (使用 Flink 的 `AsyncDataStream`)，处理速度可适应常规高频情绪面解析请求，如遇本地调试可先将 `timeout` 或并发数量调小以免内存占用过载。
