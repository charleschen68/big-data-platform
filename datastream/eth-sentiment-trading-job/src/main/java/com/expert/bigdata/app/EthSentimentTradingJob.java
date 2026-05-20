package com.expert.bigdata.app;

//import com.expert.bigdata.app.model.EthereumData;;

import com.bigdata.common.utils.MyParameter;
import com.expert.bigdata.func.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
  --kafkaUrl "localhost:9092"
  --sourceTopic "eth_social_stream"
  --sinkTopic "topic_trade_signals"
  --kafkaGroupId "eth-sentiment-group"
  --ollamaHost "localhost"
  --milvusHost "localhost"
  --milvusPort "19530"
  --dbUrl "jdbc:mysql://localhost:3306/streampark"
  --dbUsername "root"
  --dbPassword "streampark"
  --embeddingUrl "http://localhost:11434/api/embeddings"
  --ollamaUrl "http://localhost:11434/api/generate"
 */

    /**  生产启动参数
 --kafkaUrl "kafka:29092"
 --sourceTopic "eth_social_stream"
 --sinkTopic "topic_trade_signals"
 --kafkaGroupId "eth-sentiment-group"
 --ollamaHost "host.docker.internal"
 --milvusHost "milvus-standalone"
     --milvusPort "19530"
 --dbUrl "jdbc:mysql://mysql:3306/streampark"
 --dbUsername "root"
 --dbPassword "streampark"
     --embeddingUrl "http://host.docker.internal:11434/api/embeddings"
     --ollamaUrl "http://host.docker.internal:11434/api/generate"
 */

/**
 * 实时以太坊情感量化交易与风控引擎核心 Job
 * <p>
 * 数据源：从 Kafka 获取实时新闻社交情感数据。
 * 处理流程：
 * 1. EthSentimentOllamaFunction: 将原始资讯送入本地 Ollama 环境跑的大模型推理，获取情感打分与总结。
 * 2. EthPriceFeatureAsyncFunction: 异步读取 MySQL 存下来的该时间戳下 ETH 行情特征（比如 rsi_14,
 * atr_14, 当前 price）。
 * 3. EthEmbeddingFunction: 调用 Ollama 侧模型将原始资讯内容 Embedding 为向量。
 * 4. EthBacktestDecisionFunction: 结合产生的向量、指标特征（如 rsi_14）、情感得分，到 Milvus
 * 里做相似历史回测（向量和标量混合检索）。
 * 如果相似历史发生后的胜率较高（如 65%以上），则构建一个做多（BUY）等交易信号。
 * 5. 将生成的交易信号 Sink 到下游 Kafka 以供做市或提醒，并将本条处理后的记录落库至 Milvus 进行记忆长存。
 */
public class EthSentimentTradingJob {
    public static void main(String[] args) throws Exception {
        // 1. 获取配置
        ParameterTool params = ParameterTool.fromArgs(args);
        MyParameter myParameter = new MyParameter(params);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 引入带退避延迟的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启 3 次
                org.apache.flink.api.common.time.Time.seconds(10) // 每次失败后等待 10 秒再抢占 Slot
        ));

        // 2. Kafka 数据源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(myParameter.getKafkaUrl())
                .setTopics(myParameter.getSourceTopic())
                .setGroupId(myParameter.getKafkaGroupId())
                // .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "SocialStream")
                .slotSharingGroup("default")
                        .name("rawStream");
        rawStream.print("[1-RawStream]");
        /*
         * Undo：风控流处理
         * 特点：命中率高，过滤数据
         */

        // 3. 算子链装配

        // 阶段 1: 情感分析 (LLM llama.cpp)
        // 31B 的模型生成速度较慢，特别是包含 CoT 链式思考时，30秒非常容易超时。调整为 120 秒。
        // 注意：本地 31b 模型难以承受极高并发，将 Async Capacity 从 100 骤降至 2，防止显存 OOM 或 Ollama
        // 并发队列崩溃导致生成截断死机。
        DataStream<String> sentimentStream = AsyncDataStream.orderedWait(
                rawStream, new EthSentimentOllamaFunction(), 120, TimeUnit.SECONDS, 2)
                .slotSharingGroup("heavy-sentimentStream-compute")
                        .name("sentimentStream");
        sentimentStream.print("[2-SentimentStream]");

        // 阶段 2: 获取 MySQL 历史 K 线特征
        DataStream<String> featureStream = AsyncDataStream.orderedWait(
                sentimentStream, new EthPriceFeatureAsyncFunction(), 5, TimeUnit.SECONDS, 100)
                .slotSharingGroup("default")
                .name("featureStream");
        featureStream.print("[3-FeatureStream]");
        // 阶段 3: 生成向量 Embedding
        DataStream<String> embeddingStream = AsyncDataStream.orderedWait(
                featureStream, new EthEmbeddingFunction(), 10, TimeUnit.SECONDS, 50)
                .slotSharingGroup("heavy-embeddingStream-compute")
                .name("embeddingStream");
        embeddingStream.print("[4-EmbeddingStream]");
        // 阶段 4: Milvus 回测决策
        DataStream<String> decisionStream = AsyncDataStream.orderedWait(
                embeddingStream, new EthBacktestDecisionFunction(), 10, TimeUnit.SECONDS, 50)
                .slotSharingGroup("heavy-decisionStream-compute")
                .name("decisionStream");
        decisionStream.print("[5-DecisionStream]");

        /*
         * Undo：风控流处理：
         * 特点：交易前风控，也可以认为是策略
         * 例如：当市场情绪看好，但是主力资金在逃离，说明是在大户砸盘的征兆，触发卖出级别高于模型判断
         * 这个风险控制可以交给风控AI，也就是另外一个模型来做
         */

        // 4. 发送到 Kafka 交易信号 Topic
        // decisionStream.print("Trading Signal"); // 控制台预览
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(myParameter.getKafkaUrl())
                .setRecordSerializer(
                        org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
                                .builder()
                                .setTopic(myParameter.getSinkTopic())
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        decisionStream.sinkTo(sink).slotSharingGroup("default").name("KafkaSink topic_trade_signals");

        // 5. 记忆存入 Sink
        // embeddingStream.addSink(new MilvusSink());
        embeddingStream.addSink(new MilvusSink()).name("MilvusMemorySink").slotSharingGroup("default").name("MilvusSink");

        env.execute("ETH Real-time Sentiment Quant Engine");
    }
}