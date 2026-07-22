//package com.expert.bigdata.app;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.cep.CEP;
//import org.apache.flink.cep.PatternSelectFunction;
//import org.apache.flink.cep.PatternStream;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.SimpleCondition;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//
//import java.time.Duration;
//import java.util.List;
//import java.util.Map;
//
//public class SmartMoneyTrackerJob {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 1. 配置 RocksDB 状态后端并指向 MinIO
//        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
//        // 配置 Checkpoint 目录为 MinIO (S3)
//        // 注意：需要引入 flink-s3-fs-presto 插件并在 core-site.xml 中配置 minio access key
//        env.getCheckpointConfig().setCheckpointStorage("s3://flink-checkpoints/smart-money/");
//        env.enableCheckpointing(60000); // 1分钟 checkpoint
//
//        // 2. 配置 Kafka Source
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("redpanda:29092")
//                .setTopics("uniswap-raw-ticks")
//                .setGroupId("smart-money-tracker")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        DataStream<JsonNode> tickStream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Redpanda Source")
//                .map(json -> new ObjectMapper().readTree(json));
//
//        // 3. 构建 "Smart Money" 内存特征表 (Keyed State)
//        DataStream<Tuple2<String, Double>> smartMoneyFlows = tickStream
//                .keyBy(json -> json.get("sender").asText()) // 按照钱包地址 KeyBy
//                .map(new SmartMoneyFeatureMapper());
//
//        // 4. Flink CEP: 捕捉 5分钟内发生 3个大户同时买入
//        // 简化: 定义大户买入事件
//        DataStream<JsonNode> whaleBuys = tickStream.filter(json -> {
//            double amount = Math.abs(json.get("amount0").asDouble());
//            return amount > 100000; // 假设 amount > 100k 是大户
//        });
//
//        Pattern<JsonNode, ?> pattern = Pattern.<JsonNode>begin("first")
//                .next("second")
//                .next("third")
//                .within(Time.minutes(5));
//
//        PatternStream<JsonNode> patternStream = CEP.pattern(whaleBuys.keyBy(json -> json.get("pool_address").asText()), pattern);
//
//        DataStream<String> alerts = patternStream.select(
//                (PatternSelectFunction<JsonNode, String>) pattern1 -> {
//                    JsonNode first = pattern1.get("first").get(0);
//                    return String.format("{\"alert\": \"Whale cluster buy\", \"pool\": \"%s\", \"timestamp\": %d}",
//                            first.get("pool_address").asText(), System.currentTimeMillis());
//                }
//        );
//
//        // 5. 将高价值警报推入 actionable-alerts
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers("redpanda:29092")
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("actionable-alerts")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .build();
//
//        alerts.sinkTo(sink);
//
//        env.execute("Smart Money Tracker Job");
//    }
//
//    // Keyed State 实时累加特征
//    public static class SmartMoneyFeatureMapper extends RichMapFunction<JsonNode, Tuple2<String, Double>> {
//        private transient ValueState<Double> netFlowState;
//
//        @Override
//        public void open(Configuration config) {
//            ValueStateDescriptor<Double> descriptor =
//                    new ValueStateDescriptor<>("netFlow", Double.class);
//            netFlowState = getRuntimeContext().getState(descriptor);
//        }
//
//        @Override
//        public Tuple2<String, Double> map(JsonNode value) throws Exception {
//            Double currentFlow = netFlowState.value();
//            if (currentFlow == null) {
//                currentFlow = 0.0;
//            }
//
//            // 假设 amount0 > 0 表示买入流入
//            double amount0 = value.get("amount0").asDouble();
//            currentFlow += amount0;
//
//            netFlowState.update(currentFlow);
//            return new Tuple2<>(value.get("sender").asText(), currentFlow);
//        }
//    }
//}
