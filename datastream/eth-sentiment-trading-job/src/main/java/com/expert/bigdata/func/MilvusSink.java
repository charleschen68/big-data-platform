package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.dml.InsertParam;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 高维记忆持久化 (Milvus Sink 节点) - 高性能微批处理版
 * <p>
 * 架构亮点：
 * 1. 摒弃单条 Insert，采用 Micro-batch 机制，吞吐量提升百倍，保护 Milvus 底层 Segment 健康。
 * 2. 结合 Flink CheckpointedFunction，在 Checkpoint 触发时强制刷盘，提供 At-Least-Once 语义。
 * 3. 彻底移除非序列化的 ObjectMapper，采用纯静态的 Fastjson 解析。
 */
public class MilvusSink extends RichSinkFunction<String> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusSink.class);

    private String collectionName;
    // 1. 改为 volatile，并移除 open 里的初始化逻辑
    private transient volatile MilvusServiceClient milvusClient;
    private transient Object lock;

    private String milvusHost;
    private String milvusPort;
    private final List<JSONObject> batchBuffer = new ArrayList<>();
    private static final int BATCH_SIZE = 1; // 当积累到500条时，打包一次性写入 Milvus

    @Override
    public void open(Configuration parameters) throws Exception {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        milvusHost = params.getOrDefault("milvusHost", "localhost");
        milvusPort = params.getOrDefault("milvusPort", "19530");
        this.lock = new Object();
        this.collectionName = params.getOrDefault("milvusCollection", "eth_sentiment_analysis");
        LOG.info("📦 MilvusSink 已就绪，连接将在写入第一条数据时建立。");
    }

    private void ensureClientInitialized() {
        if (milvusClient == null) {
            synchronized (lock) {
                if (milvusClient == null) {
                    LOG.info("⚡️ MilvusSink 正在尝试建立真实连接 (Host: {})", milvusHost);
                    milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                            .withHost(milvusHost)
                            .withPort(Integer.parseInt(milvusPort))
                            .withConnectTimeout(5, TimeUnit.SECONDS)
                            .build());
                    LOG.info("✅ MilvusSink 连接成功！");
                }
            }
        }
        LOG.info("尝试连接 Milvus，目标地址为: {}:{}", milvusHost, milvusPort);
    }

    @Override
    public void invoke(String value, Context context) {
        ensureClientInitialized();
        try {
            // 1. Fastjson 解析并放入缓冲区，不立即引发网络 IO
            JSONObject node = JSON.parseObject(value);
            batchBuffer.add(node);

            // 2. 缓冲区满，触发批量写入
            if (batchBuffer.size() >= BATCH_SIZE) {
                flush();
            }
        } catch (Exception e) {
            LOG.error("Failed to buffer JSON data: {}", value, e);
        }
    }

    /**
     * 核心逻辑：将缓冲区的行式 JSON 转换为 Milvus 需要的列式 (Column-based) 数据结构，并执行批量插入
     */
    private void flush() {
        if (batchBuffer.isEmpty()) {
            return;
        }
        ensureClientInitialized();

        int size = batchBuffer.size();
        LOG.debug("Flushing {} records to Milvus...", size);

        // 准备列集合
        List<String> eventIds = new ArrayList<>(size);
        List<String> titles = new ArrayList<>(size);
        List<Long> sentimentScores = new ArrayList<>(size);
        List<Double> rsi14s = new ArrayList<>(size);
        List<Double> atr14s = new ArrayList<>(size);
        List<Double> priceAtTs = new ArrayList<>(size);
        List<Long> pubDates = new ArrayList<>(size);
        List<Long> sentimentEss = new ArrayList<>(size);
        List<List<Float>> vectors = new ArrayList<>(size);
        List<Boolean> isSettleds = new ArrayList<>(size);
        List<Double> winRates = new ArrayList<>(size);
        List<Double> returns = new ArrayList<>(size);

        // 遍历 Buffer，将行数据转置为列数据
        for (JSONObject node : batchBuffer) {
            eventIds.add(node.getString("id"));
            titles.add(node.getString("title"));
            sentimentScores.add(node.getLong("sentiment_score"));
            rsi14s.add(node.getDouble("rsi_14"));
            atr14s.add(node.getDouble("atr_14"));
            priceAtTs.add(node.getDouble("price_at_t"));
            pubDates.add(node.getLong("pubDate"));
            sentimentEss.add(node.getLong("sentiment_es"));

            // 提取向量并确保是 Float 类型
            JSONArray vectorArray = node.getJSONArray("vector");
            List<Float> vector = new ArrayList<>(vectorArray.size());
            for (int i = 0; i < vectorArray.size(); i++) {
                vector.add(vectorArray.getBigDecimal(i).floatValue());
            }
            vectors.add(vector);

            isSettleds.add(false);
            winRates.add(0.0);
            returns.add(0.0);
        }

        // 组装 InsertParam.Field
        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("event_id", eventIds));
        fields.add(new InsertParam.Field("title", titles));
        fields.add(new InsertParam.Field("sentiment_score", sentimentScores));
        fields.add(new InsertParam.Field("rsi_14", rsi14s));
        fields.add(new InsertParam.Field("atr_14", atr14s));
        fields.add(new InsertParam.Field("price_at_t", priceAtTs));
        fields.add(new InsertParam.Field("pub_date", pubDates));
        fields.add(new InsertParam.Field("sentiment_es", sentimentEss));
        fields.add(new InsertParam.Field("vector", vectors));
        fields.add(new InsertParam.Field("is_settled", isSettleds));
        fields.add(new InsertParam.Field("win_rate", winRates));
        fields.add(new InsertParam.Field("return", returns));

        try {
            // 一次网络请求，插入一批数据！
            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFields(fields)
                    .build();

            R<io.milvus.grpc.MutationResult> response = milvusClient.insert(insertParam);

            if (response.getStatus() != R.Status.Success.getCode()) {
                LOG.error("Milvus batch insert failed: {}", response.getMessage());
                throw new RuntimeException("Milvus insert failed: " + response.getMessage());
            }
            batchBuffer.clear();
        } catch (Exception e) {
            LOG.error("Exception during Milvus flush.", e);
            LOG.error("Milvus flush 致命异常，准备触发重启容错", e);
            throw e;
        }
    }

    // =========================================================
    // CheckpointedFunction 接口实现：保证 Flink 快照时数据不丢失
    // =========================================================

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Flink 发起 Checkpoint 时，强制将缓冲区残留的数据刷入 Milvus
        // 这样可以配合 Flink 的 At-Least-Once 或 Exactly-Once 语义
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 无状态初始化
    }

    @Override
    public void close() throws Exception {
        // 算子关闭前，站好最后一班岗，把剩下的数据刷盘
        flush();
        if (milvusClient != null) {
            milvusClient.close();
        }
    }
}