package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeUnit;
/**
 * 基于 Milvus 的特征相似度历史回测与交易决策节点
 */
public class EthBacktestDecisionFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EthBacktestDecisionFunction.class);

    // 1. 声明为 volatile，保证多线程可见性
    private transient volatile MilvusServiceClient milvusClient;
    // 2. 声明一把锁
    private transient Object lock;

    // 暂存连接参数
    private String milvusHost;
    private String milvusPort;

    @Override
    public void open(Configuration parameters) {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        milvusHost = params.getOrDefault("milvusHost", "localhost");
        milvusPort = params.getOrDefault("milvusPort", "19530");

        // 初始化锁对象
        this.lock = new Object();

        LOG.info("🚀 BacktestDecision节点已就绪。Milvus连接将延迟到第一条数据到达时建立 (Host: {})", milvusHost);
//
//        ConnectParam connectParam = ConnectParam.newBuilder()
//                .withHost(host)
//                .withPort(Integer.parseInt(portStr))
//                .withConnectTimeout(5, TimeUnit.SECONDS) // 必须增加：设置 5 秒连接超时
////                .withKeepAliveWithoutCalls(true) // 保持长连接活跃
//                .keepAliveWithoutCalls(true)
//                .withKeepAliveTime(10, TimeUnit.SECONDS)
//                .build();
//        milvusClient = new MilvusServiceClient(connectParam);
    }
    // 提取出一个专门的初始化方法，使用双重检查锁（DCL）
    private void initMilvusClient() {
        if (milvusClient == null) {
            synchronized (lock) {
                if (milvusClient == null) {
                    LOG.info("⚡️ 正在建立 Milvus 真实连接...");
                    milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                            .withHost(milvusHost)
                            .withPort(Integer.parseInt(milvusPort))
                            .withConnectTimeout(5, TimeUnit.SECONDS)
                            .build());
                    LOG.info("✅ Milvus 连接建立成功！");
                }
            }
        }
        LOG.info("尝试连接 Milvus，目标地址为: {}:{}", milvusHost, milvusPort);
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        // 修复 1: 必须使用 CompletableFuture 包装同步的 Milvus 查询，否则 Async I/O 无效
        CompletableFuture.runAsync(() -> {
            try {
                initMilvusClient();
                JSONObject node = JSON.parseObject(input);

                // 修复 2: 严格类型转换，强制将 JSON 数组解析为 List<Float>
                JSONArray vectorArray = node.getJSONArray("vector");
                List<Float> vector = new ArrayList<>();
                if (vectorArray != null) {
                    for (int i = 0; i < vectorArray.size(); i++) {
                        vector.add(vectorArray.getFloat(i));
                    }
                }

                // 构造标量过滤 (注意: 此处提取 sentiment_score 使用 getLong 规避之前的类型问题)
                String expr = DecisionLogic.buildFilterExpr(
                        node.getLong("sentiment_score"), node.getDouble("rsi_14"));

                SearchParam searchParam = SearchParam.newBuilder()
                        .withCollectionName("eth_sentiment_analysis")
                        // 修复 3: 将度量类型修改为 COSINE，以匹配你下方 score > 0.9 的业务逻辑
                        .withMetricType(io.milvus.param.MetricType.COSINE)
                        .withOutFields(java.util.Arrays.asList("win_rate", "return"))
                        .withTopK(5)
                        .withVectors(Collections.singletonList(vector))
                        .withVectorFieldName("vector")
                        .withExpr(expr)
                        .build();

                io.milvus.param.R<io.milvus.grpc.SearchResults> searchResp = milvusClient.search(searchParam);

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

                // 目标决策：最大相似度 > 90% 且 胜率 > 65%
                // if (maxSimilarity > 0.9 && avgWinRate > 0.65) {
                if (maxSimilarity >= 0.0) { // 用于测试，获取原始数据
                    JSONObject signal = new JSONObject();
                    String action = DecisionLogic.decideAction(node.getLong("sentiment_score"));
                    String eventId = node.getString("id");
                    signal.put("action", action);
                    signal.put("event_id", eventId);
                    signal.put("signal_id", DecisionLogic.signalId(eventId, action));
                    signal.put("token", "ETH");
                    signal.put("pubDate", node.getLong("pubDate"));
                    signal.put("sentiment_es", node.getLong("sentiment_es"));
                    signal.put("buy_time", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());
                    signal.put("sell_time", null);
                    signal.put("trigger_score", node.getLong("sentiment_score"));
                    signal.put("timestamp",
                            Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());

                    LOG.info("🔥 [交易指令] 匹配到相似历史！最大相似度: {}, 胜率: {}", maxSimilarity, avgWinRate);
                    resultFuture.complete(Collections.singletonList(signal.toJSONString()));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            } catch (Exception e) {
                LOG.error("Milvus 回测决策节点发生异常: ", e);
                resultFuture.complete(Collections.emptyList());
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (milvusClient != null) {
            milvusClient.close();
        }
    }
}