package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class EthPriceFeatureAsyncFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EthPriceFeatureAsyncFunction.class);
    private String jdbcUrl;
    private String user;
    private String password;

    @Override
    public void open(Configuration parameters) {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

        jdbcUrl = params.getOrDefault("dbUrl","jdbc:mysql://localhost:3306/streampark");
        user = params.getOrDefault("dbUsername","root");
        password = params.getOrDefault("dbPassword", System.getenv().getOrDefault("DB_PASSWORD", ""));

        // 建议：此处应初始化一个数据库连接池（如 Druid 或 HikariCP），而不是在 asyncInvoke 里建连接
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        CompletableFuture.runAsync(() -> {
            // 使用 Fastjson 解析输入
            JSONObject jsonObject = JSON.parseObject(input);
            Long pubDate = jsonObject.getLong("pubDate");

            if (pubDate == null) {
                LOG.warn("Input JSON missing pubDate: {}", input);
                resultFuture.complete(Collections.emptyList());
                return;
            }

            // 1. 增加日志：核对输入的时间戳
            // LOG.info("Querying features for pubDate: {}", pubDate);

            try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
                String sql = "SELECT rsi_14, atr_14, price_at_t FROM trade.eth_kline_features WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, pubDate);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            // 使用 Fastjson 填充数据
                            jsonObject.put("rsi_14", rs.getDouble("rsi_14"));
                            jsonObject.put("atr_14", rs.getDouble("atr_14"));
                            jsonObject.put("price_at_t", rs.getDouble("price_at_t"));

                            resultFuture.complete(Collections.singletonList(jsonObject.toJSONString()));
                        } else {
                            // 2. 增加日志：查询不到结果时记录 pubDate
                             LOG.info("No records found for timestamp <= {}", pubDate);
                            resultFuture.complete(Collections.emptyList());
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("JDBC Error for input {}: ", input, e);
                resultFuture.complete(Collections.emptyList());
            }
        });
    }
}