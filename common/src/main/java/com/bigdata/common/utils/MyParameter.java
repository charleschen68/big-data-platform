package com.bigdata.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.protocol.types.Field;


public class MyParameter {

    // String host = params.get("milvusHost", "localhost");

    private String kafkaUrl;
    private String dbUrl;
    private String dbUsername;
    private String dbPassword;
    private String sourceTopic;
    private String sinkTopic;
    private String sinkTablename;
    private String kafkaGroupId;
    private String kafkaTopics;
    private String ollamaHost;
    private String milvusHost;

    public MyParameter(ParameterTool parameterTool) {
        this.kafkaUrl = parameterTool.get("kafkaUrl", "localhost:9092");
        this.dbUrl = parameterTool.get("dbUrl", "jdbc:mysql://localhost:3306/streampark");
        this.dbUsername = parameterTool.get("dbUsername", "root");
        // 密码不设硬编码默认值：优先 --dbPassword 参数，其次 DB_PASSWORD 环境变量
        this.dbPassword = parameterTool.get("dbPassword", System.getenv().getOrDefault("DB_PASSWORD", ""));
        this.sourceTopic = parameterTool.get("sourceTopic", "topic1");
        this.sinkTopic = parameterTool.get("sinkTopic", "topic2");
        this.sinkTablename = parameterTool.get("sinkTablename");
        this.kafkaGroupId = parameterTool.get("kafkaGroupId", "default-group-id");
        this.kafkaTopics = parameterTool.get("kafkaTopics", "risk_control_logs");
        this.ollamaHost = parameterTool.get("ollamaHost", "localhost");
        this.milvusHost = parameterTool.get("milvusHost", "localhost");
    }

    // Getter methods
    public String getKafkaUrl() {
        return kafkaUrl;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public String getSinkTablename() {
        return sinkTablename;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getKafkaTopics() {
        return kafkaTopics;
    }

    public String getOllamaHost() {
        return ollamaHost;
    }

    public String getMilvusHost() {
        return milvusHost;
    }

    // Setter methods
    public void setKafkaUrl(String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public void setSinkTablename(String sinkTablename) {
        this.sinkTablename = sinkTablename;
    }

    public void setKafkaGroupId(String kafkaGroupId) {
        this.kafkaGroupId = kafkaGroupId;
    }

    public void setKafkaTopics(String kafkaTopics) {
        this.kafkaTopics = kafkaTopics;
    }

    public void setOllamaHost(String ollamaHost) {
        this.ollamaHost = ollamaHost;
    }

    public void setMilvusHost(String milvusHost) {
        this.milvusHost = milvusHost;
    }
}
