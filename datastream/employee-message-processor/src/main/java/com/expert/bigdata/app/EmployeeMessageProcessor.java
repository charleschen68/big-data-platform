package com.expert.bigdata.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.api.java.utils.ParameterTool;
import com.bigdata.common.utils.MyParameter;

import static org.apache.flink.table.api.Expressions.$;

// 本地idea运行参数
// --sinkTablename employee_mysql_sink

// streampark运行参数
//--kafkaUrl kafka:29092
//--dbUrl jdbc:mysql://mysql:3306/streampark
//--dbUsername root
//--dbPassword "${MYSQL_PASSWORD}"
//--sourceTopic topic1
//--sinkTopic topic2
//--sinkTablename employee_mysql_sink

public class EmployeeMessageProcessor {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用Flink自带的ParameterTool解析参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        // 创建参数工具类实例
        MyParameter myParameter = new MyParameter(parameterTool);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从参数工具类获取配置
        String kafkaBootstrapServers = myParameter.getKafkaUrl();
        String mysqlUrl = myParameter.getDbUrl();
        String mysqlUsername = myParameter.getDbUsername();
        String mysqlPassword = myParameter.getDbPassword();
        String employeeTopic1 = myParameter.getSourceTopic();
        String employeeTopic2 = myParameter.getSinkTopic();
        String mysqlTableName = myParameter.getSinkTablename();

        // 创建Kafka source表
        tableEnv.executeSql("""
                CREATE TABLE employee_source (
                  id BIGINT,
                  name STRING,
                  department STRING,
                  salary DOUBLE,
                  message_time TIMESTAMP(3),
                  WATERMARK FOR message_time AS message_time
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = '%s',
                  'properties.bootstrap.servers' = '%s',
                  'properties.group.id' = 'employee-group',
                  'scan.startup.mode' = 'earliest-offset',
                  'format' = 'json'
                )
                """.formatted(employeeTopic1, kafkaBootstrapServers));

        // 创建MySQL sink表
        tableEnv.executeSql("""
                CREATE TABLE employee_mysql_sink (
                  id BIGINT,
                  name STRING,
                  department STRING,
                  salary DOUBLE,
                  message_time TIMESTAMP(3),
                  process_time AS PROCTIME(),
                  PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                  'connector' = 'jdbc',
                  'url' = '%s',
                  'table-name' = '%s',
                  'username' = '%s',
                  'password' = '%s',
                  'driver' = 'com.mysql.cj.jdbc.Driver'
                )
                """.formatted(mysqlUrl, mysqlTableName, mysqlUsername, mysqlPassword));

        // 创建Kafka sink表
        tableEnv.executeSql("""
                CREATE TABLE employee_kafka_sink (
                  id BIGINT,
                  name STRING,
                  department STRING,
                  salary DOUBLE,
                  message_time TIMESTAMP(3)
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = '%s',
                  'properties.bootstrap.servers' = '%s',
                  'format' = 'json',
                  'sink.partitioner' = 'round-robin'
                )
                """.formatted(employeeTopic2, kafkaBootstrapServers));

        // 处理数据：从源表读取并写入目标表
        Table processedTable = tableEnv.from("employee_source")
                .select(
                        $("id"),
                        $("name"),
                        $("department"),
                        $("salary"),
                        $("message_time")
                );

        // 写入MySQL
        processedTable.executeInsert("employee_mysql_sink");

        // 写入Kafka
        processedTable.executeInsert("employee_kafka_sink");

        // 执行任务
//        env.execute("Employee Data Processing Job");
    }
}