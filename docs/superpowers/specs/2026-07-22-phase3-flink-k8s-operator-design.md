# Phase 3 设计：Flink Kubernetes Operator

日期：2026-07-22
状态：设计已确认

## 1. 目标

将 5 个 Java Flink 作业从 compose 中的 StreamPark 迁移到 k3s `flink` namespace，使用 Apache Flink Kubernetes Operator 管理作业生命周期。核心学习目标是 **operator 模式** 和 **状态化升级**。

## 2. 架构

```
compose (当前)                    k3s flink namespace (目标)
─────────────────                 ─────────────────────────
StreamPark (compose)  ──→  下线
Flink JM+TM (compose) ──→  Operator 管理的 FlinkDeployment
MinIO (compose)         ──→  MinIO (data ns, ClusterIP)
```

**5 个 Flink 作业：**
1. `eth-sentiment-analysis-job` — 情感分析
2. `eth-sentiment-trading-job` — 情感交易
3. `kafka2milvus` — Kafka→Milvus 实时同步
4. `employee-message-processor` — 员工消息处理
5. `realtime-riskcontrol-embedding-job` — 实时风控嵌入

每个作业一个 `FlinkDeployment` CR，Operator 为其拉起专属 JobManager + TaskManager pod。

## 3. 方案

**方案 A：Operator Helm 安装 + YAML CRD（已确认）**

- Helm 安装 Apache Flink K8s Operator 到 `flink` namespace
- 5 个 FlinkDeployment YAML 文件放在 `infra/k8s/flink/`
- 作业 JAR 包通过 Dockerfile 打包进镜像
- `upgradeMode: savepoint` — 声明式无损升级
- `runMode: application` — Application Mode，JAR 作为入口

## 4. 组件设计

### 4.1 Flink Operator 安装
- 镜像：`apache/flink-kubernetes-operator:1.2.0`
- Namespace：`flink`
- 监听 `FlinkDeployment` CRD，自动管理作业生命周期

### 4.2 5 个 FlinkDeployment CR
- 文件：`infra/k8s/flink/{job-name}.yaml`
- `upgradeMode: savepoint`
- `runMode: application`
- 每个 CR 指定：JAR 路径、并行度、资源、checkpoint 配置

### 4.3 Docker 镜像
- 每个 Flink 作业一个 Dockerfile：`datastream/{job}/Dockerfile`
- JAR 包打包进镜像，`imagePullPolicy: Never`
- 镜像名：`big-data/{job-name}:phase3`

### 4.4 MinIO 连接
- FlinkDeployment 通过环境变量 `S3_ENDPOINT` 指向 k3s MinIO
- Service DNS：`minio.data.svc.cluster.local:9000`
- Checkpoint 路径：`s3://flink-state/checkpoints/{job-name}/`

### 4.5 StreamPark 下线
- FlinkDeployment 全部 Ready 后，`docker compose stop streampark`
- Operator 接管作业生命周期（提交/停止/重启/savepoint）
- 调度交给 K8s CronJob 或外部调度器（Phase 5+）

## 5. 数据流与错误处理

### 5.1 数据流
```
Kafka (compose) → FlinkDeployment (k3s) → MinIO (k3s data ns)
                    │
                    ├── eth-sentiment-analysis-job
                    ├── eth-sentiment-trading-job
                    ├── kafka2milvus
                    ├── employee-message-processor
                    └── realtime-riskcontrol-embedding-job
```

### 5.2 错误处理
1. **DLQ（死信队列）**：解析失败的消息写入独立 Kafka topic，计数告警，不打挂作业
2. **Ollama 超时**：AsyncFunction 超时丢弃 + 指标计数 + 告警，禁止无限重试堵死背压
3. **重复消费**：settlement 幂等键兜底；Flink 端 Kafka 事务 exactly-once

## 6. 验收标准

1. **Operator 安装**：Flink K8s Operator 在 `flink` namespace 运行，CRD 就绪
2. **5 个 FlinkDeployment**：全部 Ready，通过 Flink REST API 可查
3. **Checkpoint**：checkpoint 写入 k3s MinIO，可验证 savepoint 文件
4. **StreamPark 下线**：compose 中 streampark 停止，不影响作业运行
5. **Live Upgrade**：修改一行作业代码 → 发版 → Operator 自动 savepoint→重建→恢复 → 状态不丢、信号不重复

## 7. 与现有系统关系

- Kafka 仍在 compose，通过 ExternalName Service 暴露
- MinIO 已在 k3s `data` namespace，Flink checkpoint 指向它
- Ollama 留在宿主机，通过 ExternalName Service 访问
- 采集器（Phase 2）在 `collectors` namespace，与 Flink 独立
