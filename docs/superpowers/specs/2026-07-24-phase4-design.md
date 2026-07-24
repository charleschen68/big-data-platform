# Phase 4 设计：迁移剩余 4 个 Flink 作业到 K8s Operator

日期：2026-07-24
状态：设计已确认

## 1. 目标

将 compose/StreamPark 中的 4 个 Java Flink 作业逐个迁移到 k3s `flink` namespace，复用 Phase 3 的 Flink K8s Operator。每个作业独立迁移、独立验证，模式与 Phase 3 完全一致。

## 2. 待迁移作业

| # | 作业 | 模块路径 | 说明 |
|---|------|----------|------|
| 1 | eth-sentiment-analysis-job | `datastream/eth-sentiment-analysis-job/` | 情感分析，与 trading-job 同源 |
| 2 | kafka2milvus | `datastream/kafka2milvus/` | Kafka→Milvus 实时同步，已有 global_lib |
| 3 | employee-message-processor | `datastream/employee-message-processor/` | 员工消息处理，已有 global_lib |
| 4 | realtime-riskcontrol-embedding-job | `datastream/realtime-riskcontrol-embedding-job/` | 实时风控嵌入，已有 global_lib |

## 3. 架构

```
compose (当前)                    k3s flink namespace (目标)
─────────────────                 ─────────────────────────
StreamPark (compose)  ──→  下线（Phase 3 已做）
Flink JM+TM (compose) ──→  Operator 管理的 FlinkDeployment
MinIO (k3s data ns)   ──→  不变，所有作业共享
Kafka (compose)         ──→  不变，ExternalName Service
```

**复用组件**：
- Flink K8s Operator（Phase 3 已安装，`apache/flink-kubernetes-operator:1.2.0`）
- MinIO（`minio.data.svc.cluster.local:9000`）
- Kafka（`kafka:29092` via ExternalName）
- `flink` namespace（Phase 3 已创建）

## 4. 每个作业的交付物

每个作业 = 3 个文件：

1. **Dockerfile** — `datastream/{job}/Dockerfile`
   - `eclipse-temurin:17-jdk-arm64-alpine` 基础镜像
   - fat JAR 打包到 `/opt/flink/jobs/{job}.jar`
   - Application Mode，`flink run-application -p 2`
   - `imagePullPolicy: Never`

2. **FlinkDeployment YAML** — `infra/k8s/flink/{job}.yaml`
   - `upgradeMode: savepoint`
   - `runMode: application`
   - JM: 1 CPU / 1Gi，TM: 2 CPU / 2Gi / 4 slots
   - S3_ENDPOINT 指向 k3s MinIO
   - KAFKA_BOOTSTRAP_SERVERS = `kafka:29092`

3. **build script 扩展** — `infra/scripts/build-and-import-flink.sh`
   - 复用 Phase 3 的脚本模式
   - 支持批量构建所有 Flink images
   - `orb -m k3s-node k3s ctr images import` 导入 k3s

## 5. 文件结构

```
infra/k8s/flink/
├── kustomization.yaml          # 更新：加入 4 个新 CR
├── operator-helm.yaml          # Phase 3 已创建，不变
├── flink-operator-values.yaml  # Phase 3 已创建，不变
├── eth-sentiment-trading-job.yaml   # Phase 3 已创建，不变
├── eth-sentiment-analysis-job.yaml  # 新增
├── kafka2milvus.yaml               # 新增
├── employee-message-processor.yaml # 新增
└── realtime-riskcontrol-embedding-job.yaml  # 新增

datastream/
├── eth-sentiment-analysis-job/
│   └── Dockerfile                  # 新增
├── kafka2milvus/
│   └── Dockerfile                  # 新增
├── employee-message-processor/
│   └── Dockerfile                  # 新增
└── realtime-riskcontrol-embedding-job/
    └── Dockerfile                  # 新增

infra/scripts/
└── build-and-import-flink.sh       # 扩展：批量构建
```

## 6. 迁移顺序

```
eth-sentiment-analysis-job      → 第 1 个（与 trading-job 最相似）
kafka2milvus                    → 第 2 个（已有 global_lib，结构清晰）
employee-message-processor      → 第 3 个（已有 global_lib）
realtime-riskcontrol-embedding-job → 第 4 个（最后，作为收尾）
```

## 7. 数据流

```
Kafka (compose) → FlinkDeployment (k3s flink ns) → MinIO (k3s data ns)
                    │
                    ├── eth-sentiment-trading-job    (Phase 3)
                    ├── eth-sentiment-analysis-job   (Phase 4)
                    ├── kafka2milvus                 (Phase 4)
                    ├── employee-message-processor   (Phase 4)
                    └── realtime-riskcontrol-embedding-job  (Phase 4)
```

## 8. 验收标准

1. **4 个 FlinkDeployment**：全部 Ready，通过 Flink REST API 可查
2. **Checkpoint**：所有作业 checkpoint 写入 k3s MinIO
3. **镜像**：4 个 Docker image 构建成功，导入 k3s containerd
4. **无冲突**：与 Phase 3 的 eth-sentiment-trading-job 独立运行
5. **StreamPark 下线**：compose 中 streampark 不再运行
6. **Live Upgrade**：修改代码 → rebuild → Operator savepoint→重建→恢复

## 9. 约束

- **Flink 版本**：1.18.1（与 Phase 3 一致）
- **Operator 版本**：1.2.0（与 Phase 3 一致）
- **镜像架构**：linux/arm64（与 Phase 2/3 一致）
- **镜像拉取策略**：Never（与 Phase 2/3 一致）
- **升级模式**：savepoint（与 Phase 3 一致）
- **命名空间**：flink（与 Phase 3 一致）
