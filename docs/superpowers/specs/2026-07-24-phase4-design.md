# Phase 4 设计：有状态服务逐个进集群

日期：2026-07-24
状态：设计已确认

## 1. 目标

将 docker-compose 中的 4 个有状态服务逐个迁移到 k3s `data` namespace，每个使用 PVC + local-path provisioner 持久化存储。最终 docker-compose 完全下线。

**迁移顺序**：MySQL → ClickHouse → Milvus → Kafka（Strimzi + KRaft）

**学习课题**：StatefulSet、Operator 模式、Helm chart、PVC/PV、生产级双跑切流。

## 2. 架构概览

```
macOS 宿主机 (48GB)
└── OrbStack Linux VM（28GB / 8 vCPU）→ k3s 单节点
    ├── ns: data
    │   ├── PVC: mysql-data      → MySQL StatefulSet (Phase 4.1)
    │   ├── PVC: clickhouse-data → ClickHouse StatefulSet (Phase 4.2)
    │   ├── PVC: milvus-data     → Milvus Deployment (Phase 4.3)
    │   ├── PVC: kafka-data      → Kafka Strimzi KafkaCluster (Phase 4.4)
    │   └── MinIO (Phase 1 已存在)
    ├── ns: flink    → Flink K8s Operator + 5 FlinkDeployment (Phase 3)
    ├── ns: collectors → Python collectors + ExternalName (Phase 2)
    ├── ns: observability → kube-prometheus-stack (Phase 1)
    └── ns: gitops   → 空，Phase 5 用
```

**核心策略**：
- 每个服务一个 PVC（local-path provisioner 自动创建 PV）
- 数据从 `data/` 目录迁移到 PV 目录（一次性 rsync）
- ExternalName 逐个更新：`orb.local` → `data.svc.cluster.local`
- 每迁一个验证一个，验证通过再迁下一个
- Kafka 迁移时新旧双跑，生产级切流
- 最终 docker-compose.yml 清空，不再运行任何服务

## 3. Phase 4.1 — MySQL（StatefulSet 练手）

### 交付物

`infra/k8s/data/mysql-statefulset.yaml`

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql
  namespace: data
spec:
  serviceName: mysql
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    spec:
      containers:
      - name: mysql
        image: mysql:8.0
        ports:
        - containerPort: 3306
        env:
        - name: MYSQL_ROOT_PASSWORD
          valueFrom:
            secretKeyRef: { name: mysql-secret, key: root-password }
        - name: MYSQL_DATABASE
          value: streampark
        volumeMounts:
        - name: mysql-data
          mountPath: /var/lib/mysql
      volumes:
      - name: mysql-data
        persistentVolumeClaim:
          claimName: mysql-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-pvc
  namespace: data
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: data
spec:
  clusterIP: None  # Headless for StatefulSet
  ports:
  - port: 3306
  selector:
    app: mysql
```

### 关键点

- **StatefulSet** 是 k8s 有状态服务的标准模式（每个 Pod 有稳定 DNS 和存储）
- PVC 用 local-path，自动创建 PV
- 数据迁移：`rsync data/mysql/` → PV 目录
- Secret 管理密码（从 .env 提取）
- 验证：`kubectl exec` 连接测试 + 业务代码改 ExternalName 指向 k3s

### 验收

1. MySQL Pod 运行正常，数据完整
2. collectors namespace 的 ExternalName 更新为 `mysql.data.svc.cluster.local`
3. 业务代码通过 ExternalName 正常访问

## 4. Phase 4.2 — ClickHouse（Altinity Operator）

### 交付物

`infra/k8s/data/clickhouse-values.yaml`（Helm values）
`infra/k8s/data/clickhouse-deployment.yaml`

### 配置概要

```yaml
# Helm values
clickhouse:
  instance:
    replicas: 1
    image: clickhouse/clickhouse-server:24.3-alpine
    resources:
      requests: { cpu: 500m, memory: 4Gi }
      limits: { cpu: 1000m, memory: 4Gi }
    clickhouseConfig:
      max_connections: 100
      keep_alive_timeout: 300
    volumeClaimTemplates:
    - name: data
      storage: 20Gi
      storageClassName: local-path

# Service: clusterIP 不固定（Operator 自动管理）
# 外部访问：ClusterIP + ExternalName
```

### 关键点

- **Altinity Operator** 是 ClickHouse 在 K8s 的事实标准
- Operator 管理 CR（ClickHouseInstance），声明式定义集群拓扑
- 数据目录 `/var/lib/clickhouse` 通过 PVC 持久化
- 学习点：Operator 模式 vs 普通 StatefulSet（Operator 能理解 ClickHouse 领域知识，如分片/副本）
- 数据迁移：`rsync data/clickhouse/` → PV 目录

### 验收

1. ClickHouse Pod 运行正常，数据完整
2. 外部访问正常（端口 8123 HTTP, 9000 native）
3. 业务代码通过 ExternalName 正常访问

## 5. Phase 4.3 — Milvus（官方 Helm，standalone）

### 交付物

`infra/k8s/data/milvus-helm-values.yaml`

### 配置概要

```yaml
# 使用 milvusdb/milvus Helm chart
etcd:
  endpoints: milvus-etcd.data.svc.cluster.local:2379  # 集群内 etcd

minio:
  address: minio.data.svc.cluster.local:9000           # 复用 Phase 1 的 MinIO
  accessKey: minioadmin
  secretKey: minioadmin
  use: existing                                        # 不新建，用已有的

standalone:
  replicas: 1
  image: milvusdb/milvus:v2.4.15
  resources:
    requests: { cpu: 1000m, memory: 4Gi }
    limits: { cpu: 4000m, memory: 8Gi }                # Milvus 最吃内存
  volume:
    type: persistentVolumeClaim
    persistentVolumeClaim:
      claimName: milvus-pvc
      storage: 20Gi
      storageClassName: local-path

# Service: clusterIP 固定，port 19530
# attu (Milvus UI): 可选，作为 Deployment 部署
```

### 关键点

- Milvus 是 Phase 4 中资源消耗最大的服务（当前 compose 给 16GB，VM 中给 8GB）
- 复用 Phase 1 的 MinIO（不新建），通过集群内 DNS 访问
- etcd 独立部署（或内嵌），standalone 模式
- 数据迁移：`rsync data/milvus/` → PV 目录；`data/etcd/` → PV 目录
- 学习点：Helm chart 部署模式 vs 手写 YAML

### 验收

1. Milvus Pod 运行正常，数据完整
2. Milvus SDK 连接验证，向量检索正常
3. attu UI 可访问（如部署）

## 6. Phase 4.4 — Kafka（Strimzi + KRaft，生产级切流）

这是 Phase 4 中最复杂的一环。

### 交付物

`infra/k8s/data/kafka-strimzi.yaml`

### 配置概要

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka
  namespace: data
spec:
  kafka:
    version: 3.7.0
    replicas: 1
    listeners:
    - name: plain
      port: 9092
      type: internal
      configuration:
        bootstrap:
          service:
            name: kafka-bootstrap
    config:
      KAFKA_BROKER_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-kafka-bootstrap.data.svc.cluster.local:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAIN:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAIN
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_HEAP_OPTS: "-Xmx1G -Xms1G"
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller    # KRaft 模式
    storage:
      type: persistentVolumeClaim
      persistentVolumeClaim:
        claimName: kafka-pvc
        storage: 20Gi
        storageClassName: local-path
    resources:
      requests: { cpu: 500m, memory: 2Gi }
      limits: { cpu: 1000m, memory: 2Gi }
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: all-existing-topics
  namespace: data
spec:
  kafkaName: kafka
  # Strimzi 会自动发现 compose 中的已有 topic
```

### 切流策略（新旧双跑）

```
阶段 1: Strimzi Kafka 启动，topic 创建完成
        compose Kafka 仍在运行（数据不丢）
        │
阶段 2: 数据同步 — 挂载 data/kafka/ 到 Strimzi PV → 启动
        （复用已有数据，无需迁移）
        │
阶段 3: 消费者切流 — 逐个消费者组
        - 采集器：改 configmap 中 KAFKA_BOOTSTRAP_SERVERS
        - Flink 作业：改 FlinkDeployment 中 KAFKA_BOOTSTRAP_SERVERS
        - settlement：改 configmap
        │
阶段 4: Producer 切流 — 改 ExternalName
        kafka.orb.local → kafka.data.svc.cluster.local
        │
阶段 5: 验证通过 → 停止 compose Kafka
```

### 关键点

- **Strimzi Operator** 管理 Kafka 生命周期（声明式 CR）
- **KRaft 模式**（无 ZooKeeper，更轻量）
- 数据迁移：`rsync data/kafka/` → PV 目录，Strimzi 复用已有数据
- 学习点：生产级双跑切流模式（新旧同时运行，逐步迁移）
- 这是整个 Phase 4 的收官之战，验证了所有前面学到的技能

### 验收

1. Strimzi Kafka Cluster 运行正常
2. 新旧双跑期间数据一致
3. 切流后所有 producer/consumer 无感知
4. compose Kafka 停止后数据不丢

## 7. 资源预算（VM 28GB）

| 组件 | 内存 | 状态 |
|------|------|------|
| MySQL | 1GB | 迁入后 compose 释放 1GB |
| ClickHouse | 4GB | 迁入后 compose 释放 4GB |
| Milvus | 8GB | 迁入后 compose 释放 8GB |
| Kafka (KRaft) | 2GB | 迁入后 compose 释放 2GB |
| MinIO (已存在) | 1GB | Phase 1 已占用 |
| Flink JM+TM | 5GB | Phase 3 已占用 |
| 采集器 | 1GB | Phase 2 已占用 |
| 可观测性栈 | 3GB | Phase 1 已占用 |
| k3s 系统 + 余量 | 6GB | |
| **总计** | **~31GB** | 峰值时新旧双跑约 35GB |

峰值出现在 Kafka 迁移阶段（新旧双跑），约 35GB，VM 28GB + macOS 余量，够用。

## 8. ExternalName 更新策略

当前 collectors namespace 的 ExternalName 指向 OrbStack 容器：

```
kafka.orb.local    → kafka:29092         (compose)
mysql.orb.local    → mysql:3306          (compose)
milvus.orb.local   → milvus-standalone:19530  (compose)
```

迁移后逐个更新为 k3s 内部 Service DNS：

```
# MySQL 迁移后
mysql.orb.local → mysql.data.svc.cluster.local:3306

# ClickHouse 迁移后（新增 ExternalName）
clickhouse.orb.local → clickhouse.data.svc.cluster.local:9000

# Milvus 迁移后
milvus.orb.local → milvus-milvus.data.svc.cluster.local:19530

# Kafka 迁移后
kafka.orb.local → kafka-kafka-bootstrap.data.svc.cluster.local:9092
```

## 9. 文件结构

```
infra/k8s/data/
├── mysql-statefulset.yaml       # 新增
├── clickhouse-values.yaml       # 新增
├── milvus-helm-values.yaml      # 新增
├── kafka-strimzi.yaml           # 新增
├── minio-values.yaml            # Phase 1 已存在，不变
└── kustomization.yaml           # 更新：加入 4 个新服务

infra/compose/
└── docker-compose.yml           # 更新：逐个注释已迁移的服务
```

## 10. 验收标准

### 各 Phase 验收

| Phase | 服务 | 验收 |
|-------|------|------|
| 4.1 | MySQL | Pod 运行正常，数据完整，ExternalName 指向 k3s |
| 4.2 | ClickHouse | Pod 运行正常，数据完整，外部访问正常 |
| 4.3 | Milvus | Pod 运行正常，数据完整，SDK 连接验证通过 |
| 4.4 | Kafka | 新旧双跑验证通过，切流后 producer/consumer 无感知 |

### 最终验收

- docker-compose.yml 中所有服务停止（`docker compose down`）
- 所有服务通过 k3s Service DNS 访问
- 采集不断流，交易信号无重复

## 11. 约束

- **存储**：PVC + local-path provisioner（k3s 自带）
- **镜像架构**：linux/arm64
- **镜像拉取策略**：Never（与 Phase 2/3 一致）
- **命名空间**：data（所有状态服务统一在一个 namespace）
- **单副本**：一律单副本，不做伪 HA
- **Ollama**：不进集群，通过 ExternalName 访问
