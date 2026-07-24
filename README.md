# 本地大数据平台 — ETH 情感量化交易

单台 Mac（M4 Pro / 48GB）上通过 k3s 单节点集群，渐进迁移 docker-compose 到生产级 k8s 架构。学习为主，顺便跑策略。

## 架构

```
macOS 宿主机
├── Ollama（Metal GPU，留在宿主机）
└── OrbStack Linux VM（28GB / 8 vCPU）→ k3s 单节点
    ├── ns: observability → kube-prometheus-stack
    ├── ns: data          → Kafka(Strimzi)、MySQL、ClickHouse、Milvus、MinIO
    ├── ns: flink         → Flink K8s Operator + 5 个 FlinkDeployment
    ├── ns: collectors    → Python 采集器(Deployment) / 重训(CronJob)
    └── ns: gitops        → ArgoCD
```

## 项目结构

```
.
├── common/                    # 共享 Java 库（Maven 多模块，不动）
├── datastream/                # Flink Java 作业（Maven 多模块）
│   ├── eth-sentiment-analysis-job/
│   ├── eth-sentiment-trading-job/
│   ├── kafka2milvus/
│   ├── employee-message-processor/
│   └── realtime-riskcontrol-embedding-job/
├── dataflow/                  # Python 采集器（后台进程 / k8s Deployment）
├── global_lib/                # 共享 Python 代码
├── flink-data/                # Flink 运行时数据（checkpoints/savepoints/usrlib）
├── infra/
│   ├── compose/               # docker-compose（过渡期保留）
│   ├── k8s/                   # k8s manifests（deployments, statefulsets, CRs）
│   ├── scripts/               # 构建/部署脚本
│   └── vm/                    # OrbStack VM 创建与 k3s 安装
├── docs/
│   ├── superpowers/
│   │   ├── specs/             # 设计文档
│   │   │   └── 2026-07-16-k3s-production-learning-design.md  ← 总纲
│   │   └── plans/             # 实施计划
│   │       └── 2026-07-24-phase5-gitops-graduation.md
│   └── runbooks/
└── prometheus/                # prometheus.yml
```

## 核心模块

### datastream/ — Flink Java 作业

5 个 Flink 作业，每个都是独立 Maven 模块，通过 `FlinkDeployment` CR 部署到 k8s：

| 作业 | 功能 |
|------|------|
| `eth-sentiment-analysis-job` | ETH 市场情绪分析（Ollama LLM + Milvus 向量） |
| `eth-sentiment-trading-job` | ETH 情感量化交易（信号生成 + 幂等下单） |
| `kafka2milvus` | Kafka → Milvus 向量嵌入流水线 |
| `employee-message-processor` | 员工消息处理 |
| `realtime-riskcontrol-embedding-job` | 实时风险控制嵌入 |

**运行模式**：Flink Kubernetes Operator + Application Mode
- 每个作业一个 `FlinkDeployment` CR
- `upgradeMode: savepoint` 声明式无损升级
- Checkpoint 指向集群内 MinIO

### dataflow/ — Python 采集器

- `market/`、`rss/`、`settlement/` — 常驻消费者（Deployment + liveness probe）
- `eth_model_retrain.py` — 模型重训（CronJob）
- 参数经 ConfigMap/Secret 注入

### infra/k8s/ — 基础设施

| 命名空间 | 服务 |
|----------|------|
| `data` | Kafka(Strimzi/KRaft)、MySQL、ClickHouse(Altinity)、Milvus、MinIO |
| `observability` | Prometheus + Grafana (kube-prometheus-stack) |
| `flink` | Flink Operator + 5 个作业 |
| `collectors` | Python 采集器 |
| `gitops` | ArgoCD |

## 快速开始

### 构建 Flink 作业

```bash
# 构建单个作业
cd datastream/eth-sentiment-analysis-job
mvn clean package

# 构建全部
cd datastream
mvn clean package
```

### 部署到 k8s

```bash
# 应用所有 Flink 作业
kubectl apply -k infra/k8s/flink/

# 查看作业状态
kubectl get flinkdeployment -n flink

# 查看 Pod
kubectl get pods -n flink
```

### 本地开发

```bash
# 编译
mvn clean package

# 本地 IDEA 运行
# 运行参数: --kafkaUrl localhost:9092

# 本地 Docker 运行
docker build -t eth-sentiment-analysis-job:latest datastream/eth-sentiment-analysis-job/
docker run --network host eth-sentiment-analysis-job:latest \
  --kafkaUrl host.docker.internal:9092
```

## 迁移进度

所有 Phase 已完成（详见 [主设计文档](docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md)）：

| Phase | 内容 | 状态 |
|-------|------|------|
| 0 | 生产化还债（MinIO savepoint、fastjson2、密码清理、单元测试） | ✓ |
| 1 | 集群地基（OrbStack VM + k3s + kube-prometheus-stack + MinIO） | ✓ |
| 2 | Python 采集器进集群（Deployment + CronJob） | ✓ |
| 3 | Flink K8s Operator（替代 StreamPark，5 个 FlinkDeployment） | ✓ |
| 4 | 有状态服务进集群（MySQL → ClickHouse → Milvus → Kafka） | ✓ |
| 5 | GitOps + 毕业演练（ArgoCD、SOPS+age、Velero） | ✓ |

## 设计文档

- [主设计文档](docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md) — 全阶段总纲
- [Phase 5 设计](docs/superpowers/specs/2026-07-24-phase5-gitops-graduation-design.md) — GitOps + 毕业演练
- [Phase 5 实施计划](docs/superpowers/plans/2026-07-24-phase5-gitops-graduation.md) — 详细步骤

## 资源预算

VM 28GB，全部单副本：

| 组件 | 内存 |
|------|------|
| Kafka (KRaft) | 2G |
| MySQL | 1G |
| ClickHouse | 4G |
| Milvus + etcd | 5G |
| MinIO | 1G |
| Flink JM+TM | 5G |
| 采集器 | 1G |
| 可观测性栈 | 3G |
| k3s 系统 + 余量 | 6G |

## 错误处理

- **解析失败**：DLQ 主题 + 计数告警
- **Ollama 超时**：AsyncFunction 超时丢弃 + 指标计数 + 告警
- **重复消费**：settlement 幂等键兜底 + Kafka exactly-once

## GitOps 工作流

1. 改代码 → `mvn package` → 打镜像
2. 改 YAML → `git push` → ArgoCD 自动同步到集群
3. Flink Operator 自动 savepoint → 重建 → 恢复
