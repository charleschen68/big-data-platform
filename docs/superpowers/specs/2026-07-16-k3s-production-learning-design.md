# 本地大数据平台生产化改造设计（k3s 学习路线）

日期：2026-07-16
状态：已与用户确认三个关键决策点（架构、仓库结构、迁移顺序）

## 1. 背景与目标

现状：ETH 情感量化交易平台，全部基础设施（Kafka、Flink 1.18.1、MySQL、StreamPark、Milvus、ClickHouse、Prometheus/Grafana）跑在单台 Mac 的 docker-compose 上；Flink 作业为 Java 17 Maven 多模块，Python 采集器以后台进程方式跑在 dev-runner 容器里。

定位（用户确认）：**学习为主，顺便跑策略**。目标是在这台 Mac（M4 Pro / 48GB / 209GB 可用磁盘）上，通过把现有系统渐进迁移到自建 k3s，系统性掌握生产级基础设施技能，迁移期间策略不停机。

选定方案：**单 Linux VM 跑 k3s 单节点集群，按学习模块逐个迁移服务**（VM 工具首选 OrbStack，若遇兼容问题备选 Colima）。已否决的备选：多 VM 模拟多节点（内存不足以承载真实负载）、k3d（存储/网络离生产太远，学习信号失真）。多节点/HA 课题留给将来的专项演练，本设计不覆盖。

## 2. 目标态架构

```
macOS 宿主机 (48GB)
├── Ollama（Metal GPU 加速，永久留在宿主机）
└── OrbStack Linux VM（28GB / 8 vCPU）→ k3s 单节点
    ├── ns: observability → kube-prometheus-stack
    ├── ns: data          → Kafka(Strimzi/KRaft)、MySQL、ClickHouse(Altinity)、
    │                        Milvus(standalone)、MinIO
    ├── ns: flink         → Flink Kubernetes Operator + 各作业(FlinkDeployment CR)
    ├── ns: collectors    → Python 采集器(Deployment) / 重训(CronJob)
    └── ns: gitops        → ArgoCD（Phase 5）
```

关键决策（已确认）：

- **Ollama 不进集群**：VM 无 GPU。集群内通过 ExternalName Service 将宿主机 Ollama 注册为集群内 DNS 名（如 `ollama.external.svc`），业务代码不出现宿主机 IP。这同时是"集群访问外部依赖"的学习课题。
- **存储**：PV 用 k3s 自带 local-path provisioner（单节点足够）；MinIO 提前进集群，作为 Flink checkpoint/savepoint 的 S3 归宿和 Velero 备份目标。
- **迁移期双栈并存**：未迁移服务留在 compose，集群内用 ExternalName/静态 Endpoints 映射；服务迁移完成后仅改映射，业务代码零感知。
- **Flink 运行模式**：Flink Kubernetes Operator + Application Mode。每个作业一个 `FlinkDeployment` CR，operator 为其拉起专属 JobManager/TaskManager pod；`upgradeMode: savepoint` 实现声明式无损升级。**替代 StreamPark**。

## 3. 仓库重构（轻量版，已确认）

不动 Maven 多模块结构（`datastream/`、`common/` 原地不动），只做：

```
big-data-platform/
├── datastream/  common/  dataflow/     # 原地不动
├── infra/
│   ├── compose/     # docker-compose.yml 移入，迁移期继续使用
│   ├── k8s/         # helm values、kustomize overlays、FlinkDeployment CR
│   └── vm/          # OrbStack VM 创建与 k3s 安装脚本
├── sql/             # 合并 flink-sql/ 与 clickhouse-sql/
└── docs/
```

- 二进制（`flink-1.18.1/`、`libs/jdk`）移出工作区，改由脚本下载。
- 每个 Flink 作业与每个 Python 采集器补 Dockerfile。
- GitOps 环境仓库（第二个 repo，存放"哪个版本部署在哪个环境"的状态）推迟到 Phase 5 再拆。

## 4. 迁移顺序（已确认：先还债，无状态先行，Kafka 最后）

### Phase 0 — 上 k8s 前先还生产化欠账（compose 内完成）
1. ~~Flink checkpoint/savepoint 指向 MinIO（S3 协议），完整演练一次 savepoint→停止→重启→恢复。~~ ✓ 已完成（`6e90209`）
2. ~~`eth_trade_settlement.py` 增加幂等键（按信号 ID 去重），防止重复下单。~~ 跳过（用户决定不执行）
3. ~~fastjson 1.2.83（已知 RCE 链）替换为 fastjson2。~~ ✓ 已完成（`973f3ff`）
4. ~~密码从 Javadoc、`MyParameter` 默认值中清除，统一走 `.env`/启动参数。~~ ✓ 已完成（`fc28476`）
5. ~~`EthBacktestDecisionFunction` 补单元测试（决策核心、纯逻辑）。~~ ✓ 已完成（`40268e1`）

### Phase 1 — 集群地基 + 可观测性（学：Helm、CRD、告警）
OrbStack VM + k3s + kube-prometheus-stack + MinIO 进集群。首批告警规则：Kafka consumer lag 持续增长、pod 重启、磁盘水位，经 Telegram/飞书 webhook 推送到手机。
✓ 验收：手机能收到测试告警。

### Phase 2 — Python 采集器进集群（学：Deployment、探针、Secret、资源限额）
采集器打镜像；常驻消费者（采集、settlement）为带 liveness probe 的 Deployment，`eth_model_retrain.py` 为 CronJob；参数经 ConfigMap/Secret 注入。Kafka 仍在 compose，经 ExternalName 访问。
✓ 验收：`kubectl delete pod` 后采集不断流；dev-runner 下线。

### Phase 3 — Flink Kubernetes Operator（学：operator 模式、状态化升级；价值最高）
安装 Flink Operator；五个作业逐个转为 `FlinkDeployment` CR（application mode），checkpoint 指向集群内 MinIO；下线 StreamPark。同阶段完成错误处理设计（见 §6 的 DLQ 与 Ollama 超时策略）。
✓ 验收：修改一行作业代码发版，operator 自动 savepoint→重建→恢复，状态不丢、信号不重复。

### Phase 4 — 有状态服务逐个进集群（学：StatefulSet、各家 operator）
顺序：MySQL（普通 StatefulSet，练手）→ ClickHouse（Altinity Operator）→ Milvus（官方 Helm，standalone）→ **Kafka 最后**（Strimzi + KRaft，新旧集群双跑、消费者切流的生产级迁移手法）。每迁一个，验收后再迁下一个。
✓ 验收：compose 完全下线。

### Phase 5 — GitOps + 毕业演练（学：ArgoCD、SOPS、备份恢复）
ArgoCD + 拆分环境仓库；secrets 用 SOPS+age 加密入 git；Velero 定时备份至 MinIO。毕业演练三场：
1. 重启整个 VM → 全系统自愈，交易信号无重复。
2. 停 Ollama 10 分钟 → 背压/超时行为符合设计，告警触发。
3. 灾难恢复：从 Velero 备份重建集群。

## 5. 资源预算（VM 28GB，全部单副本）

| 组件 | 内存 |
|------|------|
| Kafka (KRaft) | 2G |
| MySQL | 1G |
| ClickHouse | 4G |
| Milvus + etcd | 5G |
| MinIO | 1G |
| Flink JM+TM（全部作业合计） | 5G |
| 采集器 | 1G |
| 可观测性栈 | 3G |
| k3s 系统 + 余量 | 6G |

宿主机余 20GB 供 macOS 与 Ollama（7B-14B 量化模型）。单节点上一律单副本，不做伪 HA。

## 6. 错误处理设计

- **解析失败**：Flink 作业配 dead-letter 主题，脏数据进 DLQ 并计数告警，不打挂作业（Phase 3）。
- **Ollama 超时**：AsyncFunction 明确策略——超时丢弃 + 指标计数 + 告警，禁止无限重试堵死背压（Phase 3）。
- **重复消费**：settlement 幂等键兜底（Phase 0）；Flink 端 Kafka 事务 exactly-once 作为 Phase 3 进阶实验。

## 7. 验证方式

- 各 Phase 的"✓ 验收"即该阶段的验收测试，未通过不进入下一阶段。
- `verify_infrastructure.py` 扩展为面向集群的冒烟脚本，随迁移逐步覆盖集群内端点。
- Phase 5 三场毕业演练为整体验收。
