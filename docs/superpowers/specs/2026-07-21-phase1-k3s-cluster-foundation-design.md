# Phase 1：集群地基 + 可观测性 设计

日期：2026-07-21
状态：已与用户确认全部关键决策点
前置：`docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md`（总体迁移路线，Phase 0 已完成）

## 1. 背景与目标

总体设计文档已确定迁移顺序：Phase 0（还债，已完成）→ **Phase 1（本设计）** → Phase 2（采集器进集群）→ Phase 3（Flink Operator）→ Phase 4（有状态服务）→ Phase 5（GitOps）。

Phase 1 目标：在 Mac 宿主机上起一台 OrbStack Linux VM，装单节点 k3s，把可观测性栈（kube-prometheus-stack）和 MinIO 带进集群，打通"Kafka consumer lag / pod 重启 / 磁盘水位"三类告警到 Telegram。**compose 栈在整个过程中不停机、不迁移**——这是本阶段唯一的业务连续性约束。

本阶段同时完成仓库轻量重构（`infra/` 目录），为后续阶段提供落地位置。

## 2. 环境现状（2026-07-21 勘察结果）

- 无现存 OrbStack VM（`orb list` 为空）。
- `kubectl`（已装）、`orb`/`orbctl`（已装）；`helm`、k3s 未装。
- `~/.kube/config` 有一个 `orbstack` context 指向 OrbStack 内置 K8s 功能（`127.0.0.1:26443`），当前未运行、未使用。本设计使用独立的 VM + k3s 方案，与之无关，不冲突，也不启用该内置功能。
- 磁盘剩余 214GB，内存 48GB，12 核——满足总体设计 §5 的资源预算（VM 28GB/8vCPU）。

## 3. 仓库重构

```
big-data-platform/
├── datastream/  common/  dataflow/     # 原地不动
├── infra/
│   ├── compose/     # docker-compose.yml、.env、.env.example 迁入
│   ├── k8s/          # 按 namespace/组件 组织的 Helm values
│   │   ├── observability/kube-prometheus-stack-values.yaml
│   │   └── data/minio-values.yaml
│   └── vm/          # OrbStack VM 创建 + k3s 安装脚本
└── docs/
```

- `docker-compose.yml` 内的卷路径（如 `./data/clickhouse`）是相对 compose 文件自身位置的，随文件一起移动到 `infra/compose/` 后无需改路径；后续 `docker compose` 命令需在 `infra/compose/` 目录下执行（或用 `-f infra/compose/docker-compose.yml`）。
- `.env`（不提交）、`.env.example` 一并移入 `infra/compose/`。
- `global_lib/`、`flink-1.18.1/` 等二进制目录本阶段不动（总体设计里"移出工作区改脚本下载"是更后期的优化，非本阶段必需）。

## 4. VM + k3s

- 创建：`orb create ubuntu:24.04 k3s-node`，资源配额通过 OrbStack 配置（内存 28GB / CPU 8 核，具体 CLI 参数以撰写 `infra/vm/create-vm.sh` 时实测的 `orb create --help` 为准）。
- k3s 安装：官方安装脚本（`curl -sfL https://get.k3s.io | sh -`），单节点，**保留默认组件**（Traefik、ServiceLB）——本阶段不用 ingress（见 §6），但不主动关闭，为后续阶段留口子。
- kubeconfig：从 VM 内 `/etc/rancher/k3s/k3s.yaml` 取出，合并进宿主机 `~/.kube/config`，context 命名为 `k3s-node`（与现存无用的 `orbstack` context 区分）。后续所有 `kubectl`/`helm` 操作从 Mac 宿主机对 `k3s-node` context 执行。
- Helm：宿主机装 `helm`（`brew install helm`）。

## 5. Namespace 规划

按总体设计 §2 的目标态架构创建五个 namespace：`observability`、`data`、`flink`、`collectors`、`gitops`。本阶段仅 `observability`、`data` 有实际工作负载；其余三个是为 Phase 2+ 预留的空 namespace。

## 6. 可观测性栈

- Helm 安装 `kube-prometheus-stack`（chart 仓库 `prometheus-community`）到 `observability` namespace，values 文件落地 `infra/k8s/observability/kube-prometheus-stack-values.yaml`。
- **Grafana 访问方式**：`kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n observability`。不引入 ingress/域名访问——学习重心放在集群内部而非入口流量，符合总体设计"渐进学习"的定位。
- **Kafka 指标来源**：Kafka 仍在 `infra/compose` 里跑。在 `docker-compose.yml` 新增 `kafka-exporter` 服务（`danielqsj/kafka-exporter` 或等价镜像），指向 `kafka:29092`。集群内 Prometheus 通过 OrbStack 内置跨容器域名解析（`kafka-exporter.orb.local`）远程抓取，具体端口（默认 9308）和可达性在实施阶段验证；若 OrbStack 域名解析在 k3s VM 内不可达，退回到"compose 侧暴露宿主机端口，Prometheus 用宿主机 IP 抓取"的备选方案（记录在实施计划里作为已知风险，不在设计阶段展开）。
- **告警接收端**：Telegram。需要用户预先通过 BotFather 创建 bot 拿到 token，并获取目标 chat id；两者存为 k8s Secret（`kubectl create secret generic telegram-bot -n observability --from-literal=...`），不写入 git，与 Phase 0 `.env` 密码处理原则一致。Alertmanager values 里配置 `telegram_configs` receiver。
- **首批告警规则**（`PrometheusRule` 资源，位于 `infra/k8s/observability/`）：
  1. Kafka consumer lag 持续增长（基于 `kafka-exporter` 的 `kafka_consumergroup_lag` 指标，阈值与观察窗口在实施时结合实际消费速率标定）。
  2. Pod 重启（`kube-prometheus-stack` 默认规则 `KubePodCrashLooping` 等，路由到 Telegram receiver 即可，不需要新写规则）。
  3. 磁盘水位（默认规则 `KubeNodeUnschedulable`/`NodeFilesystemAlmostOutOfSpace` 等，同样复用默认规则+路由）。

## 7. MinIO 入集群

- 官方 `minio/minio` Helm chart（非 Bitnami），单副本 standalone 模式，values 落地 `infra/k8s/data/minio-values.yaml`。
- 存储：k3s 自带 `local-path` provisioner 提供 PVC，容量在实施阶段按可观测性栈之外的剩余预算给一个保守值（如 20-50Gi，214GB 总盘完全够用）。
- **本阶段不做任何数据接入**：Flink checkpoint/savepoint 继续指向 compose 里的 MinIO（`docker-compose.yml` 里 `state.checkpoints.dir` 等配置不动）。集群内这个 MinIO 纯粹是"提前把 Helm 部署方式跑通"，真正接入是 Phase 3（Flink Operator 迁移时）的工作。

## 8. 验收标准

- `kubectl --context k3s-node get pods -A`：所有 pod Running/Completed，无 CrashLoopBackOff。
- 手动触发一次测试告警（如临时调低磁盘水位阈值，或手动 kill 一个非关键 pod 触发 `KubePodCrashLooping`）→ Telegram 收到消息。
- `infra/compose/` 下 `docker compose ps`：Phase 0 遗留的服务（Kafka、Flink、MySQL、Milvus、ClickHouse 等）全部 Running，行为与迁移前一致——**compose 栈全程不停机**。
- Grafana 可通过 port-forward 在浏览器打开，能看到 kube-prometheus-stack 默认仪表盘数据。

## 9. 明确不在本阶段范围内

- Flink Kubernetes Operator（Phase 3）。
- 任何 compose 服务迁入集群（Phase 2 起才迁采集器，Phase 4 才迁有状态服务）。
- Ingress/域名对外访问（当前用 port-forward，真正需要时再引入）。
- ArgoCD/GitOps（Phase 5）。
- 二进制目录（`flink-1.18.1/` 等）移出工作区（后续优化项，非本阶段必需）。

## 10. 风险与未决细节（留给实施计划标定）

- OrbStack 跨容器/VM 域名解析在 k3s VM 内的具体可达性未实测，§6 已给出备选方案。
- `orb create` 的资源配额具体 CLI flag 名称需在实施时对照 `orb create --help` 确认。
- kafka-exporter 镜像版本、Prometheus 告警阈值的具体数值，留给实施计划的步骤里标定并验证，不在设计文档里写死。
