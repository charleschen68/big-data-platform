# Phase 5 — GitOps + 毕业演练设计

日期：2026-07-24
前置：Phases 0-4 全部完成（docker-compose 已完全下线）
状态：设计确认

## 1. 背景

Phases 0-4 已完成所有服务的 k3s 迁移。当前所有基础设施（Kafka、Flink、MySQL、ClickHouse、Milvus、Prometheus/Grafana、采集器）运行在 k3s 集群中，通过 kubectl 直接管理 YAML。

Phase 5 的目标是引入 GitOps 模式（ArgoCD），完善 secrets 管理（SOPS+age），建立备份恢复能力（Velero），并通过三场毕业演练验证整个系统的生产就绪状态。

## 2. 目标态架构

```
┌──────────────────────────────────────────────────────────────┐
│  ~/big-data-platform-envs/ (GitOps 仓库, GitHub)             │
│  ├── environments/current/                                   │
│  │   ├── kustomization.yaml                                  │
│  │   ├── applications/  ← ArgoCD Application CRs            │
│  │   ├── secrets/     ← SOPS 加密 secrets (age)             │
│  │   └── values/      ← Helm values                         │
│  └── scripts/      ← sops-encrypt, sops-decrypt, velero     │
└──────────────────────┬───────────────────────────────────────┘
                       │ git clone / watch
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  OrbStack Linux VM → k3s 单节点集群                          │
│                                                              │
│  ns: gitops    ← ArgoCD (Deployment)                         │
│  ns: data      ← Kafka(Strimzi), MySQL, ClickHouse,          │
│                  Milvus, MinIO (Velero target)               │
│  ns: flink     ← Flink K8s Operator + FlinkDeployments       │
│  ns: collectors ← Python collectors (Deployments)            │
│  ns: observability ← kube-prometheus-stack                   │
│                                                              │
│  Velero cron → 定时备份 → MinIO S3 (velero-backups bucket)   │
└──────────────────────────────────────────────────────────────┘
```

## 3. 设计决策

### 3.1 环境仓库

**决策**：拆出独立的 GitOps 仓库（生产标准做法）。

- 仓库位置：`~/big-data-platform-envs/`，独立 git repo
- 推送到 GitHub，与 `big-data-platform` 代码仓库并列
- 单环境（`environments/current/`），不预留多环境结构
- ArgoCD 在集群内运行，clone 环境仓库的声明

**理由**：生产上代码仓库（存实现）和环境仓库（存声明）分离是标准模式。ArgoCD 作为控制器持续同步环境仓库的声明到集群，实现声明式部署。

### 3.2 ArgoCD

**决策**：ArgoCD 安装到 k3s 集群内。

- 安装到 `ns: gitops`
- 使用 ApplicationSet 自动发现 `environments/current/` 下的 namespace
- 配置 `prune: true` 和 `selfHeal: true`
- repo URL 指向 GitHub 上的 `big-data-platform-envs`

### 3.3 SOPS + Secrets

**决策**：使用 SOPS + age 加密 K8s secrets。

- age 私钥存储在 VM 的 `/root/.sops/age/keys.txt`
- 加密后的 secrets 直接提交到 GitOps 仓库
- 加密的 secrets 清单：
  - Kafka 连接密码
  - MySQL root 密码
  - ClickHouse 认证凭据
  - MinIO access/secret key
  - Prometheus alertmanager 配置

### 3.4 Velero 备份

**决策**：Velero 备份到集群内的 MinIO S3。

- 备份目标：MinIO 的 `velero-backups` bucket
- 定时备份：每天凌晨 3 点 cron
- 备份内容：所有 namespaces 的 PVC、etcd 数据、关键 CRD
- 备份数据不在 Git 中（Git 只存 YAML 声明）

### 3.5 毕业演练

**三场演练**：

| 演练 | 场景 | 验证点 |
|------|------|--------|
| 1. VM 重启 | 重启整个 OrbStack VM | k3s 自动启动 → ArgoCD 同步 → 所有 pod 拉起 → 采集器连 Kafka → Flink 从 savepoint 恢复 → 交易信号不重复 |
| 2. Ollama 中断 | 停 Ollama 10 分钟 | Flink AsyncFunction 超时/背压 → Prometheus 告警 → Telegram 推送 |
| 3. 灾难恢复 | 从 Velero 备份重建集群 | PVC 恢复 → etcd 恢复 → ArgoCD 同步 → 全系统可用 |

**演练顺序**：VM 重启（重点）→ Ollama 中断 → 灾难恢复

## 4. 数据流

```
1. 开发者修改 infra/k8s/ 的 manifests → 提交到代码仓库
2. ArgoCD 检测到环境仓库的变更 → 自动同步 (auto-sync)
3. ArgoCD 比较集群状态和声明 → 应用差异
4. 集群状态 → ArgoCD UI 可见
```

## 5. 验收标准

- [ ] ArgoCD 安装成功，UI 可访问
- [ ] 环境仓库拆分完成，ArgoCD 能同步声明
- [ ] SOPS 加密的 secrets 正确应用到集群
- [ ] Velero 定时备份成功，备份数据在 MinIO 中
- [ ] 毕业演练 1：VM 重启后全系统自愈，交易信号无重复
- [ ] 毕业演练 2：Ollama 中断期间背压/超时行为符合设计，告警触发
- [ ] 毕业演练 3：从 Velero 备份恢复集群，全系统可用
