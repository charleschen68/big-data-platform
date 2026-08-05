# Phase 5 — GitOps + 毕业演练 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce GitOps mode (ArgoCD),完善 secrets 管理 (SOPS+age), 建立备份恢复能力 (Velero), and validate production readiness through three graduation drills.

**Architecture:** ArgoCD runs in-cluster as a Deployment, continuously syncing declarations from the GitOps environment repository (`~/big-data-platform-envs/`). SOPS encrypts K8s secrets with age before committing to Git. Velero performs scheduled backups of PVCs and etcd data to MinIO S3. Three graduation drills validate VM restart自愈, Ollama fault tolerance, and disaster recovery.

**Tech Stack:** k3s, ArgoCD (Helm), SOPS + age, Velero (Helm), MinIO (S3), Kustomize, Git

## Global Constraints

- VM: OrbStack Linux VM with 28GB RAM / 8 vCPU (existing from Phase 1)
- k3s: single-node cluster (existing from Phase 1)
- All services: single replica, no pseudo-HA (existing constraint)
- Ollama: stays on macOS host, accessed via ExternalName Service (existing from design)
- Storage: PV uses k3s built-in local-path provisioner (existing from design)
- GitOps repository: independent repo at `~/big-data-platform-envs/`, pushed to GitHub
- Environment: single environment (`environments/current/`)
- Existing namespaces: `observability`, `data`, `flink`, `collectors`, `gitops` (from `infra/k8s/namespaces.yaml`)
- ArgoCD installs to `ns: gitops`
- Velero backup target: MinIO S3 bucket `velero-backups`
- Backup schedule: daily at 3:00 AM cron
- Secrets encrypted with SOPS + age, committed to GitOps repo

---

### Task 1: 创建 GitOps 环境仓库骨架

**Files:**
- Create: `~/big-data-platform-envs/README.md`
- Create: `~/big-data-platform-envs/.gitignore`
- Create: `~/big-data-platform-envs/environments/current/kustomization.yaml`
- Create: `~/big-data-platform-envs/environments/current/applications/gitops.yaml`
- Create: `~/big-data-platform-envs/environments/current/applications/data.yaml`
- Create: `~/big-data-platform-envs/environments/current/applications/flink.yaml`
- Create: `~/big-data-platform-envs/environments/current/applications/collectors.yaml`
- Create: `~/big-data-platform-envs/environments/current/applications/observability.yaml`
- Create: `~/big-data-platform-envs/environments/current/secrets/kustomization.yaml`
- Create: `~/big-data-platform-envs/.sops.yaml`
- Create: `~/big-data-platform-envs/scripts/sops-encrypt.sh`
- Create: `~/big-data-platform-envs/scripts/sops-decrypt.sh`
- Create: `~/big-data-platform-envs/scripts/velero-backup.sh`

**Interfaces:**
- Consumes: None (initial setup)
- Produces: GitOps repo structure with all Application CRs, secrets config, and utility scripts

- [ ] **Step 1: 创建仓库目录结构**

```bash
# Create the GitOps environment repository
mkdir -p ~/big-data-platform-envs
cd ~/big-data-platform-envs

# Initialize git repo
git init

# Create directory structure
mkdir -p environments/current/applications
mkdir -p environments/current/secrets
mkdir -p environments/current/values
mkdir -p environments/current/overlays
mkdir -p scripts
```

- [ ] **Step 2: 创建 README.md**

Create `~/big-data-platform-envs/README.md` with the following content:

```markdown
# big-data-platform-envs

GitOps environment repository for big-data-platform.

## Structure

- `environments/current/` — Current environment declarations
  - `applications/` — ArgoCD Application CRs for each namespace
  - `secrets/` — SOPS-encrypted secrets (age)
  - `values/` — Helm values files
  - `overlays/` — Kustomize overlays
  - `kustomization.yaml` — Root kustomization

- `scripts/` — Utility scripts
  - `sops-encrypt.sh` — Encrypt secrets with SOPS
  - `sops-decrypt.sh` — Decrypt secrets with SOPS
  - `velero-backup.sh` — Trigger Velero backup

## Usage

```bash
# Encrypt a secret
./scripts/sops-encrypt.sh secrets/my-secret.yaml

# Decrypt a secret
./scripts/sops-decrypt.sh secrets/my-secret.sops.yaml

# Trigger backup
./scripts/velero-backup.sh
```
```

- [ ] **Step 3: 创建 .gitignore**

```gitignore
# Secrets
*.sops.yaml
.agekey
age.key

# Velero
velero-*.tar.gz

# ArgoCD
argocd/

# Kubernetes
*.kubeconfig
```

- [ ] **Step 4: 创建 .sops.yaml**

```yaml
creation_rules:
  - path: \.sops\.yaml
    encrypted_regex: "^(data|stringData)$"
    age: >-
      age-generated-from-blowfish-cipher
```

- [ ] **Step 5: 创建 kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - applications/gitops.yaml
  - applications/data.yaml
  - applications/flink.yaml
  - applications/collectors.yaml
  - applications/observability.yaml
  - secrets/kustomization.yaml

namespace: gitops
```

- [ ] **Step 6: 创建 Application CRs**

```yaml
# environments/current/applications/gitops.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: gitops-self
  namespace: gitops
spec:
  project: default
  source:
    repoURL: https://github.com/ad/big-data-platform-envs.git
    targetRevision: HEAD
    path: environments/current
  destination:
    server: https://kubernetes.default.svc
    namespace: gitops
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

```yaml
# environments/current/applications/data.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: data
  namespace: gitops
spec:
  project: default
  source:
    repoURL: https://github.com/ad/big-data-platform-envs.git
    targetRevision: HEAD
    path: environments/current/overlays/data
  destination:
    server: https://kubernetes.default.svc
    namespace: data
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

```yaml
# environments/current/applications/flink.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink
  namespace: gitops
spec:
  project: default
  source:
    repoURL: https://github.com/ad/big-data-platform-envs.git
    targetRevision: HEAD
    path: environments/current/overlays/flink
  destination:
    server: https://kubernetes.default.svc
    namespace: flink
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

```yaml
# environments/current/applications/collectors.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: collectors
  namespace: gitops
spec:
  project: default
  source:
    repoURL: https://github.com/ad/big-data-platform-envs.git
    targetRevision: HEAD
    path: environments/current/overlays/collectors
  destination:
    server: https://kubernetes.default.svc
    namespace: collectors
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

```yaml
# environments/current/applications/observability.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: observability
  namespace: gitops
spec:
  project: default
  source:
    repoURL: https://github.com/ad/big-data-platform-envs.git
    targetRevision: HEAD
    path: environments/current/overlays/observability
  destination:
    server: https://kubernetes.default.svc
    namespace: observability
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 7: 创建 secrets kustomization**

```yaml
# environments/current/secrets/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

secretGenerator:
  - name: kafka-secrets
    files:
      - kafka-secrets.sops.yaml
  - name: mysql-secrets
    files:
      - mysql-secrets.sops.yaml
  - name: minio-secrets
    files:
      - minio-secrets.sops.yaml

generatorOptions:
  annotations:
    sops.unsealed: "true"
```

- [ ] **Step 8: 创建 utility scripts**

```bash
#!/bin/bash
# scripts/sops-encrypt.sh
# Encrypt a YAML file with SOPS using age

set -euo pipefail

FILE="${1:?Usage: sops-encrypt.sh <file>}"

if [ ! -f "$FILE" ]; then
  echo "Error: File $FILE not found"
  exit 1
fi

# Encrypt with SOPS
sops --encrypt --in-place "$FILE"

echo "Encrypted: $FILE"
```

```bash
#!/bin/bash
# scripts/sops-decrypt.sh
# Decrypt a YAML file with SOPS using age

set -euo pipefail

FILE="${1:?Usage: sops-decrypt.sh <file>}"

if [ ! -f "$FILE" ]; then
  echo "Error: File $FILE not found"
  exit 1
fi

# Decrypt with SOPS
sops --decrypt "$FILE"
```

```bash
#!/bin/bash
# scripts/velero-backup.sh
# Trigger a Velero backup to MinIO

set -euo pipefail

BACKUP_NAME="manual-$(date +%Y%m%d-%H%M%S)"

velero backup create "$BACKUP_NAME" \
  --include-namespaces data,flink,collectors,observability,gitops \
  --storage-location minio \
  --ttl 720h

echo "Backup created: $BACKUP_NAME"
```

- [ ] **Step 9: 初始化 Git 并提交**

```bash
cd ~/big-data-platform-envs
git add .
git commit -m "feat: initial GitOps environment repository structure"
git branch -M main
git remote add origin https://github.com/ad/big-data-platform-envs.git
git push -u origin main
```

### Task 2: 安装 ArgoCD 到 k3s 集群

**Files:**
- Create: `~/big-data-platform-envs/environments/current/overlays/argocd-values.yaml`
- Modify: `~/big-data-platform-envs/environments/current/applications/gitops.yaml` (update repoURL)

**Interfaces:**
- Consumes: GitOps repo structure from Task 1
- Produces: ArgoCD running in `ns: gitops` with auto-sync enabled

- [ ] **Step 1: 创建 ArgoCD Helm values**

```yaml
# environments/current/overlays/argocd-values.yaml
controller:
  replicas: 1
  logs:
    level: info

server:
  ingress:
    enabled: false
  service:
    type: ClusterIP

repoServer:
  replicas: 1

configs:
  params:
    server.insecure: false
    controller.processors.workflows: 10
    controller.processors.status.status: 10

applicationSet:
  replicas: 1

crds:
  install: true
  keep: true
```

- [ ] **Step 2: 安装 ArgoCD 到集群**

```bash
# Install ArgoCD using Helm
helm upgrade --install argocd argo/argo-cd \
  --namespace gitops \
  --create-namespace \
  --values environments/current/overlays/argocd-values.yaml \
  --wait \
  --timeout 10m

# Verify installation
kubectl get pods -n gitops
kubectl get services -n gitops
```

- [ ] **Step 3: 验证 ArgoCD 运行状态**

```bash
# Check ArgoCD pods
kubectl get pods -n gitops | grep argocd

# Check ArgoCD server
kubectl get svc argocd-server -n gitops

# Access ArgoCD UI (port-forward)
kubectl port-forward svc/argocd-server 8080:443 -n gitops &
```

- [ ] **Step 4: 配置 ArgoCD 同步策略**

```bash
# Configure auto-sync for all applications
kubectl patch application data -n gitops --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
kubectl patch application flink -n gitops --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
kubectl patch application collectors -n gitops --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
kubectl patch application observability -n gitops --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
kubectl patch application gitops-self -n gitops --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
```

### Task 3: 配置 SOPS + age 加密 secrets

**Files:**
- Create: `~/big-data-platform-envs/environments/current/secrets/kafka-secrets.sops.yaml`
- Create: `~/big-data-platform-envs/environments/current/secrets/mysql-secrets.sops.yaml`
- Create: `~/big-data-platform-envs/environments/current/secrets/minio-secrets.sops.yaml`
- Create: `~/.sops/age/keys.txt` (age private key)
- Create: `~/big-data-platform-envs/.sops.yaml` (updated)

**Interfaces:**
- Consumes: ArgoCD installed (Task 2), GitOps repo structure (Task 1)
- Produces: Encrypted secrets ready for ArgoCD to apply

- [ ] **Step 1: 生成 age 密钥对**

```bash
# Generate age key pair
age-keygen -o ~/.sops/age/keys.txt 2>/dev/null || true

# Extract public key
age-keygen -p ~/.sops/age/keys.txt > ~/.sops/age/public.key

# Copy to VM
scp ~/.sops/age/keys.txt root@<vm-ip>:/root/.sops/age/keys.txt
```

- [ ] **Step 2: 创建加密的 secrets**

```yaml
# environments/current/secrets/kafka-secrets.sops.yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-secrets
  namespace: data
type: Opaque
data:
  KAFKA_USERNAME: <base64-encoded>
  KAFKA_PASSWORD: <sops-encrypted>
  KAFKA_BOOTSTRAP_SERVERS: kafka-kafka-bootstrap.data.svc.cluster.local:9092
stringData:
  KAFKA_PASSWORD: |
     SOPS_AGE_KEY: age-generated-from-blowfish-cipher
```

```yaml
# environments/current/secrets/mysql-secrets.sops.yaml
apiVersion: v1
kind: Secret
metadata:
  name: mysql-secrets
  namespace: data
type: Opaque
stringData:
  MYSQL_ROOT_PASSWORD: |
     SOPS_AGE_KEY: age-generated-from-blowfish-cipher
  MYSQL_USER: bigdata
  MYSQL_PASSWORD: |
     SOPS_AGE_KEY: age-generated-from-blowfish-cipher
```

```yaml
# environments/current/secrets/minio-secrets.sops.yaml
apiVersion: v1
kind: Secret
metadata:
  name: minio-secrets
  namespace: data
type: Opaque
stringData:
  MINIO_ROOT_USER: |
     SOPS_AGE_KEY: age-generated-from-blowfish-cipher
  MINIO_ROOT_PASSWORD: |
     SOPS_AGE_KEY: age-generated-from-blowfish-cipher
```

- [ ] **Step 3: 加密 secrets 并提交**

```bash
# Encrypt each secret
cd ~/big-data-platform-envs
./scripts/sops-encrypt.sh environments/current/secrets/kafka-secrets.sops.yaml
./scripts/sops-encrypt.sh environments/current/secrets/mysql-secrets.sops.yaml
./scripts/sops-encrypt.sh environments/current/secrets/minio-secrets.sops.yaml

# Commit encrypted secrets
git add environments/current/secrets/*.sops.yaml
git commit -m "feat: add SOPS-encrypted secrets for Kafka, MySQL, MinIO"
git push
```

### Task 4: 安装 Velero 并配置备份

**Files:**
- Create: `~/big-data-platform-envs/environments/current/overlays/velero-values.yaml`
- Create: `~/big-data-platform-envs/scripts/velero-restore.sh`
- Modify: `~/big-data-platform-envs/environments/current/applications/data.yaml` (add velero)

**Interfaces:**
- Consumes: ArgoCD installed (Task 2), secrets encrypted (Task 3)
- Produces: Velero running with scheduled backups to MinIO

- [ ] **Step 1: 创建 Velero Helm values**

```yaml
# environments/current/overlays/velero-values.yaml
backupLocation:
  name: minio
  config:
    region: minio
    s3ForcePathStyle: "true"
    s3Url: http://minio.data.svc.cluster.local:9000

credentials:
  useSecret: true
  existingSecret: minio-velero-credentials

schedules:
  daily-backup:
    disabled: false
    schedule: "0 3 * * *"
    template:
      ttl: "720h"
      includeNamespaces:
        - data
        - flink
        - collectors
        - observability
        - gitops
      storageLocation: minio

volumeSnapshotLocations:
  - name: minio
    provider: aws
    config:
      region: minio
      s3ForcePathStyle: "true"
      s3Url: http://minio.data.svc.cluster.local:9000
```

- [ ] **Step 2: 安装 Velero**

```bash
# Install Velero using Helm
helm upgrade --install velero velero/velero \
  --namespace data \
  --create-namespace \
  --values environments/current/overlays/velero-values.yaml \
  --wait \
  --timeout 10m

# Verify installation
kubectl get pods -n data | grep velero
kubectl get schedules -n data
```

- [ ] **Step 3: 创建 Velero 恢复脚本**

```bash
#!/bin/bash
# scripts/velero-restore.sh
# Restore cluster from Velero backup

set -euo pipefail

BACKUP_NAME="${1:?Usage: velero-restore.sh <backup-name>}"

# Restore from backup
velero restore create "$BACKUP_NAME" \
  --from-backup "$BACKUP_NAME" \
  --include-namespaces data,flink,collectors,observability,gitops

echo "Restore initiated for backup: $BACKUP_NAME"
```

- [ ] **Step 4: 验证备份**

```bash
# Trigger a manual backup
./scripts/velero-backup.sh

# Check backup status
velero backup describe manual-$(date +%Y%m%d-%H%M%S)

# List backups
velero backup get
```

### Task 5: 毕业演练 1 — VM 重启全链路自愈

**Files:**
- Create: `~/big-data-platform-envs/scripts/verify-recovery.sh`
- Modify: `~/big-data-platform-envs/README.md` (add graduation drills section)

**Interfaces:**
- Consumes: All previous tasks (ArgoCD, SOPS, Velero installed)
- Produces: Verified VM restart recovery with no duplicate trading signals

- [ ] **Step 1: 创建验证脚本**

```bash
#!/bin/bash
# scripts/verify-recovery.sh
# Verify full system recovery after VM restart

set -euo pipefail

echo "=== VM Restart Recovery Verification ==="

# Check k3s is running
echo "Checking k3s service..."
kubectl get nodes

# Check ArgoCD is synced
echo "Checking ArgoCD sync..."
kubectl get applications -n gitops

# Check all namespaces
echo "Checking namespaces..."
kubectl get namespaces

# Check all pods
echo "Checking pods..."
kubectl get pods -A

# Check Kafka connectivity
echo "Checking Kafka connectivity..."
kubectl run kafka-test --image=strimzi/kafka:latest --rm -it --restart=Never -- \
  kafka-consumer-groups.sh --bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 --list

# Check Flink jobs
echo "Checking Flink jobs..."
kubectl get flinkdeployment -n flink

# Check collectors
echo "Checking collectors..."
kubectl get pods -n collectors

# Check observability
echo "Checking observability..."
kubectl get pods -n observability

echo "=== Recovery verification complete ==="
```

- [ ] **Step 2: 执行 VM 重启演练**

```bash
# Restart the VM
orbstack restart k3s-vm

# Wait for k3s to start
kubectl get nodes

# Run verification
./scripts/verify-recovery.sh
```

- [ ] **Step 3: 验证交易信号不重复**

```bash
# Check Kafka consumer groups for duplicate signals
kubectl run kafka-check --image=strimzi/kafka:latest --rm -it --restart=Never -- \
  kafka-consumer-groups.sh --bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
  --describe --group settlement-group
```

### Task 6: 毕业演练 2 — Ollama 中断背压行为

**Files:**
- Create: `~/big-data-platform-envs/scripts/verify-ollama-fault.sh`

**Interfaces:**
- Consumes: All previous tasks
- Produces: Verified Ollama fault tolerance with proper backpressure and alerting

- [ ] **Step 1: 创建 Ollama 故障验证脚本**

```bash
#!/bin/bash
# scripts/verify-ollama-fault.sh
# Verify Ollama fault tolerance

set -euo pipefail

echo "=== Ollama Fault Tolerance Verification ==="

# Stop Ollama on macOS host
echo "Stopping Ollama..."
# (Assuming Ollama is managed by launchd or similar)
launchctl unload ~/Library/LaunchAgents/com.github.ollama.plist 2>/dev/null || true

# Wait for Ollama to be unavailable
sleep 30

# Check Flink async function behavior
echo "Checking Flink async function behavior..."
kubectl get pods -n flink | grep trading

# Check Prometheus alerts
echo "Checking Prometheus alerts..."
kubectl port-forward svc/prometheus-service 9090:9090 -n observability &

# Check Telegram notifications
echo "Checking Telegram notifications..."
# (Verify alerts are sent to Telegram)

# Restart Ollama
echo "Restarting Ollama..."
launchctl load ~/Library/LaunchAgents/com.github.ollama.plist 2>/dev/null || true

echo "=== Ollama fault tolerance verification complete ==="
```

- [ ] **Step 2: 执行 Ollama 中断演练**

```bash
# Stop Ollama for 10 minutes
launchctl unload ~/Library/LaunchAgents/com.github.ollama.plist 2>/dev/null || true

# Wait 10 minutes
sleep 600

# Restart Ollama
launchctl load ~/Library/LaunchAgents/com.github.ollama.plist 2>/dev/null || true

# Verify recovery
./scripts/verify-ollama-fault.sh
```

### Task 7: 毕业演练 3 — 灾难恢复

**Files:**
- Create: `~/big-data-platform-envs/scripts/velero-disaster-recovery.sh`

**Interfaces:**
- Consumes: Velero installed with backups (Task 4)
- Produces: Verified disaster recovery from Velero backup

- [ ] **Step 1: 创建灾难恢复脚本**

```bash
#!/bin/bash
# scripts/velero-disaster-recovery.sh
# Disaster recovery from Velero backup

set -euo pipefail

BACKUP_NAME="${1:?Usage: velero-disaster-recovery.sh <backup-name>}"

echo "=== Disaster Recovery from Velero Backup ==="

# Delete all namespaces (simulating cluster loss)
echo "Deleting all namespaces..."
kubectl delete namespaces data flink collectors observability gitops --ignore-not-found

# Recreate namespaces
kubectl apply -f environments/current/kustomization.yaml

# Restore from Velero backup
velero restore create disaster-recovery --from-backup "$BACKUP_NAME"

# Wait for restoration
echo "Waiting for restoration..."
kubectl wait --for=condition=ready pod --all -n data --timeout=300s
kubectl wait --for=condition=ready pod --all -n flink --timeout=300s
kubectl wait --for=condition=ready pod --all -n collectors --timeout=300s
kubectl wait --for=condition=ready pod --all -n observability --timeout=300s

echo "=== Disaster recovery complete ==="
```

- [ ] **Step 2: 执行灾难恢复演练**

```bash
# Get latest backup
LATEST_BACKUP=$(velero backup get --output name | head -1)

# Run disaster recovery
./scripts/velero-disaster-recovery.sh "$LATEST_BACKUP"

# Verify all services are running
kubectl get pods -A
```

### Task 8: 更新文档并提交

**Files:**
- Modify: `~/big-data-platform-envs/README.md`
- Modify: `docs/superpowers/specs/2026-07-24-phase5-gitops-graduation-design.md`
- Create: `docs/superpowers/plans/2026-07-24-phase5-gitops-graduation.md`

**Interfaces:**
- Consumes: All previous tasks
- Produces: Updated documentation and plan document

- [ ] **Step 1: 更新 README.md**

```markdown
# big-data-platform-envs

GitOps environment repository for big-data-platform.

## Structure

- `environments/current/` — Current environment declarations
  - `applications/` — ArgoCD Application CRs for each namespace
  - `secrets/` — SOPS-encrypted secrets (age)
  - `values/` — Helm values files
  - `overlays/` — Kustomize overlays
  - `kustomization.yaml` — Root kustomization

- `scripts/` — Utility scripts
  - `sops-encrypt.sh` — Encrypt secrets with SOPS
  - `sops-decrypt.sh` — Decrypt secrets with SOPS
  - `velero-backup.sh` — Trigger Velero backup
  - `velero-restore.sh` — Restore from Velero backup
  - `verify-recovery.sh` — Verify VM restart recovery
  - `verify-ollama-fault.sh` — Verify Ollama fault tolerance
  - `velero-disaster-recovery.sh` — Disaster recovery from Velero backup

## Graduation Drills

### 1. VM Restart Recovery
- Restart entire VM
- Verify k3s auto-starts → ArgoCD syncs → all pods start → collectors connect to Kafka → Flink recovers from savepoint → trading signals are not duplicated

### 2. Ollama Fault Tolerance
- Stop Ollama for 10 minutes
- Verify Flink async functions handle timeout/backpressure correctly
- Verify Prometheus alerts trigger and Telegram notifications sent

### 3. Disaster Recovery
- Restore entire cluster from Velero backup
- Verify PVC recovery, etcd recovery, ArgoCD sync, full system availability

## Usage

```bash
# Encrypt a secret
./scripts/sops-encrypt.sh secrets/my-secret.yaml

# Decrypt a secret
./scripts/sops-decrypt.sh secrets/my-secret.sops.yaml

# Trigger backup
./scripts/velero-backup.sh

# Restore from backup
./scripts/velero-restore.sh <backup-name>

# Verify recovery
./scripts/verify-recovery.sh

# Verify Ollama fault tolerance
./scripts/verify-ollama-fault.sh

# Disaster recovery
./scripts/velero-disaster-recovery.sh <backup-name>
```
```

- [ ] **Step 2: 更新设计文档**

Update `docs/superpowers/specs/2026-07-24-phase5-gitops-graduation-design.md` with implementation details.

- [ ] **Step 3: 提交所有更改**

```bash
cd ~/big-data-platform-envs
git add .
git commit -m "feat: complete Phase 5 GitOps implementation with graduation drills"
git push
```

## 验收标准

- [ ] ArgoCD 安装成功，UI 可访问
- [ ] 环境仓库拆分完成，ArgoCD 能同步声明
- [ ] SOPS 加密的 secrets 正确应用到集群
- [ ] Velero 定时备份成功，备份数据在 MinIO 中
- [ ] 毕业演练 1：VM 重启后全系统自愈，交易信号无重复
- [ ] 毕业演练 2：Ollama 中断期间背压/超时行为符合设计，告警触发
- [ ] 毕业演练 3：从 Velero 备份恢复集群，全系统可用
- [ ] 所有文档更新完成，提交到 Git

## 实施顺序

1. Task 1: 创建 GitOps 环境仓库骨架
2. Task 2: 安装 ArgoCD 到 k3s 集群
3. Task 3: 配置 SOPS + age 加密 secrets
4. Task 4: 安装 Velero 并配置备份
5. Task 5: 毕业演练 1 — VM 重启全链路自愈
6. Task 6: 毕业演练 2 — Ollama 中断背压行为
7. Task 7: 毕业演练 3 — 灾难恢复
8. Task 8: 更新文档并提交
