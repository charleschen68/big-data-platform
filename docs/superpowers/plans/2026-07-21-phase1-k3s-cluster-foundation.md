# Phase 1：集群地基 + 可观测性 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Mac 宿主机上起一台 OrbStack Linux VM 并装单节点 k3s，把 kube-prometheus-stack 和 MinIO 带进集群，打通 Kafka consumer lag / pod 重启 / 磁盘水位三类告警到 Telegram；同时把 docker-compose.yml 迁入 `infra/compose/`。全程 compose 栈不停机。

**Architecture:** OrbStack VM（k3s-node，Ubuntu 24.04，28GB/8vCPU）跑单节点 k3s；宿主机 Mac 通过合并后的 kubeconfig（context `k3s-node`）用 `kubectl`/`helm` 驱动集群。可观测性栈与 MinIO 用官方 Helm chart 装进 `observability`/`data` namespace。Kafka 仍在 compose 里跑，集群内 Prometheus 靠 OrbStack 内置跨容器域名（`*.orb.local`）远程抓取 `kafka-exporter` 指标。Grafana/Alertmanager 用 `kubectl port-forward` 访问，不引入 ingress。

**Tech Stack:** OrbStack 2.2.1、Ubuntu 24.04、k3s v1.36.2+k3s1、Helm 4.2.3、kube-prometheus-stack chart 87.18.0（Alertmanager/Prometheus/Grafana）、minio/minio chart 5.4.0、danielqsj/kafka-exporter:v1.9.0。

## Global Constraints

- OrbStack VM 固定名称 `k3s-node`，规格 `--memory 28G --cpus 8`，发行版 `ubuntu:24.04`。
- k3s 版本锁定 `v1.36.2+k3s1`（`INSTALL_K3S_VERSION` 环境变量传给官方安装脚本），保留默认组件（Traefik、ServiceLB），不禁用。
- kubectl context 固定命名 `k3s-node`；现存的 `orbstack` context（指向未使用的 OrbStack 内置 K8s，127.0.0.1:26443）不要删除，也不要用。
- Helm chart 版本锁定：`prometheus-community/kube-prometheus-stack` = `87.18.0`；`minio/minio` = `5.4.0`。
- kafka-exporter 镜像锁定 `danielqsj/kafka-exporter:v1.9.0`。
- Docker Compose 项目名必须保持 `big-data-platform`（迁移文件位置后用 `name:` 字段显式锁定），全程不允许出现容器重建或停机；每个改动 compose 文件的任务都要用容器 ID 列表比对验证零停机。
- 迁移后所有 `docker compose` 命令固定加 `-f infra/compose/docker-compose.yml`。
- Telegram bot token / chat id 一律用 `kubectl create secret` 命令行创建，不写入 git、不写入任何 values 文件。
- 本阶段范围：仓库重构（`infra/`）+ VM/k3s + 可观测性栈 + MinIO + Telegram 告警。**不做**：Flink Operator、任何 compose 服务迁入集群、ingress/域名对外访问、ArgoCD。
- 提交信息用中文短句，风格同 `git log`。
- 所有 `git`、`docker compose`（迁移后）、`mvn` 命令在仓库根目录执行，除非步骤里明确写了别的目录。

---

### Task 1: 仓库重构 — docker-compose.yml 迁入 infra/compose/（零停机）

**Files:**
- Create: `infra/compose/`、`infra/k8s/observability/`、`infra/k8s/data/`、`infra/vm/` 目录
- Modify → Move: `docker-compose.yml` → `infra/compose/docker-compose.yml`
- Move（非 git 操作）: `.env` → `infra/compose/.env`
- Modify → Move: `.env.example` → `infra/compose/.env.example`

**Interfaces:**
- Produces: 之后所有 compose 命令固定为 `docker compose -f infra/compose/docker-compose.yml <cmd>`；project 名固定 `big-data-platform`（Task 5 新增 kafka-exporter 服务、后续 Phase 都依赖这个固定路径和项目名）

- [ ] **Step 1: 创建目录结构**

```bash
mkdir -p infra/compose infra/k8s/observability infra/k8s/data infra/vm
```

- [ ] **Step 2: 给 docker-compose.yml 显式锁定 project 名**

将文件开头：

```yaml
version: '3.8'

services:
```

改为：

```yaml
name: big-data-platform
version: '3.8'

services:
```

（不加 `name:` 字段的话，文件挪到 `infra/compose/` 后 compose 会用目录名 `compose` 当 project 名，导致所有容器被当成新项目处理，可能重建/丢状态。）

- [ ] **Step 3: 验证 project 名不变，记录当前容器 ID（用于零停机比对）**

```bash
docker compose config --format json | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])"
docker compose ps -q | sort > /tmp/phase1-ids-before-move.txt
cat /tmp/phase1-ids-before-move.txt | wc -l
```

预期：第一条输出 `big-data-platform`；容器 ID 数量与当前 `docker compose ps` 显示的运行服务数一致（非 0）。

- [ ] **Step 4: 把 compose 文件和 env 文件移到 infra/compose/**

```bash
git mv docker-compose.yml infra/compose/docker-compose.yml
mv .env infra/compose/.env
git mv .env.example infra/compose/.env.example
```

- [ ] **Step 5: 修正相对路径（迁移前是相对仓库根目录，迁移后要相对 infra/compose/ 再往上两级）**

```bash
sed -i '' \
  -e 's#\./data/#../../data/#g' \
  -e 's#\./libs/#../../libs/#g' \
  -e 's#\./flink-1\.18\.1/#../../flink-1.18.1/#g' \
  -e 's#\./flink-1\.18\.1:#../../flink-1.18.1:#g' \
  -e 's#\./flink-data/#../../flink-data/#g' \
  -e 's#\./prometheus/#../../prometheus/#g' \
  infra/compose/docker-compose.yml
grep -c '\./data\|\./libs\|\./flink-1.18.1\|\./flink-data\|\./prometheus' infra/compose/docker-compose.yml
```

预期：最后一条 grep 计数为 `0`（所有相对路径都已改成 `../../` 前缀，不再有裸的 `./` 前缀残留）。

- [ ] **Step 6: 验证零停机 —— project 名、挂载路径、运行中容器 ID 三项都不变**

```bash
docker compose -f infra/compose/docker-compose.yml config --format json | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])"
docker compose -f infra/compose/docker-compose.yml config --format json | python3 -c "
import json,sys
c = json.load(sys.stdin)
print(c['services']['mysql']['volumes'][0]['source'])
print(c['services']['minio']['volumes'][0]['source'])
"
docker compose -f infra/compose/docker-compose.yml ps -q | sort > /tmp/phase1-ids-after-move.txt
diff /tmp/phase1-ids-before-move.txt /tmp/phase1-ids-after-move.txt && echo "容器 ID 完全一致，零停机确认"
```

预期：project 名仍是 `big-data-platform`；两条 volume 路径都是 `/Users/ad/big-data-platform/data/mysql` 和 `/Users/ad/big-data-platform/data/minio`（绝对路径与迁移前相同）；`diff` 无输出，打印 "容器 ID 完全一致，零停机确认"。任何一项不符，先排查再继续，不得跳过。

- [ ] **Step 7: Commit**

```bash
git add infra/compose/docker-compose.yml infra/compose/.env.example
git commit -m "仓库重构: docker-compose 迁入 infra/compose, 锁定 project 名避免重建"
```

---

### Task 2: OrbStack VM 创建 + k3s 安装 + kubeconfig 合并

**Files:**
- Create: `infra/vm/create-vm.sh`
- Create: `infra/vm/install-k3s.sh`
- Create: `infra/vm/merge-kubeconfig.sh`

**Interfaces:**
- Consumes: 无
- Produces: 可用的 `kubectl --context k3s-node`（后续所有任务默认这个 context 已经是当前 context）；VM 名 `k3s-node`（Task 5 的 OrbStack 域名解析验证依赖这个名字）

- [ ] **Step 1: 写 VM 创建脚本**

创建 `infra/vm/create-vm.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

VM_NAME="k3s-node"

if orb list 2>/dev/null | grep -q "^${VM_NAME}\b"; then
  echo "VM ${VM_NAME} 已存在，跳过创建"
else
  orb create --memory 28G --cpus 8 ubuntu:24.04 "${VM_NAME}"
fi

orb -m "${VM_NAME}" -u root true
echo "VM ${VM_NAME} 就绪"
```

- [ ] **Step 2: 运行并验证**

```bash
chmod +x infra/vm/create-vm.sh
./infra/vm/create-vm.sh
orb list
```

预期：最后一条命令输出包含一行 `k3s-node`，状态为 running。

- [ ] **Step 3: 写 k3s 安装脚本**

创建 `infra/vm/install-k3s.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

VM_NAME="k3s-node"
K3S_VERSION="v1.36.2+k3s1"

orb -m "${VM_NAME}" -u root sh -c "curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=${K3S_VERSION} INSTALL_K3S_EXEC='server --tls-san ${VM_NAME}.orb.local' sh -"
# --tls-san 必须在装机时给：k3s 自动生成的 server 证书默认 SAN 不含 OrbStack 的 .orb.local 域名，
# 而 merge-kubeconfig.sh 会把 kubeconfig 的 server 地址改写成这个域名，装完不给会导致宿主机侧
# kubectl 报 x509 SAN 不匹配（已实测踩过一次坑）。

echo "等待节点 Ready..."
for i in $(seq 1 30); do
  if orb -m "${VM_NAME}" -u root k3s kubectl get nodes --no-headers 2>/dev/null | grep -q " Ready "; then
    echo "节点已 Ready"
    break
  fi
  sleep 2
done

orb -m "${VM_NAME}" -u root k3s kubectl get nodes
orb -m "${VM_NAME}" -u root k3s --version
```

- [ ] **Step 4: 运行并验证**

```bash
chmod +x infra/vm/install-k3s.sh
./infra/vm/install-k3s.sh
```

预期：`kubectl get nodes` 那行输出里节点状态是 `Ready`；`k3s --version` 输出包含 `v1.36.2+k3s1`。

- [ ] **Step 5: 写 kubeconfig 合并脚本**

创建 `infra/vm/merge-kubeconfig.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

VM_NAME="k3s-node"
CONTEXT_NAME="k3s-node"
TMP_KUBECONFIG="$(mktemp)"

cp "${HOME}/.kube/config" "${HOME}/.kube/config.bak-$(date +%Y%m%d%H%M%S)"

# 清掉宿主机 kubeconfig 里同名的旧条目：kubectl --flatten 合并多个 KUBECONFIG 文件时按
# "先列出的文件优先" 处理同名条目，如果不清理，k3s 重装后轮换的新 CA 会被这里残留的旧 CA
# 静默盖掉（不会报错，之后连接才会因证书对不上而失败）。
kubectl config delete-context "${CONTEXT_NAME}" 2>/dev/null || true
kubectl config delete-cluster "${CONTEXT_NAME}" 2>/dev/null || true
kubectl config unset "users.${CONTEXT_NAME}" 2>/dev/null || true

orb -m "${VM_NAME}" -u root cat /etc/rancher/k3s/k3s.yaml > "${TMP_KUBECONFIG}"

sed -i '' \
  -e "s/name: default/name: ${CONTEXT_NAME}/g" \
  -e "s/cluster: default/cluster: ${CONTEXT_NAME}/g" \
  -e "s/user: default/user: ${CONTEXT_NAME}/g" \
  -e "s/current-context: default/current-context: ${CONTEXT_NAME}/g" \
  -e "s#server: https://127.0.0.1:6443#server: https://${VM_NAME}.orb.local:6443#" \
  "${TMP_KUBECONFIG}"

KUBECONFIG="${HOME}/.kube/config:${TMP_KUBECONFIG}" kubectl config view --flatten > "${TMP_KUBECONFIG}.merged"
mv "${TMP_KUBECONFIG}.merged" "${HOME}/.kube/config"
rm -f "${TMP_KUBECONFIG}"

kubectl config use-context "${CONTEXT_NAME}"
echo "已切换到 context: ${CONTEXT_NAME}"
kubectl get nodes
```

- [ ] **Step 6: 运行并验证**

```bash
chmod +x infra/vm/merge-kubeconfig.sh
./infra/vm/merge-kubeconfig.sh
kubectl config current-context
kubectl get nodes
```

预期：`current-context` 输出 `k3s-node`；`kubectl get nodes` 从宿主机直接查询成功，节点 `Ready`（证明宿主机能通过 `k3s-node.orb.local:6443` 直连集群，不需要再 `orb -m` 进 VM）。

若 `kubectl get nodes` 报连接失败：先 `ping -c1 k3s-node.orb.local` 确认域名解析是否正常（VM 域名按 OrbStack 文档是零配置直接可用，和下面 Task 5 涉及的容器域名是两回事）；再确认 `orb list` 里 `k3s-node` 状态是 running；仍不通就重启 OrbStack App 后重试。

- [ ] **Step 7: Commit**

```bash
git add infra/vm/create-vm.sh infra/vm/install-k3s.sh infra/vm/merge-kubeconfig.sh
git commit -m "添加 OrbStack VM 创建与 k3s 安装脚本 (infra/vm)"
```

---

### Task 3: 创建 namespace

**Files:**
- Create: `infra/k8s/namespaces.yaml`

**Interfaces:**
- Produces: namespace `observability`、`data`、`flink`、`collectors`、`gitops`（Task 4/6 用 `observability`，Task 7 用 `data`；`flink`/`collectors`/`gitops` 是 Phase 2+ 预留的空壳）

- [ ] **Step 1: 写 namespace 清单**

创建 `infra/k8s/namespaces.yaml`：

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: observability
---
apiVersion: v1
kind: Namespace
metadata:
  name: data
---
apiVersion: v1
kind: Namespace
metadata:
  name: flink
---
apiVersion: v1
kind: Namespace
metadata:
  name: collectors
---
apiVersion: v1
kind: Namespace
metadata:
  name: gitops
```

- [ ] **Step 2: 应用并验证**

```bash
kubectl apply -f infra/k8s/namespaces.yaml
kubectl get ns observability data flink collectors gitops
```

预期：五个 namespace 都存在，状态 `Active`。

- [ ] **Step 3: Commit**

```bash
git add infra/k8s/namespaces.yaml
git commit -m "创建 k3s namespace: observability/data/flink/collectors/gitops"
```

---

### Task 4: kube-prometheus-stack 接入集群

**Files:**
- Create: `infra/k8s/observability/kube-prometheus-stack-values.yaml`

**Interfaces:**
- Consumes: namespace `observability`（Task 3）
- Produces: Helm release `kube-prometheus-stack`（Task 5/6 会用 `helm upgrade` 修改同一个 release 的 values 文件）；Service `kube-prometheus-stack-grafana`、`kube-prometheus-stack-prometheus`、`kube-prometheus-stack-alertmanager`（Task 6 用后两个）

- [ ] **Step 1: 添加 Helm 仓库**

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

- [ ] **Step 2: 写初始 values 文件（先只给 Prometheus/Alertmanager 配持久化存储，不含告警/抓取配置）**

创建 `infra/k8s/observability/kube-prometheus-stack-values.yaml`：

```yaml
prometheus:
  prometheusSpec:
    storageSpec:
      volumeClaimTemplate:
        spec:
          storageClassName: local-path
          resources:
            requests:
              storage: 10Gi

alertmanager:
  alertmanagerSpec:
    storage:
      volumeClaimTemplate:
        spec:
          storageClassName: local-path
          resources:
            requests:
              storage: 2Gi
```

- [ ] **Step 3: 安装**

```bash
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --version 87.18.0 \
  --namespace observability \
  -f infra/k8s/observability/kube-prometheus-stack-values.yaml
```

- [ ] **Step 4: 验证 pod 全部就绪**

```bash
kubectl get pods -n observability
```

预期：所有 pod 状态 `Running`（Prometheus/Alertmanager/Grafana/kube-state-metrics/node-exporter/operator），无 `CrashLoopBackOff`/`Pending`。若有 pod 卡在 `Pending`，用 `kubectl describe pod <name> -n observability` 查看是否是 PVC 绑定失败（`local-path` provisioner 未就绪），排查后再继续。

- [ ] **Step 5: 验证 Grafana 可访问**

```bash
kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n observability &
PF_PID=$!
sleep 3
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:3000/login
kill $PF_PID
```

预期：输出 `200`。（此时也可以手动在浏览器打开 http://localhost:3000 用默认账号 admin / prom-operator 登录看一眼默认仪表盘，非必需但建议。）

- [ ] **Step 6: Commit**

```bash
git add infra/k8s/observability/kube-prometheus-stack-values.yaml
git commit -m "kube-prometheus-stack 接入集群 (observability namespace)"
```

---

### Task 5: kafka-exporter 接入 compose，集群内 Prometheus 远程抓取

**Files:**
- Modify: `infra/compose/docker-compose.yml`
- Modify: `infra/k8s/observability/kube-prometheus-stack-values.yaml`

**Interfaces:**
- Consumes: Task 1 的 `infra/compose/docker-compose.yml`（compose 网络 `big-data-network`、`kafka:29092`）；Task 4 的 Helm release
- Produces: compose 服务 `kafka-exporter`（监听宿主机 `9308`）；Prometheus 里名为 `kafka-exporter` 的 scrape job

- [ ] **Step 1: 给 VM 装 curl（后面几步要从 VM 内部发 HTTP 请求验证域名解析）**

```bash
orb -m k3s-node -u root sh -c 'command -v curl || (apt-get update && apt-get install -y curl)'
```

- [ ] **Step 2: 在 compose 里新增 kafka-exporter 服务**

在 `infra/compose/docker-compose.yml` 的 `services:` 下追加（放在 `kafka` 服务定义之后即可）：

```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:v1.9.0
    container_name: kafka-exporter
    command: ["--kafka.server=kafka:29092"]
    networks:
      - big-data-network
    ports:
      - "9308:9308"
    depends_on:
      - kafka
```

- [ ] **Step 3: 启动并验证本机可访问**

```bash
docker compose -f infra/compose/docker-compose.yml up -d kafka-exporter
sleep 3
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9308/metrics
```

预期：输出 `200`。

- [ ] **Step 4: 验证 k3s VM 内能通过 OrbStack 域名访问它**

```bash
orb -m k3s-node -u root curl -s -o /dev/null -w '%{http_code}\n' http://kafka-exporter.orb.local:9308/metrics
```

**情况 A（预期路径）**：输出 `200` → 继续 Step 5，scrape target 用 `kafka-exporter.orb.local:9308`。

**情况 B（域名不可达的兜底方案）**：命令超时或非 200 → 改用宿主机域名代替容器域名验证：

```bash
orb -m k3s-node -u root curl -s -o /dev/null -w '%{http_code}\n' http://host.orb.internal:9308/metrics
```

若这条输出 `200`，Step 5 的 scrape target 改用 `host.orb.internal:9308`；若两条都失败，先检查 OrbStack 设置里「Settings → Network → Allow access to container domains & IPs」是否开启（`kafka-exporter.orb.local` 是容器域名，依赖这个开关，和 Task 2 里的 VM 域名是两回事）；开启后重试，仍不通就记录下 VM 内 `cat /etc/resolv.conf` 的内容再继续排查，不得跳过验证直接硬编 IP。

- [ ] **Step 5: 给 Prometheus 加 scrape 配置**

在 `infra/k8s/observability/kube-prometheus-stack-values.yaml` 的 `prometheus:` 块下追加 `prometheusSpec.additionalScrapeConfigs`（若 Step 4 落到情况 B，把下面的 `kafka-exporter.orb.local` 换成 `host.orb.internal`）：

```yaml
prometheus:
  prometheusSpec:
    storageSpec:
      volumeClaimTemplate:
        spec:
          storageClassName: local-path
          resources:
            requests:
              storage: 10Gi
    additionalScrapeConfigs:
      - job_name: 'kafka-exporter'
        static_configs:
          - targets: ['kafka-exporter.orb.local:9308']
```

- [ ] **Step 6: helm upgrade 并验证 target UP**

```bash
helm upgrade kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --version 87.18.0 \
  --namespace observability \
  -f infra/k8s/observability/kube-prometheus-stack-values.yaml

kubectl port-forward svc/kube-prometheus-stack-prometheus 9090:9090 -n observability &
PF_PID=$!
sleep 3
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import json,sys
d = json.load(sys.stdin)
for t in d['data']['activeTargets']:
    if t['labels'].get('job') == 'kafka-exporter':
        print(t['health'])
"
kill $PF_PID
```

预期：输出 `up`。

- [ ] **Step 7: Commit**

```bash
git add infra/compose/docker-compose.yml infra/k8s/observability/kube-prometheus-stack-values.yaml
git commit -m "kafka-exporter 接入 compose, 集群 Prometheus 远程抓取其指标"
```

---

### Task 6: Alertmanager 接入 Telegram + Kafka lag 告警规则

**Files:**
- Modify: `infra/k8s/observability/kube-prometheus-stack-values.yaml`

**Interfaces:**
- Consumes: Task 4 的 Helm release；k8s Secret `telegram-bot`（本任务创建，不提交 git）
- Produces: Alertmanager receiver `telegram`（默认路由接收所有告警，除了保留路由到 `null` 的 `Watchdog` 心跳告警）；`PrometheusRule` `KafkaConsumerGroupLagHigh`

- [ ] **Step 1: 创建 Telegram bot，获取 token 和 chat id**

1. 在 Telegram 里找 `@BotFather`，发 `/newbot`，按提示起名字，拿到形如 `123456:ABC-DEF...` 的 bot token。
2. 跟这个新 bot 私聊，随便发一条消息（比如 "hi"）。
3. 浏览器打开 `https://api.telegram.org/bot<上面的token>/getUpdates`（把 `<上面的token>` 换成真实 token），在返回的 JSON 里找 `"chat":{"id":<一串数字>,...}`，这串数字就是 chat id。

- [ ] **Step 2: 创建 k8s Secret（不进 git）**

```bash
export TELEGRAM_BOT_TOKEN="<Step 1 拿到的 token>"
export TELEGRAM_CHAT_ID="<Step 1 拿到的 chat id>"

kubectl create secret generic telegram-bot \
  --namespace observability \
  --from-literal=token="${TELEGRAM_BOT_TOKEN}" \
  --from-literal=chat-id="${TELEGRAM_CHAT_ID}"

kubectl get secret telegram-bot -n observability
```

预期：最后一条命令显示 secret 存在，`DATA` 列为 `2`。

- [ ] **Step 3: 扩充 values 文件，加 Alertmanager 配置和告警规则**

在 `infra/k8s/observability/kube-prometheus-stack-values.yaml` 里追加 `alertmanager:` 和 `additionalPrometheusRulesMap:` 两个顶层块（和已有的 `prometheus:` 平级）：

```yaml
alertmanager:
  alertmanagerSpec:
    storage:
      volumeClaimTemplate:
        spec:
          storageClassName: local-path
          resources:
            requests:
              storage: 2Gi
    secrets:
      - telegram-bot
  config:
    global:
      resolve_timeout: 5m
    inhibit_rules:
      - source_matchers:
          - 'severity = critical'
        target_matchers:
          - 'severity =~ warning|info'
        equal:
          - 'namespace'
          - 'alertname'
      - source_matchers:
          - 'severity = warning'
        target_matchers:
          - 'severity = info'
        equal:
          - 'namespace'
          - 'alertname'
      - source_matchers:
          - 'alertname = InfoInhibitor'
        target_matchers:
          - 'severity = info'
        equal:
          - 'namespace'
      - target_matchers:
          - 'alertname = InfoInhibitor'
    route:
      group_by: ['namespace']
      group_wait: 30s
      group_interval: 5m
      repeat_interval: 12h
      receiver: 'telegram'
      routes:
      - receiver: 'null'
        matchers:
          - alertname = "Watchdog"
    receivers:
    - name: 'null'
    - name: 'telegram'
      telegram_configs:
      - bot_token_file: /etc/alertmanager/secrets/telegram-bot/token
        chat_id_file: /etc/alertmanager/secrets/telegram-bot/chat-id
    templates:
    - '/etc/alertmanager/config/*.tmpl'

additionalPrometheusRulesMap:
  kafka-lag-rules:
    groups:
      - name: kafka.rules
        rules:
          - alert: KafkaConsumerGroupLagHigh
            expr: kafka_consumergroup_lag > 1000
            for: 10m
            labels:
              severity: warning
            annotations:
              summary: "Kafka consumer group {{ $labels.consumergroup }} lag 过高"
              description: "topic={{ $labels.topic }} partition={{ $labels.partition }} lag={{ $value }}，已持续超过 10 分钟"
```

（`route.receiver: 'telegram'` 是默认接收者，意味着 chart 自带的 `KubePodCrashLooping`、`NodeFilesystemAlmostOutOfSpace` 等默认规则也会自动路由到 Telegram，不用单独配置；只有 `Watchdog` 心跳告警保留路由到 `null`，否则每几分钟就会收到一条无意义消息。`kafka_consumergroup_lag > 1000` 持续 10 分钟 是起始阈值，后续可以结合实际消费速率调整。）

- [ ] **Step 4: helm upgrade**

```bash
helm upgrade kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --version 87.18.0 \
  --namespace observability \
  -f infra/k8s/observability/kube-prometheus-stack-values.yaml

kubectl rollout status statefulset/alertmanager-kube-prometheus-stack-alertmanager -n observability --timeout=120s
kubectl get pods -n observability -l app.kubernetes.io/name=alertmanager
```

预期：`rollout status` 显示成功；pod 状态 `Running`，`READY 2/2`。

- [ ] **Step 5: 验证 Kafka lag 告警规则已加载**

```bash
kubectl port-forward svc/kube-prometheus-stack-prometheus 9090:9090 -n observability &
PF_PID=$!
sleep 3
curl -s http://localhost:9090/api/v1/rules | python3 -c "
import json,sys
d = json.load(sys.stdin)
names = [r['name'] for g in d['data']['groups'] for r in g['rules']]
print('KafkaConsumerGroupLagHigh' in names)
"
kill $PF_PID
```

预期：输出 `True`。

- [ ] **Step 6: 手动注入一条测试告警，验证 Telegram 能收到**

```bash
kubectl port-forward svc/kube-prometheus-stack-alertmanager 9093:9093 -n observability &
PF_PID=$!
sleep 3
curl -s -H "Content-Type: application/json" -X POST http://localhost:9093/api/v2/alerts -d '[
  {
    "labels": {"alertname": "Phase1TelegramTest", "severity": "warning"},
    "annotations": {"summary": "Phase 1 手动测试告警，收到请忽略"}
  }
]'
kill $PF_PID
```

预期：几秒到 `group_wait`（30 秒）内，配置的 Telegram chat 收到一条包含 `Phase1TelegramTest` 的消息。**这是本阶段总验收标准之一，必须实际在 Telegram 里看到消息才算通过，不能只看 API 返回 200。**

- [ ] **Step 7: Commit**

```bash
git add infra/k8s/observability/kube-prometheus-stack-values.yaml
git commit -m "Alertmanager 接入 Telegram, 添加 Kafka consumer lag 告警规则"
```

---

### Task 7: MinIO 官方 Helm chart 接入集群

**Files:**
- Create: `infra/k8s/data/minio-values.yaml`

**Interfaces:**
- Consumes: namespace `data`（Task 3）
- Produces: Helm release `minio`（standalone，供 Phase 3 迁移 Flink checkpoint 时复用；本阶段不接入任何数据）

- [ ] **Step 1: 添加 Helm 仓库**

```bash
helm repo add minio https://charts.min.io/
helm repo update
```

- [ ] **Step 2: 写 values 文件**

创建 `infra/k8s/data/minio-values.yaml`：

```yaml
mode: standalone
replicas: 1

rootUser: minioadmin
rootPassword: minioadmin

persistence:
  enabled: true
  storageClass: local-path
  size: 20Gi

resources:
  requests:
    memory: 512Mi
    cpu: 250m
  limits:
    memory: 1Gi
    cpu: 500m
```

（`minioadmin`/`minioadmin` 与 compose 里现有 MinIO 凭证同一约定——仅限本地学习环境；这个集群内实例本阶段不接入任何真实数据，Phase 3 迁移 Flink checkpoint 时再重新评估凭证策略。）

- [ ] **Step 3: 安装**

```bash
helm install minio minio/minio \
  --version 5.4.0 \
  --namespace data \
  -f infra/k8s/data/minio-values.yaml
```

- [ ] **Step 4: 验证**

```bash
kubectl get pods -n data
kubectl get pvc -n data
```

预期：pod 状态 `Running`；PVC 状态 `Bound`。

- [ ] **Step 5: Commit**

```bash
git add infra/k8s/data/minio-values.yaml
git commit -m "MinIO 官方 Helm chart 接入集群 (data namespace)"
```

---

### Task 8: 全量验收 + 更新总体设计文档

**Files:**
- Modify: `docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md`

- [ ] **Step 1: 集群整体健康检查**

```bash
kubectl get pods -A
```

预期：所有 pod 处于 `Running` 或 `Completed`，没有 `CrashLoopBackOff`/`Error`/`Pending`（长期卡住的）。

- [ ] **Step 2: 确认 compose 栈全程未受影响**

```bash
docker compose -f infra/compose/docker-compose.yml ps -q | sort > /tmp/phase1-ids-final.txt
comm -23 /tmp/phase1-ids-before-move.txt /tmp/phase1-ids-final.txt > /tmp/phase1-ids-missing.txt
cat /tmp/phase1-ids-missing.txt
test ! -s /tmp/phase1-ids-missing.txt && echo "compose 容器从 Task 1 到现在零重建"
docker compose -f infra/compose/docker-compose.yml ps
```

预期：`comm -23`（"只在 before 里、不在 final 里"的 ID，即被重建或消失的容器）结果为空，打印 "compose 容器从 Task 1 到现在零重建"（Task 5 新增的 `kafka-exporter` 只会出现在 final 列表里，不影响这个判断，因为它不在 before 里参与比较）；`ps` 显示 Phase 0 遗留服务 + 新增的 `kafka-exporter` 全部 `Up`。

- [ ] **Step 3: Grafana 仪表盘人工确认**

```bash
kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n observability
```

在浏览器打开 http://localhost:3000（admin / prom-operator），确认默认的 Kubernetes / Node 仪表盘有数据。确认完 `Ctrl+C` 停掉 port-forward。

- [ ] **Step 4: 更新总体设计文档的 Phase 1 状态**

在 `docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md` 里，把：

```markdown
### Phase 1 — 集群地基 + 可观测性（学：Helm、CRD、告警）
OrbStack VM + k3s + kube-prometheus-stack + MinIO 进集群。首批告警规则：Kafka consumer lag 持续增长、pod 重启、磁盘水位，经 Telegram/飞书 webhook 推送到手机。
✓ 验收：手机能收到测试告警。
```

改为（`<commit>` 换成 Task 8 Step 5 实际生成的 commit hash 前 7 位）：

```markdown
### Phase 1 — 集群地基 + 可观测性（学：Helm、CRD、告警）
~~OrbStack VM + k3s + kube-prometheus-stack + MinIO 进集群。首批告警规则：Kafka consumer lag 持续增长、pod 重启、磁盘水位，经 Telegram 推送到手机。~~ ✓ 已完成（详见 `docs/superpowers/specs/2026-07-21-phase1-k3s-cluster-foundation-design.md`，实施提交见 `<commit>`）
✓ 验收：手机能收到测试告警——已在 Task 6 Step 6 验证通过。
```

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-07-16-k3s-production-learning-design.md
git commit -m "Phase 1 完成标注: 集群地基与可观测性上线"
```

然后回填 Step 4 里的 `<commit>` 占位（用 `git commit --amend` 或直接再提交一次 `docs:` 修正都行，不强制哪种方式，但必须让文档里最终留下的是真实 commit hash，不是占位符）。
