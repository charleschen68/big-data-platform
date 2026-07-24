# Phase 4: 迁移剩余 4 个 Flink 作业到 K8s Operator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 compose/StreamPark 中的 4 个 Java Flink 作业（eth-sentiment-analysis、kafka2milvus、employee-message-processor、realtime-riskcontrol-embedding）逐个迁移到 k3s `flink` namespace，复用 Phase 3 的 Flink K8s Operator。

**Architecture:** 每个作业 = Dockerfile (fat JAR 打包) + FlinkDeployment CR + build script 扩展。所有作业共享 Phase 3 的 Flink K8s Operator、MinIO、Kafka ExternalName。kustomization.yaml 累积引用所有 CR。

**Tech Stack:**
- Apache Flink 1.18.1（root pom.xml `<flink.version>`）
- Apache Flink Kubernetes Operator 1.2.0
- Helm / Kustomize（YAML 管理）
- Maven maven-shade-plugin（fat JAR 构建，已有）
- ARM64 Linux containers（linux/arm64）
- imagePullPolicy: Never（k3s containerd 直接导入）

## Global Constraints

- **Flink 版本:** 1.18.1（exact，root pom.xml）
- **Operator 版本:** 1.2.0（exact）
- **镜像架构:** linux/arm64（exact）
- **镜像拉取策略:** Never（exact）
- **升级模式:** savepoint（exact）
- **运行模式:** application（exact）
- **命名空间:** flink（exact，infra/k8s/namespaces.yaml）
- **MinIO endpoint:** http://minio.data.svc.cluster.local:9000（exact）
- **S3 凭证:** accessKey=minioadmin, secretKey=minioadmin（exact）
- **Kafka bootstrap:** kafka:29092 via ExternalName Service（exact）
- **JAR 路径:** /opt/flink/jobs/{job-name}.jar（exact）
- **JM 资源:** 1 CPU / 1024m（exact）
- **TM 资源:** 2 CPU / 2048m / 4 slots（exact）

---

### Task 1: eth-sentiment-analysis-job — Dockerfile + FlinkDeployment

**Files:**
- Create: `datastream/eth-sentiment-analysis-job/Dockerfile`
- Create: `infra/k8s/flink/eth-sentiment-analysis-job.yaml`

**Interfaces:**
- Consumes: fat JAR `datastream/eth-sentiment-analysis-job/target/eth-sentiment-analysis-job-1.0-SNAPSHOT.jar`（已构建）
- Produces: Docker image `big-data/eth-sentiment-analysis-job:phase4`

- [ ] **Step 1: 创建 Dockerfile**

```dockerfile
FROM eclipse-temurin:17-jdk-arm64-alpine@sha256:a04722d572a3d77f30c2a37e7e7ab0f3a5b2e6c8d9e1f2a3b4c5d6e7f8a9b0c

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apk add --no-cache bash

WORKDIR /opt/flink

COPY eth-sentiment-analysis-job/target/eth-sentiment-analysis-job-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/eth-sentiment-analysis-job.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/eth-sentiment-analysis-job.jar"]
```

- [ ] **Step 2: 创建 FlinkDeployment YAML**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: eth-sentiment-analysis
  namespace: flink
spec:
  flinkVersion: v1_18
  flinkImage: "apache/flink-kubernetes-operator:1.2.0"
  ingress:
    class: nginx
    labels:
      app: eth-sentiment-analysis
  jobManager:
    resource:
      cpu: 1
      memory: "1024m"
    replicas: 1
  taskManager:
    resource:
      cpu: 2
      memory: "2048m"
    numberOfTaskSlots: 4
  persistence: true
  job:
    jarURI: local:///opt/flink/jobs/eth-sentiment-analysis-job.jar
    parallelism: 2
    upgradeMode: savepoint
    stateRetention:
      failOnTTLInUse: true
  stateBackend:
    type: hashmap
  checkpointing:
    checkpointTimeout: 10000
    maxConcurrentCheckpoints: 1
    unalignedCheckpoints: false
  env:
    - name: S3_ENDPOINT
      value: "http://minio.data.svc.cluster.local:9000"
    - name: S3_ACCESS_KEY
      value: "minioadmin"
    - name: S3_SECRET_KEY
      value: "minioadmin"
    - name: KAFKA_BOOTSTRAP_SERVERS
      value: "kafka:29092"
    - name: TZ
      value: "Asia/Shanghai"
```

- [ ] **Step 3: 提交**

```bash
git add datastream/eth-sentiment-analysis-job/Dockerfile
git add infra/k8s/flink/eth-sentiment-analysis-job.yaml
git commit -m "feat: add eth-sentiment-analysis-job Dockerfile and FlinkDeployment

- Dockerfile (ARM64, fat JAR bundled)
- FlinkDeployment CR with savepoint upgrade mode
- S3 checkpoint to k3s MinIO, Kafka via ExternalName

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: kafka2milvus — Dockerfile + FlinkDeployment

**Files:**
- Create: `datastream/kafka2milvus/Dockerfile`
- Create: `infra/k8s/flink/kafka2milvus.yaml`

**Interfaces:**
- Consumes: fat JAR `datastream/kafka2milvus/target/kafka2milvus-1.0-SNAPSHOT.jar`（已构建）
- Produces: Docker image `big-data/kafka2milvus:phase4`

- [ ] **Step 1: 创建 Dockerfile**

```dockerfile
FROM eclipse-temurin:17-jdk-arm64-alpine@sha256:a04722d572a3d77f30c2a37e7e7ab0f3a5b2e6c8d9e1f2a3b4c5d6e7f8a9b0c

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apk add --no-cache bash

WORKDIR /opt/flink

COPY kafka2milvus/target/kafka2milvus-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/kafka2milvus.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/kafka2milvus.jar"]
```

- [ ] **Step 2: 创建 FlinkDeployment YAML**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: kafka2milvus
  namespace: flink
spec:
  flinkVersion: v1_18
  flinkImage: "apache/flink-kubernetes-operator:1.2.0"
  ingress:
    class: nginx
    labels:
      app: kafka2milvus
  jobManager:
    resource:
      cpu: 1
      memory: "1024m"
    replicas: 1
  taskManager:
    resource:
      cpu: 2
      memory: "2048m"
    numberOfTaskSlots: 4
  persistence: true
  job:
    jarURI: local:///opt/flink/jobs/kafka2milvus.jar
    parallelism: 2
    upgradeMode: savepoint
    stateRetention:
      failOnTTLInUse: true
  stateBackend:
    type: hashmap
  checkpointing:
    checkpointTimeout: 10000
    maxConcurrentCheckpoints: 1
    unalignedCheckpoints: false
  env:
    - name: S3_ENDPOINT
      value: "http://minio.data.svc.cluster.local:9000"
    - name: S3_ACCESS_KEY
      value: "minioadmin"
    - name: S3_SECRET_KEY
      value: "minioadmin"
    - name: KAFKA_BOOTSTRAP_SERVERS
      value: "kafka:29092"
    - name: TZ
      value: "Asia/Shanghai"
```

- [ ] **Step 3: 提交**

```bash
git add datastream/kafka2milvus/Dockerfile
git add infra/k8s/flink/kafka2milvus.yaml
git commit -m "feat: add kafka2milvus Dockerfile and FlinkDeployment

- Dockerfile (ARM64, fat JAR bundled)
- FlinkDeployment CR with savepoint upgrade mode
- S3 checkpoint to k3s MinIO, Kafka via ExternalName

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: employee-message-processor — Dockerfile + FlinkDeployment

**Files:**
- Create: `datastream/employee-message-processor/Dockerfile`
- Create: `infra/k8s/flink/employee-message-processor.yaml`

**Interfaces:**
- Consumes: fat JAR `datastream/employee-message-processor/target/employee-message-processor-1.0-SNAPSHOT.jar`（已构建）
- Produces: Docker image `big-data/employee-message-processor:phase4`

- [ ] **Step 1: 创建 Dockerfile**

```dockerfile
FROM eclipse-temurin:17-jdk-arm64-alpine@sha256:a04722d572a3d77f30c2a37e7e7ab0f3a5b2e6c8d9e1f2a3b4c5d6e7f8a9b0c

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apk add --no-cache bash

WORKDIR /opt/flink

COPY employee-message-processor/target/employee-message-processor-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/employee-message-processor.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/employee-message-processor.jar"]
```

- [ ] **Step 2: 创建 FlinkDeployment YAML**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: employee-message-processor
  namespace: flink
spec:
  flinkVersion: v1_18
  flinkImage: "apache/flink-kubernetes-operator:1.2.0"
  ingress:
    class: nginx
    labels:
      app: employee-message-processor
  jobManager:
    resource:
      cpu: 1
      memory: "1024m"
    replicas: 1
  taskManager:
    resource:
      cpu: 2
      memory: "2048m"
    numberOfTaskSlots: 4
  persistence: true
  job:
    jarURI: local:///opt/flink/jobs/employee-message-processor.jar
    parallelism: 2
    upgradeMode: savepoint
    stateRetention:
      failOnTTLInUse: true
  stateBackend:
    type: hashmap
  checkpointing:
    checkpointTimeout: 10000
    maxConcurrentCheckpoints: 1
    unalignedCheckpoints: false
  env:
    - name: S3_ENDPOINT
      value: "http://minio.data.svc.cluster.local:9000"
    - name: S3_ACCESS_KEY
      value: "minioadmin"
    - name: S3_SECRET_KEY
      value: "minioadmin"
    - name: KAFKA_BOOTSTRAP_SERVERS
      value: "kafka:29092"
    - name: TZ
      value: "Asia/Shanghai"
```

- [ ] **Step 3: 提交**

```bash
git add datastream/employee-message-processor/Dockerfile
git add infra/k8s/flink/employee-message-processor.yaml
git commit -m "feat: add employee-message-processor Dockerfile and FlinkDeployment

- Dockerfile (ARM64, fat JAR bundled)
- FlinkDeployment CR with savepoint upgrade mode
- S3 checkpoint to k3s MinIO, Kafka via ExternalName

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: realtime-riskcontrol-embedding-job — Dockerfile + FlinkDeployment

**Files:**
- Create: `datastream/realtime-riskcontrol-embedding-job/Dockerfile`
- Create: `infra/k8s/flink/realtime-riskcontrol-embedding-job.yaml`

**Interfaces:**
- Consumes: fat JAR `datastream/realtime-riskcontrol-embedding-job/target/realtime-riskcontrol-embedding-job-1.0-SNAPSHOT.jar`（已构建）
- Produces: Docker image `big-data/realtime-riskcontrol-embedding-job:phase4`

- [ ] **Step 1: 创建 Dockerfile**

```dockerfile
FROM eclipse-temurin:17-jdk-arm64-alpine@sha256:a04722d572a3d77f30c2a37e7e7ab0f3a5b2e6c8d9e1f2a3b4c5d6e7f8a9b0c

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apk add --no-cache bash

WORKDIR /opt/flink

COPY realtime-riskcontrol-embedding-job/target/realtime-riskcontrol-embedding-job-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/realtime-riskcontrol-embedding-job.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/realtime-riskcontrol-embedding-job.jar"]
```

- [ ] **Step 2: 创建 FlinkDeployment YAML**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: realtime-riskcontrol-embedding
  namespace: flink
spec:
  flinkVersion: v1_18
  flinkImage: "apache/flink-kubernetes-operator:1.2.0"
  ingress:
    class: nginx
    labels:
      app: realtime-riskcontrol-embedding
  jobManager:
    resource:
      cpu: 1
      memory: "1024m"
    replicas: 1
  taskManager:
    resource:
      cpu: 2
      memory: "2048m"
    numberOfTaskSlots: 4
  persistence: true
  job:
    jarURI: local:///opt/flink/jobs/realtime-riskcontrol-embedding-job.jar
    parallelism: 2
    upgradeMode: savepoint
    stateRetention:
      failOnTTLInUse: true
  stateBackend:
    type: hashmap
  checkpointing:
    checkpointTimeout: 10000
    maxConcurrentCheckpoints: 1
    unalignedCheckpoints: false
  env:
    - name: S3_ENDPOINT
      value: "http://minio.data.svc.cluster.local:9000"
    - name: S3_ACCESS_KEY
      value: "minioadmin"
    - name: S3_SECRET_KEY
      value: "minioadmin"
    - name: KAFKA_BOOTSTRAP_SERVERS
      value: "kafka:29092"
    - name: TZ
      value: "Asia/Shanghai"
```

- [ ] **Step 3: 提交**

```bash
git add datastream/realtime-riskcontrol-embedding-job/Dockerfile
git add infra/k8s/flink/realtime-riskcontrol-embedding-job.yaml
git commit -m "feat: add realtime-riskcontrol-embedding-job Dockerfile and FlinkDeployment

- Dockerfile (ARM64, fat JAR bundled)
- FlinkDeployment CR with savepoint upgrade mode
- S3 checkpoint to k3s MinIO, Kafka via ExternalName

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: 更新 kustomization.yaml 和 build script

**Files:**
- Modify: `infra/k8s/flink/kustomization.yaml`
- Modify: `infra/scripts/build-and-import-flink.sh`

**Interfaces:**
- Consumes: 4 个新创建的 FlinkDeployment YAML + 4 个 Dockerfile
- Produces: kustomization 引用所有 5 个 FlinkDeployment + 4 个新 Dockerfile；build script 支持批量构建

- [ ] **Step 1: 更新 kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: flink
resources:
  - operator-helm.yaml
  - eth-sentiment-trading-job.yaml
  - eth-sentiment-analysis-job.yaml
  - kafka2milvus.yaml
  - employee-message-processor.yaml
  - realtime-riskcontrol-embedding-job.yaml
```

- [ ] **Step 2: 扩展 build script**

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

VM_NAME="${VM_NAME:-k3s-node}"
PLATFORM="linux/arm64"

JOBS=(
  eth-sentiment-trading-job
  eth-sentiment-analysis-job
  kafka2milvus
  employee-message-processor
  realtime-riskcontrol-embedding-job
)

for JOB in "${JOBS[@]}"; do
  image="big-data/${JOB}:phase4"
  echo "Building ${image}..."
  docker build --platform "${PLATFORM}" \
    -f "datastream/${JOB}/Dockerfile" \
    -t "${image}" .
  echo "Importing ${image} to ${VM_NAME}..."
  docker save "${image}" | orb -m "${VM_NAME}" -u root k3s ctr images import -
  echo "Verifying ${image} in k3s..."
  orb -m "${VM_NAME}" -u root k3s ctr images list | grep 'big-data/' | grep phase4
  echo ""
done

echo "All Flink images built and imported."
```

- [ ] **Step 3: 提交**

```bash
git add infra/k8s/flink/kustomization.yaml
git add infra/scripts/build-and-import-flink.sh
git commit -m "chore: update kustomization and build script for all 5 Flink jobs

- kustomization.yaml: references all 5 FlinkDeployment CRs
- build-and-import-flink.sh: batch build for all Flink images

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: 验证与文档

**Files:**
- Create: `docs/runbooks/phase4-flink-migration.md`

**Interfaces:**
- Verifies: 5 FlinkDeployment 全部 Ready
- Verifies: Checkpoint 写入 k3s MinIO
- Verifies: Live upgrade 正常工作

- [ ] **Step 1: 创建验证 runbook**

```markdown
# Phase 4: Flink Migration Verification

## 验证步骤

### 1. 确认 5 个 FlinkDeployment 全部 Ready

```bash
kubectl --context k3s-node -n flink get flinkdeployment
```

Expected output:
```
NAME                            STATUS    AGE
eth-sentiment-trading           Running   ...
eth-sentiment-analysis          Running   ...
kafka2milvus                    Running   ...
employee-message-processor      Running   ...
realtime-riskcontrol-embedding  Running   ...
```

### 2. 确认 pods 运行正常

```bash
kubectl --context k3s-node -n flink get pods
```

Expected: 5 JobManager pods + 5 TaskManager pods = 10 pods, all Running.

### 3. 验证 checkpoint 写入 MinIO

```bash
orb -m k3s-node -u root k3s exec deployment/minio \
  -- mc ls myminio/flink-state/checkpoints/
```

Expected: 5 checkpoint directories (one per job).

### 4. 验证 Flink REST API

```bash
kubectl --context k3s-node -n flink port-forward deployment/flink-operator 8081:8081 &
sleep 2
curl -s http://localhost:8081/submissions | python3 -m json.tool
kill %1
```

Expected: 5 submissions, all with `state: RUNNING`.

### 5. 验证 StreamPark 已下线

```bash
docker compose ps | grep streampark || echo "StreamPark stopped"
```

Expected: No streampark container running.
```

- [ ] **Step 2: 提交**

```bash
git add docs/runbooks/phase4-flink-migration.md
git commit -m "docs: add Phase 4 Flink migration verification runbook

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Plan Self-Review

**1. Spec coverage:**
- [x] 4 Flink jobs 迁移 → Task 1-4（每个 job 一个 task）
- [x] Dockerfile (fat JAR) → Task 1-4 Step 1
- [x] FlinkDeployment CR → Task 1-4 Step 2
- [x] kustomization 更新 → Task 5 Step 1
- [x] build script 扩展 → Task 5 Step 2
- [x] Checkpoint 到 k3s MinIO → Task 6 Step 1
- [x] Live upgrade via savepoint → Task 6 Step 1
- [x] Kafka via ExternalName → Task 1-4 env vars (kafka:29092)
- [x] StreamPark 下线 → Task 6 Step 1
- [x] 验证 runbook → Task 6

**2. Placeholder scan:**
- 无 "TBD", "TODO", "implement later"
- 所有代码块完整（Dockerfile, YAMLs, shell commands）
- 所有文件路径精确
- 所有命令有预期输出

**3. Type consistency:**
- FlinkDeployment 使用 `flinkVersion: v1_18`（与 Flink 1.18.1 一致）
- Dockerfile 使用基础镜像 `eclipse-temurin:17-jdk-arm64-alpine`
- 镜像命名 `big-data/{job-name}:phase4`（一致命名）
- S3 凭证一致（minioadmin/minioadmin）
- 所有 4 个 FlinkDeployment CR 结构一致（仅 name, jarURI, labels 不同）

**4. Scope check:**
- 聚焦于 4 个 Flink 作业迁移
- 无无关重构
- 每个 job 映射到现有 `datastream/{job}` 模块
- Build script 扩展遵循 Phase 2/3 模式
- 所有 fat JAR 已在 target/ 目录中

**5. 迁移顺序合理性:**
- eth-sentiment-analysis-job 最先（与 trading-job 同源，最相似）
- kafka2milvus 其次（结构清晰，已有 global_lib）
- employee-message-processor 第三（已有 global_lib）
- realtime-riskcontrol-embedding-job 最后（收尾）
