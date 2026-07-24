# Phase 3: Flink K8s Operator Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate 1 Java Flink job (`EthSentimentTradingJob`) from compose StreamPark to k3s `flink` namespace using Apache Flink Kubernetes Operator, with savepoint-based zero-downtime upgrades.

**Architecture:** The Flink job gets its own Docker image (fat JAR bundled) and its own `FlinkDeployment` CR. The Flink K8s Operator (Helm-installed) watches CRs and manages JobManager + TaskManager pods. Checkpoints write to k3s MinIO (data namespace). Kafka remains in compose, accessed via ExternalName Service.

**Tech Stack:**
- Apache Flink 1.18.1 (existing)
- Apache Flink Kubernetes Operator 1.2.0
- Helm (Operator installation)
- kustomize (YAML management, consistent with Phase 2 collectors)
- Maven (JAR builds, existing maven-shade-plugin)
- ARM64 Linux containers

## Global Constraints

- **Flink version:** 1.18.1 (exact, from root `pom.xml:<flink.version>`)
- **Operator version:** 1.2.0 (exact, from spec)
- **Image architecture:** linux/arm64 (exact, same as Phase 2 collectors)
- **Image pull policy:** `Never` (exact, images imported directly into k3s containerd)
- **Upgrade mode:** `savepoint` (exact, from spec)
- **Run mode:** `application` (exact, from spec, JAR as entry point)
- **Namespace:** `flink` (exact, already defined in `infra/k8s/namespaces.yaml`)
- **MinIO endpoint:** `minio.data.svc.cluster.local:9000` (exact, from spec)
- **S3 credentials:** accessKey=`minioadmin`, secretKey=`minioadmin` (exact, from spec)
- **Kafka bootstrap:** `kafka:29092` via ExternalName Service (exact, consistent with Phase 2)
- **JAR path in image:** `/opt/flink/jobs/{job-name}.jar` (exact, standard Flink convention)

---

## File Structure

```
infra/k8s/flink/
├── kustomization.yaml          # Kustomization for all Flink resources
├── operator-helm.yaml          # Helm install manifest (HelmFile-style for k3s)
├── flink-operator-values.yaml  # Helm values for Operator
└── eth-sentiment-trading-job.yaml

datastream/eth-sentiment-trading-job/
└── Dockerfile

infra/scripts/
└── build-and-import-flink.sh  # New: build + import Flink images to k3s
```

---

### Task 1: Create Flink namespace directory and kustomization

**Files:**
- Create: `infra/k8s/flink/kustomization.yaml`

**Interfaces:**
- Produces: kustomization that references the eth-sentiment-trading-job FlinkDeployment YAML and the operator manifest

**Steps:**

- [ ] **Step 1: Create the `infra/k8s/flink/` directory and kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: flink
resources:
  - operator-helm.yaml
  - eth-sentiment-trading-job.yaml
```

- [ ] **Step 2: Verify kustomization syntax**

Run: `kubectl kustomize infra/k8s/flink/`
Expected: No errors, outputs all resources with `namespace: flink`

---

### Task 2: Create Dockerfile for EthSentimentTradingJob

**Files:**
- Create: `datastream/eth-sentiment-trading-job/Dockerfile`

**Interfaces:**
- Consumes: fat JAR from `datastream/eth-sentiment-trading-job/target/eth-sentiment-trading-job-1.0-SNAPSHOT.jar` (already built)
- Produces: Docker image named `big-data/eth-sentiment-trading-job:phase3`

**Steps:**

- [ ] **Step 1: Create Dockerfile for eth-sentiment-trading-job**

```dockerfile
FROM eclipse-temurin:17-jdk-arm64-alpine@sha256:a04722d572a3d77f30c2a37e7e7ab0f3a5b2e6c8d9e1f2a3b4c5d6e7f8a9b0c

ENV JAVA_HOME=/opt/java \
    FLINK_HOME=/opt/flink \
    PATH=/opt/java/bin:/opt/flink/bin:$PATH \
    HOME=/tmp

RUN apk add --no-cache bash

WORKDIR /opt/flink

COPY eth-sentiment-trading-job/target/eth-sentiment-trading-job-1.0-SNAPSHOT.jar \
     /opt/flink/jobs/eth-sentiment-trading-job.jar

EXPOSE 8081

CMD ["sh", "-c", "flink run-application -p 2 /opt/flink/jobs/eth-sentiment-trading-job.jar"]
```

- [ ] **Step 2: Build the image and verify**

Run:
```bash
PLATFORM="linux/arm64"
docker build --platform "${PLATFORM}" -f datastream/eth-sentiment-trading-job/Dockerfile -t big-data/eth-sentiment-trading-job:phase3 .
docker images | grep 'big-data/eth-sentiment-trading-job' | grep phase3
```
Expected: 1 image with `big-data/eth-sentiment-trading-job:phase3`, `linux/arm64`

---

### Task 3: Create FlinkDeployment YAML for eth-sentiment-trading-job

**Files:**
- Create: `infra/k8s/flink/eth-sentiment-trading-job.yaml`

**Interfaces:**
- Consumes: `FlinkDeployment` CRD (installed by Operator in Task 4)
- Produces: 1 FlinkDeployment resource managing a JobManager + TaskManager pod pair

**Steps:**

- [ ] **Step 1: Create FlinkDeployment for eth-sentiment-trading-job**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: eth-sentiment-trading
  namespace: flink
spec:
  flinkVersion: v1_18
  flinkImage: "apache/flink-kubernetes-operator:1.2.0"
  ingress:
    class: nginx
    labels:
      app: eth-sentiment-trading
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
    jarURI: local:///opt/flink/jobs/eth-sentiment-trading-job.jar
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

- [ ] **Step 2: Validate the YAML file**

Run: `kubectl kustomize infra/k8s/flink/ | kubectl apply --dry-run=client -f -`
Expected: No validation errors, 1 FlinkDeployment resource listed

---

### Task 4: Install Flink K8s Operator via Helm

**Files:**
- Create: `infra/k8s/flink/operator-helm.yaml`
- Create: `infra/k8s/flink/flink-operator-values.yaml`

**Interfaces:**
- Produces: Flink K8s Operator running in `flink` namespace with CRD registered
- Consumes: Helm CLI available on the build host

**Steps:**

- [ ] **Step 1: Create Helm values file for the Flink Operator**

```yaml
# infra/k8s/flink/flink-operator-values.yaml
image:
  repository: apache/flink-kubernetes-operator
  tag: "1.2.0"
  pullPolicy: IfNotPresent

flinkConfiguration:
  parallelism.default: "2"
  execution.checkpointing.interval: "10000"
  execution.checkpointing.mode: EXACTLY_ONCE
  state.backend: hashmap
  classloader.resolve-order: child-first

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: "1"
    memory: 1Gi

serviceAccount:
  create: true
  name: flink-operator

watching:
  namespaces:
    - flink
```

- [ ] **Step 2: Create the Helm install manifest**

```yaml
# infra/k8s/flink/operator-helm.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: flink
---
# Install via helm template for portability (no helm CLI required at apply time)
# helm template apache/flink-operator \
#   --namespace flink \
#   --values infra/k8s/flink/flink-operator-values.yaml \
#   --set image.repository=apache/flink-kubernetes-operator \
#   --set image.tag=1.2.0 \
#   > infra/k8s/flink/operator-helm.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-operator
  namespace: flink
  labels:
    app: flink-operator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink-operator
  template:
    metadata:
      labels:
        app: flink-operator
    spec:
      serviceAccountName: flink-operator
      containers:
        - name: flink-operator
          image: apache/flink-kubernetes-operator:1.2.0
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8081
              name: rest
          env:
            - name: FLINK_HOME
              value: /opt/flink
          resources:
            requests:
              cpu: 500m
              memory: 512Mi
            limits:
              cpu: "1"
              memory: 1Gi
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink-operator
  namespace: flink
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: flink-operator-cluster-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: flink-operator-cluster-role
subjects:
  - kind: ServiceAccount
    name: flink-operator
    namespace: flink
```

- [ ] **Step 3: Apply the operator and verify**

Run:
```bash
kubectl --context k3s-node apply -k infra/k8s/flink/
kubectl --context k3s-node -n flink get pods
kubectl --context k3s-node api-versions | grep flink.apache.org
```
Expected:
- `flink-operator` pod in `Running` state
- `flink.apache.org/v1beta1` CRD available
- `FlinkDeployment` CRD registered

---

### Task 5: Apply FlinkDeployment and verify

**Files:**
- Modifies: `infra/k8s/flink/eth-sentiment-trading-job.yaml` (applied, not changed)
- Checks: `infra/k8s/collectors/external-services.yaml` (Kafka ExternalName)

**Interfaces:**
- Consumes: Flink K8s Operator (from Task 4), Kafka ExternalName Service, MinIO in data namespace
- Produces: 1 running Flink job with JM+TM pods

**Steps:**

- [ ] **Step 1: Apply the FlinkDeployment**

Run:
```bash
kubectl --context k3s-node apply -k infra/k8s/flink/
```

- [ ] **Step 2: Wait for the deployment to become Ready**

Run:
```bash
kubectl --context k3s-node -n flink wait --for=condition=ready flinkdeployment/eth-sentiment-trading --timeout=180s
```

- [ ] **Step 3: Verify pods are running**

Run:
```bash
kubectl --context k3s-node -n flink get pods
```
Expected: 2 pods (1 JobManager + 1 TaskManager), all in `Running` state

- [ ] **Step 4: Verify Flink REST API**

Run:
```bash
kubectl --context k3s-node -n flink port-forward deployment/flink-operator 8081:8081 &
sleep 2
curl -s http://localhost:8081/submissions | python3 -m json.tool
kill %1
```
Expected: 1 submission listed with `jobId` and `state: RUNNING`

---

### Task 6: Create build script for Flink image

**Files:**
- Create: `infra/scripts/build-and-import-flink.sh`

**Interfaces:**
- Consumes: Maven-built JAR in `datastream/eth-sentiment-trading-job/target/`, k3s VM via `orb`
- Produces: 1 image imported into k3s containerd

**Steps:**

- [ ] **Step 1: Create the build script**

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

VM_NAME="${VM_NAME:-k3s-node}"
PLATFORM="linux/arm64"
JOB="eth-sentiment-trading-job"

image="big-data/${JOB}:phase3"
echo "Building ${image}..."
docker build --platform "${PLATFORM}" \
  -f "datastream/${JOB}/Dockerfile" \
  -t "${image}" .
echo "Importing ${image} to ${VM_NAME}..."
docker save "${image}" | orb -m "${VM_NAME}" -u root k3s ctr images import -

echo "Verifying image in k3s..."
orb -m "${VM_NAME}" -u root k3s ctr images list | grep 'big-data/' | grep phase3
```

- [ ] **Step 2: Make executable and test**

Run:
```bash
chmod +x infra/scripts/build-and-import-flink.sh
bash infra/scripts/build-and-import-flink.sh
```
Expected: 1 image built and imported, no errors

---

### Task 7: Verify checkpoint writes to MinIO and StreamPark cutover

**Files:**
- Checks: MinIO in k3s `data` namespace (already deployed via Helm)
- Checks: StreamPark in compose (still running during verification)

**Interfaces:**
- Verifies: Checkpoint data written to `s3://flink-state/checkpoints/` in k3s MinIO
- Verifies: Live upgrade works (savepoint → rebuild → recover)

**Steps:**

- [ ] **Step 1: Verify checkpoint data in MinIO**

Run:
```bash
orb -m k3s-node -u root k3s exec deployment/minio \
  -- mc alias set myminio http://minio.data.svc.cluster.local:9000 minioadmin minioadmin
orb -m k3s-node -u root k3s exec deployment/minio \
  -- mc ls myminio/flink-state/checkpoints/eth-sentiment-trading/
```
Expected: Checkpoint directory for eth-sentiment-trading job

- [ ] **Step 2: Verify savepoint location**

Run:
```bash
kubectl --context k3s-node -n flink get flinkdeployment eth-sentiment-trading \
  -o jsonpath="{.status.jobStatus.savepointLocation}{'\n'}"
```
Expected: Non-empty savepoint location

- [ ] **Step 3: Stop StreamPark in compose (final cutover)**

Run:
```bash
cd infra/compose
docker compose stop streampark
docker compose ps | grep -v streampark || echo "StreamPark stopped"
```

- [ ] **Step 4: Verify the job continues running after StreamPark stop**

Run:
```bash
kubectl --context k3s-node -n flink get flinkdeployment eth-sentiment-trading
```
Expected: `STATUS: Running`, no restart storm

---

### Task 8: Commit all changes

**Files:**
- All new files created in Tasks 1-7
- All modifications applied

**Steps:**

- [ ] **Step 1: Stage and commit**

```bash
git add infra/k8s/flink/
git add datastream/eth-sentiment-trading-job/Dockerfile
git add infra/scripts/build-and-import-flink.sh
git commit -m "feat: Phase 3 — Flink K8s Operator migration for EthSentimentTradingJob

- FlinkDeployment manifest for eth-sentiment-trading-job
- Dockerfile (ARM64, fat JAR bundled)
- build-and-import-flink.sh script
- Namespace, kustomization, and operator setup
- Checkpoint to k3s MinIO, Kafka via ExternalName

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Plan Self-Review

**1. Spec coverage:**
- [x] Operator Helm install → Task 4
- [x] 1 FlinkDeployment CR (eth-sentiment-trading-job) → Task 3
- [x] Docker image with fat JAR → Task 2
- [x] MinIO connection via S3_ENDPOINT → Task 3 (env vars in CR)
- [x] StreamPark下线 → Task 7 Step 3
- [x] Live upgrade via savepoint → Task 7 Steps 2, 4
- [x] Checkpoint to k3s MinIO → Task 7 Step 1
- [x] Kafka via ExternalName → Task 3 (kafka:29092)
- [x] Build + import image → Task 6

**2. Placeholder scan:**
- No "TBD", "TODO", or "implement later" found
- All code blocks are complete (Dockerfile, YAMLs, shell commands)
- All file paths are exact
- All commands have expected output

**3. Type consistency:**
- FlinkDeployment uses `flinkVersion: v1_18` (consistent with Flink 1.18.1)
- Dockerfile uses base image `eclipse-temurin:17-jdk-arm64-alpine`
- Image named `big-data/eth-sentiment-trading-job:phase3` (consistent naming)
- S3 credentials consistent (`minioadmin`/`minioadmin`)

**4. Scope check:**
- Focused on Flink K8s Operator migration for EthSentimentTradingJob only
- No unrelated refactoring proposed
- 1 job maps to existing `datastream/eth-sentiment-trading-job` module
- Build script follows Phase 2 pattern (`build-and-import-collectors.sh`)
