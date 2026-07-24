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
