# Flink 作业无损升级手册（savepoint → 停止 → 部署 → 恢复）

前提：集群 state.savepoints.dir 已指向 s3://flink-state/savepoints（见 docker-compose.yml）。

> 注意：`flink-jobmanager` 容器内 `PATH` 不包含 flink 的 bin 目录，直接执行 `flink ...` 会报
> `exec: "flink": executable file not found in $PATH`。所有命令必须使用完整路径
> `/opt/flink/bin/flink`（已在下面各步骤中体现，本手册已实际验证过）。

## 1. 找到作业 ID

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
```

## 2. 带 savepoint 停止作业

```bash
docker exec flink-jobmanager /opt/flink/bin/flink stop --savepointPath s3://flink-state/savepoints <JOB_ID>
```

记下输出的 savepoint 完整路径（形如 `s3://flink-state/savepoints/savepoint-xxxxxx-yyyyyyyyyyyy`）。

## 3. 部署新版本并从 savepoint 恢复

```bash
docker cp <新构建的 shaded jar> flink-jobmanager:/tmp/job.jar
docker exec flink-jobmanager /opt/flink/bin/flink run -d -s <savepoint 完整路径> -c <主类> /tmp/job.jar <原启动参数>
```

## 4. 验证恢复成功

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list          # 状态 RUNNING
docker logs flink-jobmanager 2>&1 | grep -i "restor"  # 出现 Restoring job ... from Savepoint
```

## 注意事项

- 容器内没有把 flink 的 bin 加入 PATH，务必使用完整路径 `/opt/flink/bin/flink`，否则命令会直接报
  `executable file not found in $PATH` 而不是业务层面的错误，容易误判。
- 改了算子拓扑（增删算子）时，恢复需给算子设 uid 并在必要时加 `--allowNonRestoredState`。
- StreamPark 界面上的"停止时触发 savepoint"等价于第 2 步。

## 演练记录（本手册已按以下真实操作验证）

以 `datastream/realtime-riskcontrol-embedding-job` 的 rc-job（主类
`com.expert.bigdata.app.RealtimeRiskControlEmbeddingJob`，jar 位于容器内
`/tmp/rc-job.jar`）为对象完整演练一遍（第 3 步用同一个 jar 模拟"新版本"）：

1. `docker exec flink-jobmanager /opt/flink/bin/flink list`
   → 找到 JobID `e59fdd59ce3c73bd89ffc3212d86c6c7`（Dofi-Realtime-AI-Pipeline，RUNNING）。
2. `docker exec flink-jobmanager /opt/flink/bin/flink stop --savepointPath s3://flink-state/savepoints e59fdd59ce3c73bd89ffc3212d86c6c7`
   → 输出：
   ```
   Suspending job "e59fdd59ce3c73bd89ffc3212d86c6c7" with a CANONICAL savepoint.
   Savepoint completed. Path: s3://flink-state/savepoints/savepoint-e59fdd-03be4b6d5a56
   ```
3. `docker exec flink-jobmanager /opt/flink/bin/flink run -d -s s3://flink-state/savepoints/savepoint-e59fdd-03be4b6d5a56 -c com.expert.bigdata.app.RealtimeRiskControlEmbeddingJob /tmp/rc-job.jar --kafkaUrl kafka:29092 --milvusHost milvus-standalone --ollamaHost host.docker.internal`
   → 提交新 JobID `381cc9993d7c75033b91d6c736344d89`。
4. `docker exec flink-jobmanager /opt/flink/bin/flink list`
   → `381cc9993d7c75033b91d6c736344d89 : Dofi-Realtime-AI-Pipeline (RUNNING)`。
   `docker logs flink-jobmanager 2>&1 | grep -i "restor"`
   → `Restoring job 381cc9993d7c75033b91d6c736344d89 from Savepoint 6 @ 0 for 381cc9993d7c75033b91d6c736344d89 located at s3://flink-state/savepoints/savepoint-e59fdd-03be4b6d5a56.`

结论：命令与预期完全一致，无损升级演练通过。
