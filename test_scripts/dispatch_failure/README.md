# 派发消息发不出去时任务必须置 FAILED

复现的是真实故障（2026-08-12）：Kafka 比后端晚起了一分钟，任务 `test1001`
（`f56e56d9-…`）卡在 PENDING。后端日志：

```
10:49:58.127 发送任务创建消息到 Kafka: taskId=f56e56d9-…, topic=sync-task-created
10:50:58.188 TimeoutException: Topic sync-task-created not present in metadata after 60000 ms.
             Connection to node -1 (localhost/127.0.0.1:29092) could not be established.
             Broker may not be available.
```
（`synctask-kafka` 容器的实际启动时间是 10:52:02，比发送放弃晚 64 秒。）

## 为什么这不只是"少了一条告警"

改造前 `WorkflowService` 把失败降级成一条 WARNING 日志，任务留在 PENDING、HTTP 照常返回成功。
而"消息没投出去"意味着执行端从未收到它，任务**永远不会开始跑**。更糟的是它**连重启都做不到**：

- `launchWorkflow` 只接受 `CONFIGURING`
- `retryWorkflow` 只接受 `FAILED`

卡在 `PENDING` 的任务两条路都进不去，正常 UI 操作救不回来。
改造后置 `FAILED` + `E5004`，重试路径就能把它救回来。

## 跑法

```bash
python3 test_scripts/dispatch_failure/dispatch_failure_e2e.py
```

会**停掉 Kafka**（跑完自动恢复），请确认没有任务正在同步。
登录口令与 `V2__seed_default_data.sql` 的种子不一致时用 `E2E_USER` / `E2E_PASS` 覆盖。

## 现状

**这个脚本还没在本机跑通**：环境里 `users.password` 的哈希已与 V2 种子不同（被
`create_env.sh` 之类重置过），仓库里记着的 `user1/123456` 登不上。
改动目前由单测覆盖：`TaskDispatchFailureTest`（6 项，含"已跑起来的任务不能被迟到的失败回调
打成 FAILED"）+ `DispatchFailureCallbackTest`（5 项，异步 ack 失败必须回调）。
