# Mongo 增量：单事件应用失败时位点不得越过它

承接 `markdown/SILENT_DATA_LOSS_AUDIT_20260811.md` 第 7 项。

```bash
python3 test_scripts/mongo_failstop/mongo_apply_failure_e2e.py
```

前置：`docker-compose-synctask-mongo.yml` 起来且副本集已 `rs.initiate`；
`migration-mongo` fat jar 已 `package`。

## 判据怎么构造

目标端建一个**源端没有**的唯一索引，让第二条文档必然写失败：

- **改造前**：`applyEvent` 记一行日志继续，外层紧接着落盘 `cursor.getResumeToken()`，
  位点越过失败的那条 —— 去掉冲突约束再重启，`_id=2` 也**永远回不来**（4/8）。
- **改造后**：进程 fail-stop 退出，位点停在最后一条成功应用的事件上；
  去掉约束重启即补齐（8/8）。

注释里那句"下轮 resume 重放可自愈"是这条 bug 的根源——它假设位点没动，而位点其实动了。
