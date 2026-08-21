# 流量复制与回放：判据

两层判据，各管各的：

| 层 | 脚本 | 管什么 |
|---|---|---|
| 引擎级 | `test_scripts/traffic/traffic_e2e.py` | 直接驱动 `migration-traffic` 子进程，钉住捕获/回放引擎本身的边界 |
| 平台级 | `autotest/suites/traffic.py` | 全程走 REST API，证明"用户点得出来的那条路"能跑通 |

## 前置

```bash
# 两端 MySQL 都要在跑，且必须是**不同实例**——产品会硬拦"回放到录制源库自己"
docker start synctask-mysql synctask-mysql-b

# 判据跑的是 fat jar，不是 target/classes
mvn -pl migration-traffic -am install -Dmaven.test.skip=true
```

## 跑

```bash
python3 test_scripts/traffic/traffic_e2e.py             # 引擎级，49 项，约 3 分钟
python3 test_scripts/traffic/traffic_e2e.py -k crash    # 只跑名字含 crash 的用例

./autotest/autotest_run --group traffic                 # 平台级，3 条用例，约 3 分钟
```

## 引擎级用例覆盖

| 用例 | 钉住的东西 |
|---|---|
| `timeline` | 5s/10s/15s 场景；SELECT 能录到（binlog 里没有它，这是本功能存在的理由）；4 字节 UTF-8；`CREATE USER` 归 DCL 不归 DDL；口令被 MySQL 抹掉时打 `rd` 标；PREPARE/EXECUTE/DEALLOCATE 噪声行剔除、只留参数已替换的 `Execute`；采集连接自身无自噪声 |
| `replay` | 在**目标库**上用 general_log 量实际执行时刻，间隔必须与录制一致；数据一致；零回放错误 |
| `ordering` | 单会话 1000 条 `n=n+1`，回放后必须正好 1000 —— 少一条或乱一次序都不会相等 |
| `dangerous` | `DROP DATABASE` 被拦下且记进报告；普通语句照常回放 |
| `same_instance` | 回放到录制源库自己被引擎直接拒绝 |
| `crash_recovery` | `kill -9` 后：源库开关仍开着 → 状态文件在 → `--mode restore` 能兜底还原；被 kill 的分段里已落盘的记录能读出来；续录沿用原时间轴、记空洞、写新分段不覆盖旧的 |
| `size_limit` | 到量自动封口，收工标记是 `COMPLETED`（正常结束，不是故障） |

## 踩过的坑（改判据前先看这里）

1. **子进程 stdout 必须持续排空。** 不排空，子进程写满管道缓冲区后会阻塞在写日志上——
   现象是"进程还在、什么都不干"，极难定位。`Engine` 类专门起了一个 pump 线程。
2. **要跑 fat jar，不能只 `mvn compile`。** shade 出来的包才是线上跑的那个。
3. **读被 kill 的分段要按块解压。** `BufferedReader` 按 8KB 填充，填充时底层抛
   `EOFException`（gzip 没有 trailer），**这一块里已经解出来的内容会连同缓冲区一起丢掉**——
   一个明明有 18 行数据的分段被判成 0 条。引擎侧的 `RecordingRecovery` 与这里的
   `read_records()` 都是逐行/按块读的，改的时候别退回 `BufferedReader`。
4. **本机 MySQL 上还跑着平台自己的元数据库。** 捕获不加库过滤会把 `sync_task_db`
   的心跳 UPDATE 一起录进来，回放到没有该库的目标端必然报 `No database selected`——
   那是真实且正确的回放错误，但会污染"零错误"这条断言。判据一律带 `traffic.capture.databases`。
5. **每条用例跑完要还原源库开关。** `restore_source()` 在 `finally` 里；漏掉的话
   下一条用例会看到一个已经开着 general_log 的源库，行为与预期不符。
6. **源库互斥锁是会话级的。** 上一个捕获任务刚停时它的连接可能还没被服务端回收，
   引擎侧会等（`LOCK_WAIT_MS`=20s），判据之间也留了几秒。
