# 故障注入 / 断点续传测试

在同步过程中注入**进程崩溃**（SIGKILL）与**线程僵死**（SIGSTOP），验证：

1. 崩溃后子进程被 ProcessGuard 自动重启、从 checkpoint 断点续传，最终数据一致（不丢不重）；
2. 进程僵死（存活但不推进）时，监控能发现“假活”并把任务上报为 FAILED。

覆盖链路：mysql→mysql、tidb→mysql（共用 capture+extract+increment 三段 SQL 管线）、
redis→redis（单进程引擎）。oracle 源共用 SQL 管线，机制同 mysql。

## 前置

后端 + agent + 各数据源已启动（`./start.sh`；tidb/redis 各自的 compose 已 up）。
依赖 `mysql-connector-python`、`requests`；redis 用例用 `docker exec redis-cli`。

## 用例

```bash
# SQL 管线（mysql / tidb）：全量+增量，注入进程崩溃，验证断点续传一致性
python3 fault_injection/sql_resume.py mysql --minutes 5
python3 fault_injection/sql_resume.py tidb  --minutes 5

# SQL 管线：仅全量，全量搬运途中杀 migration-full，retry 恢复后续传一致
python3 fault_injection/sql_resume.py mysql --mode full

# 线程僵死检测（冻结 capture / extract / increment 任一）
python3 fault_injection/hang_detect.py capture
python3 fault_injection/hang_detect.py extract
python3 fault_injection/hang_detect.py increment

# Redis：崩溃自愈一致性 + 引擎僵死检测
python3 fault_injection/redis_resume.py resume --minutes 5
python3 fault_injection/redis_resume.py hang
```

退出码 0 = 全通过。

## 机制说明

### 断点续传一致性

- capture / extract / increment（以及 redis 引擎）都受 ProcessGuard 守护；SIGKILL 后自动重启，
  按各自 checkpoint 续传。重启窗口内重复投递的事件由**幂等应用**吸收
  （SQL：`INSERT ... ON DUPLICATE KEY UPDATE`；Redis：`RESTORE ... REPLACE`）。
- 判定用顺序无关、对增删改敏感的整表指纹（SQL：`BIT_XOR(CRC32(...))`；Redis：整库键值 md5）。
- migration-full **不受守护**，崩溃即 FAILED，靠 `retry` 触发恢复：按 `migration_progress`
  跳过已完成表、从断点续传。

> 注意：mysql→mysql 用例源库、目标库同在一个 MySQL 实例（本地只有一个），目标写入会回灌
> 源库 binlog，放大事件流、拖慢崩溃后追平（分钟级）。真实场景源/目标为不同实例，无此放大。
> 测试用 `resource_quotas.max_increment_rows_per_sec` 临时抬高限速（跑完在 finally 还原）。

### 僵死看门狗

`checkProcessHealth()` 只看 `process.isAlive()`，冻结/死锁的进程仍 alive，检测不到。为此新增
**活性文件看门狗**（`AbstractTaskExecutor.checkPipelineStalled`）：

- SQL 管线监控 `binlog_output/` 下三个文件——`rpo_metric`（capture 写）、
  `capture_queue_depth`（extract 每轮写）、`rto_metric`（increment 写）。三者由各进程自身的
  常在活动驱动（心跳/扫描循环，与用户是否写入无关），谁冻结谁的文件立刻停更。任一超
  `monitor.stall.threshold.ms`（默认 90s）未刷新即判僵死、上报 FAILED。
- 仅当三个守护进程都 RUNNING 时才检查（`guardsHealthyForStallCheck`）：崩溃重启窗口内
  文件本就短暂停更，此时交给崩溃恢复路径，避免把“正在重启”误判为“僵死”。SIGSTOP 冻结下
  进程 isAlive 仍为 true，不受此门控影响，照常检出。
- Redis 引擎（`RedisSyncTask`）同理监控 `redis_progress.json`：增量阶段引擎靠 PSYNC 每 ~10s
  的 PING 按时间兜底刷新该文件，冻结即停更被检出。
