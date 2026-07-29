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

# 异构链路 pg2pg / mysql2pg / pg2mysql / mongo2mongo（dblib.py 引擎适配 + 跨引擎 Python 指纹）
#   需 venv：python3 -m venv --system-site-packages v && v/bin/pip install psycopg2-binary pymongo
python3 fault_injection/xdb_resume.py pg2pg       --minutes 5           # 全量+增量断点续传一致
python3 fault_injection/xdb_resume.py mysql2pg    --minutes 5
python3 fault_injection/xdb_resume.py pg2mysql    --mode full           # 仅全量：杀全量进程→retry→一致
python3 fault_injection/xdb_resume.py mongo2mongo --mode full           # mongo 单进程受守护，早期崩溃自愈
python3 fault_injection/xdb_hang.py   pg2pg    capture                  # 僵死检测（冻结 capture/extract/increment）
python3 fault_injection/xdb_hang.py   mongo2mongo                       # mongo 冻结 → 进度文件停更 → FAILED
```

> 覆盖 pg/mysql/mongo 异构链路。要点见 [[fault-injection-pg-mongo-2026-07]]：mysql→pg 目标表在「源库名」
> schema（非 public）；mongo 同名库镜像（忽略 targetDbName）；PG 逻辑槽 `migration_slot_<taskId>` 按库隔离。

### 灾备（DR）：单向 / 双向 / 主备倒换

灾备任务另起一套脚本（`drlib.py` + `dr_*.py`），因为它与普通同步任务差别很大：
`taskType=DR`（后端强制 fullAndIncre）、源/目标必须是**不同实例**、双向灾备还有一条隐藏的
反向影子通道（`DR_SHADOW`）、以及独有的主备倒换（failover）流程。

前置：`docker compose -f docker-compose-synctask-dr.yml up -d` 起两对独立实例
（dr-mysql-a/b = 33320/33321，dr-pg-a/b = 55432/55433；两侧都开 binlog / wal_level=logical，
因为倒换后原目标要当新源）。

```bash
# 单向灾备：全量阶段注入崩溃(杀 migration-full→retry 续传) + 增量阶段注入崩溃(受守护自愈)
python3 fault_injection/dr_resume.py mysql2mysql --phase both  --minutes 5 --seed-rows 200000
python3 fault_injection/dr_resume.py pg2pg       --phase full  --seed-rows 8000000   # 全量 >3 分钟的数据量
python3 fault_injection/dr_resume.py pg2pg       --phase incre --minutes 5

# 双向灾备：两端同时写 + 正/反两条通道都注入崩溃，验证收敛一致且不回环放大
python3 fault_injection/dr_resume.py mysql2mysql --mode bidi --minutes 4
python3 fault_injection/dr_resume.py pg2pg       --mode bidi --minutes 4

# 灾备任务僵死检测（冻结正向通道；--shadow 冻结双向的反向通道）
python3 fault_injection/dr_hang.py mysql2mysql capture
python3 fault_injection/dr_hang.py pg2pg       increment
python3 fault_injection/dr_hang.py mysql2mysql capture --shadow

# 主备倒换 + 倒换前/中/后注入崩溃，验证倒换后两端仍精确一致
python3 fault_injection/dr_failover.py mysql2mysql --inject before
python3 fault_injection/dr_failover.py mysql2mysql --inject during
python3 fault_injection/dr_failover.py pg2pg       --inject after --switch-back
```

#### MongoDB 副本集灾备（mongo2mongo）

前置：`docker compose -f docker-compose-synctask-mongo.yml up -d`（mongo-a/b = 27117/27118，
两个**独立副本集** rsA/rsB —— 灾备要求源目标不同实例，且防回环用的多文档事务与 Change Streams
都只在副本集/分片集群可用）。

Mongo 灾备是**单进程引擎**（migration-mongo，全量 + Change Streams），没有 capture/extract/increment
三段管线，所以 `engines` 只有 `mongo` 一个，且它受 ProcessGuard 守护 —— 连全量阶段崩溃都会自愈，
不像 migration-full 那样崩了就判 FAILED 等 retry。

```bash
python3 fault_injection/dr_resume.py   mongo2mongo --phase both --minutes 3   # 全量+增量崩溃自愈一致
python3 fault_injection/dr_resume.py   mongo2mongo --mode bidi  --minutes 3   # 双向：两端写 + 收敛不回环
python3 fault_injection/dr_hang.py     mongo2mongo                            # 冻结引擎 → 90s 上报 FAILED
python3 fault_injection/dr_failover.py mongo2mongo --inject during            # 倒换窗口内崩溃仍精确一致
python3 fault_injection/dr_failover.py mongo2mongo --inject after --switch-back

# 全 BSON 类型保真 + 「防回环真的生效」的证据（单独一套判定，见脚本头注释）
python3 fault_injection/dr_mongo_types.py --mode uni
python3 fault_injection/dr_mongo_types.py --mode bidi --minutes 1
```

`dr_mongo_types.py` 存在的理由：`dr_resume.py` 的指纹只覆盖 5 个标量字段，
既证明不了 Decimal128 / Binary / 正则 / 嵌套数组能原样落到对端，也证明不了双向防回环真在工作
——两端写的是同一份文档，回环回来是同值覆盖，**最终指纹照样相等**，光看"收敛了"完全掩盖得住
无限 ping-pong。它因此另加两把尺子：canonical 扩展 JSON 逐字段比（带类型标签），
以及"停写沉降后事件计数必须完全不动"（回环唯一无法伪装的特征）。

### 订阅（SUBSCRIBE）：mysql / pg / oracle / tidb / mongo 五种源 → Kafka

订阅任务的目标端是 Kafka 而不是数据库，判定方式与前面都不同，因此另起 `sublib.py` + `sub_*.py`。

前置：**必须先起下游专用 Kafka**（与控制面 29092 那套隔离，否则几十万条 CDC 消息会挤占
任务下发/状态上报，测试结论不可信）：

```bash
docker compose -f docker-compose-synctask-kafka-sub.yml up -d      # localhost:39092
.venv_fi/bin/pip install python-snappy oracledb                    # 消费端解 snappy / oracle 源
```

```bash
# 断点续传 + 一致性：播种 20 万行大表存量 → 起订阅 → 持续 3 分钟增删改 →
# 轮流 SIGKILL subscribe/capture/extract → 停写后比对
python3 fault_injection/sub_resume.py mysql  --minutes 3
python3 fault_injection/sub_resume.py pg     --minutes 3
python3 fault_injection/sub_resume.py oracle --minutes 3
python3 fault_injection/sub_resume.py tidb   --minutes 3    # 增量走 TiCDC changefeed
python3 fault_injection/sub_resume.py mongo  --minutes 3    # Change Streams 单进程直投
python3 fault_injection/sub_resume.py mysql  --minutes 3 --no-inject   # 不注入故障的基线对照
python3 fault_injection/sub_resume.py mysql  --minutes 3 --keep        # 跑完保留任务不删

# 僵死检测：冻结管线中任一段，验证监控上报 FAILED
python3 fault_injection/sub_hang.py mysql  subscribe
python3 fault_injection/sub_hang.py pg     capture
python3 fault_injection/sub_hang.py oracle extract
python3 fault_injection/sub_hang.py tidb   capture
python3 fault_injection/sub_hang.py mongo               # 单进程引擎，只有 mongo 一个可冻结

# 全类型保真：TiDB 全 31 个 MySQL 列类型 / Mongo 全 BSON 类型，逐列(逐字段)比对 INSERT/UPDATE/DELETE
python3 fault_injection/sub_types.py tidb
python3 fault_injection/sub_types.py mongo
```

**TiDB 与 MongoDB 两条链路的形态差异**：TiDB 讲 MySQL 协议，走的还是 capture→extract→subscribe
三段管线（只是 capture 换成消费 TiCDC changefeed），因此三个进程都能注入；MongoDB 没有可落成 THL
的物理日志，订阅出口就是 Change Streams，由**单个** migration-mongo 进程直投 Kafka，
`SUB_ENGINES` 里只有 `mongo` 一项。

**Mongo 订阅的 UPDATE 为什么必须看 `updateDescription`**：change stream 的 `fullDocument`
（UPDATE_LOOKUP）是"事件读出时再查一次"的结果，不是这次更新当时的后像 —— 同一文档被连续快速更新
两次时，第一条事件查到的往往已是第二次的值，中间那次更新的取值在整条流里再也找不到
（实测 2 分钟高频写入丢 22 条）。因此消息里额外带 `updateDescription.updatedFields`
（这次到底改了哪些字段成什么值，与查询时机无关），`sublib.parse_events` 会把
`documentKey ⊕ updatedFields` 合成等价后像参与「不丢」判定。

### 实时监控指标可见性（灾备 / 同步 / 订阅）

`monitoring_visibility.py` 按监控页的真实取数路径，验证三类任务都能在下拉里出现、
且能取到真实指标（含进程健康 N/N）：

```bash
python3 fault_injection/monitoring_visibility.py                          # 只看当前在跑的
python3 fault_injection/monitoring_visibility.py --create-dr --create-sync --keep
```

订阅一致性用**两把尺子**，缺一不可：

1. **不丢** —— 写入线程给每次 INSERT/UPDATE 打唯一 `opseq`（写进 `val`/`n`），这些真值三元组
   必须都能在 Kafka 事件里找到。只比最终状态会漏掉"中间某次更新丢了但后来又被覆盖"的情况。
2. **可收敛** —— 按 Kafka 投递顺序（单分区即 offset 顺序）回放 `c/u/d`、last-write-wins，
   得到的最终状态必须与源表逐行相等。崩溃重启后重投的是"刚发过的那一段"（回退重放，
   不是乱序补发），因此按投递顺序回放仍应收敛；若不收敛说明重投顺序真的错乱了，
   下游拿到的就是脏数据。

存量播种发生在建任务**之前**，不进 CDC 流——这样既有"大表"，又能让 Kafka 里的事件与
写入线程记录的真值一一对应。

灾备用例的要点：

- **一致性判定改为库内聚合指纹**（`drlib.fingerprint`，MySQL `BIT_XOR(CRC32(...))` / PG
  `bit_xor(hashtext(...))`）。灾备两端恒为同引擎，不需要 dblib 那种跨引擎 Python 指纹，
  库内算才跑得动百万级数据量（8,000,000 行的指纹 0.2s，Python 版要几 GB 内存）。
- **写入线程显式指定主键**（A 段 1000 万起、B 段 2000 万起）。双向灾备两端各自的自增序列
  会生成相同 id，那是测试制造的写写冲突（active-active 本就无法消解），不是产品缺陷；
  倒换后新主库的自增/identity 序列也未必随全量数据推进。
- **倒换点必须先静默并等两端收敛**：倒换后新方向的 capture 从新源的最新位点起步，
  这是计划内切换的语义；未收敛就切换属于 RPO 窗口内的数据丢失，是灾备的固有语义而非缺陷。

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
- 订阅链路（`SubscribeTask`）监控 `capture_liveness`、`capture_queue_depth`、
  `subscribe_liveness` 三个文件（subscribe 主循环每轮 + 处理大文件时每 ~2s 改写第三个）。
  实测冻结 subscribe 进程后 95s 上报 FAILED（阈值 90s + 监控轮询 5s）。
  注意不能拿 `subscribe_rto_ms` 当活性信号：它只在碰到带源时间戳的事件时才写，空闲即停更。
