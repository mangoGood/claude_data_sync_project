# 同步平台端到端自动化测试

把"建库 → 建任务 → 同步 → 校验 → 清理"这套反复手跑的动作固化成可重复、可作 CI 门禁的用例集。
覆盖平台已有的**全部链路**：同步（含数据对比与差异修复）、灾备（单向 + 主备倒换 + 双向双写）、数据订阅。

## 快速开始

```bash
./autotest/autotest_run                          # 默认用例集（standard），约 8 分钟
```

```bash
./autotest/autotest_run --list                   # 先看看有哪些用例
```

退出码：`0` 全部通过 · `1` 有用例未通过 · `2` 环境不可用（后端没起来等）。

## 只跑某条链路

```bash
./autotest/autotest_run --suite sync_mysql2mysql   # 只跑 mysql→mysql 同步
```

```bash
./autotest/autotest_run --suite dr_mysql           # 只跑 mysql 灾备（单向 + 双向都跑）
```

```bash
./autotest/autotest_run --suite subscribe_mysql    # 只跑 mysql 数据订阅
```

`--suite` 支持精确名、前缀和通配符（`dr_mysql` 命中单向+双向，`sync_*` 命中所有同步链路），
可以重复传多个。也可以按分组挑：`--group sync` / `--group dr` / `--group subscribe`。

## 用例集（profile）

| 用例集 | 内容 | 实测耗时 |
| --- | --- | --- |
| `quick` | mysql→mysql 同步 + mysql 单向灾备 + mysql 订阅 | ≈2 分钟 |
| `standard` | 主干链路：4 条 SQL 同步 + mongo 同步 + mysql 单向/双向灾备 + mysql 订阅 | ≈8 分钟 |
| `full`（默认） | 全部 20 条用例（含 redis / ES / TiDB / Oracle / PG 灾备 / 各源订阅） | ≈13 分钟 |

`--budget-mins`（默认 30）是总时长预算：塞不下的用例会被跳过，并在报告里写明"因预算跳过"，
而不是跑到一半被 kill 掉留一地残留。`--budget-mins 0` 关掉限制。


## 中断与续跑

用例跑到一半出错退出、或被 `autotest_stop` / Ctrl-C 停掉之后：

```bash
./autotest/autotest_run --resume    # 接着跑，跳过上一轮已通过的用例
```

```bash
./autotest/autotest_run             # 不加 --resume 就是从头重跑
```

续跑的粒度是**整条用例**：一条跑到一半的实时同步场景从中间接着跑没有意义，必须整条重来。

## 停止

```bash
./autotest/autotest_stop            # 当前用例跑完即停，现场保留，可 --resume 接着跑
```

```bash
./autotest/autotest_stop --force    # 立即停
```

```bash
./autotest/autotest_stop --clean    # 停掉之后把登记在案的残留任务/库/索引/topic 清干净
```

## 清理策略

| `--cleanup` | 行为 |
| --- | --- |
| `auto`（默认） | 用例通过就立刻清理它建的任务和库；**未通过则全部保留**，任务 id 打在控制台和报告里 |
| `always` | 一律清理 |
| `never` | 一律保留（排查时用） |

测试用的库都是跑用例时现建的（统一 `at_` 前缀），跑完没出错就删掉。
所有创建的资源都会以**可序列化的描述符**登记进状态文件，所以哪怕主进程被 kill 掉，
`autotest_stop --clean` 也能照着状态文件收尾。

## 测试报告

每轮生成 `autotest/reports/<运行ID>/`，`autotest/reports/latest` 指向最近一轮：

| 文件 | 用途 |
| --- | --- |
| `report.html` | 人看的报告：通过/未通过/跳过、逐条断言、未通过用例的任务 id |
| `report.md` | 同上，markdown 版，方便贴进 issue 或 PR |
| `junit.xml` | Jenkins / GitLab CI 直接吃，每条断言是一个 testcase |
| `report.json` | 机器读，便于二次加工 |

控制台会实时打印进度（`[3/8] 37% ...`、已用时长、每条断言的 PASS/FAIL）。

## Jenkins

`autotest/Jenkinsfile` 是现成的流水线：参数化选 profile / suite / 清理策略，
跑完自动归档 `junit.xml`（测试趋势图）、HTML 报告和日志。用例未通过时构建标成
`UNSTABLE` 而不是直接失败，好让报告归档照常执行。

## 用例覆盖

| 分组 | 用例 | 判据要点 |
| --- | --- | --- |
| sync | `sync_mysql2mysql` `sync_mysql2pg` `sync_pg2pg` `sync_pg2mysql` `sync_tidb2mysql` `sync_mongo2mongo` `sync_redis2redis` `sync_mysql2es` `sync_oracle2pg` | 全量一致；增量（含 UPDATE/DELETE）追平；行数对比；内容对比；**在目标端人为改坏一行，对比必须抓到**；一键修复后复核一致 |
| dr | `dr_mysql_uni` `dr_pg_uni` `dr_mongo_uni` | 灾备全量/增量一致；**主备倒换不丢数据**（倒换前故意写一批不等追平）；倒换后源目标对调、旧主被置只读、反向同步生效 |
| dr | `dr_mysql_bidi` `dr_pg_bidi` `dr_mongo_bidi` | 反向影子任务自动创建并进增量；**双写两个方向都同步**；防回环（两端行数相等、静置后不再变化）；最终一致；双向灾备拒绝倒换 |
| subscribe | `subscribe_mysql` `subscribe_pg` `subscribe_tidb` `subscribe_mongo` `subscribe_oracle` | 任务进订阅态；**不丢**（写入真值都能在 Kafka 事件里找到）；**可收敛**（按投递顺序回放的最终状态 == 源表） |

## 当前实测基线

第一轮跑 `full` 是 15 通过 / 5 未通过，5 条未通过全部是产品缺陷（见下方"曾经跑出来的产品缺陷"）。
缺陷修完后重跑：**20 条全部通过，152 条判据全过，整轮 12 分 29 秒**
（缺陷修好后灾备用例不再走满等待超时，整轮从 29 分钟降到 12 分钟）。

| 分组 | 用例数 | 结论 |
| --- | --- | --- |
| sync | 9 | 全部通过 |
| dr | 6 | 全部通过 |
| subscribe | 5 | 全部通过 |

## 前置环境

- 后端 `38080` + agent 已启动（`./start.sh`）
- 基础设施：`synctask-mysql`(33306)、`postgres_db`(5432)、`synctask-mongo-a/b`(27117/27118)、
  `synctask-kafka`(29092)
- 按需：`synctask-redis-a/b`(6390/6391)、`synctask-es`(9200)、`synctask-tidb`(14000)、
  `oracle_db`(1521)、订阅下游 `synctask-kafka-sub`(39092)
- 灾备用例需要**独立实例**（源/目标不同实例是后端预校验的硬要求）：

```bash
docker compose -f docker-compose-synctask-dr.yml up -d
```

  `autotest_run` 在选到灾备用例时会自己尝试拉起这一套。

**环境不满足的用例会被标成 SKIPPED 并写明原因，不算失败** —— 没装 Oracle 驱动、没起 TiDB，
都不会把整轮染红。

## 为什么是单线程串行

一次只跑一个任务。并发跑多条链路会让 agent 同时拉起十几个 JVM 子进程，机器扛不住时
失败原因全变成资源不足，测不出产品问题；而且多条链路同时改同一个 MySQL 实例上的库，
彼此的 binlog 会互相串扰。用例内部也不起后台写入线程，写完再等追平。

## 目录结构

```
autotest/
├── autotest_run          启动入口（挑解释器、注入密钥环境、写 PID、落日志）
├── autotest_stop         停止入口（可选清理残留）
├── Jenkinsfile           Jenkins 流水线
├── main.py               CLI 入口
├── framework/
│   ├── config.py         端点、规模、超时（全部可用环境变量覆盖）
│   ├── api.py            后端 REST 客户端（纯标准库）
│   ├── endpoints.py      各引擎端点：建库/播种/写增量/指纹
│   ├── case.py           用例上下文（断言、资源登记）+ suite 注册表
│   ├── runner.py         调度：选用例、串行执行、预算、进度、逐条清理
│   ├── state.py          续跑状态 + 资源登记表
│   ├── cleanup.py        按描述符清理（主进程死了也能收尾）
│   ├── kafka_util.py     订阅链路的 Kafka 消费与事件回放
│   └── report.py         JUnit / HTML / Markdown / JSON 报告
└── suites/
    ├── sync.py           同步链路
    ├── dr.py             灾备链路
    └── subscribe.py      订阅链路
```

## 加一条新链路

在 `suites/` 里用 `@suite` 装饰一个 `run(ctx)` 函数即可，`ctx` 提供断言与资源登记：

```python
@suite("sync_foo2bar", "Foo → Bar 同步", "sync", est_secs=200, requires=_foo_alive)
def sync_foo(ctx):
    ctx.step("重建源/目标库")
    ...
    ctx.reg_task(tid)                       # 登记后失败会保留、通过会自动清理
    ctx.require("任务进入运行态", ok, st)     # require 失败即中止本条用例
    ctx.check("增量追平", ok, detail)        # check 失败继续跑完剩余断言
```

## 曾经跑出来的产品缺陷（已修）

这套用例第一轮跑出 5 个真实缺陷，均已修复并由对应用例守住。留档是为了说明这些判据为什么长这样：

1. **mysql→pg 行数对比把同步正常的任务报成"目标端 0 行"。**
   `ValidationTaskService.getRowCount()` 对 PG 目标发的是不带 schema 的
   `SELECT COUNT(*) FROM "at_load"`，走连接默认 `search_path`（public）；而 mysql→pg 的
   同步引擎把表建在**源库名**那个 schema 下。取不到就抛 relation does not exist，
   `getRowCountSafe()` 吞掉并返回 0。
   → 改为先查 `information_schema.tables` 定位表**实际所在的 schema 与实际大小写**再计数
   （`resolvePgTable`）。oracle→pg 会把表名转小写，大小写这一半同样必需。

2. **计划内主备切换会丢数据（引擎无关的竞态）。**
   `SwitchoverService.drainState()` 的"已追平"判据只有 `pending_events`（THL 已产出 − 已应用）
   和未应用 THL 文件数，**没把源端位点边界纳入**——boundary 早就算出来了，却只塞进 details。
   capture 还没把最后一批写入读成 THL 时 `pending_events` 恰好为 0，于是判成已追平就切，
   方向一对调，那批数据永远留在旧主。PG 必现，MySQL 4 次里复现 2 次。
   → 追平改成两把尺子都要过：capture 已越过停写位点（MySQL 比 binlog file:pos，
   PG 比复制槽 `confirmed_flush_lsn`）**且** 已捕获的都应用完了。判不出来一律不算追平。
   顺带补上 `PostgresWalCapture` 缺失的位点按时间兜底落盘（原来只按每 1000 事件存，
   停写后不再有新事件就永远不刷新）。

3. **PG 双向灾备只同步 INSERT、静默丢 UPDATE。**
   两端行数一致、任务全绿，但同 id 的值停在更新前。根因在 `ConflictResolver.decideOnMismatch()`：
   PG 的 UPDATE 在 REPLICA IDENTITY DEFAULT 下**不带前镜像**，前镜像守卫必然落空 →
   走进冲突裁决 → 而 `storedNode == nodeId`（上次写这行的就是同一个源）时进不了 LWW 的
   时间戳比较，退化成"节点 id 与 localNodeId 比字典序"，`"127.0.0.1:5432/db".compareTo("local") < 0`
   恒成立 → 每条 UPDATE 都判本端赢、被丢掉。MySQL 侧碰不到，因为 binlog 带全前镜像。
   → **同源不是冲突**：`storedNode` 与来源相同就直接应用，也不再计入冲突数。
   回归用例见 `ConflictResolverTest`。

4. **Oracle 源做不了数据对比。**`ValidationTaskService.CONNECTION_PATTERN` 只认
   `mysql|postgresql|mongodb|elastic`，Oracle 连接串直接匹配失败，对比任务以
   "连接串格式不正确"进 FAILED。
   → 正则加上 oracle，补 `jdbc:oracle:thin:` 的 URL 拼装与 Oracle 的 schema 限定计数；
   Oracle 的**内容**对比仍不支持，但改成在创建时就明确拒绝，而不是一路走到解析才报个不相干的错。

5. **Oracle→MySQL 全量不建目标表，却把这一步记成"成功"。**
   `SchemaMigration` 在源端不提供 CREATE TABLE SQL（Oracle 就不提供）且该库对没有建表翻译器时
   直接 `return`，调用方随即记 "结构迁移成功"，接着 300 行数据全部以
   `Table ... doesn't exist` 写失败，最后报一个跟真实原因毫无关系的"全量迁移失败，退出码 1"。
   → 跳过前先看目标表在不在：在（用户预建）就沿用，不在就**明确报错**说清是哪个库对不支持自动建表。
   注意 oracle→mysql 的类型翻译器本就没实现（`TypeTranslatorTest` 里就是这么断言的），
   这里修的是"假成功"，不是补上这个库对——本用例集的 Oracle 链路因此打到 PostgreSQL。

## 一个坑（别再踩）

跨引擎比对数据用的"指纹"**不能用 XOR 聚合 CRC32**。CRC32 在 GF(2) 上是线性的：
等长的两行只差一个常量时，逐行 crc 的差也是常量；**偶数**条这样的行同时存在，XOR 就整体抵消——
两份完全不同的数据会算出同一个指纹，用例把"数据丢了"判成"已追平"。
按 id 分段造数的测试数据恰好天然满足"等长 + 常量差"这个条件。
现在的实现是：规范化行**排序后整体做一次 SHA-256**（`framework/endpoints.py: fp_from_rows`）。
