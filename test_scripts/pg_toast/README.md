# PG 逻辑复制值保真判据

`pg_value_e2e.py` —— pg → pg 增量链路的值保真判据，承接
`markdown/SILENT_DATA_LOSS_AUDIT_20260811.md` 的第 1、2 项。

```bash
python3 test_scripts/pg_toast/pg_value_e2e.py
```

前置：`postgres_db` 容器在跑；三个 fat jar 已 `package`（只 `compile` 不 `package` = 跑旧代码）。
脚本自己建/删 `toast_src`、`toast_tgt` 两个库与复制槽，跑完会清理。

## 覆盖

| 场景 | 改造前 | 改造后 |
|---|---|---|
| 未变更的 TOAST 值（`u` 标志） | 目标端 40KB 大字段被清成 NULL，**任务全绿** | 该列整个不进 SET，值原样保留 |
| 空字符串 | 目标端落进四个字符的 `'NULL'` | 空串 |
| 大字段真被改写 / 显式赋 NULL / DELETE | 正常 | 正常（回归护栏） |

## 两个坑

1. **光把值造大不会触发 `u`**。PG 推到行外存储之前会**先压缩**：`repeat('X',40000)`
   压完只剩几十字节，直接压缩存在行内，pgoutput 照发完整值 —— 第一版判据就是这么全绿
   却什么也没测到的。`u` 的判定条件是 `VARATT_IS_EXTERNAL_ONDISK`，所以要
   `ALTER COLUMN … SET STORAGE EXTERNAL` 关掉压缩，并**断言 TOAST 附属表真的有字节**。
2. **任务目录要整个删重建**。只清 `.cap`/`.thl` 而留下位点与进度文件，本轮会从上一轮的
   LSN 续传，判据看到的是上一轮的数据。macOS 上目录项用 `os.remove` 删不掉，用 `shutil.rmtree`。

---

## `pg_schema_truncate_e2e.py`

同一套脚手架的第二个判据，覆盖审查报告的第 8、10、11 项与附录：

```bash
python3 test_scripts/pg_toast/pg_schema_truncate_e2e.py
```

| 场景 | 改造前 | 改造后 |
|---|---|---|
| 链路积压期间 `DROP COLUMN` | 任务停摆（E3004，删列后的行一条都过不去） | 正常同步、不错位 |
| `TRUNCATE` | 目标端仍有旧行 | 目标端清空 |
| 运行中删掉复制槽 | 默默建新槽继续跑，中间变更消失 | E3006 停机、不自建槽 |
| `.cap` 处理完成标记 | 永远置不上，清理从不生效 | 旧文件标 true 并被清理 |

基线 4/10 → 修复后 13/13。

### 又一个坑

判据里的辅助函数**别写死列名**：中途 `DROP COLUMN` 之后按老列名取值会 SQL 报错，
而 `wait_until` 把异常当成"还没同步到"，一路等到超时 —— 判据永远失败且看不出原因。
本目录用 `row_json()`（`row_to_json`）与表结构解耦。

---

## `pg_latency_e2e.py` —— 增量延迟指标是否可信

```bash
python3 test_scripts/pg_toast/pg_latency_e2e.py
```

| 场景 | 改造前 | 改造后 |
|---|---|---|
| capture 被暂停 12s 期间提交的变更 | rto **969ms**（只量 capture→apply） | rto **13145ms**（起点是源库提交时刻） |
| capture 进程被杀掉 | rto_metric 每几秒仍被刷成 ≈0，陈旧度 1.8s | 指标停止刷新、随即陈旧，agent 按无数据处理 |
| 源库空闲 | 靠 extract 的**本机时钟**心跳（恒 ≈0） | capture 打**源端时钟**心跳，量的是真实链路耗时 |

基线 8/11 → 修复后 11/11。

### 判据暴露出的一个设计问题

第一版跑出来 rto 只有 1038ms，查 `.cap` 才发现事件时间戳本身是对的（提交时刻），
是**每 2 秒一次的空闲心跳把 12 秒的真实样本覆盖掉了**。心跳只覆盖 capture→apply 一段，
数据事件覆盖全程；有数据在流动时必须以数据事件为准，否则真实积压会被掩盖成一个漂亮的小数字。
现在心跳在数据事件上报后的静默期内不抢话（`HEARTBEAT_YIELD_MS`）。
