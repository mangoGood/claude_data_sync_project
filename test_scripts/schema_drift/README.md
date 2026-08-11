# 表结构漂移判据

验证[表结构时序库](../../markdown/SCHEMA_TIMELINE_DESIGN_20260811.md)：用"事件当时"的表结构
解析 binlog，而不是源库"现在"的定义。

要证的只有一句话：**要么正确同步，要么明确报错停下，不允许静默写坏。**

## 跑法

```bash
mvn -o package -Dmaven.test.skip=true      # 判据跑的是 fat jar，改完代码必须重新打包
python3 test_scripts/schema_drift/drift_e2e.py
python3 test_scripts/schema_drift/drift_e2e.py --only rename_column
DRIFT_MODE=OFF python3 test_scripts/schema_drift/drift_e2e.py --only rename_column   # 对拍
```

依赖 `docker-compose-synctask-dr.yml` 起的 `dr-mysql-a`（源）与 `dr-mysql-b`（目标）。

## 六个场景

| # | 场景 | 改造前（`DRIFT_MODE=OFF`） | 改造后 |
|---|---|---|---|
| ① | 积压期 `RENAME COLUMN` | **静默写坏**：值以 `0x…` 十六进制字面量落库，无任何报错 | 正确 |
| ② | 积压期 `DROP a` + `ADD b`（列数不变） | 正确（FULL 的事件列名自愈已经覆盖） | 正确 |
| ③ | 积压期 enum 增删取值 | E3004 停机（`Data truncated for column 'st'`） | 正确 |
| ④ | `binlog_row_metadata=MINIMAL` 下加列 | 靠列数校验兜底 | 正确 |
| ⑤ | pt-osc 收尾的 `RENAME TABLE` 交换 | — | 明确报错（影子表不在同步范围，目标端无从跟随） |
| ⑥ | 目标表少一列 | — | 明确报错 E3004，不静默跳过 |

①③ 是这次改造真正买到的东西：①从"静默数据损坏"变成正确，③从"任务停摆"变成正确。
②诚实地说，改造前就是对的——`binlog_row_metadata=FULL` 的事件列名已经能自愈列的增删。

### ① 为什么改造前会写成十六进制

capture 按 `CHAR_AND_BINARY_AS_BYTE_ARRAY` 把字符列交付成 `0x…`，抽取端再按**列类型**决定
要不要解码回文本。改造前的自愈只救了列**名**：`resolveColumns` 用事件自带的列名
（`[id, old_name, amt]`），然后 `columnMetaByName` 拿这些名字去当前表定义
（`[id, new_name, amt]`）里找类型——`old_name` 找不到，类型退化成空串，
`isTextType("")` 为假，于是十六进制原样落库。

**列名自愈了、类型没有**，这正是时序库要补的那一维。

## 关键约束：必须造出"漂移窗口"

漂移只发生在一个窗口里——事件已经进了 `.cap`、extract 还没消化，源库在这中间改了表结构。
真实链路里这个窗口等于全量耗时（extract 在全量做完之后才开工），可能是几小时。

每个用例用 `driftlib.LaggingExtract` 把它压缩成几秒：**先只起 capture**，让事件在 `.cap` 里
堆着；等源端把 DDL 做完，再放 extract/increment 进去消化。

造不出窗口的判据全是假绿灯——源库改完结构、抽取端才开始读的话，查 `information_schema`
当然是对的。

## 写判据时踩过的两个坑

* **不要手工改目标端的表结构**。DDL 会顺着同一条 THL 流按序 replay 到目标端；手工先把目标
  改掉，"改名前的那一行"到达时目标列已经没了，报 `Unknown column`——测出来的是判据自己制造
  的顺序错乱，不是链路的行为。只有 ⑥（目标少列）是故意让两端不同，走 `fresh_task(tgt_ddl=…)`。
* **判据跑的是 fat jar**。只 `mvn compile` 不 `package` 的话，跑的还是旧代码，
  表现为"开关明明打开了却一点效果都没有"（`files/<taskId>/schema_history.jsonl` 压根不生成，
  这是最快的判断依据）。

## 灰度顺序

1. `extract.schema.timeline.mode=SHADOW` —— 两条路都算，产出仍走旧路径，只记差异；
2. 盯四个指标：`timeline_shadow_diff` / `timeline_ddl_parse_failed` /
   `timeline_cross_check_failed` / `timeline_miss`。**影子差异不等于时序库错了**——
   积压期做过 DDL 时，"两条路不同"恰恰说明时序库在干正事，要看着日志逐条判断；
3. `timeline_ddl_parse_failed` 与 `timeline_cross_check_failed` 归零后切 `ON`；
4. 跑稳一段时间再把 `extract.schema.timeline.fallback` 从 `RESNAPSHOT` 收紧到 `FAIL_STOP`。
