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
