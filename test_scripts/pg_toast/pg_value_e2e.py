#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PG → PG 增量的值保真判据。

覆盖三类形态，前两类在修复前是**静默写坏**（任务全绿、位点照进、error_status 为空）：

  1. **未变更的 TOAST 值**：行外存储的大字段本次 UPDATE 没改动 → pgoutput 发列标志 `u`
     （值不发送）。改造前 capture 把 `u` 与真 NULL 合流，目标端那一列被清成 NULL。
  2. **空字符串**：wire 上是 `t` + 长度 0，与 NULL(`n`) 本来分得开。改造前被返回成
     不带引号的 `NULL`，下游当普通字符串处理，目标端落进四个字符的 `'NULL'`。
  3. 普通 UPDATE / NULL 赋值 / 大字段真被改写 —— 回归护栏，改造前后都该是对的。

用法：
    python3 test_scripts/pg_toast/pg_value_e2e.py            # 判据（期望全绿）
    BASELINE=1 python3 test_scripts/pg_toast/pg_value_e2e.py # 基线：期望 1、2 复现失败

前置：postgres_db 容器在跑；三个 fat jar 已 package（只 compile 不 package = 跑旧代码）。
"""
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pgtoastlib as L

TASK = "pg-toast-e2e"
TABLE = "vals"
SLOT = "pg_toast_slot"
PUB = "pg_toast_pub"
CAP_DIR = f"files/{TASK}/binlog_output"
THL_DIR = f"files/{TASK}/thl_output"
BASELINE = os.environ.get("BASELINE") == "1"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def prepare():
    L.reset_db(L.SRC_DB)
    L.reset_db(L.TGT_DB)
    L.drop_slot(SLOT)
    for db in (L.SRC_DB, L.TGT_DB):
        L.psql(L.TABLE_DDL.format(t=TABLE), db=db)
        L.psql(L.STORAGE_DDL.format(t=TABLE), db=db)
    # 判据只测增量，两端起点一致：源端预置的行同时写进目标端
    for db in (L.SRC_DB, L.TGT_DB):
        L.psql(f"INSERT INTO {TABLE}(id,tag,note,blob_txt) VALUES "
               f"(1,'t1','n1',repeat('X',40000)), "
               f"(2,'t2','n2',repeat('Y',40000)), "
               f"(3,'t3','n3',NULL)", db=db)
    # 大字段必须真的落到行外存储，否则 pgoutput 不发 'u'，判据什么也测不到
    size = L.toast_bytes(L.SRC_DB, TABLE)
    if size <= 0:
        raise SystemExit("大字段没有落到行外存储（TOAST 附属表 0 字节），判据前提不成立")
    print(f"  (前提) TOAST 附属表 {size} 字节，行外存储已生效")

    # 整目录重建：.cap/THL/位点/进度文件必须一起清干净，留一个旧位点就会让本轮从上次的
    # LSN 续传，判据看到的是上一轮的数据（macOS 上目录项用 os.remove 删不掉，别逐个删）
    task_dir = os.path.join(L.PROJECT_ROOT, "files", TASK)
    shutil.rmtree(task_dir, ignore_errors=True)
    L.write_config(task_dir, TASK, TABLE, CAP_DIR, THL_DIR, SLOT, PUB)
    return task_dir


def synced(pk, pred):
    def check():
        row = L.row_of(L.TGT_DB, TABLE, pk)
        return row is not None and pred(row)
    return check


def main():
    L.require_jars()
    task_dir = prepare()
    pipe = L.Pipeline(TASK, task_dir)
    print(f"=== PG 值保真判据（{'基线复现' if BASELINE else '修复验证'}）===")
    try:
        pipe.start_all()
        # 槽要先建起来再发数据，否则那段 WAL 不被保留
        if not pipe.wait_until(
                lambda: L.scalar(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{SLOT}'",
                                 L.SRC_DB) == "1",
                90, "复制槽建立"):
            record("复制槽建立", False, "capture 没能建槽")
            return
        time.sleep(3)

        # --- 场景 1：未变更的 TOAST 值 -------------------------------------
        L.psql(f"UPDATE {TABLE} SET tag='t1-upd' WHERE id=1", db=L.SRC_DB)
        ok = pipe.wait_until(synced(1, lambda r: r["tag"] == "t1-upd"), 90, "id=1 tag 同步")
        row = L.row_of(L.TGT_DB, TABLE, 1)
        record("未变更 TOAST：tag 更新已同步", ok, str(row))
        record("未变更 TOAST：大字段未被抹掉",
               row is not None and row["blob"] == "40000:XX",
               f"目标端 blob_txt={row['blob'] if row else '<行不存在>'}（期望 40000:XX）")

        # --- 场景 2：空字符串 ---------------------------------------------
        L.psql(f"UPDATE {TABLE} SET note='' WHERE id=2", db=L.SRC_DB)
        ok = pipe.wait_until(synced(2, lambda r: r["note"] != "n2"), 90, "id=2 note 同步")
        row = L.row_of(L.TGT_DB, TABLE, 2)
        record("空串：UPDATE 已同步", ok, str(row))
        record("空串：目标端是空串而不是字符串 'NULL'",
               row is not None and row["note"] == "",
               f"目标端 note={row['note'] if row else '<行不存在>'!r}（期望空串）")
        record("空串：同一条 UPDATE 没有连带抹掉大字段",
               row is not None and row["blob"] == "40000:YY",
               f"目标端 blob_txt={row['blob'] if row else '<行不存在>'}（期望 40000:YY）")

        # --- 场景 3：回归护栏 ------------------------------------------------
        L.psql(f"UPDATE {TABLE} SET blob_txt=repeat('Z',40000), tag='t3-upd' WHERE id=3", db=L.SRC_DB)
        ok = pipe.wait_until(synced(3, lambda r: r["tag"] == "t3-upd"), 90, "id=3 同步")
        row = L.row_of(L.TGT_DB, TABLE, 3)
        record("大字段真被改写时正常同步",
               ok and row is not None and row["blob"] == "40000:ZZ",
               f"目标端 blob_txt={row['blob'] if row else '<行不存在>'}（期望 40000:ZZ）")

        L.psql(f"UPDATE {TABLE} SET note=NULL WHERE id=1", db=L.SRC_DB)
        ok = pipe.wait_until(synced(1, lambda r: r["note"] == "<NULL>"), 90, "id=1 note 置 NULL")
        row = L.row_of(L.TGT_DB, TABLE, 1)
        record("显式赋 NULL 仍然是 NULL", ok, str(row))
        record("赋 NULL 那条 UPDATE 也没抹掉大字段",
               row is not None and row["blob"] == "40000:XX",
               f"目标端 blob_txt={row['blob'] if row else '<行不存在>'}（期望 40000:XX）")

        L.psql(f"INSERT INTO {TABLE}(id,tag,note,blob_txt) "
               f"VALUES (4,'t4','',repeat('W',40000))", db=L.SRC_DB)
        ok = pipe.wait_until(synced(4, lambda r: True), 90, "id=4 INSERT 同步")
        row = L.row_of(L.TGT_DB, TABLE, 4)
        record("INSERT 的空串与大字段保真",
               ok and row is not None and row["note"] == "" and row["blob"] == "40000:WW",
               str(row))

        L.psql(f"DELETE FROM {TABLE} WHERE id=2", db=L.SRC_DB)
        ok = pipe.wait_until(lambda: L.row_of(L.TGT_DB, TABLE, 2) is None, 90, "id=2 DELETE 同步")
        record("DELETE 正常同步", ok)

        err = L.error_status(CAP_DIR)
        record("全程无 error_status", err == "", err)
    finally:
        logs = pipe.stop_all()
        L.drop_slot(SLOT)
        L.drop_publication(L.SRC_DB, PUB)
        log_dir = os.path.join(task_dir, "judge_logs")
        os.makedirs(log_dir, exist_ok=True)
        for name, text in logs.items():
            with open(os.path.join(log_dir, name + ".log"), "w") as f:
                f.write(text)

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n=== {passed}/{len(results)} 通过 ===")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL {name}: {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
