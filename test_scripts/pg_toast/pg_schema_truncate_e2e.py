#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PG → PG 增量的三个场景判据（承接 SILENT_DATA_LOSS_AUDIT 的第 8、10、11 项）：

  1. **链路积压期间 DROP COLUMN**（第 11 项）：改造前 capture/extract 两侧的列名类型缓存都是
     回查 information_schema 查一次永不失效，源端删列之后 wire 上的 tuple 少一列而缓存还是老的，
     被删列之后的每一列整体错位一格 —— 写进去的是合法值、看不出异常。
     改造后按 Relation('R') 消息（与行值同一时刻的权威结构，PG 在结构变化后会重发）解析。
  2. **TRUNCATE**（第 10 项）：改造前 'T' 消息直接丢弃，源端清表在目标端不发生。
  3. **复制槽被删**（第 8 项）：改造前重连路径会顺手建一个新槽，服务端不报错、从新位置开始发，
     中间的变更静默消失。改造后必须停机上报 E3006。

用法：python3 test_scripts/pg_toast/pg_schema_truncate_e2e.py
前置：postgres_db 容器在跑；三个 fat jar 已 package。
"""
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pgtoastlib as L

TASK = "pg-schema-e2e"
TABLE = "drift"
SLOT = "pg_schema_slot"
PUB = "pg_schema_pub"
CAP_DIR = f"files/{TASK}/binlog_output"
THL_DIR = f"files/{TASK}/thl_output"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


DDL = """CREATE TABLE {t} (
  id   INT PRIMARY KEY,
  doomed TEXT,
  keep_a TEXT,
  keep_b TEXT
)"""


def prepare():
    L.reset_db(L.SRC_DB)
    L.reset_db(L.TGT_DB)
    L.drop_slot(SLOT)
    for db in (L.SRC_DB, L.TGT_DB):
        L.psql(DDL.format(t=TABLE), db=db)
    task_dir = os.path.join(L.PROJECT_ROOT, "files", TASK)
    shutil.rmtree(task_dir, ignore_errors=True)
    # 故意把每个 .cap 的事件数压到很小，逼出文件轮转 —— 只有轮转过才谈得上"旧文件已处理完"
    L.write_config(task_dir, TASK, TABLE, CAP_DIR, THL_DIR, SLOT, PUB,
                   extra="capture.max.events.per.file=4\nextract.cap.settle.ms=3000\n")
    return task_dir


def row(pk, db=None):
    """整行 列名→值。用 row_to_json 取，判据中途 DROP COLUMN 也照样能读。"""
    return L.row_json(db or L.TGT_DB, TABLE, pk)


def cols_of(db):
    return L.psql(f"SELECT string_agg(column_name, ',' ORDER BY ordinal_position) "
                  f"FROM information_schema.columns WHERE table_name='{TABLE}'", db=db, want=True)


def main():
    L.require_jars()
    task_dir = prepare()
    pipe = L.Pipeline(TASK, task_dir)
    print("=== PG schema 漂移 / TRUNCATE / 槽被删 判据 ===")
    try:
        pipe.start_all()
        if not pipe.wait_until(
                lambda: L.scalar(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{SLOT}'",
                                 L.SRC_DB) == "1", 90, "复制槽建立"):
            record("复制槽建立", False, "capture 没能建槽")
            return
        time.sleep(3)

        # --- 场景 1：老结构下的一行，确认链路通 ---------------------------------
        L.psql(f"INSERT INTO {TABLE}(id,doomed,keep_a,keep_b) "
               f"VALUES (1,'D1','A1','B1')", db=L.SRC_DB)
        ok = pipe.wait_until(lambda: row(1) is not None, 90, "老结构 INSERT 同步")
        r = row(1)
        record("改结构之前的行正常同步",
               ok and r == {"id": 1, "doomed": "D1", "keep_a": "A1", "keep_b": "B1"},
               str(r))

        # --- 场景 2：源端 DROP COLUMN 之后再写 ----------------------------------
        # 目标端同步改结构（DDL 不在本判据范围内，这里手工对齐两端）
        for db in (L.SRC_DB, L.TGT_DB):
            L.psql(f"ALTER TABLE {TABLE} DROP COLUMN doomed", db=db)
        record("两端已删列", cols_of(L.SRC_DB) == "id,keep_a,keep_b" == cols_of(L.TGT_DB),
               f"src={cols_of(L.SRC_DB)} tgt={cols_of(L.TGT_DB)}")

        L.psql(f"INSERT INTO {TABLE}(id,keep_a,keep_b) VALUES (2,'A2','B2')", db=L.SRC_DB)
        ok = pipe.wait_until(lambda: row(2) is not None, 90, "新结构 INSERT 同步")
        r = row(2)
        # 列少了一个，若仍按老缓存（id,doomed,keep_a,keep_b）配值，A2 会落到 doomed 位、B2 落到 keep_a，
        # keep_b 变成 NULL —— 全是合法值，只有逐列比对才看得出来
        record("删列后新行没有错位",
               ok and r == {"id": 2, "keep_a": "A2", "keep_b": "B2"},
               f"目标端整行={r}（期望 keep_a=A2 keep_b=B2）")

        L.psql(f"UPDATE {TABLE} SET keep_a='A2-upd' WHERE id=2", db=L.SRC_DB)
        ok = pipe.wait_until(lambda: (row(2) or {}).get("keep_a") == "A2-upd", 90, "删列后 UPDATE 同步")
        r = row(2)
        record("删列后 UPDATE 没有错位",
               ok and r == {"id": 2, "keep_a": "A2-upd", "keep_b": "B2"},
               f"目标端整行={r}（期望 keep_a=A2-upd keep_b=B2）")

        # --- 场景 3：TRUNCATE -------------------------------------------------
        L.psql(f"TRUNCATE TABLE {TABLE}", db=L.SRC_DB)
        ok = pipe.wait_until(
            lambda: L.scalar(f"SELECT count(*) FROM {TABLE}", L.TGT_DB) == "0", 90, "TRUNCATE 同步")
        record("源端 TRUNCATE 在目标端生效",
               ok, f"目标端剩余 {L.scalar(f'SELECT count(*) FROM {TABLE}', L.TGT_DB)} 行（期望 0）")

        L.psql(f"INSERT INTO {TABLE}(id,keep_a,keep_b) VALUES (3,'A3','B3')", db=L.SRC_DB)
        ok = pipe.wait_until(lambda: row(3) is not None, 90, "TRUNCATE 后继续同步")
        record("TRUNCATE 之后链路继续工作", ok, str(row(3)))

        err = L.error_status(CAP_DIR)
        record("前三个场景全程无 error_status", err == "", err)

        # --- .cap 处理完成标记（附录项）------------------------------------------
        # 旧判据是"连续 3 轮字节数没变"，而没有新行时上游提前返回、有新行时字节数必然也变，
        # 于是 completed 永远置不上：清理从不生效、每轮扫描还要把每个 .cap 整文件重读一遍。
        # 实测约 100 个真实任务的 .extract_progress 里 completed 全是 false。
        progress_path = os.path.join(L.PROJECT_ROOT, THL_DIR, ".extract_progress")
        prog_lines = [l for l in open(progress_path).read().splitlines() if l.strip()]
        caps = sorted(f for f in os.listdir(os.path.join(L.PROJECT_ROOT, CAP_DIR))
                      if f.endswith(".cap"))
        record("capture 确实轮转出了多个 .cap（判据前提）", len(caps) > 1, str(caps))
        done = [l for l in prog_lines if l.endswith("|true")]
        record("已读完且不再写入的 .cap 被标记为完成",
               len(done) >= 1, "进度文件:\n    " + "\n    ".join(prog_lines))
        record("最新的那个 .cap 不会被误标完成",
               all(not l.startswith(caps[-1] + "|") or l.endswith("|false") for l in prog_lines),
               f"最新文件={caps[-1]}")

        # --- 场景 4：运行中把复制槽删掉 ------------------------------------------
        # 槽是 WAL 保留的唯一凭据。改造前重连路径会建一个新槽、从新位置开始发，中间静默丢；
        # 现在必须停机上报
        L.psql(f"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
               f"WHERE slot_name='{SLOT}' AND active_pid IS NOT NULL")
        time.sleep(1)
        L.drop_slot(SLOT)
        gone = L.scalar(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{SLOT}'", L.SRC_DB)
        record("复制槽已被删除（判据前提）", gone == "0", f"count={gone}")

        # 制造一次重连：槽没了，capture 会去 ensureReplicationSlot
        L.psql(f"INSERT INTO {TABLE}(id,keep_a,keep_b) VALUES (4,'A4','B4')", db=L.SRC_DB)
        got_err = pipe.wait_until(lambda: "E3006" in L.error_status(CAP_DIR), 120,
                                  "槽被删后上报 E3006")
        record("槽被删后停机上报 E3006（不再静默建新槽）",
               got_err, L.error_status(CAP_DIR) or "(error_status 为空)")
        record("槽被删后没有静默重建",
               L.scalar(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{SLOT}'",
                        L.SRC_DB) == "0",
               "capture 不应自己把槽建回来")
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
