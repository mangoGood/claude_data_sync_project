#!/usr/bin/env python3
"""
无主键表的行定位判据（mysql→mysql 增量）。

背景：无主键表的 UPDATE/DELETE 在目标端只能按<b>整行前镜像</b>做 WHERE
（`TypedDmlConverter#appendWhere`：`pks` 为空时把每一列都写进 WHERE）。
无主键表允许存在**完全重复的行**，而这条 WHERE 会同时命中所有重复行：

    源端 3 条完全相同的行，DELETE 掉其中 1 条
      → binlog 里是一条"删 1 行"的事件
      → 目标端执行 `DELETE FROM t WHERE c1=? AND c2=?` **删掉全部 3 条**

UPDATE 同理（一行改值 → 目标端所有重复行一起被改）。两者都不报错、不进死信，
只有对数时才发现——正是"最终一致"判据抓不到的那一类。

三把尺子：

1. **DELETE 一条重复行** —— 源端剩 2 条，目标端也必须剩 2 条（现行为：剩 0 条）。
2. **UPDATE 一条重复行** —— 源端 1 改 2 不改，目标端被改的行数必须是 1（现行为：3 行全改）。
3. **对照：有主键表不受影响** —— 同样的操作在带主键的表上两端必须一致。

用例只做判定、不做修复；修复方向见 CAPABILITY_GAP_ANALYSIS §12。

用法：
    python3 test_scripts/fault_injection/nopk_dup_rows.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
SRC_DB = "nopk_src"
TGT_DB = "nopk_tgt"

DDL_NOPK = """
CREATE TABLE `dup_nopk` (
  `c1` INT NOT NULL,
  `c2` VARCHAR(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

DDL_PK = """
CREATE TABLE `dup_pk` (
  `id` INT NOT NULL,
  `c1` INT NOT NULL,
  `c2` VARCHAR(32) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def rebuild():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}", f"CREATE DATABASE {TGT_DB}"])
    F.sql_exec(CFG, [DDL_NOPK, DDL_PK], db=SRC_DB)
    # 存量各 3 条完全重复 + 1 条不重复，全量阶段搬过去
    F.sql_exec(CFG, [
        "INSERT INTO dup_nopk (c1, c2) VALUES (1,'x'),(1,'x'),(1,'x'),(9,'keep')",
        "INSERT INTO dup_pk (id, c1, c2) VALUES (1,1,'x'),(2,1,'x'),(3,1,'x'),(9,9,'keep')",
    ], db=SRC_DB)


def counts(db, table, where):
    rows = F.sql_fetch(CFG, db, f"SELECT COUNT(*) FROM `{table}` WHERE {where}")
    return rows[0][0]


def wait_converge(pred, timeout=90):
    """等目标端稳定：pred() 连续 3 次取到同一个值即认为追平。"""
    deadline = time.time() + timeout
    last, stable = None, 0
    while time.time() < deadline:
        cur = pred()
        if cur == last:
            stable += 1
            if stable >= 3:
                return cur
        else:
            last, stable = cur, 0
        time.sleep(2)
    return last


def main():
    token = F.login()
    rebuild()

    sync_objects = {SRC_DB: {"tables": ["dup_nopk", "dup_pk"], "targetDb": TGT_DB}}
    task_id = F.create_task(token, f"nopk-dup-{int(time.time())}", "mysql", "mysql",
                            CONN, CONN, "fullAndIncre", json.dumps(sync_objects), TGT_DB,
                            source_db=SRC_DB, force=True)  # 无主键表现在被预检拦，本用例就是要测它
    print(f"    taskId={task_id}")
    passed, failed = [], []
    try:
        if F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=300) != "INCREMENT_RUNNING":
            print("任务未进入增量，放弃")
            sys.exit(2)
        # 全量搬完再动手，否则改的是"还没搬的行"，测的就不是增量路径了
        wait_converge(lambda: counts(TGT_DB, "dup_nopk", "1=1"))
        time.sleep(3)

        # ---- 尺子1：删掉 3 条重复行中的 1 条 ----
        F.sql_exec(CFG, ["DELETE FROM dup_nopk WHERE c1=1 AND c2='x' LIMIT 1"], db=SRC_DB)
        src_n = counts(SRC_DB, "dup_nopk", "c1=1 AND c2='x'")
        tgt_n = wait_converge(lambda: counts(TGT_DB, "dup_nopk", "c1=1 AND c2='x'"))
        ok = (tgt_n == src_n)
        (passed if ok else failed).append(
            f"无主键表 DELETE 一条重复行：源剩 {src_n} 条，目标剩 {tgt_n} 条"
            + ("" if ok else "  ← 整行 WHERE 命中全部重复行，连带删除"))

        # ---- 尺子2：改掉剩余重复行中的 1 条 ----
        F.sql_exec(CFG, ["UPDATE dup_nopk SET c2='y' WHERE c1=1 AND c2='x' LIMIT 1"], db=SRC_DB)
        src_y = counts(SRC_DB, "dup_nopk", "c2='y'")
        tgt_y = wait_converge(lambda: counts(TGT_DB, "dup_nopk", "c2='y'"))
        ok = (tgt_y == src_y)
        (passed if ok else failed).append(
            f"无主键表 UPDATE 一条重复行：源端 {src_y} 行为 'y'，目标端 {tgt_y} 行"
            + ("" if ok else "  ← 整行 WHERE 命中全部重复行，连带修改"))

        # ---- 尺子3：对照组，有主键表 ----
        F.sql_exec(CFG, ["DELETE FROM dup_pk WHERE id=1",
                         "UPDATE dup_pk SET c2='y' WHERE id=2"], db=SRC_DB)
        src_p = counts(SRC_DB, "dup_pk", "1=1")
        tgt_p = wait_converge(lambda: counts(TGT_DB, "dup_pk", "1=1"))
        src_py = counts(SRC_DB, "dup_pk", "c2='y'")
        tgt_py = counts(TGT_DB, "dup_pk", "c2='y'")
        ok = (src_p == tgt_p and src_py == tgt_py)
        (passed if ok else failed).append(
            f"对照组（有主键表）：行数 {src_p}/{tgt_p}，改中行数 {src_py}/{tgt_py}")
    finally:
        try:
            F.stop_task(token, task_id)
            F.delete_task(token, task_id)
        except Exception:
            pass

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
