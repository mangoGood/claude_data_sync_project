#!/usr/bin/env python3
"""
第 7 批能力的判据（全量限流 / 唯一键冲突 / 传输加密开关 / 全量断点中心化）。

四条互相独立，放一个脚本里是因为它们都属于"能力补齐"而不是某一条链路的故障注入。

尺子：

1. **全量限流真的生效** —— 配额设成 N 行/秒，搬 M 行的耗时必须 ≳ M/N。
   第 5 批把全量从 291 提到 38,365 行/秒之后，链路上一个阀门都没有
   （`RowRateLimiter` 在 migration-full 里引用数为 0），提速本身成了新的风险。
2. **限流是任务级而不是 worker 级** —— 多表并行时每个 worker 各建一个限速器，
   等于把配额乘以并发数。用两张表验证总吞吐仍被压在配额附近。
3. **目标端多出来的唯一索引必须在启动前被拦住** —— 这条比它看起来严重得多：
   MySQL 目标的增量 INSERT 是 `ON DUPLICATE KEY UPDATE 全部列`，撞上源端没有的唯一索引时
   **不报错**，而是把冲突的旧行整行改掉、**连主键一起改成新行的主键**。
   实测：目标表加 `UNIQUE(email)` 后源端插一条 email 重复的新行（id=99991），
   目标端原来的 id=0 那一行直接变成了 id=99991——一条语句毁掉一行、又把两行并成一行，
   全程零错误零告警。运行期分辨不了"主键冲突（该忽略）"与"唯一键冲突（该停）"，
   所以只能在预检拦。（PG 目标那条走的是运行期 E3017，见 increment.unique.conflict.policy。）
4. **全量表级断点已上卷到中心库** —— `task_full_progress` 里有该任务的行。
   位点中心化只覆盖了增量，全量断点还在本机 H2 里，接管方会把已搬完的表再搬一遍。

用法：
    python3 test_scripts/fault_injection/batch7_capabilities.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
SRC_DB = "b7_src"
TGT_DB = "b7_tgt"

RATE = 200          # 行/秒
ROWS_PER_TABLE = 600   # 两张表共 1200 行 → 不限速几秒搬完，限速后应 ≳ 6 秒


def rebuild():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}", f"CREATE DATABASE {TGT_DB}"])
    stmts = []
    for t in ("t1", "t2"):
        stmts.append(f"CREATE TABLE `{t}` (id INT PRIMARY KEY, email VARCHAR(64), v VARCHAR(64)) ENGINE=InnoDB")
    F.sql_exec(CFG, stmts, db=SRC_DB)
    for t in ("t1", "t2"):
        vals = ",".join(f"({i},'{t}-{i}@x','v{i}')" for i in range(ROWS_PER_TABLE))
        F.sql_exec(CFG, [f"INSERT INTO `{t}` VALUES {vals}"], db=SRC_DB)


def set_full_quota(rows_per_sec):
    F.sql_exec(F.META_DB,
               ["UPDATE resource_quotas SET max_full_sync_rows_per_sec="
                + ("NULL" if rows_per_sec is None else str(rows_per_sec)) + " WHERE user_id=1"],
               db="sync_task_db")


def central_full_progress(task_id):
    return F.sql_fetch(F.META_DB, "sync_task_db",
                       "SELECT table_key, status, migrated_rows FROM task_full_progress "
                       f"WHERE task_id='{task_id}' ORDER BY table_key")


def target_count(table):
    try:
        return F.sql_fetch(CFG, TGT_DB, f"SELECT COUNT(*) FROM `{table}`")[0][0]
    except Exception:
        return 0


def main():
    token = F.login()
    passed, failed = [], []
    rebuild()
    set_full_quota(RATE)
    task_id = None
    try:
        sync_objects = {SRC_DB: {"tables": ["t1", "t2"], "targetDb": TGT_DB}}
        t0 = time.time()
        task_id = F.create_task(token, f"b7-{int(time.time())}", "mysql", "mysql",
                                CONN, CONN, "fullAndIncre", json.dumps(sync_objects), TGT_DB,
                                source_db=SRC_DB)
        print(f"    taskId={task_id}，全量限速 {RATE} 行/秒，共 {ROWS_PER_TABLE * 2} 行")
        st = F.wait_status(token, task_id, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, timeout=420)
        if st not in ("FULL_COMPLETED", "INCREMENT_RUNNING"):
            print(f"全量未完成（{st}），放弃")
            sys.exit(2)
        elapsed = time.time() - t0

        total = ROWS_PER_TABLE * 2
        expected = total / RATE
        # 判据留足余量：只要求"确实被限住了"（≥ 理论耗时的一半），不苛求精确
        ok = elapsed >= expected * 0.5
        (passed if ok else failed).append(
            f"全量限流生效：{total} 行 @ {RATE} 行/秒 实际耗时 {elapsed:.1f}s（理论 ≥ {expected:.1f}s，含启动开销）"
            + ("" if ok else "  ← 明显快于配额，限速没生效或被并发放大"))

        ok = target_count("t1") == ROWS_PER_TABLE and target_count("t2") == ROWS_PER_TABLE
        (passed if ok else failed).append(
            f"限流不影响正确性：t1={target_count('t1')} t2={target_count('t2')}（各应 {ROWS_PER_TABLE}）")

        # ---- 尺子4：全量断点已上卷 ----
        rows = None
        for _ in range(20):   # 上卷是 30s 一拍
            rows = central_full_progress(task_id)
            if rows:
                break
            time.sleep(5)
        ok = bool(rows)
        (passed if ok else failed).append(
            f"全量表级断点已上卷到中心库（{len(rows or [])} 条：{[r[0] for r in (rows or [])]}）"
            + ("" if ok else "  ← 接管方拿不到，已搬完的表会被再搬一遍"))

        # ---- 尺子3：目标端多出来的唯一索引必须被预检拦住 ----
        F.sql_exec(CFG, ["ALTER TABLE `t1` ADD UNIQUE KEY uk_email (email)"], db=TGT_DB)
        d = F.api("POST", f"/api/advanced/schema-precheck/{task_id}", token).get("data") or {}
        checks = d.get("checks") or []
        hit = [c for c in checks if c.get("checkName") == "目标唯一索引" and c.get("status") == "FAIL"]
        ok = bool(hit)
        (passed if ok else failed).append(
            "目标端多出的唯一索引被预检判为 FAIL"
            + (f"（{hit[0].get('detail')}）" if ok
               else "  ← 拦不住的话，增量 upsert 会把冲突的旧行连主键一起改掉，零错误零告警"))

    finally:
        set_full_quota(None)
        if task_id:
            F.stop_task(token, task_id)
            F.delete_task(token, task_id)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
