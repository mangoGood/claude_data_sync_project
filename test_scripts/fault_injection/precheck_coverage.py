#!/usr/bin/env python3
"""
对象级预检的**源类型覆盖面**与传输加密项的判据。

第 6 批把预检做成了后端门禁，但对象级检查当时只覆盖 MySQL/TiDB 与 PostgreSQL——
Oracle 与 MongoDB 源仍然是一句"暂不支持该源类型，已跳过"，也就是这两条链路的
对象级预检覆盖率仍然是 0。这个脚本盯的就是"别再退回去"。

五把尺子：

1. **Oracle 源产出实质检查项** —— 而不是"已跳过"。至少要有对象存在性与增量主键。
2. **Oracle 的补充日志被检查到** —— 没有最小补充日志时，LogMiner 的 UPDATE/DELETE
   不带行标识，增量表现为"位点一直推进、目标端一行不动"，是最难查的一类。
3. **MongoDB 源产出实质检查项**，且**副本集**被检查到 —— Change Streams 只在副本集/
   分片集群可用，单机 mongod 上增量任务起得来但一条变更都收不到。
4. **不存在的集合/表被判 FAIL** —— 证明检查项真的在查库，而不是只返回一堆 PASS。
5. **传输加密项覆盖控制面** —— 检查项的明细里必须同时提到数据面与控制面
   （后端直连用户库、元数据库、Kafka）。数据面加密而控制面明文，
   等于同一批数据换条路又明文走了一遍。

用法：
    python3 test_scripts/fault_injection/precheck_coverage.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

MYSQL_CONN = "mysql://root:rootpassword@127.0.0.1:33306"
ORACLE_CONN = "oracle://app_user:userpassword@127.0.0.1:1521/FREEPDB1"
MONGO_CONN = "mongodb://root:rootpassword@127.0.0.1:27117"


def create_unlaunched(token, name, src_type, src_conn, sync_objects, src_db, tgt_type="mysql"):
    r = F.api("POST", "/api/workflows", token,
              json={"name": name, "sourceType": src_type, "targetType": tgt_type, "taskType": "SYNC"})
    task_id = r["data"]["id"]
    F.api("PUT", f"/api/workflows/{task_id}/config", token, json={
        "sourceConnection": src_conn,
        "targetConnection": MONGO_CONN if tgt_type == "mongodb" else MYSQL_CONN,
        "migrationMode": "fullAndIncre", "syncObjects": json.dumps(sync_objects),
        "sourceDbName": src_db, "targetDbName": src_db,
        "sourceType": src_type, "targetType": tgt_type,
    })
    return task_id


def precheck(token, task_id):
    return F.api("POST", f"/api/advanced/schema-precheck/{task_id}", token).get("data") or {}


def names(d):
    return [c.get("checkName") for c in (d.get("checks") or [])]


def skipped(d):
    return any("暂不支持该源类型" in str(c.get("message", "")) or "仅支持 MySQL" in str(c.get("message", ""))
               for c in (d.get("checks") or []))


def main():
    token = F.login()
    passed, failed = [], []
    created = []
    try:
        # ---- Oracle ----
        ora = create_unlaunched(token, f"pcov-ora-{int(time.time())}", "oracle", ORACLE_CONN,
                                {"APP_USER": {"tables": ["ORA_TABLE_THAT_DOES_NOT_EXIST"]}},
                                "APP_USER")
        created.append(ora)
        d = precheck(token, ora)
        ok = not skipped(d) and len(names(d)) > 1
        (passed if ok else failed).append(
            f"Oracle 源产出实质预检项（{names(d)}）"
            + ("" if ok else "  ← 仍是一句「已跳过」，该链路对象级预检覆盖率为 0"))

        ok = "补充日志" in names(d)
        (passed if ok else failed).append(
            "Oracle 补充日志被检查到"
            + ("" if ok else "  ← 缺它时增量会「位点一直推进、目标端一行不动」，只有跑起来才发现"))

        ok = any(c.get("checkName") == "源库对象存在性" and c.get("status") == "FAIL"
                 for c in (d.get("checks") or []))
        (passed if ok else failed).append(
            "Oracle 不存在的表被判 FAIL（证明确实在查数据字典，不是一路 PASS）")

        # ---- MongoDB ----
        mon = create_unlaunched(token, f"pcov-mongo-{int(time.time())}", "mongodb", MONGO_CONN,
                                {"pcov_db": {"tables": ["coll_that_does_not_exist"]}},
                                "pcov_db", tgt_type="mongodb")
        created.append(mon)
        d2 = precheck(token, mon)
        ok = not skipped(d2) and len(names(d2)) > 1
        (passed if ok else failed).append(
            f"MongoDB 源产出实质预检项（{names(d2)}）"
            + ("" if ok else "  ← 仍是一句「已跳过」"))

        ok = "副本集/分片集群" in names(d2)
        (passed if ok else failed).append(
            "MongoDB 副本集被检查到"
            + ("" if ok else "  ← 单机 mongod 上增量起得来但一条变更都收不到"))

        # ---- 传输加密覆盖控制面 ----
        mysql_task = create_unlaunched(token, f"pcov-tls-{int(time.time())}", "mysql", MYSQL_CONN,
                                       {"sync_task_db": {"tables": ["workflows"]}}, "sync_task_db")
        created.append(mysql_task)
        d3 = precheck(token, mysql_task)
        tls = [c for c in (d3.get("checks") or []) if c.get("checkName") == "传输加密"]
        detail = tls[0].get("detail", "") if tls else ""
        ok = bool(tls) and "控制面" in detail and "元数据库" in detail and "Kafka" in detail
        (passed if ok else failed).append(
            f"传输加密项覆盖控制面（{detail[:80]}…）"
            + ("" if ok else "  ← 只报了数据面；控制面（后端直连用户库/元数据库/Kafka）没被算进去"))
    finally:
        for t in created:
            F.stop_task(token, t)
            F.delete_task(token, t)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
