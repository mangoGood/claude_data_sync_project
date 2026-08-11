#!/usr/bin/env python3
"""
DDL 爆炸半径判据（mysql→mysql 增量，跨实例，两端库名一致）。

背景：`SchemaEvolutionService#applyDdl` 的表级同步守卫只挡「源库**不在**同步范围」的 DDL：

    else if (sourceDb != null && !includedDatabases.contains(sourceDb.toLowerCase())) { skip }

范围**之内**的库级 DDL（`DROP DATABASE <同步中的源库>` / `CREATE DATABASE`）既不被这道守卫拦，
`isTableScopedDdl()` 也不认它（只列了 CREATE/ALTER/DROP TABLE、TRUNCATE、RENAME、*_INDEX），
于是一路落到执行处，**原样**（库名不改写）打到目标连接上，日志还写「DDL 应用成功」。

三种现实场景下这就是灾难：

  * 灾备/双向（两端库名一致，这是倒换的前提）→ 主库一条 `DROP DATABASE app` 把**整个备库**删干净；
  * 同实例迁移 / 库名映射（src 与 tgt 在同一个实例）→ 直接把**源库**删掉；
  * 多任务共用目标实例、目标端恰有同名库 → 删掉别的任务的数据。

本脚本用 docker-compose-synctask-dr.yml 的两个实例（33320 / 33321）复现最坏的那一种：
**两端同名库**，表级同步只选了一张表，源端 DROP DATABASE。

四把尺子：

1. **对照：清单内表的 ALTER 照常同步** —— 否则后面三条都可能只是"DDL 同步压根没工作"。
2. **对照：范围外库的 DDL 仍被拦** —— 既有守卫没坏。
3. **源端 CREATE DATABASE 不得在目标实例造库**。
4. **源端 `DROP DATABASE <同步中的源库>` 不得删掉目标实例上的同名库**（含库里那张**不在**同步
   范围的表——它证明被毁掉的不只是"本来就在同步的那份数据"）。

前置：
    docker compose -f docker-compose-synctask-dr.yml up -d dr-mysql-a dr-mysql-b

用法：
    python3 test_scripts/fault_injection/ddl_blast_radius.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

A = dict(host="127.0.0.1", port=33320, user="root", password="rootpassword")
B = dict(host="127.0.0.1", port=33321, user="root", password="rootpassword")
CONN_A = "mysql://root:rootpassword@127.0.0.1:33320"
CONN_B = "mysql://root:rootpassword@127.0.0.1:33321"

DB = "blast_db"          # 两端同名（灾备语义）
OUT_DB = "blast_outside"  # 同步范围之外
NEW_DB = "blast_created"  # 源端新建
TABLE = "blast_t"
BYSTANDER = "bystander"   # 目标端同库里、**不在**同步范围的表


def databases(cfg):
    return {r[0] for r in F.sql_fetch(cfg, None, "SHOW DATABASES")}


def rebuild():
    for cfg in (A, B):
        F.sql_exec(cfg, [f"DROP DATABASE IF EXISTS {d}" for d in (DB, OUT_DB, NEW_DB)])
    F.sql_exec(A, [f"CREATE DATABASE {DB}", f"CREATE DATABASE {OUT_DB}"])
    F.sql_exec(B, [f"CREATE DATABASE {DB}"])
    F.sql_exec(A, [
        f"CREATE TABLE `{TABLE}` (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB",
        f"INSERT INTO `{TABLE}` VALUES (1,'a'),(2,'b')",
    ], db=DB)
    F.sql_exec(A, ["CREATE TABLE outside_t (id INT PRIMARY KEY) ENGINE=InnoDB"], db=OUT_DB)
    # 目标库里放一张与本任务无关的表：它被毁掉最能说明"爆炸半径超出了任务范围"
    F.sql_exec(B, [f"CREATE TABLE `{BYSTANDER}` (id INT PRIMARY KEY) ENGINE=InnoDB",
                   f"INSERT INTO `{BYSTANDER}` VALUES (1),(2),(3)"], db=DB)


def target_columns():
    rows = F.sql_fetch(B, None,
                       "SELECT column_name FROM information_schema.columns "
                       f"WHERE table_schema='{DB}' AND table_name='{TABLE}'")
    return {r[0].lower() for r in rows}


def table_exists(cfg, db, table):
    rows = F.sql_fetch(cfg, None,
                       "SELECT COUNT(*) FROM information_schema.tables "
                       f"WHERE table_schema='{db}' AND table_name='{table}'")
    return rows[0][0] > 0


def main():
    token = F.login()
    rebuild()
    sync_objects = {DB: {"tables": [TABLE], "targetDb": DB}}
    task_id = F.create_task(token, f"ddl-blast-{int(time.time())}", "mysql", "mysql",
                            CONN_A, CONN_B, "fullAndIncre", json.dumps(sync_objects), DB, source_db=DB)
    print(f"    taskId={task_id}  A=33320(源) B=33321(目标)，两端库名同为 {DB}")
    passed, failed = [], []
    try:
        if F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=300) != "INCREMENT_RUNNING":
            print("任务未进入增量，放弃")
            sys.exit(2)
        time.sleep(5)

        # ---- 尺子1：清单内表的 ALTER 应照常同步 ----
        F.sql_exec(A, [f"ALTER TABLE `{TABLE}` ADD COLUMN extra INT NULL"], db=DB)
        deadline = time.time() + 60
        while time.time() < deadline and "extra" not in target_columns():
            time.sleep(2)
        ok = "extra" in target_columns()
        (passed if ok else failed).append(
            "对照：清单内表的 ALTER 照常同步" + ("" if ok else "  ← DDL 同步本身没工作，后面几把尺子失去意义"))

        # ---- 尺子2：范围外库的 DDL 必须被拦 ----
        F.sql_exec(A, ["DROP TABLE outside_t"], db=OUT_DB)
        time.sleep(8)
        ok = OUT_DB not in databases(B) or not table_exists(B, OUT_DB, "outside_t")
        (passed if ok else failed).append("对照：范围外库的 DDL 未波及目标实例（既有守卫有效）")

        # ---- 尺子3：源端 CREATE DATABASE 不得在目标实例造库 ----
        F.sql_exec(A, [f"CREATE DATABASE {NEW_DB}"])
        time.sleep(10)
        ok = NEW_DB not in databases(B)
        (passed if ok else failed).append(
            f"源端 CREATE DATABASE 未在目标实例造出 {NEW_DB}"
            + ("" if ok else "  ← 库级 DDL 原样打到了目标实例"))

        # ---- 尺子4：源端 DROP DATABASE 不得删掉目标实例上的同名库 ----
        b_before = databases(B)
        F.sql_exec(A, [f"DROP DATABASE {DB}"])
        time.sleep(15)
        b_after = databases(B)
        db_ok = DB in b_after
        bystander_ok = db_ok and table_exists(B, DB, BYSTANDER)
        ok = db_ok and bystander_ok
        (passed if ok else failed).append(
            f"源端 DROP DATABASE 未删掉目标实例的同名库 {DB}（含无关表 {BYSTANDER}）"
            + ("" if ok else "  ← 目标库被整个删掉，日志里写的是「DDL 应用成功」；"
                             "灾备两端库名一致，这一条等于主库一句话清空整个备库"))
        print(f"    目标实例库变化: 消失 {sorted(b_before - b_after)}")
    finally:
        F.stop_task(token, task_id)
        F.delete_task(token, task_id)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
