#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
binlog 边界形态判据（capture → extract → increment 三进程真链路）。

四类源端形态，改造前每一类都是**静默**出问题——数据没到目标库、任务却显示健康：

  ① 压缩 binlog（binlog_transaction_compression=ON）：整个事务被打包成一个
     TRANSACTION_PAYLOAD 事件，连接器只投外层 → 整事务丢失
  ② JSON 差量（binlog_row_value_options=PARTIAL_JSON）：MySQL 发
     PARTIAL_UPDATE_ROWS_EVENT，连接器不认识 → UPDATE 丢失
  ③ 生成列（STORED/VIRTUAL）：行事件带生成列的值，apply 照单全收去 INSERT
     → MySQL 3105 fail-stop
  ④ 事务中间的 SAVEPOINT：extract 把它当 DDL 清掉 tx_id
     → 事务一致模式下源事务被切成两个目标事务

每个用例独占一套任务目录与三进程（②会把 capture 停掉，不能和别人共用）。

用法：
    python3 test_scripts/edge/edge_e2e.py
    python3 test_scripts/edge/edge_e2e.py --only compression
"""
import argparse
import sys
import time

import edgelib as E

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def boot(pipe, table, tgt_db, marker_id, wait=180):
    """起三进程并用一行普通数据确认链路通了。

    自检窗口给得很宽：三个进程冷启动要各自连库、建心跳表、读 information_schema，
    容器刚重启时首轮能到几十秒。这里等的是"链路通不通"，不是延迟指标，宽一点不影响判据。
    """
    pipe.start_all()
    time.sleep(8)
    E.mysql(E.SRC_CT, f"INSERT INTO {table} (id) VALUES ({marker_id})", db=table_db[0])
    return pipe.wait_until(lambda: marker_id in E.ids_of(E.TGT_CT, tgt_db, table), wait, "链路自检")


table_db = [None]


# ---------------------------------------------------------------- ① 压缩 binlog

def case_compression():
    ddl = "CREATE TABLE t_c (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
    src, tgt, tbl = "edge_c_src", "edge_c_tgt", "t_c"
    table_db[0] = src
    task_dir, pipe = E.fresh_task("edge-compression", src, tgt, tbl, ddl)
    try:
        if not boot(pipe, tbl, tgt, 1):
            record("① 压缩 binlog", False, "链路自检就没通过")
            return
        with E.SourceVar("binlog_transaction_compression", 1):
            E.mysql(E.SRC_CT, f"""SET SESSION binlog_transaction_compression=ON;
BEGIN;
INSERT INTO {tbl} VALUES (2,'zstd-a');
INSERT INTO {tbl} VALUES (3,'zstd-b');
COMMIT;""", db=src)
            ok = pipe.wait_until(
                lambda: {2, 3} <= E.ids_of(E.TGT_CT, tgt, tbl), 90, "压缩事务")
        record("① 压缩事务能同步", ok,
               "TRANSACTION_PAYLOAD 已在 capture 侧拆包"
               if ok else f"目标库只有 {sorted(E.ids_of(E.TGT_CT, tgt, tbl))}")
        record("① 压缩事务无错误上报", E.error_status(task_dir) == "",
               E.error_status(task_dir)[:120] or "error_status 为空")
    finally:
        pipe.stop_all()


# ---------------------------------------------------------------- ② PARTIAL_JSON

def case_partial_json():
    ddl = "CREATE TABLE t_j (id INT PRIMARY KEY, j JSON, v VARCHAR(32)) ENGINE=InnoDB"
    src, tgt, tbl = "edge_j_src", "edge_j_tgt", "t_j"
    table_db[0] = src
    task_dir, pipe = E.fresh_task("edge-partial-json", src, tgt, tbl, ddl)
    try:
        if not boot(pipe, tbl, tgt, 1):
            record("② PARTIAL_JSON", False, "链路自检就没通过")
            return
        big = '{"a":"' + "x" * 300 + '","b":1}'
        E.mysql(E.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'{big}','init')", db=src)
        pipe.wait_until(lambda: 2 in E.ids_of(E.TGT_CT, tgt, tbl), 60, "初始行")

        with E.SourceVar("binlog_row_value_options", "PARTIAL_JSON"):
            E.mysql(E.SRC_CT, f"""SET SESSION binlog_row_value_options='PARTIAL_JSON';
UPDATE {tbl} SET j=JSON_SET(j,'$.b',999), v='patched' WHERE id=2;""", db=src)
            # 判据不是"能同步"（连接器确实不支持 JSON 差量），而是"绝不能静默丢"：
            # 必须停下来上报，让人去把源端参数关掉
            reported = pipe.wait_until(
                lambda: any(c in E.error_status(task_dir) for c in ("E3019", "E3020")),
                90, "上报 E3019/E3020")
        err = E.error_status(task_dir)
        record("② PARTIAL_JSON 不再静默丢弃", reported,
               err.split("|")[1] + " 已上报" if reported and "|" in err else f"error_status={err[:140] or '(空)'}")
        tgt_v = E.scalar(E.TGT_CT, f"SELECT v FROM {tbl} WHERE id=2", tgt)
        record("② 丢失的 UPDATE 没被当成成功", tgt_v == "init",
               "目标库仍是改前的值且任务已停（人工处置后重放）"
               if tgt_v == "init" else f"目标库 v={tgt_v}")
    finally:
        pipe.stop_all()


# ---------------------------------------------------------------- ③ 生成列

def case_generated_columns():
    ddl = """CREATE TABLE t_g (
  id INT PRIMARY KEY,
  a INT,
  b INT AS (a*2) STORED,
  c INT AS (a+1) VIRTUAL,
  d VARCHAR(32)
) ENGINE=InnoDB"""
    src, tgt, tbl = "edge_g_src", "edge_g_tgt", "t_g"
    table_db[0] = src
    task_dir, pipe = E.fresh_task("edge-generated", src, tgt, tbl, ddl)
    try:
        pipe.start_all()
        time.sleep(8)
        E.mysql(E.SRC_CT, f"INSERT INTO {tbl} (id,a,d) VALUES (1,10,'x')", db=src)
        ok = pipe.wait_until(
            lambda: E.scalar(E.TGT_CT, f"SELECT COUNT(*) FROM {tbl}", tgt) == "1", 90, "生成列 INSERT")
        record("③ 生成列表 INSERT 能同步", ok,
               E.error_status(task_dir)[:140] or "无错误上报")

        if ok:
            E.mysql(E.SRC_CT, f"UPDATE {tbl} SET a=50 WHERE id=1", db=src)
            upd = pipe.wait_until(
                lambda: E.scalar(E.TGT_CT, f"SELECT a FROM {tbl} WHERE id=1", tgt) == "50", 90, "UPDATE")
            record("③ 生成列表 UPDATE 能同步", upd)
            gen = E.scalar(E.TGT_CT, f"SELECT CONCAT(b,'/',c) FROM {tbl} WHERE id=1", tgt)
            record("③ 生成列由目标库自己算", gen == "100/51",
                   f"目标库 b/c = {gen}（应为 100/51，由目标库按表达式算出）")

            E.mysql(E.SRC_CT, f"DELETE FROM {tbl} WHERE id=1", db=src)
            dele = pipe.wait_until(
                lambda: E.scalar(E.TGT_CT, f"SELECT COUNT(*) FROM {tbl}", tgt) == "0", 90, "DELETE")
            record("③ 生成列表 DELETE 能同步", dele)
    finally:
        pipe.stop_all()


# ---------------------------------------------------------------- ④ SAVEPOINT

def case_savepoint():
    ddl = "CREATE TABLE t_s (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
    src, tgt, tbl = "edge_s_src", "edge_s_tgt", "t_s"
    table_db[0] = src
    task_dir, pipe = E.fresh_task("edge-savepoint", src, tgt, tbl, ddl)
    try:
        if not boot(pipe, tbl, tgt, 1):
            record("④ SAVEPOINT", False, "链路自检就没通过")
            return

        # 事务：savepoint 之前 1 行、之后 3 行。事务一致模式下四行必须一起出现；
        # 只要观测到"部分到达"，就说明 savepoint 把源事务切开了
        E.mysql(E.SRC_CT, f"""BEGIN;
INSERT INTO {tbl} VALUES (10,'before-sp');
SAVEPOINT sp1;
INSERT INTO {tbl} VALUES (11,'after-sp');
INSERT INTO {tbl} VALUES (12,'after-sp');
INSERT INTO {tbl} VALUES (13,'after-sp');
COMMIT;""", db=src)

        want = {10, 11, 12, 13}
        partial = []
        deadline = time.time() + 90
        while time.time() < deadline:
            got = want & E.ids_of(E.TGT_CT, tgt, tbl)
            if got and len(got) < 4:
                partial.append(sorted(got))
            if len(got) == 4:
                break
            time.sleep(0.05)
        arrived = want <= E.ids_of(E.TGT_CT, tgt, tbl)
        record("④ 带 SAVEPOINT 的事务能同步", arrived)
        record("④ SAVEPOINT 不切开源事务", arrived and not partial,
               "高频采样未观测到半个事务" if not partial else f"观测到中间态 {partial[:3]}")
        record("④ SAVEPOINT 语句不打到目标库", E.error_status(task_dir) == "",
               E.error_status(task_dir)[:140] or "无错误上报")
    finally:
        pipe.stop_all()


# ------------------------------------------------- ③b 生成列（全量）

def case_generated_full():
    """全量搬运也不能把生成列写进目标库（同引擎 mysql→mysql 目标端那些列还是生成列）。"""
    import os
    import shutil
    import subprocess

    ddl = """CREATE TABLE t_gf (
  id INT PRIMARY KEY,
  a INT,
  b INT AS (a*2) STORED,
  c INT AS (a+1) VIRTUAL,
  d VARCHAR(32)
) ENGINE=InnoDB"""
    src, tgt, tbl = "edge_gf_src", "edge_gf_tgt", "t_gf"
    task = "edge-generated-full"
    task_dir = os.path.join(E.PROJECT_ROOT, "files", task)

    E.reset_db(E.SRC_CT, src)
    E.reset_db(E.TGT_CT, tgt)
    E.create(E.SRC_CT, src, ddl)
    E.create(E.TGT_CT, tgt, ddl)
    E.mysql(E.SRC_CT, f"INSERT INTO {tbl} (id,a,d) VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')", db=src)

    shutil.rmtree(task_dir, ignore_errors=True)
    os.makedirs(task_dir, exist_ok=True)
    src_url = (f"jdbc:mysql://localhost:{E.SRC_PORT}/?useSSL=false&serverTimezone=UTC"
               "&characterEncoding=utf8&allowPublicKeyRetrieval=true")
    tgt_url = (f"jdbc:mysql://localhost:{E.TGT_PORT}/?useSSL=false&serverTimezone=UTC"
               "&characterEncoding=utf8&allowPublicKeyRetrieval=true")
    cfg = f"""task.id={task}
source.db.type=mysql
source.db.host=localhost
source.db.port={E.SRC_PORT}
source.db.username=root
source.db.password={E.PWD}
source.db.database={src}
source.db.jdbc.url={src_url}
target.db.type=mysql
target.db.host=localhost
target.db.port={E.TGT_PORT}
target.db.username=root
target.db.password={E.PWD}
target.db.database={tgt}
target.db.jdbc.url={tgt_url}
target.db.quote.char=`
migration.included.databases={src}
migration.included.tables={src}.{tbl}
migration.sync.objects={{"{src}":{{"tables":["{tbl}"],"targetDb":"{tgt}"}}}}
schema.mapping.db.{src}={tgt}
migration.full.parallelism=1
migration.enable.resume=true
"""
    with open(os.path.join(task_dir, "config.properties"), "w") as f:
        f.write(cfg)

    full_jar = os.path.join(E.PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
    if not os.path.isfile(full_jar):
        record("③ 全量生成列", False, "缺少 migration-full jar，先 mvn -pl migration-full -am package")
        return
    proc = subprocess.run(
        ["java", "-Dtask.id=" + task, "-cp", full_jar, "com.migration.full.Main",
         "--config", os.path.join(task_dir, "config.properties")],
        cwd=E.PROJECT_ROOT, capture_output=True, text=True, timeout=600)
    out = (proc.stdout or "") + (proc.stderr or "")

    cnt = E.scalar(E.TGT_CT, f"SELECT COUNT(*) FROM {tbl}", tgt)
    record("③ 全量：生成列表能搬完", cnt == "3",
           f"目标库 {cnt} 行" + ("" if cnt == "3" else "；" + _tail_3105(out)))
    if cnt == "3":
        gen = E.scalar(E.TGT_CT, f"SELECT GROUP_CONCAT(CONCAT(b,'/',c) ORDER BY id) FROM {tbl}", tgt)
        record("③ 全量：生成列由目标库自己算", gen == "20/11,40/21,60/31", f"目标库 b/c = {gen}")


def _tail_3105(out):
    for line in out.splitlines():
        if "3105" in line or "generated column" in line.lower():
            return line.strip()[:160]
    return out.strip().splitlines()[-1][:160] if out.strip() else ""


# ------------------------------------------------- ⑤ 积压期间 ALTER TABLE

def case_schema_drift():
    """抽取落后时做 ALTER TABLE：老事件不能按新表定义解析（会整行错位）。

    制造延迟的办法是先把 extract 停掉，让 .cap 里攒下一批老布局的事件，
    再在源库加一列，然后把 extract 放回来——它面对的就是"事件是旧布局、
    information_schema 是新布局"这个局面。
    """
    ddl = "CREATE TABLE t_d (id INT PRIMARY KEY, a VARCHAR(32)) ENGINE=InnoDB"
    src, tgt, tbl = "edge_d_src", "edge_d_tgt", "t_d"
    table_db[0] = src
    task_dir, pipe = E.fresh_task("edge-drift", src, tgt, tbl, ddl)
    try:
        if not boot(pipe, tbl, tgt, 1):
            record("⑤ schema 漂移", False, "链路自检就没通过")
            return

        pipe.stop("extract")
        time.sleep(1)
        # 老布局（2 列）的事件先进 .cap
        E.mysql(E.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'old-layout')", db=src)
        time.sleep(3)
        # 源库加一列并插新数据（新布局 3 列）
        E.mysql(E.SRC_CT, f"ALTER TABLE {tbl} ADD COLUMN b INT AFTER id", db=src)
        E.mysql(E.SRC_CT, f"INSERT INTO {tbl} VALUES (3,99,'new-layout')", db=src)
        E.mysql(E.TGT_CT, f"ALTER TABLE {tbl} ADD COLUMN b INT AFTER id", db=tgt)
        pipe.start("extract")

        ok = pipe.wait_until(lambda: {2, 3} <= E.ids_of(E.TGT_CT, tgt, tbl), 120, "两种布局的行")
        err = E.error_status(task_dir)
        if ok:
            a2 = E.scalar(E.TGT_CT, f"SELECT a FROM {tbl} WHERE id=2", tgt)
            a3 = E.scalar(E.TGT_CT, f"SELECT CONCAT(IFNULL(b,'-'),'/',a) FROM {tbl} WHERE id=3", tgt)
            record("⑤ 老布局的行没有错位", a2 == "old-layout", f"目标库 id=2 的 a={a2}")
            record("⑤ 新布局的行正确落库", a3 == "99/new-layout", f"目标库 id=3 的 b/a={a3}")
        else:
            # 拿不到列名时（binlog_row_metadata=MINIMAL）只能停下来报 E3021，这同样是通过：
            # 判据是"绝不静默错位"，不是"任何情况都能自动搬过去"
            record("⑤ 列布局对不上时停机上报", "E3021" in err, f"error_status={err[:160] or '(空)'}")
    finally:
        pipe.stop_all()


CASES = {
    "compression": case_compression,
    "partial-json": case_partial_json,
    "generated": case_generated_columns,
    "generated-full": case_generated_full,
    "savepoint": case_savepoint,
    "schema-drift": case_schema_drift,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=sorted(CASES), action="append")
    args = ap.parse_args()
    E.require_jars()

    print("=" * 72)
    print("binlog 边界形态判据（压缩 / JSON差量 / 生成列 / SAVEPOINT）")
    print("=" * 72)

    for name in (args.only or list(CASES)):
        print(f"\n--- {name} ---")
        CASES[name]()

    print("\n" + "=" * 72)
    passed = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail else ""))
    print(f"总计: {passed}/{len(results)}")
    print("=" * 72)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
