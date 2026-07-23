#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
postgresql -> postgresql 列处理（列过滤 / 列名映射 / 附加列）全量端到端测试。

直接驱动 migration-full（com.migration.full.Main），在 postgres_db(5432) 上做
cp_src -> cp_tgt 全量同步（两个数据库），覆盖：
  - 列过滤 amount < 100 排除（边界 99/100、零、负数、NULL 保留、NUMERIC 小数边界）
  - 列名映射 note -> remark（建表期改列名，PG 双引号标识符）
  - 附加列 sync_time(CREATE_TIME -> TIMESTAMP DEFAULT CURRENT_TIMESTAMP)、
            src_tag(CUSTOM -> 'prod@cp_src@orders')
  - 大数据量（5000 行）+ unicode 保真

前置：postgres_db 容器在跑（user app_user / pwd userpassword）；migration-full fat jar 已 package。
用法：python3 test_scripts/column_processing/pg_column_processing_full_e2e.py
"""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
CT = "postgres_db"
USER = "app_user"
SRC_DB = "cp_src"
TGT_DB = "cp_tgt"
TASK = "cp-pg-full"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def psql(sql, db="myapp_db", want=False):
    args = ["docker", "exec", "-i", CT, "psql", "-U", USER, "-d", db]
    if want:
        args += ["-tAc", sql]
    else:
        args += ["-c", sql]
    p = subprocess.run(args, capture_output=True, text=True, timeout=180,
                       env={**os.environ, "PGPASSWORD": "userpassword"})
    return (p.stdout or "").strip(), (p.stderr or "").strip()


def scalar(sql, db):
    out, _ = psql(sql, db=db, want=True)
    return out.strip()


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)

    def url(db):
        return f"jdbc:postgresql://localhost:5432/{db}?currentSchema=public&stringtype=unspecified"

    cfg = f"""source.db.type=postgresql
source.db.host=localhost
source.db.port=5432
source.db.database={SRC_DB}
source.db.username={USER}
source.db.password=userpassword
source.db.jdbc.driver=org.postgresql.Driver
source.db.jdbc.url={url(SRC_DB)}
target.db.type=postgresql
target.db.host=localhost
target.db.port=5432
target.db.database={TGT_DB}
target.db.username={USER}
target.db.password=userpassword
target.db.jdbc.driver=org.postgresql.Driver
target.db.jdbc.url={url(TGT_DB)}
target.db.quote.char="
migration.included.tables={SRC_DB}.orders
migration.sync.objects={{"{SRC_DB}":{{"tables":["orders"],"targetDb":"{TGT_DB}"}}}}
schema.mapping.db.{SRC_DB}={TGT_DB}
migration.full.parallelism=2
column.filter.{SRC_DB}.orders=amount|<|100
column.mapping.{SRC_DB}.orders=note:remark
column.extra.{SRC_DB}.orders=sync_time:CREATE_TIME,src_tag:CUSTOM:prod
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def seed():
    # 两个数据库需先从默认库断开连接再 drop
    psql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('{SRC_DB}','{TGT_DB}');")
    psql(f"DROP DATABASE IF EXISTS {SRC_DB};")
    psql(f"DROP DATABASE IF EXISTS {TGT_DB};")
    psql(f"CREATE DATABASE {SRC_DB};")
    psql(f"CREATE DATABASE {TGT_DB};")
    psql("""CREATE TABLE orders(
      id INT PRIMARY KEY,
      amount NUMERIC(20,2),
      note VARCHAR(255),
      ts TIMESTAMP
    );""", db=SRC_DB)
    rows = [
        "(1,50,'a',NULL)", "(2,99,'b',NULL)", "(3,100,'c',NULL)", "(4,101,'d',NULL)",
        "(5,0,'zero',NULL)", "(6,-5,'neg',NULL)", "(7,NULL,'nullamt',NULL)",
        "(8,99.99,'dec-lo',NULL)", "(9,100.01,'dec-hi',NULL)", "(10,250,'中文📦unicode',NULL)",
    ]
    psql(f"INSERT INTO orders(id,amount,note,ts) VALUES {','.join(rows)};", db=SRC_DB)
    big = ",".join(f"({i},{i-1000},'n{i}',NULL)" for i in range(1000, 6000))
    psql(f"INSERT INTO orders(id,amount,note,ts) VALUES {big};", db=SRC_DB)


def run_full():
    p = subprocess.run(["java", "-cp", JAR, "com.migration.full.Main", "--task-id", TASK],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    return p.returncode == 0, (p.stdout or "") + (p.stderr or "")


def has(_id):
    return scalar(f"SELECT COUNT(*) FROM orders WHERE id={_id}", TGT_DB) == "1"


def field(_id, col):
    return scalar(f'SELECT "{col}" FROM orders WHERE id={_id}', TGT_DB)


def main():
    if not os.path.exists(JAR):
        print(f"缺少 fat jar：{JAR}\n先执行：mvn -pl migration-full -am package -DskipTests")
        sys.exit(2)
    print("=" * 72)
    print("PostgreSQL 列处理 全量 E2E：cp_src -> cp_tgt @ postgres_db(5432)")
    print("=" * 72)

    seed()
    sc = scalar("SELECT COUNT(*) FROM orders", SRC_DB)
    record("源 cp_src.orders 种子 5010 行", sc == "5010", f"count={sc}")

    write_config()
    ok, log = run_full()
    if not ok:
        record("migration-full 全量退出码 0", False, log[-600:])
        _summary()
        return
    record("migration-full 全量退出码 0", True)

    exists = scalar(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='orders'", TGT_DB)
    record("目标 cp_tgt.orders 已建表", exists == "1")

    tc = scalar("SELECT COUNT(*) FROM orders", TGT_DB)
    record("过滤后目标 4905 行（排除 amount<100；NULL 保留）", tc == "4905", f"count={tc}")

    record("边界：amount=100 保留(id=3)", has(3))
    record("边界：amount=99 排除(id=2)", not has(2))
    record("特殊：amount=0 排除(id=5)", not has(5))
    record("特殊：amount=-5 排除(id=6)", not has(6))
    record("特殊：amount=NULL 保留(id=7)", has(7))
    record("NUMERIC 99.99 排除(id=8)", not has(8))
    record("NUMERIC 100.01 保留(id=9)", has(9))

    cols_out, _ = psql(f"SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='orders'", db=TGT_DB, want=True)
    colset = set(c.strip() for c in cols_out.split("\n") if c.strip())
    record("列名映射：目标含 remark 列", "remark" in colset, f"cols={sorted(colset)}")
    record("列名映射：目标不含 note 列", "note" not in colset)
    record("列名映射值正确(id=3 remark=c)", field(3, "remark") == "c")
    record("unicode 值保真(id=10 remark=中文📦unicode)", field(10, "remark") == "中文📦unicode")

    record("附加列：目标含 sync_time 列", "sync_time" in colset)
    record("附加列：目标含 src_tag 列", "src_tag" in colset)
    record("附加列 src_tag = prod@cp_src@orders(id=4)", field(4, "src_tag") == "prod@cp_src@orders")
    st = field(3, "sync_time")
    record("附加列 sync_time 非空(id=3)", st not in ("", "NULL"), f"sync_time={st}")

    record("大数据量：amount=99 排除(id=1099)", not has(1099))
    record("大数据量：amount=100 保留+映射(id=1100 remark=n1100)", field(1100, "remark") == "n1100")
    record("大数据量：附加列注入(id=5999 src_tag)", field(5999, "src_tag") == "prod@cp_src@orders")

    _summary()


def _summary():
    print("\n" + "=" * 72)
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} — {detail}")
    print(f"结果：{passed}/{total} 通过")
    print("=" * 72)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
