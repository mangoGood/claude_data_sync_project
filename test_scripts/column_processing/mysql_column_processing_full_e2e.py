#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mysql -> mysql 列处理（列过滤 / 列名映射 / 附加列）全量端到端测试。

直接驱动 migration-full（com.migration.full.Main，读 files/<taskId>/config.properties），
在 synctask-mysql(33306) 上做 cp_src -> cp_tgt 全量同步，覆盖：
  - 列过滤 amount < 100 排除（含边界 99/100、零、负数、NULL 保留、DECIMAL 小数边界）
  - 列名映射 note -> remark（建表期改列名）
  - 附加列 sync_time(CREATE_TIME, DATETIME DEFAULT CURRENT_TIMESTAMP)、
            src_tag(CUSTOM -> 'prod@cp_src@orders')
  - 大数据量（5000 行）过滤 + 映射 + 附加列
  - unicode 值保真

前置：synctask-mysql 容器在跑（docker-compose-synctask.yml）；migration-full fat jar 已 package。
用法：python3 test_scripts/column_processing/mysql_column_processing_full_e2e.py
"""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
CT = "synctask-mysql"
SRC_DB = "cp_src"
TGT_DB = "cp_tgt"
TASK = "cp-mysql-full"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def mysql(sql, db=None, want=False):
    args = ["docker", "exec", "-i", CT, "mysql", "-uroot", "-prootpassword",
            "--default-character-set=utf8mb4"]
    if want:
        args.append("-N")  # skip column names for scalar reads
    if db:
        args += ["-D", db]
    p = subprocess.run(args, input=sql, capture_output=True, text=True, timeout=180)
    if p.returncode != 0 and "Using a password" not in p.stderr:
        # mysql 客户端的密码告警走 stderr，忽略；其余错误打印
        if p.stderr.strip() and "password on the command line" not in p.stderr:
            pass
    return (p.stdout or "").strip()


def scalar(sql, db):
    return mysql(sql, db=db, want=True).strip()


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)
    url = "jdbc:mysql://localhost:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    cfg = f"""source.db.type=mysql
source.db.host=localhost
source.db.port=33306
source.db.username=root
source.db.password=rootpassword
source.db.database=
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url={url}
target.db.type=mysql
target.db.host=localhost
target.db.port=33306
target.db.username=root
target.db.password=rootpassword
target.db.database=
target.db.jdbc.driver=com.mysql.cj.jdbc.Driver
target.db.jdbc.url={url}
target.db.quote.char=`
migration.included.databases={SRC_DB}
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
    mysql(f"DROP DATABASE IF EXISTS {SRC_DB}; CREATE DATABASE {SRC_DB} CHARACTER SET utf8mb4;")
    mysql(f"DROP DATABASE IF EXISTS {TGT_DB}; CREATE DATABASE {TGT_DB} CHARACTER SET utf8mb4;")
    mysql("""CREATE TABLE orders(
      id INT PRIMARY KEY,
      amount DECIMAL(20,2),
      note VARCHAR(255),
      ts DATETIME
    ) CHARACTER SET utf8mb4;""", db=SRC_DB)
    # 10 条边界/特殊值
    rows = [
        "(1,50,'a',NULL)", "(2,99,'b',NULL)", "(3,100,'c',NULL)", "(4,101,'d',NULL)",
        "(5,0,'zero',NULL)", "(6,-5,'neg',NULL)", "(7,NULL,'nullamt',NULL)",
        "(8,99.99,'dec-lo',NULL)", "(9,100.01,'dec-hi',NULL)", "(10,250,'中文📦unicode',NULL)",
    ]
    mysql(f"INSERT INTO orders(id,amount,note,ts) VALUES {','.join(rows)};", db=SRC_DB)
    # 大数据量 5000 行：id 1000..5999，amount = id-1000（0..4999）
    big = ",".join(f"({i},{i-1000},'n{i}',NULL)" for i in range(1000, 6000))
    mysql(f"INSERT INTO orders(id,amount,note,ts) VALUES {big};", db=SRC_DB)


def run_full():
    p = subprocess.run(["java", "-cp", JAR, "com.migration.full.Main", "--task-id", TASK],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    return p.returncode == 0, (p.stdout or "") + (p.stderr or "")


def has(_id):
    return scalar(f"SELECT COUNT(*) FROM orders WHERE id={_id}", TGT_DB) == "1"


def field(_id, col):
    v = scalar(f"SELECT `{col}` FROM orders WHERE id={_id}", TGT_DB)
    return v


def main():
    if not os.path.exists(JAR):
        print(f"缺少 fat jar：{JAR}\n先执行：mvn -pl migration-full -am package -DskipTests")
        sys.exit(2)
    print("=" * 72)
    print("MySQL 列处理 全量 E2E：cp_src -> cp_tgt @ synctask-mysql(33306)")
    print("=" * 72)

    seed()
    sc = scalar("SELECT COUNT(*) FROM orders", SRC_DB)
    record("源 cp_src.orders 种子 5010 行", sc == "5010", f"count={sc}")

    write_config()
    ok, log = run_full()
    if not ok:
        record("migration-full 全量退出码 0", False, log[-500:])
        _summary()
        return
    record("migration-full 全量退出码 0", True)

    # 目标表存在
    exists = scalar(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='{TGT_DB}' AND table_name='orders'", TGT_DB)
    record("目标 cp_tgt.orders 已建表", exists == "1")

    # 总行数：保留 = 边界5(3,4,7,9,10) + 大数据量4900(amount>=100) = 4905
    tc = scalar("SELECT COUNT(*) FROM orders", TGT_DB)
    record("过滤后目标 4905 行（排除 amount<100；NULL 保留）", tc == "4905", f"count={tc}")

    # 边界/特殊值
    record("边界：amount=100 保留(id=3)", has(3))
    record("边界：amount=99 排除(id=2)", not has(2))
    record("特殊：amount=0 排除(id=5)", not has(5))
    record("特殊：amount=-5 排除(id=6)", not has(6))
    record("特殊：amount=NULL 保留(id=7)", has(7))
    record("DECIMAL 99.99 排除(id=8)", not has(8))
    record("DECIMAL 100.01 保留(id=9)", has(9))

    # 列名映射：目标有 remark 列、无 note 列
    cols = mysql(f"SELECT column_name FROM information_schema.columns WHERE table_schema='{TGT_DB}' AND table_name='orders'", want=True)
    colset = set(c.strip() for c in cols.split("\n") if c.strip())
    record("列名映射：目标含 remark 列", "remark" in colset, f"cols={sorted(colset)}")
    record("列名映射：目标不含 note 列", "note" not in colset)
    record("列名映射值正确(id=3 remark=c)", field(3, "remark") == "c")
    record("unicode 值保真(id=10 remark=中文📦unicode)", field(10, "remark") == "中文📦unicode")

    # 附加列
    record("附加列：目标含 sync_time 列", "sync_time" in colset)
    record("附加列：目标含 src_tag 列", "src_tag" in colset)
    record("附加列 src_tag = prod@cp_src@orders(id=4)", field(4, "src_tag") == "prod@cp_src@orders")
    st = field(3, "sync_time")
    record("附加列 sync_time 非空(id=3)", st not in ("", "NULL"), f"sync_time={st}")

    # 大数据量
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
