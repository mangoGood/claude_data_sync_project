#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
<b>异构库对</b>（mysql → postgresql）的聚合路由全量端到端测试。

异构此前是直接拒的，拒的理由是两处真实缺口：
  1. 汇聚的来源标识列由<b>同构</b>建表分支追加，异构分支走翻译器生成建表语句后直接 return，
     目标表根本没有这几列 —— 跑到写数据才报 "Unknown column '_src_db'"，
     报的还是个跟原因无关的列名。
  2. 拆分预建分片表读的是<b>源端</b> CREATE TABLE 文本，异构下方言不同（Oracle 源甚至没有），
     直接抛"没有建表语句，无法预建分片表"。

两处都改成走翻译器产出目标方言 DDL 之后，本脚本验证异构下汇聚与拆分都真的能跑通。

前置：synctask-mysql(33306) 与 postgres_db(5432, app_user/userpassword) 在跑；
      migration-full fat jar 已 clean install。
用法：python3 test_scripts/sharding/mysql2pg_route_e2e.py
"""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
MYSQL_CT = "synctask-mysql"
PG_CT = "postgres_db"
PG_USER, PG_PASS, PG_PORT = "app_user", "userpassword", 5432
SRC_DBS = ["x2p_shard_1", "x2p_shard_2"]
SPLIT_SRC_DB = "x2p_app"
PG_MERGE_DB = "x2p_dw"
PG_SPLIT_DB = "x2p_app"   # PG 一条连接跨不了库：分片表只能落在任务目标库里

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def mysql(sql, db=None, want=False):
    args = ["docker", "exec", "-i", MYSQL_CT, "mysql", "-uroot", "-prootpassword",
            "--default-character-set=utf8mb4"]
    if want:
        args.append("-N")
    if db:
        args += ["-D", db]
    return (subprocess.run(args, input=sql, capture_output=True, text=True, timeout=180).stdout or "").strip()


def pg(sql, db="postgres", want=False):
    args = ["docker", "exec", "-i", PG_CT, "psql", "-U", PG_USER, "-d", db]
    if want:
        args += ["-t", "-A"]
    args += ["-c", sql]
    p = subprocess.run(args, capture_output=True, text=True, timeout=180)
    return (p.stdout or "").strip()


def pg_scalar(sql, db):
    return pg(sql, db=db, want=True).strip()


def recreate_pg_db(name):
    # 连到自带的 myapp_db 上执行 DROP/CREATE（不能在要删的库里执行）
    pg(f"DROP DATABASE IF EXISTS {name};", db="myapp_db")
    pg(f"CREATE DATABASE {name};", db="myapp_db")


def setup_merge_source():
    stmts = []
    for db in SRC_DBS:
        stmts.append(f"DROP DATABASE IF EXISTS {db};")
        stmts.append(f"CREATE DATABASE {db} DEFAULT CHARACTER SET utf8mb4;")
    mysql("\n".join(stmts))
    for i, db in enumerate(SRC_DBS, start=1):
        mysql("""
CREATE TABLE order_001 (
  id BIGINT NOT NULL AUTO_INCREMENT,
  amount DECIMAL(10,2) DEFAULT NULL,
  note VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""", db=db)
        # 跨库主键完全重复：汇聚最容易丢数据的形态
        rows = ",".join(f"({r}, {r * 10 + i}.50, '{db}-{r}')" for r in (1, 2, 3))
        mysql(f"INSERT INTO order_001 (id, amount, note) VALUES {rows};", db=db)
    recreate_pg_db(PG_MERGE_DB)


def setup_split_source():
    mysql(f"DROP DATABASE IF EXISTS {SPLIT_SRC_DB}; CREATE DATABASE {SPLIT_SRC_DB} DEFAULT CHARACTER SET utf8mb4;")
    mysql("""
CREATE TABLE orders (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  amount DECIMAL(10,2) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""", db=SPLIT_SRC_DB)
    mysql("INSERT INTO orders (id, user_id, amount) VALUES "
          + ",".join(f"({i},{i},{i * 10}.00)" for i in range(1, 13)) + ";", db=SPLIT_SRC_DB)
    recreate_pg_db(PG_SPLIT_DB)


def write_config(task, src_dbs, included, target_db, route_lines):
    d = os.path.join(PROJECT_ROOT, "files", task)
    os.makedirs(d, exist_ok=True)
    my_url = "jdbc:mysql://localhost:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    pg_url = f"jdbc:postgresql://localhost:{PG_PORT}/{target_db}?currentSchema=public&stringtype=unspecified"
    cfg = f"""source.db.type=mysql
source.db.host=localhost
source.db.port=33306
source.db.username=root
source.db.password=rootpassword
source.db.database=
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url={my_url}
target.db.type=postgresql
target.db.host=localhost
target.db.port={PG_PORT}
target.db.username={PG_USER}
target.db.password={PG_PASS}
target.db.database={target_db}
target.db.schema=public
target.db.jdbc.driver=org.postgresql.Driver
target.db.jdbc.url={pg_url}
target.db.quote.char="
migration.included.databases={",".join(src_dbs)}
migration.included.tables={included}
migration.create.tables=true
migration.drop.tables=false
migration.migrate.data=true
migration.batch.size=500
migration.enable.resume=true
migration.continue.on.error=false
migration.record.checkpoint=false
route.node.id=mysql-33306
{route_lines}
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)
    for name in os.listdir(d):
        if name.startswith("migration_progress"):
            os.remove(os.path.join(d, name))


def run_full(task):
    p = subprocess.run(["java", "-Dh2.bindAddress=127.0.0.1", f"-Dtask.id={task}", "-jar", JAR],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=600)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def main():
    if not os.path.exists(JAR):
        print("缺少 fat jar，请先 mvn clean install -DskipTests -pl migration-common,migration-full -am")
        return 2
    if "1" not in pg_scalar("SELECT 1;", "myapp_db"):
        print("postgres_db 不可用，跳过")
        return 2

    print("== 异构汇聚：mysql 2 个分库 → pg 一张合并表 ==")
    setup_merge_source()
    # PG 一条连接跨不了库：汇聚目标库必须显式写成任务的目标库，
    # 省略时会退回"每个源库各自的默认目标库"，结果是每个源库建一张自己的合并表
    write_config("x2p-merge", SRC_DBS,
                 ",".join(f"{db}.order_001" for db in SRC_DBS), PG_MERGE_DB,
                 f"""route.mode=MERGE
route.merge.1.match=x2p_shard_*.order_001
route.merge.1.target={PG_MERGE_DB}.order_all
route.merge.1.pk.strategy=COMPOSITE_SOURCE
route.merge.1.ddl.policy=FIRST_WINS""")
    rc, out = run_full("x2p-merge")
    record("异构汇聚全量退出码为 0", rc == 0, "" if rc == 0 else out[-500:])
    if rc == 0:
        cols = pg_scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) "
                         "FROM information_schema.columns WHERE table_name='order_all';", PG_MERGE_DB)
        record("来源标识列进了异构建表语句（此前跑到写数据才报 Unknown column）",
               all(c in cols for c in ("_src_node", "_src_db", "_src_table")), cols)
        pk = pg_scalar("SELECT string_agg(a.attname, ',' ORDER BY a.attnum) FROM pg_index i "
                       "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                       "WHERE i.indrelid='order_all'::regclass AND i.indisprimary;", PG_MERGE_DB)
        record("目标主键 = 源主键 + 来源标识列", "id" in pk and "_src_db" in pk, pk)
        total = pg_scalar("SELECT COUNT(*) FROM order_all;", PG_MERGE_DB)
        record("汇聚行数 = 2 库 × 3 行 = 6（跨库同主键都在）", total == "6", f"实际 {total}")
        per_src = pg_scalar("SELECT COUNT(DISTINCT _src_db) FROM order_all;", PG_MERGE_DB)
        record("两个来源都写进去了", per_src == "2", f"实际 {per_src}")
        rc2, _ = run_full("x2p-merge")
        again = pg_scalar("SELECT COUNT(*) FROM order_all;", PG_MERGE_DB)
        record("幂等重跑不翻倍（PG 侧 ON CONFLICT 冲突键含来源列）", rc2 == 0 and again == "6",
               f"rc={rc2}, 实际 {again}")

    print("== 异构拆分：mysql 一张源表 → pg 4 张分片表 ==")
    # 分片库名模板对 PG 没有意义（一条连接跨不了库），分片表一律落在任务目标库的 public 下，
    # 与 shardTableRef / RouteCompareSupport 的既有口径一致
    setup_split_source()
    write_config("x2p-split", [SPLIT_SRC_DB], f"{SPLIT_SRC_DB}.orders", PG_SPLIT_DB,
                 """route.mode=SPLIT
route.split.1.match=x2p_app.orders
route.split.1.shard.key=user_id
route.split.1.algo=HASH_MOD
route.split.1.count=4
route.split.1.target.table=orders_${shard}
route.split.1.unrouted=DEADLETTER""")
    rc, out = run_full("x2p-split")
    record("异构拆分全量退出码为 0（此前直接报没有建表语句）", rc == 0, "" if rc == 0 else out[-500:])
    if rc == 0:
        n = pg_scalar("SELECT COUNT(*) FROM information_schema.tables "
                      "WHERE table_schema='public' AND table_name LIKE 'orders\\_%';", PG_SPLIT_DB)
        record("4 张分片表已由翻译器预建", n == "4", f"实际 {n}")
        total = pg_scalar(" UNION ALL ".join(
            f"SELECT COUNT(*) FROM orders_{i}" for i in range(4)), PG_SPLIT_DB)
        counts = [int(x) for x in total.splitlines() if x.strip().isdigit()]
        record("总行数守恒（12 行散在 4 片）", sum(counts) == 12, f"各片 {counts}")
        wrong = pg_scalar("SELECT COUNT(*) FROM (" + " UNION ALL ".join(
            f"SELECT user_id FROM orders_{i} WHERE user_id % 4 <> {i}" for i in range(4)) + ") t;",
            PG_SPLIT_DB)
        record("每行都落在按分片键算出的那一片上", wrong == "0", f"错片 {wrong} 行")

    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 异构库对路由 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
