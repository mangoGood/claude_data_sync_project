#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mysql -> mysql 分库分表<b>汇聚</b>（route.mode=MERGE）全量端到端测试。

直接驱动 migration-full（com.migration.full.Main，读 files/<taskId>/config.properties），
在 synctask-mysql(33306) 上把 3 个分库 × 2 张分表汇聚到 dw.order_all，覆盖：
  - N:1 汇聚：6 张源表 → 1 张目标表，行数 = 各源之和
  - 来源标识列 _src_node/_src_db/_src_table 自动建列并注值
  - 复合主键（源主键 + 来源标识列）：跨分片同主键的行互不覆盖
  - 幂等装载：重跑一次全量，目标行数与内容不变（D1 的核心验收）
  - 未命中规则的表（shard_db_1.users）仍按 1:1 路径迁到同名目标库
  - 结构一致性校验：多出一列的分表被拒绝（不静默丢列）

前置：synctask-mysql 容器在跑；migration-full fat jar 已 clean install。
用法：python3 test_scripts/sharding/mysql_merge_full_e2e.py
"""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
CT = "synctask-mysql"
SHARD_DBS = ["mrg_shard_1", "mrg_shard_2", "mrg_shard_3"]
TGT_DB = "mrg_dw"
TASK = "mrg-mysql-full"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def mysql(sql, db=None, want=False):
    args = ["docker", "exec", "-i", CT, "mysql", "-uroot", "-prootpassword",
            "--default-character-set=utf8mb4"]
    if want:
        args.append("-N")
    if db:
        args += ["-D", db]
    p = subprocess.run(args, input=sql, capture_output=True, text=True, timeout=180)
    return (p.stdout or "").strip()


def scalar(sql, db):
    return mysql(sql, db=db, want=True).strip()


def setup_source(extra_column_on_last=False):
    """建 3 个分库，每库 2 张分表 order_001/order_002；跨库主键故意重复。"""
    stmts = []
    for db in SHARD_DBS + [TGT_DB]:
        stmts.append(f"DROP DATABASE IF EXISTS {db};")
        stmts.append(f"CREATE DATABASE {db} DEFAULT CHARACTER SET utf8mb4;")
    mysql("\n".join(stmts))

    for i, db in enumerate(SHARD_DBS, start=1):
        for t in ["order_001", "order_002"]:
            extra = ""
            if extra_column_on_last and db == SHARD_DBS[-1] and t == "order_002":
                extra = ", ext_col VARCHAR(32) DEFAULT NULL"
            mysql(f"""
CREATE TABLE {t} (
  id BIGINT NOT NULL,
  amount DECIMAL(10,2) DEFAULT NULL,
  note VARCHAR(64) DEFAULT NULL{extra},
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""", db=db)
            # 每张表 3 行，id 在所有分表间<b>完全重复</b>（1/2/3）——这正是汇聚最容易丢数据的形态
            rows = ",".join(
                f"({r}, {r * 10 + i}.50, '{db}-{t}-{r}')" for r in (1, 2, 3))
            cols = "id, amount, note"
            mysql(f"INSERT INTO {t} ({cols}) VALUES {rows};", db=db)

    # 未命中汇聚规则的表：应按 1:1 迁到同名目标库
    mysql("""
CREATE TABLE users (
  id INT NOT NULL,
  name VARCHAR(32) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO users VALUES (1,'u1'),(2,'u2');
""", db=SHARD_DBS[0])


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)
    url = "jdbc:mysql://localhost:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    included_tables = ",".join(
        f"{db}.{t}" for db in SHARD_DBS for t in ("order_001", "order_002"))
    included_tables += f",{SHARD_DBS[0]}.users"
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
migration.included.databases={",".join(SHARD_DBS)}
migration.included.tables={included_tables}
migration.create.tables=true
migration.drop.tables=false
migration.migrate.data=true
migration.batch.size=500
migration.enable.resume=true
migration.continue.on.error=false
migration.record.checkpoint=false
route.mode=MERGE
route.node.id=mysql-33306
route.merge.1.match=mrg_shard_*.order_*
route.merge.1.target={TGT_DB}.order_all
route.merge.1.pk.strategy=COMPOSITE_SOURCE
route.merge.1.ddl.policy=FIRST_WINS
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def run_full():
    """跑一次全量；返回 (returncode, 输出)。"""
    env = dict(os.environ)
    env["JAVA_HOME"] = subprocess.run(
        ["/usr/libexec/java_home", "-v", "21"], capture_output=True, text=True).stdout.strip()
    java = os.path.join(env["JAVA_HOME"], "bin", "java")
    p = subprocess.run([java, "-Dh2.bindAddress=127.0.0.1", "-jar", JAR, "--task-id", TASK],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=900, env=env)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def clean_task_state():
    """清掉断点进度库，保证每次用例从零开始。"""
    for name in ("migration_progress.mv.db", "migration_progress.trace.db"):
        path = os.path.join(PROJECT_ROOT, "files", TASK, name)
        if os.path.exists(path):
            os.remove(path)


def main():
    if not os.path.exists(JAR):
        print(f"✗ 未找到 fat jar: {JAR}\n  先跑 mvn clean install -DskipTests -pl migration-common,migration-full -am")
        return 1

    print("== 准备源数据（3 分库 × 2 分表，跨库主键重复）==")
    setup_source()
    write_config()
    clean_task_state()

    print("== 第一次全量（汇聚）==")
    rc, out = run_full()
    if rc != 0:
        print(out[-4000:])
        record("全量进程退出码为 0", False, f"rc={rc}")
        return summarize()
    record("全量进程退出码为 0", True)

    total = scalar("SELECT COUNT(*) FROM order_all;", TGT_DB)
    record("汇聚行数 = 6 表 × 3 行 = 18", total == "18", f"实际 {total}")

    distinct_src = scalar(
        "SELECT COUNT(DISTINCT _src_db, _src_table) FROM order_all;", TGT_DB)
    record("来源标识覆盖 6 个 (库,表) 组合", distinct_src == "6", f"实际 {distinct_src}")

    node = scalar("SELECT DISTINCT _src_node FROM order_all;", TGT_DB)
    record("_src_node 取 route.node.id", node == "mysql-33306", f"实际 {node}")

    dup_ids = scalar("SELECT COUNT(*) FROM order_all WHERE id = 1;", TGT_DB)
    record("跨分片同主键行全部保留（id=1 有 6 行）", dup_ids == "6", f"实际 {dup_ids}")

    pk_cols = mysql(
        "SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA='{TGT_DB}' AND TABLE_NAME='order_all' AND INDEX_NAME='PRIMARY';",
        want=True)
    record("目标表主键 = 源主键 + 来源标识列",
           pk_cols == "id,_src_node,_src_db,_src_table", f"实际 {pk_cols}")

    sample = scalar(
        "SELECT note FROM order_all WHERE id=2 AND _src_db='mrg_shard_2' AND _src_table='order_001';",
        TGT_DB)
    record("行内容按来源可定位", sample == "mrg_shard_2-order_001-2", f"实际 {sample}")

    users = scalar("SELECT COUNT(*) FROM users;", SHARD_DBS[0])
    record("未命中规则的表仍按 1:1 迁移（源库同名目标库）", users == "2", f"实际 {users}")

    print("== 第二次全量（幂等重跑，D1 验收）==")
    rc2, out2 = run_full()
    record("重跑退出码为 0", rc2 == 0, f"rc={rc2}")
    total2 = scalar("SELECT COUNT(*) FROM order_all;", TGT_DB)
    record("重跑后行数不变（幂等 upsert，未清表也未翻倍）", total2 == "18", f"实际 {total2}")

    print("== 断点续传：清掉进度模拟崩溃后重启 ==")
    clean_task_state()
    rc3, _ = run_full()
    total3 = scalar("SELECT COUNT(*) FROM order_all;", TGT_DB)
    record("崩溃重启后行数仍为 18（未清掉其它来源的数据）",
           rc3 == 0 and total3 == "18", f"rc={rc3}, 实际 {total3}")

    print("== 结构不一致：分表多一列时必须拒绝 ==")
    setup_source(extra_column_on_last=True)
    clean_task_state()
    rc4, out4 = run_full()
    rejected = "汇聚结构不一致" in out4
    record("多出一列的分表被结构校验拦下", rejected,
           "未出现结构不一致错误" if not rejected else "")
    # 只报错不停任务等于"这个来源的列静默丢失"，必须 fail-stop
    record("结构不一致时任务失败退出（不带病搬完）", rc4 != 0, f"rc={rc4}")

    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 汇聚全量 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
