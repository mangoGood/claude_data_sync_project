#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分库分表<b>汇聚</b> × <b>列处理</b>叠加的全量端到端测试。

两者此前是互斥的（配了也不生效，还是静默的），本脚本盯的就是那几处静默错：

  1. 列处理规则的 key 是 "源库.源表"，而全量侧曾拿<b>连接上的库名</b>去查——
     汇聚一条通道要搬 3 个源库，只有一个库能命中，其余两个库的过滤/映射<b>悄悄不生效</b>。
     所以这里给三个分库配<b>不同的过滤阈值</b>，各源保留行数必须各不相同。
  2. CUSTOM 附加列的值是 "输入值@源库@源表"，曾由建表 DEFAULT 承载——
     而合并表只由第一个来源建出来，其余来源的行会全部带上<b>第一个来源</b>的库表名。
  3. 列名映射后，汇聚的结构一致性校验曾拿源列名去比目标表列名，第二个来源起<b>全部报缺列</b>。

前置：synctask-mysql 容器在跑；migration-full fat jar 已 clean install。
用法：python3 test_scripts/sharding/mysql_merge_column_processing_e2e.py
"""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
CT = "synctask-mysql"
SHARD_DBS = ["mcp_shard_1", "mcp_shard_2", "mcp_shard_3"]
TGT_DB = "mcp_dw"
TASK = "mcp-mysql-full"
# 每个分库一个不同的过滤阈值：amount < N 的行不同步 → 各源保留行数必须不同
THRESHOLDS = {"mcp_shard_1": 20, "mcp_shard_2": 50, "mcp_shard_3": 80}
ROWS_PER_TABLE = 10        # amount = 10,20,...,100

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


def setup_source(mapping_conflict=False):
    """3 个分库各 1 张分表；跨库主键完全重复（汇聚最容易丢数据的形态）。"""
    stmts = []
    for db in SHARD_DBS + [TGT_DB]:
        stmts.append(f"DROP DATABASE IF EXISTS {db};")
        stmts.append(f"CREATE DATABASE {db} DEFAULT CHARACTER SET utf8mb4;")
    mysql("\n".join(stmts))
    for db in SHARD_DBS:
        mysql("""
CREATE TABLE order_001 (
  id BIGINT NOT NULL,
  amount DECIMAL(10,2) DEFAULT NULL,
  note VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""", db=db)
        rows = ",".join(f"({r}, {r * 10}.00, '{db}-{r}')" for r in range(1, ROWS_PER_TABLE + 1))
        mysql(f"INSERT INTO order_001 (id, amount, note) VALUES {rows};", db=db)


def kept_rows(db):
    """该源库按自己的阈值应保留的行数：amount < 阈值 的行被过滤掉。"""
    return sum(1 for r in range(1, ROWS_PER_TABLE + 1) if not (r * 10 < THRESHOLDS[db]))


def write_config(mapping_conflict=False):
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)
    url = "jdbc:mysql://localhost:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    included_tables = ",".join(f"{db}.order_001" for db in SHARD_DBS)

    colproc = []
    for db in SHARD_DBS:
        colproc.append(f"column.filter.{db}.order_001=amount|<|{THRESHOLDS[db]}")
        # 第三个分库故意映射成别的列名，用来验证"映射不一致必须 fail-stop"
        target_col = "memo_x" if (mapping_conflict and db == SHARD_DBS[-1]) else "memo"
        colproc.append(f"column.mapping.{db}.order_001=note:{target_col}")
        colproc.append(f"column.extra.{db}.order_001=src_tag:CUSTOM:from,sync_ct:CREATE_TIME")

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
route.merge.1.match=mcp_shard_*.order_001
route.merge.1.target={TGT_DB}.order_all
route.merge.1.pk.strategy=COMPOSITE_SOURCE
route.merge.1.ddl.policy=FIRST_WINS
{chr(10).join(colproc)}
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def run_full():
    """跑一次全量；返回退出码与合并后的输出。"""
    env = dict(os.environ)
    p = subprocess.run(
        ["java", "-Dh2.bindAddress=127.0.0.1", f"-Dtask.id={TASK}", "-jar", JAR],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=600, env=env)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def clear_progress():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    for name in os.listdir(d) if os.path.isdir(d) else []:
        if name.startswith("migration_progress"):
            os.remove(os.path.join(d, name))


def main():
    if not os.path.exists(JAR):
        print("缺少 fat jar，请先 mvn clean install -DskipTests -pl migration-common,migration-full -am")
        return 2

    print("== 汇聚 + 列处理（每个源库不同的过滤阈值 / 统一列名映射 / 自定义附加列）==")
    setup_source()
    write_config()
    clear_progress()
    rc, out = run_full()
    record("全量进程退出码为 0", rc == 0, "" if rc == 0 else out[-600:])
    if rc != 0:
        return summarize()

    # 1. 列名映射：目标表列名是映射后的 memo，且源列名 note 不复存在
    cols = mysql("SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) "
                 f"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{TGT_DB}' "
                 "AND TABLE_NAME='order_all';", want=True)
    record("列名映射生效：目标表有 memo、没有 note", "memo" in cols and ",note" not in cols, cols)
    record("附加列已建出（自定义列 + 创建时间列）", "src_tag" in cols and "sync_ct" in cols, cols)

    # 2. 行过滤按<b>各源自己的阈值</b>生效——这正是"用连接库名当 key"会静默漏掉的
    expected = {db: kept_rows(db) for db in SHARD_DBS}
    actual = {}
    for db in SHARD_DBS:
        actual[db] = int(scalar(f"SELECT COUNT(*) FROM order_all WHERE _src_db='{db}';", TGT_DB))
    record("每个源库按自己的阈值过滤（三个数各不相同）", actual == expected,
           f"实际 {actual}，期望 {expected}")
    total = int(scalar("SELECT COUNT(*) FROM order_all;", TGT_DB))
    record("汇聚总行数 = 各源保留行数之和", total == sum(expected.values()),
           f"实际 {total}，期望 {sum(expected.values())}")

    # 3. 被过滤掉的行确实没进来（不是"少搬了别的行"凑上的数）
    leaked = int(scalar(
        "SELECT COUNT(*) FROM order_all WHERE "
        + " OR ".join(f"(_src_db='{db}' AND amount < {THRESHOLDS[db]})" for db in SHARD_DBS)
        + ";", TGT_DB))
    record("没有任何低于本源阈值的行漏进来", leaked == 0, f"漏进 {leaked} 行")

    # 4. CUSTOM 附加列：每个来源带自己的库表名，不是第一个来源的
    tags = mysql("SELECT DISTINCT _src_db, src_tag FROM order_all ORDER BY _src_db;",
                 db=TGT_DB, want=True)
    pairs = dict(line.split("\t") for line in tags.splitlines() if "\t" in line)
    ok_tags = all(pairs.get(db) == f"from@{db}@order_001" for db in SHARD_DBS)
    record("自定义附加列按各自来源取值（不是全部烤成第一个来源）", ok_tags, str(pairs))

    # 5. CREATE_TIME 仍由建表 DEFAULT 承载（与来源无关，不该改成逐行注值）
    ct_default = mysql("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS "
                       f"WHERE TABLE_SCHEMA='{TGT_DB}' AND TABLE_NAME='order_all' "
                       "AND COLUMN_NAME='sync_ct';", want=True)
    record("创建时间附加列仍走建表默认值", "CURRENT_TIMESTAMP" in ct_default.upper(), ct_default)
    ct_null = int(scalar("SELECT COUNT(*) FROM order_all WHERE sync_ct IS NULL;", TGT_DB))
    record("创建时间附加列每行都有值", ct_null == 0, f"{ct_null} 行为 NULL")

    # 6. 幂等重跑：列处理叠加下 upsert 仍然不翻倍（冲突键用的是映射后的列名）
    clear_progress()
    rc2, out2 = run_full()
    total2 = int(scalar("SELECT COUNT(*) FROM order_all;", TGT_DB))
    record("幂等重跑：行数不变（映射后的冲突键仍然有效）", rc2 == 0 and total2 == total,
           f"rc={rc2}, 实际 {total2}")

    # 7. 映射不一致必须 fail-stop（第三个源映射成 memo_x，目标表没有这列）
    print("== 映射不一致：汇入同一张表的各源必须映射成同样的列名 ==")
    setup_source()
    write_config(mapping_conflict=True)
    clear_progress()
    rc3, out3 = run_full()
    record("各源映射不一致时任务失败退出（不带病搬完）", rc3 != 0, f"rc={rc3}")
    # 报错点名的是"本源映射出来的列在目标表上没有"（note→memo），这比点名别人的 memo_x 更好定位
    record("报错点名了具体是哪个源、哪个列映射不上",
           "汇聚结构不一致" in out3 and "note→memo" in out3 and "映射结果必须一致" in out3,
           next((l.strip()[-120:] for l in out3.splitlines() if "汇聚结构不一致" in l), ""))

    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 汇聚 × 列处理 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
