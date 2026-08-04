#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mysql -> mysql 分库分表<b>汇聚增量</b>（route.mode=MERGE）端到端测试。

直驱三个引擎子进程（capture → extract → increment），在 synctask-mysql(33306) 上
把 2 个分库的 order_001 汇聚到 mrg_inc_dw.order_all，覆盖：
  - INSERT：两个分库写<b>相同主键</b>，目标表两行并存（不互相覆盖）
  - UPDATE：只改本来源那一行，另一来源的同主键行不动（WHERE 必须带来源标识列）
  - DELETE：只删本来源那一行
  - DDL：两个分库各发一条同样的 ALTER，汇聚表只应用一次（FIRST_WINS）
  - 破坏性 DDL：某个分表被 DROP，汇聚表不受影响

前置：synctask-mysql 容器在跑；capture/extract/increment/full 四个 fat jar 已 clean install。
用法：python3 test_scripts/sharding/mysql_merge_increment_e2e.py
"""
import os
import shutil
import signal
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CT = "synctask-mysql"
SHARDS = ["mrg_inc_1", "mrg_inc_2"]
TGT_DB = "mrg_inc_dw"
TASK = "mrg-mysql-inc"
JARS = {
    "full": "migration-full/target/migration-full-1.0.0.jar",
    "capture": "migration-capture/target/migration-capture-1.0.0.jar",
    "extract": "migration-extract/target/migration-extract-1.0.0.jar",
    "increment": "migration-increment/target/migration-increment-1.0.0.jar",
}

results = []
procs = []


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


def scalar(sql, db=None):
    return mysql(sql, db=db, want=True).strip()


def java_home():
    return subprocess.run(["/usr/libexec/java_home", "-v", "21"],
                          capture_output=True, text=True).stdout.strip()


def setup_source():
    stmts = []
    for db in SHARDS + [TGT_DB]:
        stmts.append(f"DROP DATABASE IF EXISTS {db};")
        stmts.append(f"CREATE DATABASE {db} DEFAULT CHARACTER SET utf8mb4;")
    mysql("\n".join(stmts))
    for db in SHARDS:
        mysql("""
CREATE TABLE order_001 (
  id BIGINT NOT NULL,
  amount DECIMAL(10,2) DEFAULT NULL,
  note VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""", db=db)
        mysql(f"INSERT INTO order_001 (id, amount, note) VALUES (1, 1.00, 'seed-{db}');", db=db)


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    url = "jdbc:mysql://127.0.0.1:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    included = ",".join(f"{db}.order_001" for db in SHARDS)
    cfg = f"""task.id={TASK}
source.db.type=mysql
source.db.flavor=mysql
source.db.host=127.0.0.1
source.db.port=33306
source.db.username=root
source.db.password=rootpassword
source.db.database=
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url={url}
source.host=127.0.0.1
source.port=33306
source.user=root
source.password=rootpassword
source.type=mysql
target.db.type=mysql
target.db.host=127.0.0.1
target.db.port=33306
target.db.username=root
target.db.password=rootpassword
target.db.database={TGT_DB}
target.db.jdbc.driver=com.mysql.cj.jdbc.Driver
target.db.jdbc.url=jdbc:mysql://127.0.0.1:33306/{TGT_DB}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8
target.db.quote.char=`
target.host=127.0.0.1
target.port=33306
target.user=root
target.password=rootpassword
target.type=mysql
migration.included.databases={",".join(SHARDS)}
migration.included.tables={included}
migration.create.tables=true
migration.drop.tables=false
migration.migrate.data=true
migration.enable.resume=true
migration.continue.on.error=false
migration.record.checkpoint=false
capture.type=binlog
capture.server.id=9911
capture.output.dir=files/{TASK}/binlog_output
extract.continuous=true
extract.input.dir=files/{TASK}/binlog_output
extract.output.dir=files/{TASK}/thl_output
extract.skip.before.checkpoint=false
increment.thl.dir=files/{TASK}/thl_output
route.mode=MERGE
route.node.id=node-a
route.merge.1.match=mrg_inc_*.order_*
route.merge.1.target={TGT_DB}.order_all
route.merge.1.pk.strategy=COMPOSITE_SOURCE
route.merge.1.ddl.policy=FIRST_WINS
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def run_jar(kind, background=False):
    env = dict(os.environ)
    env["JAVA_HOME"] = java_home()
    java = os.path.join(env["JAVA_HOME"], "bin", "java")
    cmd = [java, "-Dh2.bindAddress=127.0.0.1", f"-Dtask.id={TASK}", "-jar",
           os.path.join(PROJECT_ROOT, JARS[kind])]
    if background:
        log = open(os.path.join(PROJECT_ROOT, "files", TASK, f"{kind}.log"), "w")
        p = subprocess.Popen(cmd, cwd=PROJECT_ROOT, stdout=log, stderr=subprocess.STDOUT, env=env)
        procs.append((kind, p, log))
        return p
    r = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=900, env=env)
    return r.returncode, (r.stdout or "") + (r.stderr or "")


def stop_all():
    for kind, p, log in procs:
        try:
            p.send_signal(signal.SIGTERM)
        except Exception:
            pass
    time.sleep(3)
    for kind, p, log in procs:
        try:
            if p.poll() is None:
                p.kill()
        except Exception:
            pass
        try:
            log.close()
        except Exception:
            pass


def wait_for(fn, expected, timeout=60, interval=2):
    """轮询直到 fn() == expected；返回最后一次取到的值。"""
    deadline = time.time() + timeout
    value = None
    while time.time() < deadline:
        value = fn()
        if value == expected:
            return value
        time.sleep(interval)
    return value


def increment_log():
    path = os.path.join(PROJECT_ROOT, "files", TASK, "increment.log")
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return ""


def main():
    missing = [k for k, v in JARS.items() if not os.path.exists(os.path.join(PROJECT_ROOT, v))]
    if missing:
        print(f"✗ 缺少 fat jar: {missing}\n  先跑 mvn clean install -DskipTests")
        return 1

    print("== 准备源数据（2 分库，主键故意相同）==")
    setup_source()
    write_config()

    print("== 全量：建出带来源标识列的汇聚表 ==")
    rc, out = run_full_and_check()
    if rc != 0:
        print(out[-3000:])
        record("全量退出码为 0", False, f"rc={rc}")
        return summarize()
    record("全量退出码为 0", True)
    record("全量后汇聚表 2 行（两个来源的 id=1 并存）",
           scalar("SELECT COUNT(*) FROM order_all;", TGT_DB) == "2")

    print("== 启动 capture / extract / increment ==")
    run_jar("capture", background=True)
    time.sleep(6)
    run_jar("extract", background=True)
    time.sleep(3)
    run_jar("increment", background=True)
    time.sleep(8)

    try:
        print("== 增量 INSERT：两个分库写相同主键 ==")
        for db in SHARDS:
            mysql(f"INSERT INTO order_001 (id, amount, note) VALUES (2, 2.00, 'ins-{db}');", db=db)
        got = wait_for(lambda: scalar("SELECT COUNT(*) FROM order_all WHERE id=2;", TGT_DB), "2")
        record("两个来源的 id=2 各自落一行", got == "2", f"实际 {got}")

        print("== 增量 UPDATE：只改本来源那一行 ==")
        mysql("UPDATE order_001 SET note='upd-shard1' WHERE id=2;", db=SHARDS[0])
        got = wait_for(lambda: scalar(
            f"SELECT note FROM order_all WHERE id=2 AND _src_db='{SHARDS[0]}';", TGT_DB), "upd-shard1")
        record("本来源的行已更新", got == "upd-shard1", f"实际 {got}")
        other = scalar(f"SELECT note FROM order_all WHERE id=2 AND _src_db='{SHARDS[1]}';", TGT_DB)
        record("另一来源的同主键行未被改动", other == f"ins-{SHARDS[1]}", f"实际 {other}")

        print("== 增量 DELETE：只删本来源那一行 ==")
        mysql("DELETE FROM order_001 WHERE id=2;", db=SHARDS[0])
        got = wait_for(lambda: scalar("SELECT COUNT(*) FROM order_all WHERE id=2;", TGT_DB), "1")
        record("只剩另一来源的那一行", got == "1", f"实际 {got}")
        left = scalar("SELECT _src_db FROM order_all WHERE id=2;", TGT_DB)
        record("留下的正是未删除的来源", left == SHARDS[1], f"实际 {left}")

        print("== 增量 DDL：两个分库发同一条 ALTER，汇聚表只应用一次 ==")
        for db in SHARDS:
            mysql("ALTER TABLE order_001 ADD COLUMN memo VARCHAR(32) DEFAULT NULL;", db=db)
        got = wait_for(lambda: scalar(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='"
            + TGT_DB + "' AND TABLE_NAME='order_all' AND COLUMN_NAME='memo';"), "1")
        record("汇聚表新增列 memo", got == "1", f"实际 {got}")
        # 匹配去重那条日志本身，而不是 "FIRST_WINS" 三个字——配置回显里也有这个词
        deduped = "汇聚 DDL 已由其它来源应用过" in increment_log()
        record("第二条同样的 DDL 被 FIRST_WINS 跳过", deduped,
               "" if deduped else "日志里没有去重记录")

        print("== 破坏性 DDL：某个分表被 DROP，汇聚表必须不受影响 ==")
        before = scalar("SELECT COUNT(*) FROM order_all;", TGT_DB)
        mysql("DROP TABLE order_001;", db=SHARDS[0])
        time.sleep(10)
        exists = scalar("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='"
                        + TGT_DB + "' AND TABLE_NAME='order_all';")
        after = scalar("SELECT COUNT(*) FROM order_all;", TGT_DB) if exists == "1" else "-"
        record("汇聚表仍在且行数不变", exists == "1" and after == before,
               f"exists={exists}, before={before}, after={after}")
        skipped = "汇聚表的破坏性 DDL 不应用" in increment_log()
        record("破坏性 DDL 被记为跳过", skipped, "" if skipped else "日志里没有破坏性 DDL 跳过记录")
    finally:
        stop_all()

    return summarize()


def run_full_and_check():
    return run_jar("full")


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 汇聚增量 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        stop_all()
        sys.exit(130)
