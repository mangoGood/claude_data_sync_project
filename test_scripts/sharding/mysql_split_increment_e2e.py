#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mysql -> mysql <b>拆分增量</b>（route.mode=SPLIT）端到端测试。

直驱 capture → extract → increment 三个引擎子进程，把 spls_src.orders 按 user_id 拆成 4 片，覆盖：
  - INSERT：新行落到 floorMod(user_id, 4) 对应的分片
  - UPDATE（不改分片键）：就地更新那一片
  - UPDATE（改分片键）：<b>跨分片搬迁</b>——旧片删掉、新片出现，且只出现一次
  - DELETE：只删所在分片的那一行
  - DDL：一条 ALTER 广播到全部 4 个分片表
  - 破坏性 DDL：源表被 DROP 时不连带删掉分片表

前置：synctask-mysql 容器在跑；四个 fat jar 已 clean install。
用法：python3 test_scripts/sharding/mysql_split_increment_e2e.py
"""
import os
import shutil
import signal
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CT = "synctask-mysql"
SRC_DB = "spls_src"
TGT_DB = "spls_dw"
SHARDS = 4
TASK = "spls-mysql-inc"
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


def shard_table(user_id):
    return f"orders_{user_id % SHARDS}"


def setup_source():
    mysql(f"DROP DATABASE IF EXISTS {SRC_DB}; CREATE DATABASE {SRC_DB} DEFAULT CHARACTER SET utf8mb4;"
          f"DROP DATABASE IF EXISTS {TGT_DB}; CREATE DATABASE {TGT_DB} DEFAULT CHARACTER SET utf8mb4;")
    mysql("""
CREATE TABLE orders (
  id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  note VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO orders (id, user_id, note) VALUES (1, 4, 'seed');
""", db=SRC_DB)


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    url = f"jdbc:mysql://127.0.0.1:33306/{SRC_DB}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    cfg = f"""task.id={TASK}
source.db.type=mysql
source.db.flavor=mysql
source.db.host=127.0.0.1
source.db.port=33306
source.db.username=root
source.db.password=rootpassword
source.db.database={SRC_DB}
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
migration.included.databases={SRC_DB}
migration.included.tables={SRC_DB}.orders
migration.create.tables=true
migration.drop.tables=false
migration.migrate.data=true
migration.enable.resume=true
migration.continue.on.error=false
migration.record.checkpoint=false
capture.type=binlog
capture.server.id=9913
capture.output.dir=files/{TASK}/binlog_output
extract.continuous=true
extract.input.dir=files/{TASK}/binlog_output
extract.output.dir=files/{TASK}/thl_output
extract.skip.before.checkpoint=false
increment.thl.dir=files/{TASK}/thl_output
route.mode=SPLIT
route.split.1.match={SRC_DB}.orders
route.split.1.shard.key=user_id
route.split.1.algo=HASH_MOD
route.split.1.count={SHARDS}
route.split.1.target.db={TGT_DB}
route.split.1.target.table=orders_${{shard}}
route.split.1.unrouted=DEADLETTER
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def run_jar(kind, background=False):
    env = dict(os.environ)
    env["JAVA_HOME"] = subprocess.run(
        ["/usr/libexec/java_home", "-v", "21"], capture_output=True, text=True).stdout.strip()
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
    for _, p, _ in procs:
        try:
            p.send_signal(signal.SIGTERM)
        except Exception:
            pass
    time.sleep(3)
    for _, p, log in procs:
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
    deadline = time.time() + timeout
    value = None
    while time.time() < deadline:
        value = fn()
        if value == expected:
            return value
        time.sleep(interval)
    return value


def increment_log():
    try:
        with open(os.path.join(PROJECT_ROOT, "files", TASK, "increment.log"),
                  encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return ""


def total_rows():
    return sum(int(scalar(f"SELECT COUNT(*) FROM orders_{s};", TGT_DB) or 0) for s in range(SHARDS))


def main():
    missing = [k for k, v in JARS.items() if not os.path.exists(os.path.join(PROJECT_ROOT, v))]
    if missing:
        print(f"✗ 缺少 fat jar: {missing}")
        return 1

    print("== 准备源数据 + 全量预建 4 张分片表 ==")
    setup_source()
    write_config()
    rc, out = run_jar("full")
    if rc != 0:
        print(out[-3000:])
        record("全量退出码为 0", False, f"rc={rc}")
        return summarize()
    record("全量退出码为 0", True)
    record("种子行落在 orders_0（user_id=4）",
           scalar("SELECT COUNT(*) FROM orders_0;", TGT_DB) == "1")

    print("== 启动 capture / extract / increment ==")
    run_jar("capture", background=True)
    time.sleep(6)
    run_jar("extract", background=True)
    time.sleep(3)
    run_jar("increment", background=True)
    time.sleep(8)

    try:
        print("== 增量 INSERT：按分片键路由 ==")
        mysql("INSERT INTO orders (id, user_id, note) VALUES (10, 5, 'ins5'), (11, 6, 'ins6');", db=SRC_DB)
        got = wait_for(lambda: scalar("SELECT note FROM orders_1 WHERE id=10;", TGT_DB), "ins5")
        record("user_id=5 落在 orders_1", got == "ins5", f"实际 {got}")
        got = wait_for(lambda: scalar("SELECT note FROM orders_2 WHERE id=11;", TGT_DB), "ins6")
        record("user_id=6 落在 orders_2", got == "ins6", f"实际 {got}")

        print("== 增量 UPDATE（不改分片键）：就地更新 ==")
        mysql("UPDATE orders SET note='upd5' WHERE id=10;", db=SRC_DB)
        got = wait_for(lambda: scalar("SELECT note FROM orders_1 WHERE id=10;", TGT_DB), "upd5")
        record("同片内更新生效", got == "upd5", f"实际 {got}")

        print("== 增量 UPDATE（改分片键）：跨分片搬迁 ==")
        mysql("UPDATE orders SET user_id=7, note='moved' WHERE id=10;", db=SRC_DB)
        got = wait_for(lambda: scalar("SELECT note FROM orders_3 WHERE id=10;", TGT_DB), "moved")
        record("行已出现在新分片 orders_3", got == "moved", f"实际 {got}")
        gone = wait_for(lambda: scalar("SELECT COUNT(*) FROM orders_1 WHERE id=10;", TGT_DB), "0")
        record("旧分片 orders_1 里的陈行已删除", gone == "0", f"实际 {gone}")
        dup = sum(int(scalar(f"SELECT COUNT(*) FROM orders_{s} WHERE id=10;", TGT_DB) or 0)
                  for s in range(SHARDS))
        record("全局只剩一份（没在多个分片里各留一行）", dup == 1, f"实际 {dup}")

        print("== 增量 DELETE：只删所在分片 ==")
        before = total_rows()
        mysql("DELETE FROM orders WHERE id=11;", db=SRC_DB)
        got = wait_for(lambda: scalar("SELECT COUNT(*) FROM orders_2 WHERE id=11;", TGT_DB), "0")
        record("目标分片里的行已删除", got == "0", f"实际 {got}")
        record("其它分片行数不受影响", total_rows() == before - 1, f"{before} -> {total_rows()}")

        print("== 增量 DDL：一条 ALTER 广播到全部分片 ==")
        mysql("ALTER TABLE orders ADD COLUMN memo VARCHAR(32) DEFAULT NULL;", db=SRC_DB)
        got = wait_for(lambda: scalar(
            f"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{TGT_DB}' "
            f"AND TABLE_NAME LIKE 'orders_%' AND COLUMN_NAME='memo';"), str(SHARDS))
        record(f"{SHARDS} 张分片表都加上了 memo 列", got == str(SHARDS), f"实际 {got}")

        print("== 破坏性 DDL：源表被 DROP 不连带删分片表 ==")
        mysql("DROP TABLE orders;", db=SRC_DB)
        time.sleep(10)
        left = scalar(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='{TGT_DB}' "
                      f"AND TABLE_NAME LIKE 'orders_%';")
        record("分片表都还在（破坏性 DDL 不广播）", left == str(SHARDS), f"实际 {left}")
        skipped = "拆分表的破坏性 DDL 不广播" in increment_log()
        record("破坏性 DDL 被记为跳过", skipped, "" if skipped else "日志里没有跳过记录")

        print("== 分片命中分布指标 ==")
        metric_path = os.path.join(PROJECT_ROOT, "files", TASK, "binlog_output", "route_metric")
        metric = ""
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with open(metric_path, encoding="utf-8") as f:
                    metric = f.read().strip()
                if metric:
                    break
            except OSError:
                pass
            time.sleep(2)
        record("route_metric 已落盘", bool(metric), metric[:100])
        if metric:
            import json as _json
            data = _json.loads(metric)
            record("指标里是 SPLIT 模式且有分片命中", data.get("mode") == "SPLIT" and data.get("hits"),
                   str(data.get("hits"))[:80])
            record("跨分片搬迁被计数", int(data.get("crossShardMoves", 0)) >= 1,
                   f"crossShardMoves={data.get('crossShardMoves')}")
    finally:
        stop_all()

    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 拆分增量 E2E: {passed}/{len(results)} 通过 =====")
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
