#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
<b>跨实例拆分</b>端到端测试：一张源表按分片键拆到<b>两个 MySQL 实例</b>上。

源与分片 0/1 在 synctask-mysql(33306)，分片 2/3 在 mysql_db2(3307)。覆盖：
  - 全量：分片表分别预建在各自实例上，行按分片键落到对的实例+对的表
  - 增量：INSERT/UPDATE/DELETE 路由到对应实例；改分片键触发<b>跨实例</b>搬迁
  - 一致性档位：跨实例拆分 + TRANSACTIONAL 必须在启动时被拒（跨连接无法原子提交）

前置：synctask-mysql 在跑；四个 fat jar 已 clean install。
第二个目标实例 synctask-mysql-b（宿主 33307）由本脚本按需拉起，跑完保留。
用法：python3 test_scripts/sharding/mysql_split_cross_instance_e2e.py
"""
import os
import shutil
import signal
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_CT, SRC_PORT = "synctask-mysql", 33306
NODE1_CT, NODE1_PORT = "synctask-mysql", 33306      # 分片 0/1：与源同实例
NODE2_CT, NODE2_PORT = "synctask-mysql-b", 33307    # 分片 2/3：另一个实例（本脚本按需拉起）
NODE2_IMAGE = "docker.1ms.run/mysql:8.0"
SRC_DB = "xsplit_src"
TGT_DB = "xsplit_dw"
SHARDS = 4
TASK = "xsplit-mysql"
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


def mysql(ct, sql, db=None, want=False):
    args = ["docker", "exec", "-i", ct, "mysql", "-uroot", "-prootpassword",
            "--default-character-set=utf8mb4"]
    if want:
        args.append("-N")
    if db:
        args += ["-D", db]
    p = subprocess.run(args, input=sql, capture_output=True, text=True, timeout=180)
    return (p.stdout or "").strip()


def node_of(shard):
    """分片 0/1 在实例 A，分片 2/3 在实例 B（与 route.split.1.target.node=${shard/2} 对应）。"""
    return (NODE1_CT if shard < 2 else NODE2_CT)


def ensure_node2():
    """
    第二个目标实例。现有的 mysql_db2/mysql_db3 容器把宿主端口映到了容器 3307/3309，
    而容器内 MySQL 监听 3306——宿主连不上（引擎跑在宿主上），所以这里自带一个映射正确的实例。
    已存在就复用，不重复创建。
    """
    existing = subprocess.run(["docker", "ps", "-a", "--filter", f"name=^{NODE2_CT}$",
                               "--format", "{{.Names}} {{.State}}"],
                              capture_output=True, text=True).stdout.strip()
    if not existing:
        print(f"  拉起第二个目标实例 {NODE2_CT}（宿主 {NODE2_PORT} → 容器 3306）...")
        subprocess.run(["docker", "run", "-d", "--name", NODE2_CT,
                        "-e", "MYSQL_ROOT_PASSWORD=rootpassword",
                        "-p", f"{NODE2_PORT}:3306", NODE2_IMAGE,
                        # 与 docker-compose-synctask.yml 里的 synctask-mysql 同样的认证插件：
                        # 默认的 caching_sha2_password 在 useSSL=false 下要求客户端开
                        # allowPublicKeyRetrieval，而全量链路的 JDBC URL 没带这个参数
                        "--default-authentication-plugin=mysql_native_password",
                        "--server-id=200", "--log-bin=binlog"],
                       capture_output=True, text=True, timeout=300)
    elif "running" not in existing:
        subprocess.run(["docker", "start", NODE2_CT], capture_output=True, text=True, timeout=120)
    # 等它能接受连接
    deadline = time.time() + 180
    while time.time() < deadline:
        probe = mysql(NODE2_CT, "SELECT 1;", want=True)
        if probe.strip() == "1":
            return True
        time.sleep(3)
    return False


def setup():
    mysql(SRC_CT, f"DROP DATABASE IF EXISTS {SRC_DB}; CREATE DATABASE {SRC_DB} DEFAULT CHARACTER SET utf8mb4;"
                  f"DROP DATABASE IF EXISTS {TGT_DB}; CREATE DATABASE {TGT_DB} DEFAULT CHARACTER SET utf8mb4;")
    mysql(NODE2_CT, f"DROP DATABASE IF EXISTS {TGT_DB}; CREATE DATABASE {TGT_DB} DEFAULT CHARACTER SET utf8mb4;")
    mysql(SRC_CT, """
CREATE TABLE orders (
  id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  note VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""", db=SRC_DB)
    # 4 行，user_id 0..3 各落一片（0/1 在实例 A，2/3 在实例 B）
    rows = ",".join(f"({i + 1}, {i}, 'seed{i}')" for i in range(SHARDS))
    mysql(SRC_CT, f"INSERT INTO orders (id, user_id, note) VALUES {rows};", db=SRC_DB)


def write_config(consistency="EVENTUAL"):
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    cfg = f"""task.id={TASK}
source.db.type=mysql
source.db.flavor=mysql
source.db.host=127.0.0.1
source.db.port={SRC_PORT}
source.db.username=root
source.db.password=rootpassword
source.db.database={SRC_DB}
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url=jdbc:mysql://127.0.0.1:{SRC_PORT}/{SRC_DB}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8
source.host=127.0.0.1
source.port={SRC_PORT}
source.user=root
source.password=rootpassword
source.type=mysql
target.db.type=mysql
target.db.host=127.0.0.1
target.db.port={NODE1_PORT}
target.db.username=root
target.db.password=rootpassword
target.db.database={TGT_DB}
target.db.jdbc.driver=com.mysql.cj.jdbc.Driver
target.db.jdbc.url=jdbc:mysql://127.0.0.1:{NODE1_PORT}/{TGT_DB}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8
target.db.quote.char=`
target.host=127.0.0.1
target.port={NODE1_PORT}
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
sync.consistency.mode={consistency}
capture.type=binlog
capture.server.id=9915
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
route.split.1.target.group=g1
route.split.1.target.node=${{shard/2}}
route.split.1.unrouted=DEADLETTER
route.node.g1.0.host=127.0.0.1
route.node.g1.0.port={NODE1_PORT}
route.node.g1.0.username=root
route.node.g1.0.password=rootpassword
route.node.g1.1.host=127.0.0.1
route.node.g1.1.port={NODE2_PORT}
route.node.g1.1.username=root
route.node.g1.1.password=rootpassword
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


def shard_count(shard):
    return mysql(node_of(shard), f"SELECT COUNT(*) FROM orders_{shard};", db=TGT_DB, want=True).strip()


def wait_for(fn, expected, timeout=60, interval=2):
    deadline = time.time() + timeout
    value = None
    while time.time() < deadline:
        value = fn()
        if value == expected:
            return value
        time.sleep(interval)
    return value


def main():
    missing = [k for k, v in JARS.items() if not os.path.exists(os.path.join(PROJECT_ROOT, v))]
    if missing:
        print(f"✗ 缺少 fat jar: {missing}")
        return 1

    if not ensure_node2():
        print(f"✗ 第二个目标实例 {NODE2_CT} 起不来")
        return 1

    print("== 一致性档位互斥：跨实例拆分 + TRANSACTIONAL 必须被拒 ==")
    setup()
    write_config(consistency="TRANSACTIONAL")
    run_jar("full")   # 全量不受档位限制，先建表
    rc, out = run_jar("increment")
    rejected = "无法在一个事务里原子提交" in out or "EVENTUAL" in out
    record("跨实例拆分 + 事务一致语义被拒绝", rc != 0 and rejected, f"rc={rc}")

    print("== 全量：分片表建在各自实例上，行落到对的实例 ==")
    setup()
    write_config()
    rc, out = run_jar("full")
    if rc != 0:
        print(out[-3000:])
        record("全量退出码为 0", False, f"rc={rc}")
        return summarize()
    record("全量退出码为 0", True)

    a_tables = mysql(NODE1_CT, f"SELECT COUNT(*) FROM information_schema.TABLES "
                               f"WHERE TABLE_SCHEMA='{TGT_DB}' AND TABLE_NAME LIKE 'orders_%';", want=True)
    b_tables = mysql(NODE2_CT, f"SELECT COUNT(*) FROM information_schema.TABLES "
                               f"WHERE TABLE_SCHEMA='{TGT_DB}' AND TABLE_NAME LIKE 'orders_%';", want=True)
    record("实例 A 上建了 2 张分片表", a_tables == "2", f"实际 {a_tables}")
    record("实例 B 上建了 2 张分片表", b_tables == "2", f"实际 {b_tables}")

    placement_ok = all(shard_count(s) == "1" for s in range(SHARDS))
    record("4 行分别落在 4 个分片（跨两个实例）", placement_ok,
           " ".join(f"s{s}={shard_count(s)}" for s in range(SHARDS)))
    note_b = mysql(NODE2_CT, "SELECT note FROM orders_2 WHERE user_id=2;", db=TGT_DB, want=True)
    record("实例 B 上的分片内容正确", note_b == "seed2", f"实际 {note_b}")

    print("== 启动增量链路 ==")
    run_jar("capture", background=True)
    time.sleep(6)
    run_jar("extract", background=True)
    time.sleep(3)
    run_jar("increment", background=True)
    time.sleep(8)

    try:
        print("== 增量 INSERT：路由到另一个实例 ==")
        mysql(SRC_CT, "INSERT INTO orders (id, user_id, note) VALUES (10, 3, 'ins-nodeB');", db=SRC_DB)
        got = wait_for(lambda: mysql(NODE2_CT, "SELECT note FROM orders_3 WHERE id=10;",
                                     db=TGT_DB, want=True), "ins-nodeB")
        record("新行写到了实例 B 的 orders_3", got == "ins-nodeB", f"实际 {got}")

        print("== 增量 UPDATE：同片内更新 ==")
        mysql(SRC_CT, "UPDATE orders SET note='upd-nodeB' WHERE id=10;", db=SRC_DB)
        got = wait_for(lambda: mysql(NODE2_CT, "SELECT note FROM orders_3 WHERE id=10;",
                                     db=TGT_DB, want=True), "upd-nodeB")
        record("实例 B 上的行已更新", got == "upd-nodeB", f"实际 {got}")

        print("== 增量 UPDATE 改分片键：跨<b>实例</b>搬迁 ==")
        mysql(SRC_CT, "UPDATE orders SET user_id=0, note='moved-to-A' WHERE id=10;", db=SRC_DB)
        got = wait_for(lambda: mysql(NODE1_CT, "SELECT note FROM orders_0 WHERE id=10;",
                                     db=TGT_DB, want=True), "moved-to-A")
        record("行已搬到实例 A 的 orders_0", got == "moved-to-A", f"实际 {got}")
        gone = wait_for(lambda: mysql(NODE2_CT, "SELECT COUNT(*) FROM orders_3 WHERE id=10;",
                                      db=TGT_DB, want=True), "0")
        record("实例 B 上的旧行已删除", gone == "0", f"实际 {gone}")

        print("== 增量 DELETE ==")
        mysql(SRC_CT, "DELETE FROM orders WHERE id=10;", db=SRC_DB)
        got = wait_for(lambda: mysql(NODE1_CT, "SELECT COUNT(*) FROM orders_0 WHERE id=10;",
                                     db=TGT_DB, want=True), "0")
        record("删除已作用到实例 A", got == "0", f"实际 {got}")
    finally:
        stop_all()

    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 跨实例拆分 E2E: {passed}/{len(results)} 通过 =====")
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
