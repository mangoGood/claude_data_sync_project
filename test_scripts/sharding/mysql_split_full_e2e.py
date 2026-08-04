#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mysql -> mysql <b>拆分</b>（route.mode=SPLIT）全量端到端测试。

直接驱动 migration-full，把 spl_src.orders 按 user_id 哈希拆成 2 库 × 4 表（共 8 片），覆盖：
  - 目标表预建：8 张分片表自动建出，AUTO_INCREMENT 被剥掉
  - 行级路由：每行落在 floorMod(user_id, 8) 对应的分片上（逐片核对行数与内容）
  - 总行数守恒：各分片之和 = 源表行数（未路由行另计）
  - 未路由行（user_id 为 NULL）按默认 BROADCAST 策略进每一片
  - 断点重跑：清掉进度重跑，各分片行数不变（不会翻倍、也不会只清一张表）

前置：synctask-mysql 容器在跑；migration-full fat jar 已 clean install。
用法：python3 test_scripts/sharding/mysql_split_full_e2e.py
"""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
CT = "synctask-mysql"
SRC_DB = "spl_src"
TGT_DBS = ["spl_dw_0", "spl_dw_1"]
SHARDS = 8
TASK = "spl-mysql-full"
ROWS = 200

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


def scalar(sql, db=None):
    return mysql(sql, db=db, want=True).strip()


def shard_of(user_id):
    """与引擎侧 HASH_MOD 的整型口径一致：floorMod(user_id, 8)。"""
    return user_id % SHARDS


def shard_location(shard):
    """target.db=spl_dw_${shard/4}, target.table=orders_${shard}"""
    return f"spl_dw_{shard // 4}", f"orders_{shard}"


def setup_source():
    stmts = [f"DROP DATABASE IF EXISTS {SRC_DB};",
             f"CREATE DATABASE {SRC_DB} DEFAULT CHARACTER SET utf8mb4;"]
    for db in TGT_DBS:
        stmts.append(f"DROP DATABASE IF EXISTS {db};")
    mysql("\n".join(stmts))
    mysql("""
CREATE TABLE orders (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT DEFAULT NULL,
  amount DECIMAL(10,2) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
""", db=SRC_DB)
    values = ",".join(f"({i}, {i * 7 % 97}, {i}.25)" for i in range(1, ROWS + 1))
    mysql(f"INSERT INTO orders (id, user_id, amount) VALUES {values};", db=SRC_DB)
    # 未路由行：分片键为 NULL，默认 BROADCAST 策略应进每一片
    mysql(f"INSERT INTO orders (id, user_id, amount) VALUES ({ROWS + 1}, NULL, 9.99);", db=SRC_DB)


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)
    url = "jdbc:mysql://localhost:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    cfg = f"""source.db.type=mysql
source.db.host=localhost
source.db.port=33306
source.db.username=root
source.db.password=rootpassword
source.db.database={SRC_DB}
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url=jdbc:mysql://localhost:33306/{SRC_DB}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8
target.db.type=mysql
target.db.host=localhost
target.db.port=33306
target.db.username=root
target.db.password=rootpassword
target.db.database={TGT_DBS[0]}
target.db.jdbc.driver=com.mysql.cj.jdbc.Driver
target.db.jdbc.url=jdbc:mysql://localhost:33306/{TGT_DBS[0]}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8
target.db.quote.char=`
migration.included.tables={SRC_DB}.orders
migration.create.tables=true
migration.drop.tables=false
migration.migrate.data=true
migration.batch.size=100
migration.enable.resume=true
migration.continue.on.error=false
migration.record.checkpoint=false
route.mode=SPLIT
route.split.1.match={SRC_DB}.orders
route.split.1.shard.key=user_id
route.split.1.algo=HASH_MOD
route.split.1.count={SHARDS}
route.split.1.target.db=spl_dw_${{shard/4}}
route.split.1.target.table=orders_${{shard}}
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def run_full():
    env = dict(os.environ)
    env["JAVA_HOME"] = subprocess.run(
        ["/usr/libexec/java_home", "-v", "21"], capture_output=True, text=True).stdout.strip()
    java = os.path.join(env["JAVA_HOME"], "bin", "java")
    p = subprocess.run([java, "-Dh2.bindAddress=127.0.0.1", "-jar", JAR, "--task-id", TASK],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=900, env=env)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def clean_task_state():
    for name in ("migration_progress.mv.db", "migration_progress.trace.db"):
        path = os.path.join(PROJECT_ROOT, "files", TASK, name)
        if os.path.exists(path):
            os.remove(path)


def expected_counts():
    """按 user_id 算出每片应有的行数（NULL 行广播到每一片，单独计）。"""
    counts = {s: 0 for s in range(SHARDS)}
    for i in range(1, ROWS + 1):
        counts[shard_of(i * 7 % 97)] += 1
    return counts


def main():
    if not os.path.exists(JAR):
        print(f"✗ 未找到 fat jar: {JAR}")
        return 1

    print("== 准备源数据（201 行，其中 1 行分片键为 NULL）==")
    setup_source()
    write_config()
    clean_task_state()

    print("== 全量拆分 ==")
    rc, out = run_full()
    if rc != 0:
        print(out[-4000:])
        record("全量退出码为 0", False, f"rc={rc}")
        return summarize()
    record("全量退出码为 0", True)

    created = scalar("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('"
                     + "','".join(TGT_DBS) + "') AND TABLE_NAME LIKE 'orders_%';")
    record("8 张分片表已预建", created == str(SHARDS), f"实际 {created}")

    auto_inc = scalar("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA IN ('"
                      + "','".join(TGT_DBS) + "') AND TABLE_NAME LIKE 'orders_%' AND EXTRA LIKE '%auto_increment%';")
    record("分片表已剥掉 AUTO_INCREMENT（各片自增会撞主键）", auto_inc == "0", f"实际 {auto_inc}")

    expected = expected_counts()
    total = 0
    per_shard_ok = True
    detail = ""
    for shard in range(SHARDS):
        db, tbl = shard_location(shard)
        got = scalar(f"SELECT COUNT(*) FROM {tbl};", db)
        # NULL 分片键的那一行按 BROADCAST 进每一片
        want = expected[shard] + 1
        total += int(got or 0)
        if got != str(want):
            per_shard_ok = False
            detail += f"{db}.{tbl} 期望 {want} 实际 {got}; "
    record("每片行数与哈希口径一致（含广播行）", per_shard_ok, detail)
    record("总行数守恒（200 数据行 + 8 份广播行）", total == ROWS + SHARDS, f"实际 {total}")

    # 逐行核对落点：随机取几个 user_id，确认只出现在它该在的分片里
    placement_ok = True
    detail = ""
    for i in (1, 37, 99, 200):
        uid = i * 7 % 97
        shard = shard_of(uid)
        db, tbl = shard_location(shard)
        here = scalar(f"SELECT COUNT(*) FROM {tbl} WHERE id={i};", db)
        elsewhere = 0
        for other in range(SHARDS):
            if other == shard:
                continue
            odb, otbl = shard_location(other)
            elsewhere += int(scalar(f"SELECT COUNT(*) FROM {otbl} WHERE id={i};", odb) or 0)
        if here != "1" or elsewhere != 0:
            placement_ok = False
            detail += f"id={i}(user_id={uid}) 应在片{shard}: here={here}, elsewhere={elsewhere}; "
    record("抽样行落在正确的分片上，且不出现在别的分片", placement_ok, detail)

    null_row = 0
    for shard in range(SHARDS):
        db, tbl = shard_location(shard)
        null_row += int(scalar(f"SELECT COUNT(*) FROM {tbl} WHERE id={ROWS + 1};", db) or 0)
    record("分片键为 NULL 的行按 BROADCAST 进了每一片（未静默丢弃）",
           null_row == SHARDS, f"实际 {null_row}")

    print("== 幂等重跑：同一份配置再跑一次不应翻倍 ==")
    clean_task_state()
    rc2, _ = run_full()
    total2 = 0
    for shard in range(SHARDS):
        db, tbl = shard_location(shard)
        total2 += int(scalar(f"SELECT COUNT(*) FROM {tbl};", db) or 0)
    record("重跑后总行数不变（重复主键被跳过，未翻倍）",
           rc2 == 0 and total2 == ROWS + SHARDS, f"rc={rc2}, 实际 {total2}")

    crash_resume_scenario()
    return summarize()


def crash_resume_scenario():
    """
    真崩溃续传：大表搬到一半 SIGKILL，再跑到完成，核对<b>一行不少</b>。

    这是"清空所有分片再重搬"那段逻辑存在的唯一理由——只清一张分片表的话，
    其余分片会带着半截数据按 lastMigratedId 续扫，跳过的区间就是永久丢失的行。
    只比总数是查不出"清错表"的（重复主键会被跳过），能查出来的是<b>缺行</b>。
    """
    print("== 崩溃续传：大表搬一半 SIGKILL 后重跑，核对一行不少 ==")
    big_rows = 100000
    big_shards = 4
    mysql(f"""
DROP TABLE IF EXISTS orders_big;
CREATE TABLE orders_big (
  id BIGINT NOT NULL,
  user_id BIGINT DEFAULT NULL,
  amount DECIMAL(10,2) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET SESSION cte_max_recursion_depth = 1000000;
INSERT INTO orders_big (id, user_id, amount)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < {big_rows})
SELECT n, n * 7 % 997, n / 2 FROM seq;
""", db=SRC_DB)
    src_count = scalar("SELECT COUNT(*) FROM orders_big;", SRC_DB)
    if src_count != str(big_rows):
        record("大表数据准备", False, f"源表 {src_count} 行，期望 {big_rows}")
        return

    # 只跑这张大表，拆 4 片
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    with open(os.path.join(d, "config.properties")) as f:
        cfg = f.read()
    cfg = cfg.replace(f"migration.included.tables={SRC_DB}.orders",
                      f"migration.included.tables={SRC_DB}.orders_big")
    cfg = cfg.replace(f"route.split.1.match={SRC_DB}.orders",
                      f"route.split.1.match={SRC_DB}.orders_big")
    cfg = cfg.replace(f"route.split.1.count={SHARDS}", f"route.split.1.count={big_shards}")
    cfg = cfg.replace("route.split.1.target.db=spl_dw_${shard/4}",
                      "route.split.1.target.db=spl_dw_0")
    cfg = cfg.replace("route.split.1.target.table=orders_${shard}",
                      "route.split.1.target.table=big_${shard}")
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)
    clean_task_state()

    env = dict(os.environ)
    env["JAVA_HOME"] = subprocess.run(
        ["/usr/libexec/java_home", "-v", "21"], capture_output=True, text=True).stdout.strip()
    java = os.path.join(env["JAVA_HOME"], "bin", "java")
    cmd = [java, "-Dh2.bindAddress=127.0.0.1", "-jar", JAR, "--task-id", TASK]

    import signal
    import time
    def loaded():
        return sum(int(scalar(f"SELECT COUNT(*) FROM big_{s};", TGT_DBS[0]) or 0)
                   for s in range(big_shards))

    p = subprocess.Popen(cmd, cwd=PROJECT_ROOT, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, env=env)
    # 固定睡眠会赌运气（搬得快就已经搬完了）：轮询到<b>确实有一部分落库</b>再动手杀
    killed_midway = False
    deadline = time.time() + 120
    while time.time() < deadline:
        time.sleep(0.3)
        if p.poll() is not None:
            break
        try:
            if loaded() > 1000:
                p.send_signal(signal.SIGKILL)
                p.wait(timeout=30)
                killed_midway = True
                break
        except Exception:
            continue   # 分片表尚未建出来
    partial = loaded()
    record("搬运中途被 SIGKILL（构造出半截数据）",
           killed_midway and 0 < partial < big_rows, f"已落 {partial} 行")

    rc, out = run_full()
    total = sum(int(scalar(f"SELECT COUNT(*) FROM big_{s};", TGT_DBS[0]) or 0)
                for s in range(big_shards))
    record("崩溃重跑后一行不少", rc == 0 and total == big_rows, f"rc={rc}, 实际 {total}")

    # 逐片核对：每片的行必须都满足 floorMod(user_id, 4) == 片号
    misplaced = 0
    for s in range(big_shards):
        bad = scalar(f"SELECT COUNT(*) FROM big_{s} WHERE MOD(user_id, {big_shards}) <> {s};",
                     TGT_DBS[0])
        misplaced += int(bad or 0)
    record("崩溃重跑后没有错片的行", misplaced == 0, f"错片 {misplaced} 行")


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 拆分全量 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
