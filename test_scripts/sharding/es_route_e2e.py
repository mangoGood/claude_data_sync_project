#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL → Elasticsearch 聚合路由（汇聚 N 表 → 1 索引 / 拆分 1 表 → N 索引）端到端测试。

直接驱动 migration-elastic 子进程，源 synctask-mysql(33306) → 目标 synctask-es(9200)。

ES 侧的两个要害：
  1. <b>汇聚必须给 _id 加来源前缀</b>。ES 的 index 操作就是 upsert，两张源表里主键相同的行
     会写成同一个 _id 互相覆盖——不报错，只少数据。
  2. <b>拆分的增量能算准落点</b>。binlog 的 UPDATE/DELETE 都带整行前镜像，
     旧分片算得出来，因此跨分片搬迁是"旧片精确删 + 新片写"，不必像 Mongo 那样广播删。

前置：synctask-mysql(33306)、synctask-es(9200, elastic/espassword) 在跑；
      migration-elastic fat jar 已 clean install。
用法：python3 test_scripts/sharding/es_route_e2e.py
"""
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-elastic", "target", "migration-elastic-1.0.0.jar")
CT = "synctask-mysql"
ES = "http://localhost:9200"
ES_USER, ES_PASS = "elastic", "espassword"
MRG_DB = "esr_mrg"
SPL_DB = "esr_spl"
SHARDS = 4

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
    return (subprocess.run(args, input=sql, capture_output=True, text=True, timeout=180).stdout or "").strip()


def es(path, method="GET", body=None):
    req = urllib.request.Request(ES + path, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Basic " + base64.b64encode(
        f"{ES_USER}:{ES_PASS}".encode()).decode())
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data, timeout=30) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {}


def es_count(index):
    es(f"/{index}/_refresh", "POST")
    r = es(f"/{index}/_count")
    return int(r.get("count", -1)) if "count" in r else -1


def es_doc(index, doc_id):
    es(f"/{index}/_refresh", "POST")
    return es(f"/{index}/_doc/{urllib.parse.quote(doc_id, safe='')}")


def shard_of(user_id):
    return user_id % SHARDS


def drop_indexes(prefix):
    es(f"/{prefix}*", "DELETE")


def write_config(task_id, mode, sync_objects, route_lines):
    d = os.path.join(PROJECT_ROOT, "files", task_id)
    os.makedirs(d, exist_ok=True)
    lines = [
        "source.db.type=mysql", "target.db.type=elasticsearch",
        "source.db.host=localhost", "source.db.port=33306",
        "source.db.username=root", "source.db.password=rootpassword",
        "target.db.host=localhost", "target.db.port=9200",
        f"target.db.username={ES_USER}", f"target.db.password={ES_PASS}",
        f"migration.sync.objects={json.dumps(sync_objects)}",
        f"migration.mode={mode}",
        "route.node.id=mysql-33306",
    ] + route_lines
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write("\n".join(lines) + "\n")
    ckpt = os.path.join(d, "checkpoint", "elastic_binlog_position.json")
    if os.path.exists(ckpt):
        os.remove(ckpt)


def run_full(task_id):
    p = subprocess.run(["java", "-cp", JAR, "com.migration.elastic.ElasticSyncMain",
                        "--task-id", task_id],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def start_incre(task_id):
    proc = subprocess.Popen(["java", "-cp", JAR, "com.migration.elastic.ElasticSyncMain",
                             "--task-id", task_id],
                            cwd=PROJECT_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    prog = os.path.join(PROJECT_ROOT, "files", task_id, "elastic_progress.json")
    for _ in range(60):
        try:
            with open(prog) as f:
                if json.load(f).get("phase") == "INCREMENT":
                    return proc
        except Exception:
            pass
        if proc.poll() is not None:
            break
        time.sleep(1)
    return proc


def check_merge():
    print("== 汇聚：2 张源表 → 1 个索引 ==")
    mysql(f"DROP DATABASE IF EXISTS {MRG_DB}; CREATE DATABASE {MRG_DB} DEFAULT CHARACTER SET utf8mb4;")
    for t in ("order_001", "order_002"):
        mysql(f"CREATE TABLE {t} (id BIGINT NOT NULL, amount INT, PRIMARY KEY(id)) ENGINE=InnoDB;", db=MRG_DB)
        # 两张表主键完全重复：不加来源前缀就会互相覆盖
        mysql(f"INSERT INTO {t} VALUES " + ",".join(f"({i},{i * 10})" for i in (1, 2, 3)) + ";", db=MRG_DB)
    drop_indexes("esr_")

    task = "esr-merge"
    write_config(task, "full", {MRG_DB: {"tables": ["order_001", "order_002"]}}, [
        "route.mode=MERGE",
        f"route.merge.1.match={MRG_DB}.order_*",
        f"route.merge.1.target={MRG_DB}.orders_all",
    ])
    rc, out = run_full(task)
    record("汇聚全量退出码为 0", rc == 0, "" if rc == 0 else out[-400:])
    if rc != 0:
        return

    index = f"{MRG_DB}_orders_all".lower()
    total = es_count(index)
    record("汇聚后文档数 = 2 表 × 3 = 6（同主键没有互相覆盖）", total == 6, f"实际 {total}")
    doc = es_doc(index, "mysql-33306|" + MRG_DB + "|order_001|1")
    record("_id 带来源前缀", doc.get("found") is True, str(doc.get("_id"))[:60])
    src_tables = doc.get("_source", {}).get("_src_table")
    record("来源标识写进了文档字段", src_tables == "order_001", str(src_tables))
    record("原样同名索引没有被建出来（数据只进合并索引）",
           es_count(f"{MRG_DB}_order_001".lower()) in (-1, 0),
           str(es_count(f"{MRG_DB}_order_001".lower())))


def check_split():
    print("== 拆分：1 张源表 → 4 个索引（含增量搬迁）==")
    mysql(f"DROP DATABASE IF EXISTS {SPL_DB}; CREATE DATABASE {SPL_DB} DEFAULT CHARACTER SET utf8mb4;")
    mysql("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT NOT NULL, amount INT, "
          "PRIMARY KEY(id)) ENGINE=InnoDB;", db=SPL_DB)
    mysql("INSERT INTO orders VALUES " + ",".join(f"({i},{i},{i * 10})" for i in range(1, 13)) + ";",
          db=SPL_DB)
    drop_indexes("esr_spl")

    task = "esr-split"
    write_config(task, "fullAndIncre", {SPL_DB: {"tables": ["orders"]}}, [
        "route.mode=SPLIT",
        f"route.split.1.match={SPL_DB}.orders",
        "route.split.1.shard.key=user_id",
        "route.split.1.algo=HASH_MOD",
        f"route.split.1.count={SHARDS}",
        "route.split.1.target.table=orders_${shard}",
        "route.split.1.unrouted=DEADLETTER",
    ])
    proc = start_incre(task)
    try:
        if proc.poll() is not None:
            record("拆分任务进入增量", False, "进程已退出")
            return
        record("拆分任务进入增量", True)

        idx = [f"{SPL_DB}_orders_{i}".lower() for i in range(SHARDS)]
        counts = [es_count(i) for i in idx]
        record("全量：12 行散到 4 个索引，总数守恒", sum(c for c in counts if c > 0) == 12, f"各片 {counts}")
        placed = all(es_doc(idx[shard_of(i)], str(i)).get("found") for i in range(1, 13))
        record("全量：每行都落在按分片键算出的那个索引", placed)

        # 增量 INSERT
        mysql("INSERT INTO orders VALUES (100,100,1);", db=SPL_DB)
        time.sleep(6)
        record("增量 INSERT 落到正确索引",
               es_doc(idx[shard_of(100)], "100").get("found") is True)

        # 增量 UPDATE 改分片键：binlog 带前镜像，旧片能精确删
        mysql("UPDATE orders SET user_id=3 WHERE id=1;", db=SPL_DB)
        time.sleep(6)
        in_new = es_doc(idx[shard_of(3)], "1").get("found") is True
        in_old = es_doc(idx[shard_of(1)], "1").get("found") is True
        record("增量改分片键：新索引有、旧索引已删（跨分片搬迁）", in_new and not in_old,
               f"新 {in_new} / 旧 {in_old}")

        # 增量 DELETE：前镜像里有分片键，落点算得准
        mysql("DELETE FROM orders WHERE id=2;", db=SPL_DB)
        time.sleep(6)
        left = sum(1 for i in idx if es_doc(i, "2").get("found"))
        record("增量 DELETE 精确删掉那一片上的文档", left == 0, f"残留 {left} 份")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except Exception:
            proc.kill()


def main():
    if not os.path.exists(JAR):
        print("缺少 fat jar，请先 mvn clean install -DskipTests -pl migration-common,migration-elastic -am")
        return 2
    if not es("/").get("version"):
        print("Elasticsearch 不可用，跳过")
        return 2
    check_merge()
    check_split()
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== Elasticsearch 聚合路由 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
