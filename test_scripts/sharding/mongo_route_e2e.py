#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 聚合路由（汇聚 N 集合 → 1 / 拆分 1 → N）端到端测试。

直接驱动 migration-mongo 子进程，源 synctask-mongo-a(27117) → 目标 synctask-mongo-b(27118)。

Mongo 侧最容易错的两件事，本脚本各有专门用例：
  1. <b>汇聚必须换 _id</b>。沿用原 _id 的话，两个来源里 _id 相同的文档 upsert 会互相覆盖，
     数据只会少、不会报错——与关系库"必须用复合主键"是同一件事。
  2. <b>change stream 没有前镜像</b>。DELETE 事件只带 _id，算不出分片键；UPDATE 改了分片键时
     旧落点也算不出来。做法是：删按 _id 广播到每一片，更新先清其余片再写新落点。
     没有这两条，拆分下会留下删不掉的幽灵文档和跨片重复。

前置：synctask-mongo-a / synctask-mongo-b 副本集在跑；migration-mongo fat jar 已 clean install。
用法：python3 test_scripts/sharding/mongo_route_e2e.py
"""
import json
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-mongo", "target", "migration-mongo-1.0.0.jar")
SRC, DST = "synctask-mongo-a", "synctask-mongo-b"
MRG_DBS = ["rt_shard_1", "rt_shard_2"]
MRG_TGT_DB = "rt_dw"
SPL_DB = "rt_app"
SHARDS = 4

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def msh(container, js):
    p = subprocess.run(
        ["docker", "exec", container, "mongosh", "-u", "root", "-p", "rootpassword",
         "--quiet", "--eval", js], capture_output=True, text=True, timeout=120)
    lines = [l for l in (p.stdout or "").strip().splitlines() if l.strip()]
    return lines[-1].strip() if lines else ""


def src(js):
    return msh(SRC, js)


def dst(js):
    return msh(DST, js)


def shard_of(user_id):
    """与引擎 SplitRule#hashOf 同口径：整数按数值取模。"""
    return user_id % SHARDS


def write_config(task_id, mode, sync_objects, route_lines):
    d = os.path.join(PROJECT_ROOT, "files", task_id)
    os.makedirs(d, exist_ok=True)
    lines = [
        "source.db.type=mongodb", "target.db.type=mongodb",
        "source.db.host=localhost", "source.db.port=27117",
        "source.db.username=root", "source.db.password=rootpassword",
        "target.db.host=localhost", "target.db.port=27118",
        "target.db.username=root", "target.db.password=rootpassword",
        f"migration.sync.objects={json.dumps(sync_objects)}",
        f"migration.mode={mode}",
        "route.node.id=mongo-a",
    ] + route_lines
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write("\n".join(lines) + "\n")
    ckpt = os.path.join(d, "checkpoint", "mongo_resume_token.json")
    if os.path.exists(ckpt):
        os.remove(ckpt)


def run_full(task_id):
    p = subprocess.run(["java", "-cp", JAR, "com.migration.mongo.MongoSyncMain", "--task-id", task_id],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def start_incre(task_id):
    proc = subprocess.Popen(["java", "-cp", JAR, "com.migration.mongo.MongoSyncMain",
                             "--task-id", task_id],
                            cwd=PROJECT_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    prog = os.path.join(PROJECT_ROOT, "files", task_id, "mongo_progress.json")
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


def count(db, coll):
    return int(dst(f"print(db.getSiblingDB('{db}').{coll}.countDocuments({{}}))") or "0")


def check_merge():
    print("== 汇聚：2 个源集合 → 1 个目标集合 ==")
    for db in MRG_DBS:
        src(f"db.getSiblingDB('{db}').dropDatabase()")
        # 两个来源的 _id 完全重复：不换 _id 就会互相覆盖
        src(f"db.getSiblingDB('{db}').orders.insertMany(["
            + ",".join(f"{{_id:{i},amount:{i * 10},tag:'{db}'}}" for i in (1, 2, 3)) + "])")
    dst(f"db.getSiblingDB('{MRG_TGT_DB}').dropDatabase()")

    task = "rt-mongo-merge"
    write_config(task, "full", {db: {"tables": ["orders"]} for db in MRG_DBS}, [
        "route.mode=MERGE",
        "route.merge.1.match=rt_shard_*.orders",
        f"route.merge.1.target={MRG_TGT_DB}.orders_all",
    ])
    rc, out = run_full(task)
    record("汇聚全量退出码为 0", rc == 0, "" if rc == 0 else out[-400:])
    if rc != 0:
        return

    total = count(MRG_TGT_DB, "orders_all")
    record("汇聚后文档数 = 2 源 × 3 = 6（跨源同 _id 都在，没有互相覆盖）", total == 6, f"实际 {total}")
    ids = dst(f"print(db.getSiblingDB('{MRG_TGT_DB}').orders_all.find({{}},{{_id:1}})"
              ".toArray().map(d=>d._id).sort().join(','))")
    record("_id 带来源前缀（mongo-a|库|集合|原_id）",
           "mongo-a|rt_shard_1|orders|1" in ids and "mongo-a|rt_shard_2|orders|1" in ids, ids[:90])
    srcs = dst(f"print(db.getSiblingDB('{MRG_TGT_DB}').orders_all.distinct('_src_db').sort().join(','))")
    record("来源标识写进了文档字段", srcs == ",".join(MRG_DBS), srcs)

    rc2, _ = run_full(task)
    again = count(MRG_TGT_DB, "orders_all")
    record("幂等重跑不翻倍", rc2 == 0 and again == 6, f"rc={rc2}, 实际 {again}")


def check_split():
    print("== 拆分：1 个源集合 → 4 个分片集合（含增量搬迁/广播删）==")
    src(f"db.getSiblingDB('{SPL_DB}').dropDatabase()")
    src(f"db.getSiblingDB('{SPL_DB}').orders.insertMany(["
        + ",".join(f"{{_id:{i},user_id:{i},amount:{i * 10}}}" for i in range(1, 13)) + "])")
    dst(f"db.getSiblingDB('{SPL_DB}').dropDatabase()")

    task = "rt-mongo-split"
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

        counts = [count(SPL_DB, f"orders_{i}") for i in range(SHARDS)]
        record("全量：12 个文档散到 4 片，总数守恒", sum(counts) == 12, f"各片 {counts}")
        wrong = dst("var n=0; " + " ".join(
            f"n+=db.getSiblingDB('{SPL_DB}').orders_{i}.countDocuments({{user_id:{{$mod:[{SHARDS},{i}]}}}})"
            f"!==db.getSiblingDB('{SPL_DB}').orders_{i}.countDocuments({{}})?1:0;" for i in range(SHARDS))
            + " print(n)")
        record("全量：每个文档都落在按分片键算出的那一片", wrong == "0", f"错片集合数 {wrong}")

        # 增量 INSERT
        src(f"db.getSiblingDB('{SPL_DB}').orders.insertOne({{_id:100,user_id:100,amount:1}})")
        time.sleep(4)
        got = count(SPL_DB, f"orders_{shard_of(100)}")
        record("增量 INSERT 落到正确分片",
               dst(f"print(db.getSiblingDB('{SPL_DB}').orders_{shard_of(100)}"
                   ".countDocuments({_id:100}))") == "1", f"该片共 {got}")

        # 增量 UPDATE 改分片键 → 跨分片搬迁（change stream 没有前镜像，靠"先清其余片"实现）
        old_shard, new_user = shard_of(1), 3
        src(f"db.getSiblingDB('{SPL_DB}').orders.updateOne({{_id:1}},{{$set:{{user_id:{new_user}}}}})")
        time.sleep(4)
        in_new = dst(f"print(db.getSiblingDB('{SPL_DB}').orders_{shard_of(new_user)}"
                     ".countDocuments({_id:1}))")
        in_old = dst(f"print(db.getSiblingDB('{SPL_DB}').orders_{old_shard}.countDocuments({{_id:1}}))")
        record("增量改分片键：新片有、旧片已清（跨分片搬迁）", in_new == "1" and in_old == "0",
               f"新片 {in_new} / 旧片 {in_old}")
        dup = dst("var n=0; " + " ".join(
            f"n+=db.getSiblingDB('{SPL_DB}').orders_{i}.countDocuments({{_id:1}});" for i in range(SHARDS))
            + " print(n)")
        record("全局只剩一份（没在多片各留一条）", dup == "1", f"共 {dup} 份")

        # 增量 DELETE：事件只有 _id，算不出分片键 → 必须广播删
        src(f"db.getSiblingDB('{SPL_DB}').orders.deleteOne({{_id:2}})")
        time.sleep(4)
        left = dst("var n=0; " + " ".join(
            f"n+=db.getSiblingDB('{SPL_DB}').orders_{i}.countDocuments({{_id:2}});" for i in range(SHARDS))
            + " print(n)")
        record("增量 DELETE 按 _id 广播删，不留幽灵文档", left == "0", f"残留 {left} 份")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except Exception:
            proc.kill()


def main():
    if not os.path.exists(JAR):
        print("缺少 fat jar，请先 mvn clean install -DskipTests -pl migration-common,migration-mongo -am")
        return 2
    if not src("print(1)"):
        print("mongo 副本集不可用，跳过")
        return 2
    check_merge()
    check_split()
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== MongoDB 聚合路由 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
