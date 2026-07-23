#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mongodb -> mongodb 列处理（列过滤 / 列名映射 / 附加列）端到端测试。

直接驱动 migration-mongo 子进程（MongoSyncMain，读 files/<taskId>/config.properties），
对真实副本集 synctask-mongo-a(27117, 源) -> synctask-mongo-b(27118, 目标) 做全量与增量同步，
覆盖：
  - 全量：边界值 / 特殊值（Decimal128、null、缺失字段、负数、零、unicode、嵌套/数组）+ 大数据量。
  - 增量（Change Streams，fullAndIncre）：insert/update/delete，含
      * 后镜像命中过滤 -> 目标删除（"原本符合、改后不符合"）
      * 前镜像被过滤、后镜像符合 -> 目标 upsert（"原本不符合、改后符合"）
      * 大数据量增量批量写入。
列处理配置（对 testdb.orders / testdb.bigcoll）：
  过滤 amount < 100 排除；列名 note -> remark；附加列 sync_time(CREATE_TIME)、src_tag(CUSTOM:prod)。

前置：docker compose -f docker-compose-synctask-mongo.yml up -d 且副本集已 rs.initiate
      （create_env.sh 会做；或见本目录 README）。migration-mongo fat jar 已 mvn package。
用法：python3 test_scripts/column_processing/mongo_column_processing_e2e.py
"""
import json
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-mongo", "target", "migration-mongo-1.0.0.jar")
SRC = "synctask-mongo-a"   # rsA, host 27117
DST = "synctask-mongo-b"   # rsB, host 27118
DB = "testdb"

FILTER = "amount|<|100"
MAPPING = "note:remark"
EXTRA = "sync_time:CREATE_TIME,src_tag:CUSTOM:prod"

results = []  # (name, ok, detail)


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def msh(container, js):
    """在容器内跑 mongosh，返回 stdout 最后一非空行（--quiet 抑制横幅）。"""
    p = subprocess.run(
        ["docker", "exec", container, "mongosh", "-u", "root", "-p", "rootpassword",
         "--quiet", "--eval", js],
        capture_output=True, text=True, timeout=120)
    out = (p.stdout or "").strip().splitlines()
    lines = [l for l in out if l.strip() != ""]
    return lines[-1].strip() if lines else ""


def src(js):
    return msh(SRC, js)


def dst(js):
    return msh(DST, js)


def write_config(task_id, mode, collections):
    d = os.path.join(PROJECT_ROOT, "files", task_id)
    os.makedirs(d, exist_ok=True)
    sync_objects = json.dumps({DB: {"tables": collections}})
    lines = [
        "source.db.host=localhost", "source.db.port=27117",
        "source.db.username=root", "source.db.password=rootpassword",
        "target.db.host=localhost", "target.db.port=27118",
        "target.db.username=root", "target.db.password=rootpassword",
        f"migration.sync.objects={sync_objects}",
        f"migration.mode={mode}",
    ]
    for c in collections:
        lines.append(f"column.filter.{DB}.{c}={FILTER}")
        lines.append(f"column.mapping.{DB}.{c}={MAPPING}")
        extra = EXTRA if c == "orders" else "src_tag:CUSTOM:prod"
        lines.append(f"column.extra.{DB}.{c}={extra}")
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write("\n".join(lines) + "\n")


def run_full(task_id):
    """全量模式：MongoSyncMain 完成即退出。"""
    p = subprocess.run(
        ["java", "-cp", JAR, "com.migration.mongo.MongoSyncMain", "--task-id", task_id],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    return p.returncode == 0, (p.stdout or "") + (p.stderr or "")


def start_incre(task_id):
    """fullAndIncre 模式：后台常驻，等 phase 进入 INCREMENT。"""
    proc = subprocess.Popen(
        ["java", "-cp", JAR, "com.migration.mongo.MongoSyncMain", "--task-id", task_id],
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


def target_count(coll):
    return int(dst(f"print(db.getSiblingDB('{DB}').{coll}.countDocuments({{}}))") or "0")


def target_field(coll, _id, field):
    js = (f"var d=db.getSiblingDB('{DB}').{coll}.findOne({{_id:{_id}}}); "
          f"print(d===null?'__MISSING__':(d.{field}===undefined?'__NOFIELD__':d.{field}))")
    return dst(js)


def target_has(coll, _id):
    return target_field(coll, _id, "_id") not in ("__MISSING__",)


def cleanup():
    for c in (SRC, DST):
        msh(c, f"db.getSiblingDB('{DB}').dropDatabase()")


# =========================== Test 1: 全量 + 边界/特殊值 + 大数据量 ===========================
def test_full():
    print("\n[Test 1] 全量同步：边界/特殊值 + 大数据量")
    cleanup()

    # orders：12 条覆盖边界/特殊值
    seed = f"""
    var c = db.getSiblingDB('{DB}').orders;
    c.insertMany([
      {{_id:1, amount:50,  note:'a'}},
      {{_id:2, amount:99,  note:'b'}},
      {{_id:3, amount:100, note:'c'}},
      {{_id:4, amount:101, note:'d'}},
      {{_id:5, amount:0,   note:'zero'}},
      {{_id:6, amount:-5,  note:'neg'}},
      {{_id:7, amount:null, note:'nullamt'}},
      {{_id:8, note:'noamt'}},
      {{_id:9, amount:NumberDecimal('99.99'),  note:'dec-lo'}},
      {{_id:10, amount:NumberDecimal('100.01'), note:'dec-hi'}},
      {{_id:11, amount:1000000000000, note:'中文📦unicode'}},
      {{_id:12, amount:250, note:'nested', meta:{{a:{{b:1}}}}, tags:[1,2,3]}}
    ]);
    db.getSiblingDB('{DB}').orders.createIndex({{note:1}}, {{name:'note_idx'}});
    print(c.countDocuments({{}}));
    """
    n = src(seed)
    record("源 orders 种子写入 12 条", n == "12", f"count={n}")

    # bigcoll：5000 条大数据量，amount=i（0..4999），过滤后保留 amount>=100 共 4900
    big = f"""
    var b = db.getSiblingDB('{DB}').bigcoll;
    var docs=[];
    for (var i=0;i<5000;i++) {{ docs.push({{_id:i, amount:i, note:'n'+i}}); }}
    b.insertMany(docs);
    print(b.countDocuments({{}}));
    """
    nb = src(big)
    record("源 bigcoll 种子写入 5000 条", nb == "5000", f"count={nb}")

    write_config("cp-mongo-full", "full", ["orders", "bigcoll"])
    ok, log = run_full("cp-mongo-full")
    if not ok:
        record("全量子进程退出码 0", False, log[-400:])
        return
    record("全量子进程退出码 0", True)

    # ---- orders 断言 ----
    # 保留：3,4,7,8,10,11,12 = 7 条（<100 与 Decimal128<100 排除；null/缺失保留）
    oc = target_count("orders")
    record("orders 过滤后目标 7 条（排除 amount<100 含 Decimal128；null/缺失保留）",
           oc == 7, f"count={oc}")
    record("边界：amount=100 保留", target_has("orders", 3))
    record("边界：amount=99 排除", not target_has("orders", 2))
    record("特殊：amount=0 排除", not target_has("orders", 5))
    record("特殊：amount=-5(负) 排除", not target_has("orders", 6))
    record("特殊：amount=null 保留", target_has("orders", 7))
    record("特殊：amount 缺失 保留", target_has("orders", 8))
    record("Decimal128 99.99 排除", not target_has("orders", 9))
    record("Decimal128 100.01 保留", target_has("orders", 10))

    # 列名映射：note -> remark（且原 note 不存在）
    record("列名映射 note->remark（_id=3 remark=c）", target_field("orders", 3, "remark") == "c")
    record("原列名 note 已不存在（_id=3）", target_field("orders", 3, "note") == "__NOFIELD__")
    record("unicode 值随映射保真（_id=11 remark=中文📦unicode）",
           target_field("orders", 11, "remark") == "中文📦unicode")
    # 嵌套 / 数组保真
    record("嵌套字段保真（_id=12 meta.a.b=1）", target_field("orders", 12, "meta.a.b") == "1")
    record("数组保真（_id=12 tags[2]=3）", target_field("orders", 12, "tags[2]") == "3")
    # 附加列
    record("附加列 src_tag = prod@testdb@orders（_id=4）",
           target_field("orders", 4, "src_tag") == "prod@testdb@orders")
    st = target_field("orders", 4, "sync_time")
    record("附加列 sync_time 为时间值（_id=4）", st not in ("__NOFIELD__", "__MISSING__", ""), f"sync_time={st}")
    # 索引 key 随列名映射改写：note_idx 应建在 remark 上
    idxkey = dst(f"var ix=db.getSiblingDB('{DB}').orders.getIndexes().find(x=>x.name=='note_idx'); "
                 f"print(ix?Object.keys(ix.key)[0]:'__NOIDX__')")
    record("索引 note_idx key 随映射改写为 remark", idxkey == "remark", f"key={idxkey}")

    # ---- bigcoll 大数据量断言 ----
    bc = target_count("bigcoll")
    record("大数据量：bigcoll 过滤后目标 4900 条（amount>=100）", bc == 4900, f"count={bc}")
    record("大数据量：amount=99 排除（bigcoll _id=99）", not target_has("bigcoll", 99))
    record("大数据量：amount=100 保留且映射（bigcoll _id=100 remark=n100）",
           target_field("bigcoll", 100, "remark") == "n100")
    record("大数据量：附加列注入（bigcoll _id=4999 src_tag）",
           target_field("bigcoll", 4999, "src_tag") == "prod@testdb@bigcoll")


# =========================== Test 2: 增量 CRUD + 大数据量增量 ===========================
def test_increment():
    print("\n[Test 2] 增量同步（fullAndIncre / Change Streams）：CRUD + 大数据量")
    cleanup()
    # 初始少量数据：_id 1(50,排除) 3(100,保留) 4(101,保留) 10(200,保留)
    src(f"""db.getSiblingDB('{DB}').orders.insertMany([
      {{_id:1, amount:50,  note:'a'}},
      {{_id:3, amount:100, note:'c'}},
      {{_id:4, amount:101, note:'d'}},
      {{_id:10, amount:200, note:'x'}}
    ]);""")

    write_config("cp-mongo-incre", "fullAndIncre", ["orders"])
    proc = start_incre("cp-mongo-incre")
    try:
        prog = os.path.join(PROJECT_ROOT, "files", "cp-mongo-incre", "mongo_progress.json")
        phase = ""
        try:
            with open(prog) as f:
                phase = json.load(f).get("phase")
        except Exception:
            pass
        if phase != "INCREMENT":
            record("增量子进程进入 INCREMENT 阶段", False, f"phase={phase}")
            return
        record("增量子进程进入 INCREMENT 阶段", True)
        # 全量部分：保留 3,4,10（1 被过滤）
        time.sleep(2)
        record("全量阶段目标 3 条（_id 1 被过滤）", target_count("orders") == 3, f"count={target_count('orders')}")

        # ---- 增量 CRUD ----
        src(f"db.getSiblingDB('{DB}').orders.insertOne({{_id:100, amount:200, note:'ins-keep'}});")
        src(f"db.getSiblingDB('{DB}').orders.insertOne({{_id:101, amount:50, note:'ins-excl'}});")
        src(f"db.getSiblingDB('{DB}').orders.updateOne({{_id:3}}, {{$set:{{note:'updated'}}}});")
        # _id=4：101 -> 50，后镜像被过滤 -> 目标应删除
        src(f"db.getSiblingDB('{DB}').orders.updateOne({{_id:4}}, {{$set:{{amount:50}}}});")
        # _id=1：原被过滤(50) -> 500，后镜像符合 -> 目标应 upsert 出现
        src(f"db.getSiblingDB('{DB}').orders.updateOne({{_id:1}}, {{$set:{{amount:500}}}});")
        # _id=10 删除
        src(f"db.getSiblingDB('{DB}').orders.deleteOne({{_id:10}});")

        # 等增量应用
        deadline = time.time() + 30
        while time.time() < deadline:
            if (target_has("orders", 100) and not target_has("orders", 4)
                    and target_has("orders", 1) and not target_has("orders", 10)):
                break
            time.sleep(1)

        record("增量 INSERT 符合 -> 目标出现（_id=100）", target_has("orders", 100))
        record("增量 INSERT 被过滤 -> 目标不出现（_id=101）", not target_has("orders", 101))
        record("增量 UPDATE -> 目标改名值更新（_id=3 remark=updated）",
               target_field("orders", 3, "remark") == "updated")
        record("增量 UPDATE 后镜像被过滤 -> 目标删除（_id=4）", not target_has("orders", 4))
        record("增量 UPDATE 前过滤后符合 -> 目标 upsert（_id=1 出现, remark=a）",
               target_has("orders", 1) and target_field("orders", 1, "remark") == "a")
        record("增量 DELETE -> 目标删除（_id=10）", not target_has("orders", 10))
        record("增量插入的文档带附加列（_id=100 src_tag）",
               target_field("orders", 100, "src_tag") == "prod@testdb@orders")

        # ---- 大数据量增量：批量插入 2000 条（半数被过滤）----
        src(f"""var docs=[];
        for (var i=1000;i<3000;i++) {{ docs.push({{_id:i, amount:(i%2==0?200:50), note:'b'+i}}); }}
        db.getSiblingDB('{DB}').orders.insertMany(docs);""")
        # 保留者 amount=200（偶数 _id），共 1000 条
        deadline = time.time() + 60
        target = 0
        while time.time() < deadline:
            target = int(dst(f"print(db.getSiblingDB('{DB}').orders.countDocuments({{_id:{{$gte:1000,$lt:3000}}}}))") or "0")
            if target >= 1000:
                break
            time.sleep(2)
        record("大数据量增量：2000 条批量插入后目标保留 1000 条（偶数 amount=200）",
               target == 1000, f"count={target}")
        record("大数据量增量：奇数 _id(amount=50) 被过滤（_id=1001 不出现）",
               not target_has("orders", 1001))
        record("大数据量增量：偶数 _id 保留且映射（_id=2000 remark=b2000）",
               target_field("orders", 2000, "remark") == "b2000")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()


def main():
    if not os.path.exists(JAR):
        print(f"缺少 fat jar：{JAR}\n先执行：mvn -pl migration-mongo -am package -DskipTests")
        sys.exit(2)
    print("=" * 72)
    print("MongoDB 列处理 E2E：synctask-mongo-a(27117) -> synctask-mongo-b(27118)")
    print("=" * 72)
    try:
        test_full()
        test_increment()
    finally:
        cleanup()

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
