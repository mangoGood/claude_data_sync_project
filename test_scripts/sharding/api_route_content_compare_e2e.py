#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
路由任务的<b>内容对比</b>端到端测试（汇聚 N:1 / 拆分 1:N）。

走真实 API：建任务 → 跑到增量中 → 人为在目标端制造四种差异 → 内容对比要全部抓到并定位到主键
→ 一键修复 → 复核收敛。

四种差异里 <b>WRONG_SHARD</b>（行落在错误的分片上）是行数对比和 1:1 内容对比都发现不了的：
总行数一样、每一行都在，只是待错了地方。它正是"分片哈希口径漂移"和"跨分片搬迁只插没删"
这两类 bug 的直接探针。

前置：./start.sh 已起好后端(38080)与 agent；synctask-mysql 在跑。
用法：python3 test_scripts/sharding/api_route_content_compare_e2e.py
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zlib

BASE = "http://localhost:38080/api"
CT = "synctask-mysql"
MRG_SRC, MRG_TGT = "cc_mrg_src", "cc_mrg_dw"
SPL_SRC, SPL_TGT = "cc_spl_src", "cc_spl_dw"
SHARDS = 4

results = []
token = None


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def api(path, method="GET", body=None):
    req = urllib.request.Request(BASE + path, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data, timeout=120) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        payload = e.read().decode()
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {"success": False, "message": payload}


def mysql(sql, db=None, want=False):
    args = ["docker", "exec", "-i", CT, "mysql", "-uroot", "-prootpassword"]
    if want:
        args.append("-N")
    if db:
        args += ["-D", db]
    return (subprocess.run(args, input=sql, capture_output=True, text=True, timeout=120).stdout or "").strip()


def conn_str(db=""):
    return "mysql://root:rootpassword@127.0.0.1:33306" + ("/" + db if db else "")


def shard_of(user_id):
    """与引擎 SplitRule#hashOf 同口径：整数按数值取模。"""
    return user_id % SHARDS


# ---------------- 建任务 ----------------

def setup_merge():
    mysql(f"DROP DATABASE IF EXISTS {MRG_SRC}; CREATE DATABASE {MRG_SRC};"
          f"DROP DATABASE IF EXISTS {MRG_TGT}; CREATE DATABASE {MRG_TGT};")
    for t, n in (("order_001", 6), ("order_002", 6)):
        mysql(f"CREATE TABLE {t} (id BIGINT NOT NULL, amount INT, memo VARCHAR(50), PRIMARY KEY(id)) ENGINE=InnoDB;",
              db=MRG_SRC)
        mysql(f"INSERT INTO {t} VALUES " + ",".join(f"({i},{i * 10},'{t}-{i}')" for i in range(1, n + 1)) + ";",
              db=MRG_SRC)


def setup_split():
    mysql(f"DROP DATABASE IF EXISTS {SPL_SRC}; CREATE DATABASE {SPL_SRC};"
          f"DROP DATABASE IF EXISTS {SPL_TGT}; CREATE DATABASE {SPL_TGT};")
    mysql("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT NOT NULL, amount INT, PRIMARY KEY(id)) ENGINE=InnoDB;",
          db=SPL_SRC)
    mysql("INSERT INTO orders VALUES " + ",".join(f"({i},{i},{i * 10})" for i in range(1, 17)) + ";", db=SPL_SRC)


def create_task(name, src_db, tgt_db, tables, route_config):
    created = api("/workflows", "POST", {"name": name + "-" + str(int(time.time()))})
    task_id = (created.get("data") or {}).get("id")
    if not task_id:
        print("  建任务失败:", created.get("message"))
        return None
    api(f"/workflows/{task_id}/config", "PUT", {
        "sourceConnection": conn_str(), "targetConnection": conn_str(tgt_db),
        "migrationMode": "fullAndIncre", "sourceType": "mysql", "targetType": "mysql",
        "sourceDbName": src_db, "targetDbName": tgt_db,
        "syncObjects": json.dumps({src_db: {"tables": tables}}),
    })
    r = api(f"/workflows/{task_id}/route-config", "PUT", {"routeConfig": route_config})
    if not r.get("success"):
        print("  路由配置保存失败:", r.get("message"))
        return None
    api(f"/workflows/{task_id}/launch", "POST", {})
    return task_id


def wait_increment(task_id, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = mysql(f"SELECT status FROM workflows WHERE id='{task_id}';", db="sync_task_db", want=True)
        if st in ("INCREMENT_RUNNING", "FAILED"):
            return st
        time.sleep(3)
    return "TIMEOUT"


def stop_previous_runs():
    """
    停掉并删除上一轮自己建的任务（只认 cc- 前缀）。

    两个都得做：
      - <b>停</b>：上一轮的增量管线还活着的话，本轮 setup 的 DROP/CREATE DATABASE 会被它重放到
        目标端，把本轮刚建好的汇聚表连库一起删掉（实测现象是目标端查出来是空）。
      - <b>删</b>：任务是配额资源（每用户 50 个），不删的话跑两轮就建不出新任务了。
    """
    listed = api("/workflows?page=1&pageSize=100")
    mine = [it for it in ((listed.get("data") or {}).get("list") or [])
            if str(it.get("name", "")).startswith("cc-")]
    for it in mine:
        api(f"/workflows/{it.get('id')}/stop", "POST", {})
    if mine:
        print(f"  停止上一轮遗留任务 {len(mine)} 个，等待管线退出...")
        time.sleep(20)
        for it in mine:
            api(f"/workflows/{it.get('id')}", "DELETE")


# ---------------- 对比 ----------------

def run_compare(task_id, compare_type="CONTENT"):
    r = api("/validation-tasks", "POST", {"workflowId": task_id, "compareType": compare_type})
    if not r.get("success"):
        return None, r.get("message")
    vid = (r.get("data") or {}).get("id")
    deadline = time.time() + 240
    while time.time() < deadline:
        d = (api(f"/validation-tasks/{vid}").get("data") or {})
        if d.get("status") in ("COMPLETED", "FAILED", "PARTIAL"):
            d["_id"] = vid
            return d, None
        time.sleep(3)
    return None, "对比超时"


def compare_tables(result):
    raw = result.get("compareResult")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return (raw or {}).get("tables", []) if isinstance(raw, dict) else []


def diffs_of(result, source_table):
    for t in compare_tables(result):
        if t.get("sourceTable") == source_table:
            return t.get("diffs") or []
    return []


def diff_types(diffs):
    out = {}
    for d in diffs:
        out.setdefault(d.get("diffType"), []).append(d)
    return out


# ---------------- 场景 ----------------

def check_merge():
    print("== 汇聚：2 张分表 → 1 张合并表 ==")
    setup_merge()
    task_id = create_task("cc-merge", MRG_SRC, MRG_TGT, ["order_001", "order_002"], {
        "mode": "MERGE",
        "merge": [{"match": f"{MRG_SRC}.order_*", "target": f"{MRG_TGT}.order_all",
                   "pkStrategy": "COMPOSITE_SOURCE", "ddlPolicy": "FIRST_WINS"}]})
    if not task_id:
        record("汇聚任务创建", False)
        return
    st = wait_increment(task_id)
    record("汇聚任务已进入增量同步", st == "INCREMENT_RUNNING", st)
    if st != "INCREMENT_RUNNING":
        return

    result, err = run_compare(task_id)
    if result is None:
        record("汇聚任务允许创建内容对比", False, str(err))
        return
    record("汇聚任务允许创建内容对比（此前直接被拒）", True)
    record("无差异时判为一致", result.get("failedTables") == 0,
           f"failed={result.get('failedTables')}")

    # 人为制造三种差异：只动 order_001 的那一片，order_002 的行必须不受影响
    node = mysql(f"SELECT DISTINCT _src_node FROM order_all LIMIT 1;", db=MRG_TGT, want=True)
    where1 = f"_src_db='{MRG_SRC}' AND _src_table='order_001'"
    mysql(f"DELETE FROM order_all WHERE id=2 AND {where1};", db=MRG_TGT)
    mysql(f"UPDATE order_all SET amount=99999 WHERE id=3 AND {where1};", db=MRG_TGT)
    mysql(f"INSERT INTO order_all (id,amount,memo,_src_node,_src_db,_src_table) "
          f"VALUES (777,0,'ghost','{node}','{MRG_SRC}','order_001');", db=MRG_TGT)

    result, err = run_compare(task_id)
    if result is None:
        record("汇聚：制造差异后再对比", False, str(err))
        return
    by_type = diff_types(diffs_of(result, "order_001"))
    record("汇聚：目标端少一行 → SOURCE_ONLY",
           any(str(d.get("primaryKeyValue")) == "2" for d in by_type.get("SOURCE_ONLY", [])),
           str(list(by_type.keys())))
    record("汇聚：字段被改 → CONTENT_DIFF",
           any(str(d.get("primaryKeyValue")) == "3" for d in by_type.get("CONTENT_DIFF", [])))
    record("汇聚：目标端多一行 → TARGET_ONLY",
           any(str(d.get("primaryKeyValue")) == "777" for d in by_type.get("TARGET_ONLY", [])))
    record("汇聚：另一个来源的同主键行没被误判",
           len(diffs_of(result, "order_002")) == 0,
           f"order_002 差异 {len(diffs_of(result, 'order_002'))} 条")

    repair = api(f"/validation-tasks/{result['_id']}/repair", "POST", {})
    record("汇聚：一键修复执行成功", repair.get("success") is True, str(repair.get("message"))[:60])
    left = mysql(f"SELECT COUNT(*) FROM order_all WHERE {where1};", db=MRG_TGT, want=True)
    record("汇聚：修复后该来源行数回到 6（幽灵行已删、缺行已补）", left == "6", f"实际 {left}")
    other = mysql(f"SELECT COUNT(*) FROM order_all WHERE _src_table='order_002';", db=MRG_TGT, want=True)
    record("汇聚：修复没有波及另一个来源", other == "6", f"实际 {other}")

    after, _ = run_compare(task_id)
    record("汇聚：复核已收敛", after is not None and after.get("failedTables") == 0,
           f"failed={after.get('failedTables') if after else 'n/a'}")


def check_split():
    print("== 拆分：1 张源表 → 4 张分片表 ==")
    setup_split()
    task_id = create_task("cc-split", SPL_SRC, SPL_TGT, ["orders"], {
        "mode": "SPLIT",
        "split": [{"match": f"{SPL_SRC}.orders", "shardKey": "user_id", "algo": "HASH_MOD",
                   "count": SHARDS, "targetDb": SPL_TGT, "targetTable": "orders_${shard}",
                   "unrouted": "DEADLETTER"}]})
    if not task_id:
        record("拆分任务创建", False)
        return
    st = wait_increment(task_id)
    record("拆分任务已进入增量同步", st == "INCREMENT_RUNNING", st)
    if st != "INCREMENT_RUNNING":
        return

    result, err = run_compare(task_id)
    if result is None:
        record("拆分任务允许创建内容对比", False, str(err))
        return
    record("拆分任务允许创建内容对比（此前直接被拒）", True)
    record("无差异时判为一致", result.get("failedTables") == 0, f"failed={result.get('failedTables')}")

    # ---- 关键用例：错片是<b>唯一</b>的问题 ----
    # 行级摘要（COUNT + SUM(CRC32)）对顺序不敏感——这正是它能跨分片相加的原因，
    # 但也意味着"一行从这片挪到那片"算出来的摘要分毫不差。只靠摘要就会判成"一致"。
    lone = 9
    lone_right, lone_wrong = shard_of(lone), (shard_of(lone) + 2) % SHARDS
    mysql(f"DELETE FROM orders_{lone_right} WHERE id={lone};"
          f"INSERT INTO orders_{lone_wrong} VALUES ({lone},{lone},{lone * 10});", db=SPL_TGT)
    result, err = run_compare(task_id)
    lone_diffs = diff_types(diffs_of(result, "orders")) if result else {}
    record("只有错片一个问题时也能抓到（摘要一致，靠逐行核落点）",
           any(str(d.get("primaryKeyValue")) == str(lone) for d in lone_diffs.get("WRONG_SHARD", [])),
           str(list(lone_diffs.keys())))
    record("只有错片时只报一条差异（搬迁是一个问题，不该拆成 错片+缺行 两条）",
           set(lone_diffs.keys()) <= {"WRONG_SHARD"}
           and len(lone_diffs.get("WRONG_SHARD", [])) == 1, str(list(lone_diffs.keys())))
    # 复原
    mysql(f"DELETE FROM orders_{lone_wrong} WHERE id={lone};"
          f"INSERT INTO orders_{lone_right} VALUES ({lone},{lone},{lone * 10});", db=SPL_TGT)

    # ---- 同一行在两个分片里各留一份：跨分片搬迁"只插没删"就是这个形态 ----
    dup = 6
    dup_right, dup_wrong = shard_of(dup), (shard_of(dup) + 1) % SHARDS
    mysql(f"INSERT INTO orders_{dup_wrong} VALUES ({dup},{dup},{dup * 10});", db=SPL_TGT)
    result, err = run_compare(task_id)
    dup_diffs = diff_types(diffs_of(result, "orders")) if result else {}
    dup_ws = [d for d in dup_diffs.get("WRONG_SHARD", []) if str(d.get("primaryKeyValue")) == str(dup)]
    record("同一行在两片各一份 → 报错片（SUM 摘要能发现，BIT_XOR 会成对抵消看不见）",
           len(dup_ws) == 1, str(list(dup_diffs.keys())))
    record("重复副本的差异不带源行（对的片上已有正确一份，修复只该删多余的那份）",
           bool(dup_ws) and not dup_ws[0].get("sourceData"),
           "sourceData=" + str(dup_ws[0].get("sourceData"))[:30] if dup_ws else "")
    if result:
        repair = api(f"/validation-tasks/{result['_id']}/repair", "POST", {})
        right_n = mysql(f"SELECT COUNT(*) FROM orders_{dup_right} WHERE id={dup};", db=SPL_TGT, want=True)
        wrong_n = mysql(f"SELECT COUNT(*) FROM orders_{dup_wrong} WHERE id={dup};", db=SPL_TGT, want=True)
        record("修复后只剩对的片上那一份", repair.get("success") is True and right_n == "1" and wrong_n == "0",
               f"对片 {right_n} 行 / 错片 {wrong_n} 行")

    # 四种差异，每种挑一行；id 与 user_id 相同，落点可直接算
    mysql(f"DELETE FROM orders_{shard_of(2)} WHERE id=2;", db=SPL_TGT)                    # SOURCE_ONLY
    mysql(f"UPDATE orders_{shard_of(3)} SET amount=99999 WHERE id=3;", db=SPL_TGT)        # CONTENT_DIFF
    mysql(f"INSERT INTO orders_{shard_of(777)} VALUES (777,777,0);", db=SPL_TGT)          # TARGET_ONLY
    # 错片：把 id=5 从它该在的片搬到隔壁片。总行数不变、每行都在——只有落点是错的
    wrong = (shard_of(5) + 1) % SHARDS
    mysql(f"DELETE FROM orders_{shard_of(5)} WHERE id=5;"
          f"INSERT INTO orders_{wrong} VALUES (5,5,50);", db=SPL_TGT)

    total = mysql("SELECT " + "+".join(f"(SELECT COUNT(*) FROM orders_{i})" for i in range(SHARDS)) + ";",
                  db=SPL_TGT, want=True)
    record("错片行不改变总行数（行数对比看不出来）", total == "16", f"实际 {total}")

    result, err = run_compare(task_id)
    if result is None:
        record("拆分：制造差异后再对比", False, str(err))
        return
    by_type = diff_types(diffs_of(result, "orders"))
    record("拆分：某片少一行 → SOURCE_ONLY",
           any(str(d.get("primaryKeyValue")) == "2" for d in by_type.get("SOURCE_ONLY", [])),
           str(list(by_type.keys())))
    record("拆分：字段被改 → CONTENT_DIFF",
           any(str(d.get("primaryKeyValue")) == "3" for d in by_type.get("CONTENT_DIFF", [])))
    record("拆分：多出的孤儿行 → TARGET_ONLY（逐行点查发现不了）",
           any(str(d.get("primaryKeyValue")) == "777" for d in by_type.get("TARGET_ONLY", [])))
    ws = [d for d in by_type.get("WRONG_SHARD", []) if str(d.get("primaryKeyValue")) == "5"]
    record("拆分：错片行 → WRONG_SHARD", len(ws) > 0, str(list(by_type.keys())))
    if ws:
        record("拆分：错片行报出实际分片与应在分片",
               ws[0].get("targetShard", "").endswith(f"orders_{wrong}")
               and ws[0].get("expectedShard", "").endswith(f"orders_{shard_of(5)}"),
               f"{ws[0].get('targetShard')} → {ws[0].get('expectedShard')}")

    repair = api(f"/validation-tasks/{result['_id']}/repair", "POST", {})
    record("拆分：一键修复执行成功", repair.get("success") is True, str(repair.get("message"))[:60])
    moved = mysql(f"SELECT COUNT(*) FROM orders_{shard_of(5)} WHERE id=5;", db=SPL_TGT, want=True)
    stale = mysql(f"SELECT COUNT(*) FROM orders_{wrong} WHERE id=5;", db=SPL_TGT, want=True)
    record("拆分：错片行被搬回正确分片（错片删、对片插）", moved == "1" and stale == "0",
           f"对片 {moved} 行 / 错片 {stale} 行")
    ghost = mysql(f"SELECT COUNT(*) FROM orders_{shard_of(777)} WHERE id=777;", db=SPL_TGT, want=True)
    record("拆分：孤儿行已删", ghost == "0", f"实际 {ghost}")

    after, _ = run_compare(task_id)
    record("拆分：复核已收敛", after is not None and after.get("failedTables") == 0,
           f"failed={after.get('failedTables') if after else 'n/a'}")


def main():
    global token
    login = api("/auth/login", "POST", {"username": "admin", "password": "admin123"})
    token = login.get("token") or (login.get("data") or {}).get("token")
    if not token:
        print("登录失败:", login)
        return 1
    record("登录成功", True)
    stop_previous_runs()
    check_merge()
    check_split()
    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 路由任务内容对比 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
