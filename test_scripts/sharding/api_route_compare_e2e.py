#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
路由感知的<b>行数对比</b>端到端测试。

走真实 API：建汇聚任务与拆分任务各一个 → 跑全量 → 创建行数对比任务 → 核对结论。
没有路由感知的话，两种任务的对比都会去目标端找同名表（汇聚/拆分下根本不存在），
结果是"目标端 0 行"，看着像数据全丢了。

同时验证：路由任务不允许创建内容对比（逐行比对没有可靠口径）。

前置：./start.sh 已起好后端(38080)与 agent；synctask-mysql 在跑。
用法：python3 test_scripts/sharding/api_route_compare_e2e.py
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:38080/api"
CT = "synctask-mysql"
MRG_SRC, MRG_TGT = "rc_mrg_src", "rc_mrg_dw"
SPL_SRC, SPL_TGT = "rc_spl_src", "rc_spl_dw"

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
        with urllib.request.urlopen(req, data, timeout=60) as resp:
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


def setup_merge():
    mysql(f"DROP DATABASE IF EXISTS {MRG_SRC}; CREATE DATABASE {MRG_SRC};"
          f"DROP DATABASE IF EXISTS {MRG_TGT}; CREATE DATABASE {MRG_TGT};")
    for t, n in (("order_001", 3), ("order_002", 5)):
        mysql(f"CREATE TABLE {t} (id BIGINT NOT NULL, v INT, PRIMARY KEY(id)) ENGINE=InnoDB;", db=MRG_SRC)
        mysql(f"INSERT INTO {t} VALUES " + ",".join(f"({i},{i})" for i in range(1, n + 1)) + ";", db=MRG_SRC)


def setup_split():
    mysql(f"DROP DATABASE IF EXISTS {SPL_SRC}; CREATE DATABASE {SPL_SRC};"
          f"DROP DATABASE IF EXISTS {SPL_TGT}; CREATE DATABASE {SPL_TGT};")
    mysql("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT NOT NULL, PRIMARY KEY(id)) ENGINE=InnoDB;",
          db=SPL_SRC)
    mysql("INSERT INTO orders VALUES " + ",".join(f"({i},{i})" for i in range(1, 13)) + ";", db=SPL_SRC)


def create_task(name, src_db, tgt_db, tables, route_config):
    created = api("/workflows", "POST", {"name": name + "-" + str(int(time.time()))})
    task_id = (created.get("data") or {}).get("id")
    if not task_id:
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


def wait_full_done(task_id, timeout=240):
    """等到进入增量同步中——对比任务只允许给增量中/灾备中的任务创建（既有产品规则）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = mysql(f"SELECT status FROM workflows WHERE id='{task_id}';", db="sync_task_db", want=True)
        if st in ("INCREMENT_RUNNING", "FAILED"):
            return st
        time.sleep(3)
    return "TIMEOUT"


def compare_tables(result):
    """对比明细：compareResult 是 JSON 串 {"tables":[...]}。"""
    raw = result.get("compareResult")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return (raw or {}).get("tables", []) if isinstance(raw, dict) else []


def run_compare(task_id, compare_type="ROW_COUNT"):
    r = api("/validation-tasks", "POST", {"workflowId": task_id, "compareType": compare_type})
    if not r.get("success"):
        return None, r.get("message")
    vid = (r.get("data") or {}).get("id")
    deadline = time.time() + 180
    while time.time() < deadline:
        detail = api(f"/validation-tasks/{vid}")
        d = detail.get("data") or {}
        if d.get("status") in ("COMPLETED", "FAILED", "PARTIAL"):
            return d, None
        time.sleep(3)
    return None, "对比超时"


def stop_previous_runs():
    """
    停掉上一轮遗留的同名任务。它们的增量管线还活着，会把本轮 setup 的
    DROP/CREATE DATABASE 与 INSERT 重放到目标端，跟本轮任务互相踩
    （实测表现为目标表被清空、或"database exists"）。
    """
    listed = api("/workflows?page=1&pageSize=100")
    items = ((listed.get("data") or {}).get("list") or [])
    stopped = 0
    for it in items:
        if str(it.get("name", "")).startswith("rc-"):
            api(f"/workflows/{it.get('id')}/stop", "POST", {})
            stopped += 1
    if stopped:
        print(f"  已停止 {stopped} 个上一轮遗留任务，等待管线退出...")
        time.sleep(12)


def main():
    global token
    login = api("/auth/login", "POST", {"username": "admin", "password": "admin123"})
    token = login.get("token") or (login.get("data") or {}).get("token")
    if not token:
        print("登录失败:", login)
        return 1
    record("登录成功", True)
    stop_previous_runs()

    print("== 汇聚任务：2 张分表 → 1 张汇聚表 ==")
    setup_merge()
    merge_id = create_task("rc-merge", MRG_SRC, MRG_TGT, ["order_001", "order_002"], {
        "mode": "MERGE",
        "merge": [{"match": f"{MRG_SRC}.order_*", "target": f"{MRG_TGT}.order_all",
                   "pkStrategy": "COMPOSITE_SOURCE", "ddlPolicy": "FIRST_WINS"}]})
    if not merge_id:
        record("汇聚任务创建", False)
        return summarize()
    st = wait_full_done(merge_id)
    record("汇聚任务已进入增量同步", st == "INCREMENT_RUNNING", st)

    result, err = run_compare(merge_id)
    if result is None:
        record("汇聚任务行数对比", False, str(err))
    else:
        by_table = {t.get("sourceTable"): t for t in compare_tables(result)}
        ok1 = by_table.get("order_001", {}).get("targetRowCount") == 3
        ok2 = by_table.get("order_002", {}).get("targetRowCount") == 5
        record("汇聚：每个源表按来源标识切片统计到正确行数（3 / 5）", ok1 and ok2,
               json.dumps([{k: v.get("targetRowCount")} for k, v in by_table.items()], ensure_ascii=False))
        record("汇聚：对比结论为一致（无差异表）", result.get("failedTables") == 0,
               f"status={result.get('status')}, failed={result.get('failedTables')}")

    content, err = run_compare(merge_id, "CONTENT")
    record("汇聚任务不允许内容对比", content is None and err and "内容对比" in str(err), str(err)[:60])

    print("== 拆分任务：1 张源表 → 4 张分片表 ==")
    setup_split()
    split_id = create_task("rc-split", SPL_SRC, SPL_TGT, ["orders"], {
        "mode": "SPLIT",
        "split": [{"match": f"{SPL_SRC}.orders", "shardKey": "user_id", "algo": "HASH_MOD",
                   "count": 4, "targetDb": SPL_TGT, "targetTable": "orders_${shard}",
                   "unrouted": "DEADLETTER"}]})
    if not split_id:
        record("拆分任务创建", False)
        return summarize()
    st = wait_full_done(split_id)
    record("拆分任务已进入增量同步", st == "INCREMENT_RUNNING", st)

    result, err = run_compare(split_id)
    if result is None:
        record("拆分任务行数对比", False, str(err))
    else:
        orders = next((t for t in compare_tables(result) if t.get("sourceTable") == "orders"), {})
        record("拆分：目标行数 = 各分片之和（12）", orders.get("targetRowCount") == 12,
               f"targetRowCount={orders.get('targetRowCount')}")
        record("拆分：对比结论为一致（无差异表）", result.get("failedTables") == 0,
               f"status={result.get('status')}, failed={result.get('failedTables')}")

    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 路由感知对比 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
