#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
跨实例汇聚的<b>后端链路</b>端到端测试（B3b + B5）。

走真实 API：登录 → 建任务 → 存路由配置（含跨实例来源）→ 启动，验证：
  - 路由配置能存下来并回填（PUT/GET /workflows/{id}/route-config）
  - 非法配置在保存时就被拒（模板不含分片号占位、实例标识重复）
  - 启动后派生出隐藏的 MERGE_LEG 子任务，且不出现在用户任务列表里
  - 父任务与 leg 的 config.properties 都写出了 route.*，leg 带自己的 route.node.id

前置：./start.sh 已起好后端(38080)与 agent；synctask-mysql 在跑。
用法：python3 test_scripts/sharding/api_merge_legs_e2e.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:38080/api"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CT = "synctask-mysql"
SRC_DB = "api_mrg_1"
TGT_DB = "api_mrg_dw"

results = []
token = None


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def api(path, method="GET", body=None, expect_ok=True):
    req = urllib.request.Request(BASE + path, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data, timeout=30) as resp:
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
    p = subprocess.run(args, input=sql, capture_output=True, text=True, timeout=120)
    return (p.stdout or "").strip()


def setup_source():
    mysql(f"DROP DATABASE IF EXISTS {SRC_DB}; CREATE DATABASE {SRC_DB} DEFAULT CHARACTER SET utf8mb4;"
          f"DROP DATABASE IF EXISTS {TGT_DB}; CREATE DATABASE {TGT_DB} DEFAULT CHARACTER SET utf8mb4;")
    mysql("""
CREATE TABLE order_001 (
  id BIGINT NOT NULL, amount DECIMAL(10,2) DEFAULT NULL, PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO order_001 VALUES (1, 1.00);
""", db=SRC_DB)


def conn_str(db=""):
    """agent 的 ConnectionStringParser 只认 mysql://user:pass@host:port/db 这一种格式。"""
    return "mysql://root:rootpassword@127.0.0.1:33306" + ("/" + db if db else "")


def main():
    global token
    print("== 登录 ==")
    login = api("/auth/login", "POST", {"username": "admin", "password": "admin123"})
    token = login.get("token") or (login.get("data") or {}).get("token")
    if not token:
        print("登录失败:", login)
        return 1
    record("登录成功", True)

    setup_source()

    print("== 建任务 ==")
    created = api("/workflows", "POST", {"name": "api-merge-legs-" + str(int(time.time()))})
    task_id = (created.get("data") or {}).get("id") or created.get("id")
    if not task_id:
        print("建任务失败:", created)
        return 1
    record("任务已创建", True, task_id)

    api(f"/workflows/{task_id}/config", "PUT", {
        "sourceConnection": conn_str(), "targetConnection": conn_str(TGT_DB),
        "migrationMode": "full", "sourceType": "mysql", "targetType": "mysql",
        "sourceDbName": SRC_DB, "targetDbName": TGT_DB,
        "syncObjects": json.dumps({SRC_DB: {"tables": ["order_001"]}}),
    })

    print("== 非法路由配置必须被拒 ==")
    bad = api(f"/workflows/{task_id}/route-config", "PUT", {"routeConfig": {
        "mode": "SPLIT",
        "split": [{"match": "app.orders", "shardKey": "user_id", "algo": "HASH_MOD",
                   "count": 4, "targetTable": "orders_all"}]}})
    record("模板不含分片号占位的拆分规则被拒", not bad.get("success"),
           str(bad.get("message"))[:60])

    dup = api(f"/workflows/{task_id}/route-config", "PUT", {"routeConfig": {
        "mode": "MERGE",
        "merge": [{"match": f"{SRC_DB}.order_*", "target": f"{TGT_DB}.order_all"}],
        "legs": [{"nodeId": "same", "host": "127.0.0.1", "port": 33306},
                 {"nodeId": "same", "host": "127.0.0.2", "port": 33306}]}})
    record("实例标识重复被拒", not dup.get("success"), str(dup.get("message"))[:60])

    print("== 存合法路由配置（含 1 个跨实例来源）==")
    saved = api(f"/workflows/{task_id}/route-config", "PUT", {"routeConfig": {
        "mode": "MERGE",
        "merge": [{"match": f"{SRC_DB}.order_*", "target": f"{TGT_DB}.order_all",
                   "pkStrategy": "COMPOSITE_SOURCE", "ddlPolicy": "FIRST_WINS"}],
        "legs": [{"nodeId": "inst-b", "host": "127.0.0.1", "port": 33306,
                  "username": "root", "password": "rootpassword"}]}})
    record("路由配置保存成功", saved.get("success"), str(saved.get("message"))[:60])

    fetched = api(f"/workflows/{task_id}/route-config")
    route_json = (fetched.get("data") or {}).get("routeConfig") or ""
    record("路由配置可回填", "order_all" in route_json and "inst-b" in route_json,
           route_json[:80])

    print("== 启动任务，应派生隐藏的 MERGE_LEG 子任务 ==")
    launched = api(f"/workflows/{task_id}/launch", "POST", {})
    record("任务启动成功", launched.get("success"), str(launched.get("message"))[:80])

    time.sleep(3)
    legs = mysql("SELECT id, task_type, merge_parent_id FROM workflows WHERE merge_parent_id='"
                 + task_id + "';", db="sync_task_db", want=True)
    leg_id = legs.split("\t")[0] if legs else ""
    record("派生出 MERGE_LEG 子任务", bool(leg_id) and "MERGE_LEG" in legs, legs[:80])

    listed = api("/workflows?page=1&pageSize=50")
    items = ((listed.get("data") or {}).get("records")
             or (listed.get("data") or {}).get("content") or [])
    leg_visible = any(it.get("id") == leg_id for it in items)
    record("MERGE_LEG 不出现在任务列表里", not leg_visible,
           f"列表 {len(items)} 条")

    print("== config.properties 应写出 route.* ==")
    deadline = time.time() + 60
    parent_cfg = leg_cfg = ""
    while time.time() < deadline:
        parent_cfg = read_config(task_id)
        leg_cfg = read_config(leg_id) if leg_id else ""
        if "route.mode" in parent_cfg and (not leg_id or "route.mode" in leg_cfg):
            break
        time.sleep(2)
    record("父任务的 config.properties 含 route.mode=MERGE", "route.mode=MERGE" in parent_cfg)
    record("父任务含汇聚规则", f"route.merge.1.target={TGT_DB}.order_all" in parent_cfg)
    record("leg 的 config.properties 带自己的 route.node.id",
           "route.node.id=inst-b" in leg_cfg, leg_cfg_line(leg_cfg))

    print("== 两条通道的数据都要汇进目标表（跨实例并发建表不能互相踩） ==")
    deadline = time.time() + 90
    nodes = ""
    while time.time() < deadline:
        nodes = mysql(f"SELECT GROUP_CONCAT(DISTINCT _src_node ORDER BY _src_node) "
                      f"FROM {TGT_DB}.order_all;", want=True)
        if nodes.count(",") >= 1:
            break
        time.sleep(3)
    record("父任务与 leg 的数据都落进汇聚表（两个来源标识）",
           "inst-b" in nodes and nodes.count(",") >= 1, f"_src_node = {nodes}")

    leg_status = mysql("SELECT status FROM workflows WHERE id='" + leg_id + "';",
                       db="sync_task_db", want=True)
    record("leg 没有因为并发建表而失败", leg_status != "FAILED", f"状态 {leg_status}")

    return summarize()


def read_config(task_id):
    path = os.path.join(PROJECT_ROOT, "files", task_id, "config.properties")
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return ""


def leg_cfg_line(cfg):
    for line in cfg.splitlines():
        if line.startswith("route.node.id"):
            return line
    return "（无 route.node.id）"


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 跨实例汇聚后端链路 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
