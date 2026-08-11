#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚合路由<b>适用范围拦截</b>端到端测试。

聚合路由的适用边界会随实现推进而变，这个脚本盯的就是"边界与实现是否一致"：

  <b>已支持</b>：关系库 mysql/pg 任意组合（含异构）、mongodb→mongodb（集合级）、
  mysql→elasticsearch（索引级）、以及与列处理叠加。
  <b>仍拒绝</b>：Redis（没有表的概念）、Oracle（upsert 要 MERGE INTO，没实现）、
  TiDB 源（增量走 TiCDC，未验证）、灾备/订阅任务（路由改写未在这两条链路验证）。

拒绝必须发生在<b>保存或改配置时</b>，而不是任务跑起来之后——不支持的组合各有各的错法，
其中"配了不生效"这种静默错比报错更糟。

任务是配额资源（每用户 50 个），所以这里<b>同一时刻只留一个任务</b>：建 → 断言 → 删。
不落数据、不需要 agent。

前置：./start.sh 已起好后端(38080)。
用法：python3 test_scripts/sharding/api_route_guard_e2e.py
"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:38080/api"
MERGE_CFG = {"mode": "MERGE",
             "merge": [{"match": "g_src_*.order_*", "target": "g_dw.order_all",
                        "pkStrategy": "COMPOSITE_SOURCE", "ddlPolicy": "FIRST_WINS"}]}
PLAIN_OBJECTS = {"g_src_1": {"tables": ["order_001"]}}
COLPROC_OBJECTS = {"g_src_1": {"tables": ["order_001"],
                               "columnFilter": {"order_001": "amount|<|100"}}}

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
        with urllib.request.urlopen(req, data, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        payload = e.read().decode()
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {"success": False, "message": payload}


def conn_str(db=""):
    return "mysql://root:rootpassword@127.0.0.1:33306" + ("/" + db if db else "")


def create_task(source_type="mysql", target_type="mysql", task_type="SYNC", name="t"):
    created = api("/workflows", "POST", {
        "name": f"rg-{name}-{int(time.time() * 1000) % 100000000}",
        "sourceType": source_type, "targetType": target_type, "taskType": task_type})
    task_id = (created.get("data") or {}).get("id")
    if not task_id:
        print("  建任务失败:", created.get("message"))
    return task_id


def set_config(task_id, **kwargs):
    """只提交要改的字段：updateConfig 对 null 字段一律不动。"""
    return api(f"/workflows/{task_id}/config", "PUT", kwargs)


def base_config(task_id, sync_objects=None):
    return set_config(task_id,
                      sourceConnection=conn_str(), targetConnection=conn_str("g_dw"),
                      migrationMode="fullAndIncre", sourceDbName="g_src_1", targetDbName="g_dw",
                      syncObjects=json.dumps(sync_objects or PLAIN_OBJECTS))


def save_route(task_id, cfg=None):
    return api(f"/workflows/{task_id}/route-config", "PUT",
               {"routeConfig": MERGE_CFG if cfg is None else cfg})


def rejected(result, keyword):
    """被拒 + 原因里点名了具体是什么（只看 success=False 会把别的错误也算过）。"""
    return not result.get("success") and keyword in str(result.get("message", ""))


def drop(task_id):
    if task_id:
        api(f"/workflows/{task_id}", "DELETE")


def cleanup_leftovers():
    listed = api("/workflows?page=1&pageSize=100")
    n = 0
    for it in ((listed.get("data") or {}).get("list") or []):
        if str(it.get("name", "")).startswith("rg-"):
            api(f"/workflows/{it.get('id')}", "DELETE")
            n += 1
    if n:
        print(f"  清理上一轮遗留任务 {n} 个")


def check_baseline():
    """mysql→mysql 实时同步：路由配置照常存取（零回归），改成不支持的库类型时才拒。"""
    tid = create_task(name="ok")
    if not tid:
        record("基线任务创建", False, "配额或接口异常")
        return
    try:
        base_config(tid)
        record("mysql→mysql 能保存路由配置（零回归）", save_route(tid).get("success") is True)
        got = api(f"/workflows/{tid}/route-config")
        saved = json.loads(((got.get("data") or {}).get("routeConfig") or "{}"))
        record("保存后能回填出来", saved.get("mode") == "MERGE", str(saved.get("mode")))
        # 只在保存路由那一刻判就会从这里绕过去：先存好路由，再把库类型改成不支持的
        changed = set_config(tid, sourceType="redis", targetType="redis")
        record("存好路由后把库类型改成 redis → 改配置时被拒",
               rejected(changed, "Redis"), str(changed.get("message"))[:70])
    finally:
        drop(tid)


def check_engine_pairs():
    """
    库类型的支持边界。每换一次类型前先清掉路由配置——否则"改配置"这一步就会被
    上一次存下的路由配置拦住，测不到"保存路由"那一层。
    """
    tid = create_task(name="pairs")
    if not tid:
        record("库类型边界用例创建", False, "配额或接口异常")
        return
    try:
        base_config(tid)
        supported = [("mongodb", "mongodb"), ("mysql", "elasticsearch"),
                     ("mysql", "postgresql"), ("postgresql", "mysql")]
        for st, tt in supported:
            api(f"/workflows/{tid}/route-config", "PUT", {"routeConfig": None})
            changed = set_config(tid, sourceType=st, targetType=tt)
            ok = changed.get("success") is True and save_route(tid).get("success") is True
            record(f"{st}→{tt} 可以配聚合路由", ok, str(changed.get("message"))[:60])

        rejected_pairs = [("redis", "redis", "Redis"), ("oracle", "oracle", "upsert"),
                          ("tidb", "mysql", "TiDB")]
        for st, tt, kw in rejected_pairs:
            api(f"/workflows/{tid}/route-config", "PUT", {"routeConfig": None})
            changed = set_config(tid, sourceType=st, targetType=tt)
            # 库类型本身可能被别的规则挡（如 oracle 目标），只要能改过去就再验保存路由被拒
            result = save_route(tid) if changed.get("success") else changed
            record(f"{st}→{tt} 仍然拒绝配路由", rejected(result, kw),
                   str(result.get("message"))[:60])
    finally:
        drop(tid)


def check_task_types():
    for task_type, kw, label in (("DR", "灾备", "灾备任务"), ("SUBSCRIBE", "订阅", "订阅任务")):
        tid = create_task(task_type=task_type, name=task_type.lower())
        if not tid:
            record(f"{label}保存路由配置被拒", False, "配额或接口异常")
            continue
        try:
            record(f"{label}保存路由配置被拒", rejected(save_route(tid), kw))
        finally:
            drop(tid)


def check_column_processing():
    """列处理与路由现已可以叠加（汇聚下自定义附加列改为逐行注值）。"""
    tid = create_task(name="cp")
    if not tid:
        record("列处理叠加用例创建", False, "配额或接口异常")
        return
    try:
        base_config(tid, COLPROC_OBJECTS)
        record("先配列处理，再存路由配置 → 放行", save_route(tid).get("success") is True)
        added = set_config(tid, syncObjects=json.dumps(COLPROC_OBJECTS))
        record("先存路由配置，再加列处理 → 放行", added.get("success") is True,
               str(added.get("message"))[:70])
    finally:
        drop(tid)


def main():
    global token
    login = api("/auth/login", "POST", {"username": "admin", "password": "admin123"})
    token = login.get("token") or (login.get("data") or {}).get("token")
    if not token:
        print("登录失败:", login)
        return 1
    record("登录成功", True)
    cleanup_leftovers()

    print("== 基线：mysql→mysql 实时同步不受影响 ==")
    check_baseline()
    print("== 1/2. 库类型边界：已支持的放行、不支持的拒绝 ==")
    check_engine_pairs()
    print("== 3. 灾备 / 订阅任务 ==")
    check_task_types()
    print("== 4. 叠加列处理（两个方向都应放行）==")
    check_column_processing()

    cleanup_leftovers()
    return summarize()


def summarize():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== 路由拦截 E2E: {passed}/{len(results)} 通过 =====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
