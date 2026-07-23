#!/usr/bin/env python3
"""
Redis → Redis 端到端测试（走真实 backend + agent + Kafka 全链路）。

覆盖：
  - 元数据接口：test-connection / databases / validate（redis）
  - 场景 A：全量同步（migration.mode=full）→ FULL_COMPLETED，校验目标 == 源
  - 场景 B：全量+增量（fullAndIncre）→ INCREMENT_RUNNING，写增量后校验目标

前置：docker 容器 redis-src(6390) / redis-tgt(6391)，均 --requirepass syncredis。
用法：python3 test_scripts/redis/redis_e2e.py
"""
import json
import subprocess
import sys
import time

import requests

BASE_URL = "http://localhost:38080"
USER, PASSWORD = "admin", "admin123"
PW = "syncredis"
SRC_CONN = f"redis://default:{PW}@127.0.0.1:6390"
TGT_CONN = f"redis://default:{PW}@127.0.0.1:6391"

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'✓' if cond else '✗'}] {name}{(' — ' + detail) if detail else ''}")
    return cond


def rc(container, *args):
    """redis-cli into a container, returns stripped stdout."""
    out = subprocess.run(
        ["docker", "exec", container, "redis-cli", "-a", PW, "--no-auth-warning", *args],
        capture_output=True, text=True)
    return out.stdout.strip()


def src(*a):
    return rc("redis-src", *a)


def tgt(*a):
    return rc("redis-tgt", *a)


def api(method, path, token=None, **kw):
    headers = kw.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.request(method, f"{BASE_URL}{path}", headers=headers, timeout=30, **kw).json()


def login():
    d = api("POST", "/api/auth/login", json={"username": USER, "password": PASSWORD})
    if "token" not in d:
        print(f"登录失败: {d}")
        sys.exit(1)
    return d["token"]


def seed_full():
    """源库全类型种子数据（db0 + db3）。"""
    src("FLUSHALL")
    tgt("FLUSHALL")
    src("-n", "0", "SET", "str:1", "hello")
    src("-n", "0", "RPUSH", "list:1", "a", "b", "c")
    src("-n", "0", "SADD", "set:1", "x", "y", "z")
    src("-n", "0", "HSET", "hash:1", "f1", "v1", "f2", "v2")
    src("-n", "0", "ZADD", "zset:1", "1", "one", "2", "two")
    src("-n", "0", "SET", "ttlkey", "v", "EX", "100000")
    src("-n", "3", "SET", "db3key", "indb3")  # 仅 db0 选中时不应同步


def create(token, name, mode):
    d = api("POST", "/api/workflows", token,
            json={"name": name, "sourceType": "redis", "targetType": "redis", "taskType": "SYNC"})
    if not d.get("success"):
        print(f"创建失败: {d}"); sys.exit(1)
    tid = d["data"]["id"]
    cfg = {
        "sourceConnection": SRC_CONN,
        "targetConnection": TGT_CONN,
        "migrationMode": mode,
        "syncObjects": '{"0":{"dbLevel":true}}',  # 仅同步逻辑库 db0
        "sourceDbName": "0",
        "targetDbName": "0",
        "sourceType": "redis",
        "targetType": "redis",
    }
    d = api("PUT", f"/api/workflows/{tid}/config", token, json=cfg)
    if not d.get("success"):
        print(f"配置失败: {d}"); sys.exit(1)
    d = api("POST", f"/api/workflows/{tid}/launch", token)
    if not d.get("success"):
        print(f"启动失败: {d}"); sys.exit(1)
    return tid


def wait_status(token, tid, targets, timeout=90):
    start = time.time()
    last = ""
    while time.time() - start < timeout:
        t = api("GET", f"/api/workflows/{tid}", token).get("data", {})
        st = t.get("status", "")
        if st != last:
            print(f"    状态: {st}  进度: {t.get('progress', 0)}%  err: {t.get('error_message', '')}")
            last = st
        if st in targets:
            return st
        if st == "FAILED":
            return st
        time.sleep(2)
    return last


def stop_delete(token, tid):
    api("POST", f"/api/workflows/{tid}/stop", token)
    time.sleep(2)
    api("DELETE", f"/api/workflows/{tid}", token)


def main():
    token = login()
    print("✓ 登录成功\n")

    # ---- 元数据接口 ----
    print("=== 元数据接口 ===")
    def connected(resp):
        return bool(resp.get("success") and (resp.get("data") or {}).get("connected"))
    r = api("POST", "/api/metadata/test-connection", token,
            json={"sourceConnection": SRC_CONN, "dbType": "redis"})
    check("test-connection 源库", connected(r), json.dumps(r, ensure_ascii=False)[:120])
    r = api("POST", "/api/metadata/test-connection", token,
            json={"sourceConnection": TGT_CONN, "dbType": "redis"})
    check("test-connection 目标库", connected(r), json.dumps(r, ensure_ascii=False)[:120])

    seed_full()
    r = api("POST", "/api/metadata/databases", token, json={"sourceConnection": SRC_CONN})
    dbs = r.get("data", {}).get("databases", []) if r.get("success") else []
    check("databases 列出逻辑库", "0" in dbs and "3" in dbs, f"dbs={dbs}")

    r = api("POST", "/api/metadata/validate", token,
            json={"sourceConnection": SRC_CONN, "targetConnection": TGT_CONN,
                  "migrationMode": "fullAndIncre", "sourceType": "redis", "targetType": "redis"})
    vr = r.get("data", {})
    check("validate 全部通过", vr.get("allPassed") is True,
          "items=" + ",".join(f"{i.get('name')}:{i.get('level')}" for i in vr.get("checkItems", [])))

    # ---- 场景 A：全量 ----
    print("\n=== 场景 A：全量同步（full）===")
    seed_full()
    tid = create(token, "E2E-Redis-全量", "full")
    st = wait_status(token, tid, ["FULL_COMPLETED"], timeout=90)
    check("场景A 到达 FULL_COMPLETED", st == "FULL_COMPLETED", f"status={st}")
    time.sleep(1)
    check("场景A db0 键数=6", tgt("-n", "0", "DBSIZE") == "6", f"target db0={tgt('-n','0','DBSIZE')}")
    check("场景A db3 未同步（=0）", tgt("-n", "3", "DBSIZE") == "0", f"target db3={tgt('-n','3','DBSIZE')}")
    check("场景A string 值", tgt("-n", "0", "GET", "str:1") == "hello")
    check("场景A zset 值", tgt("-n", "0", "ZSCORE", "zset:1", "two") == "2")
    check("场景A TTL 保留", tgt("-n", "0", "TTL", "ttlkey").isdigit() and int(tgt("-n", "0", "TTL", "ttlkey")) > 0)
    stop_delete(token, tid)

    # ---- 场景 B：全量 + 增量 ----
    print("\n=== 场景 B：全量+增量（fullAndIncre）===")
    seed_full()
    tid = create(token, "E2E-Redis-全量增量", "fullAndIncre")
    st = wait_status(token, tid, ["INCREMENT_RUNNING"], timeout=90)
    check("场景B 到达 INCREMENT_RUNNING", st == "INCREMENT_RUNNING", f"status={st}")
    check("场景B 全量后 db0 键数=6", tgt("-n", "0", "DBSIZE") == "6", f"target db0={tgt('-n','0','DBSIZE')}")

    print("  -- 写增量（更新/删除/非幂等 INCR/新键/db3 隔离）--")
    src("-n", "0", "SET", "str:1", "updated")
    src("-n", "0", "DEL", "set:1")
    src("-n", "0", "INCR", "counter")
    src("-n", "0", "INCR", "counter")
    src("-n", "0", "INCR", "counter")
    src("-n", "0", "RPUSH", "list:1", "d")
    src("-n", "0", "SET", "newkey", "brand")
    src("-n", "3", "SET", "db3new", "nope")   # db3 不选，不应同步
    time.sleep(6)

    check("场景B string 增量更新", tgt("-n", "0", "GET", "str:1") == "updated", tgt("-n", "0", "GET", "str:1"))
    check("场景B DEL 生效", tgt("-n", "0", "EXISTS", "set:1") == "0")
    check("场景B 非幂等 INCR=3", tgt("-n", "0", "GET", "counter") == "3", tgt("-n", "0", "GET", "counter"))
    check("场景B list 追加", tgt("-n", "0", "LRANGE", "list:1", "0", "-1").split() == ["a", "b", "c", "d"])
    check("场景B 新键同步", tgt("-n", "0", "GET", "newkey") == "brand")
    check("场景B db3 增量仍隔离", tgt("-n", "3", "DBSIZE") == "0", f"target db3={tgt('-n','3','DBSIZE')}")
    stop_delete(token, tid)

    # ---- 汇总 ----
    print("\n" + "=" * 60)
    print(f"通过: {len(PASS)}   失败: {len(FAIL)}")
    if FAIL:
        print("失败项: " + ", ".join(FAIL))
        sys.exit(1)
    print("✓ 全部通过")


if __name__ == "__main__":
    main()
