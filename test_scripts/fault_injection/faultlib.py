#!/usr/bin/env python3
"""
断点续传 / 故障注入测试公共库。

覆盖四条链路（mysql→mysql、tidb→mysql、redis→redis、oracle→postgresql）在同步过程中
注入进程崩溃（SIGKILL）后的自愈与数据一致性，以及进程僵死（SIGSTOP）下监控能否发现并上报失败。

设计要点：
  - 子进程都以 `-Dtask.id=<taskId>` 启动，据此用 pgrep 精确定位某任务的 capture/extract/increment 子进程。
  - SIGKILL 模拟进程崩溃：ProcessGuard 应自动重启并从 checkpoint 续传，幂等应用吸收重复。
  - SIGSTOP 模拟线程僵死：进程仍 alive 但不推进，考验监控是否能发现“假活”。
  - 一致性判定按引擎语义逐行比对（SQL 用主键对齐，Redis 用 key 全量比对）。
"""
import os
import subprocess
import sys
import time

import requests

BASE_URL = os.environ.get("FI_BASE_URL", "http://localhost:38080")
USER = os.environ.get("FI_USER", "admin")
PASSWORD = os.environ.get("FI_PASS", "admin123")


# ------------------------------------------------------------------ backend api

def login():
    r = requests.post(f"{BASE_URL}/api/auth/login",
                      json={"username": USER, "password": PASSWORD}, timeout=30)
    d = r.json()
    if "token" not in d:
        print(f"登录失败: {d}")
        sys.exit(1)
    return d["token"]


def api(method, path, token, **kw):
    headers = kw.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    return requests.request(method, f"{BASE_URL}{path}", headers=headers, timeout=60, **kw).json()


def create_task(token, name, source_type, target_type, src_conn, tgt_conn,
                mode, sync_objects, target_db, source_db=None):
    r = api("POST", "/api/workflows", token,
            json={"name": name, "sourceType": source_type, "targetType": target_type, "taskType": "SYNC"})
    if not r.get("success"):
        print(f"创建任务失败: {r}")
        sys.exit(1)
    task_id = r["data"]["id"]
    cfg = {
        "sourceConnection": src_conn,
        "targetConnection": tgt_conn,
        "migrationMode": mode,
        "syncObjects": sync_objects,
        "targetDbName": target_db,
        "sourceType": source_type,
        "targetType": target_type,
    }
    if source_db:
        cfg["sourceDbName"] = source_db
    api("PUT", f"/api/workflows/{task_id}/config", token, json=cfg)
    api("POST", f"/api/workflows/{task_id}/launch", token)
    return task_id


def get_status(token, task_id):
    d = api("GET", f"/api/workflows/{task_id}", token)
    return (d.get("data") or {}).get("status")


def wait_status(token, task_id, wanted, timeout=360, quiet=False):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        st = get_status(token, task_id)
        if st != last:
            if not quiet:
                print(f"    状态: {st}")
            last = st
        if st in wanted or st == "FAILED":
            return st
        time.sleep(3)
    return last


def stop_task(token, task_id):
    try:
        api("POST", f"/api/workflows/{task_id}/stop", token)
    except Exception:
        pass


def delete_task(token, task_id):
    try:
        api("DELETE", f"/api/workflows/{task_id}", token)
    except Exception:
        pass


# ------------------------------------------------------------------ 进程故障注入

def child_pids(task_id, engine):
    """返回某任务某类子进程的 pid 列表（engine ∈ capture/extract/increment/full/redis）。"""
    jar = f"migration-{engine}"
    pat = f"task.id={task_id}"
    out = subprocess.run(["pgrep", "-f", pat], capture_output=True, text=True).stdout
    pids = []
    for pid in out.split():
        # 二次确认命令行里含对应 jar，避免误杀同任务的其它子进程
        cmd = subprocess.run(["ps", "-o", "command=", "-p", pid], capture_output=True, text=True).stdout
        if jar in cmd:
            pids.append(int(pid))
    return pids


def all_child_pids(task_id):
    out = subprocess.run(["pgrep", "-f", f"task.id={task_id}"], capture_output=True, text=True).stdout
    return [int(p) for p in out.split()]


def signal_child(task_id, engine, sig):
    pids = child_pids(task_id, engine)
    for pid in pids:
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
    return pids


# ------------------------------------------------------------------ SQL 数据源/目标

import mysql.connector  # noqa: E402


def sql_conn(cfg, db=None):
    c = mysql.connector.connect(database=db, use_pure=True, autocommit=True, **cfg)
    cur = c.cursor()
    cur.execute("SET time_zone = '+00:00'")
    cur.close()
    return c


def sql_exec(cfg, sqls, db=None):
    c = sql_conn(cfg, db)
    cur = c.cursor()
    for s in sqls:
        cur.execute(s)
    cur.close()
    c.close()


def sql_fetch(cfg, db, query):
    c = sql_conn(cfg, db)
    cur = c.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    c.close()
    return rows


META_DB = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")


def get_increment_quota(user_id=1):
    """读取 resource_quotas.max_increment_rows_per_sec（无记录返回 None）。"""
    rows = sql_fetch(META_DB, "sync_task_db",
                     f"SELECT max_increment_rows_per_sec FROM resource_quotas WHERE user_id={user_id}")
    return rows[0][0] if rows else None


def set_increment_quota(value, user_id=1):
    """临时调整增量应用限速配额（测试用；跑完应还原）。"""
    sql_exec(META_DB,
             [f"UPDATE resource_quotas SET max_increment_rows_per_sec={value} WHERE user_id={user_id}"],
             db="sync_task_db")


def print_result(passed, failed):
    print("\n" + "=" * 64)
    print(f"  通过 {len(passed)} / 失败 {len(failed)}")
    for f in failed:
        print(f"   ✗ {f}")
    print("=" * 64)
    return 0 if not failed else 1
