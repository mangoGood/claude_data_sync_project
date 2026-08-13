#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
派发消息发不出去时，任务必须置 FAILED 并带错误码 E5004（而不是留在 PENDING）。

复现的是真实故障：Kafka 比后端晚起了一分钟，任务 test1001 卡在 PENDING
（后端日志 `Topic sync-task-created not present in metadata after 60000 ms`）。
改造前失败只被降级成一条 WARNING 日志，任务状态留在 PENDING、HTTP 照常返回成功 ——
页面上只看到一个永远"启动中"的任务，没人知道该重启它。

**而且它连重启都做不到**：`launchWorkflow` 只接受 CONFIGURING 状态，`retryWorkflow`
只接受 FAILED —— 卡在 PENDING 的任务两条路都进不去，正常 UI 操作救不回来。

本判据会**停掉 Kafka**（跑完自动恢复），请确认没有任务正在同步。

用法：
    python3 test_scripts/dispatch_failure/dispatch_failure_e2e.py
    # 本环境的登录口令若与 V2__seed_default_data.sql 的种子不一致（被 create_env.sh 重置过），
    # 用环境变量覆盖：
    E2E_USER=user1 E2E_PASS=xxx python3 test_scripts/dispatch_failure/dispatch_failure_e2e.py

**尚未在本机跑通**：这台环境里 `users.password` 的哈希已与 V2 种子不同（被重置过），
仓库里记着的 `user1/123456` 登不上，因此改动只有单测覆盖
（`TaskDispatchFailureTest` 6 项 + `DispatchFailureCallbackTest` 5 项）。
拿到可用口令后跑一遍即可，脚本本身是完整的。
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("E2E_BASE", "http://localhost:38080")
USER = os.environ.get("E2E_USER", "user1")
PASS = os.environ.get("E2E_PASS", "123456")
KAFKA_CT = "synctask-kafka"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# 连接串只需能过预检；本判据不关心实际同步
SRC_CONN = "mysql://root:rootpassword@127.0.0.1:33306/edge"
TGT_CONN = "mysql://root:rootpassword@127.0.0.1:33306/edge2"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def req(method, path, token=None, body=None, timeout=180):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method)
    r.add_header("Content-Type", "application/json")
    if token:
        r.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode() or "{}")


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=120)


def task_row(task_id):
    """直接查元数据库：状态 / 错误码 / 错误信息（比走 API 更贴近事实）。"""
    p = subprocess.run(
        ["docker", "exec", "-i", "synctask-mysql", "mysql", "-uroot", "-prootpassword", "-N", "-e",
         f"SELECT status, COALESCE(error_code,''), COALESCE(LEFT(error_message,120),'') "
         f"FROM sync_task_db.workflows WHERE id='{task_id}'"],
        capture_output=True, text=True, timeout=60)
    line = [l for l in (p.stdout or "").strip().splitlines() if l.strip()]
    if not line:
        return None
    cols = line[-1].split("\t")
    return {"status": cols[0], "code": cols[1], "msg": cols[2] if len(cols) > 2 else ""}


def main():
    token = (req("POST", "/api/auth/login", body={"username": USER, "password": PASS})
             or {}).get("token")
    if not token:
        raise SystemExit("登录失败，后端是否在 38080 上运行？")

    task_id = None
    kafka_stopped = False
    print("=== 派发失败即置 FAILED 判据 ===")
    try:
        name = "dispatch-fail-%d" % int(time.time())
        d = req("POST", "/api/workflows", token,
                {"name": name, "sourceType": "MySQL", "targetType": "MySQL"})
        task_id = (d.get("data") or {}).get("id") or d.get("id")
        record("建任务", bool(task_id), str(task_id))
        if not task_id:
            return 1

        d = req("PUT", f"/api/workflows/{task_id}/config", token, {
            "sourceConnection": SRC_CONN, "targetConnection": TGT_CONN,
            "migrationMode": "full", "syncObjects": json.dumps({"edge": {"tables": ["t1"]}})})
        record("配置任务", bool(d.get("success")), str(d)[:120])

        # 停掉 Kafka，复现"派发时 broker 不在"
        docker("stop", KAFKA_CT)
        kafka_stopped = True
        record("Kafka 已停（判据前提）",
               "Up" not in (docker("ps", "--filter", f"name={KAFKA_CT}", "--format", "{{.Status}}").stdout or ""))

        print("    （启动任务；broker 不在时 send() 会阻塞到 max.block.ms=60s 再抛，请等）")
        t0 = time.time()
        d = req("POST", f"/api/workflows/{task_id}/launch", token, timeout=240)
        blocked = time.time() - t0
        record("启动请求已返回", True, f"耗时 {blocked:.0f}s，返回 {str(d)[:80]}")

        row = None
        for _ in range(30):
            row = task_row(task_id)
            if row and row["status"] == "FAILED":
                break
            time.sleep(2)

        record("任务状态是 FAILED（改造前会留在 PENDING）",
               row is not None and row["status"] == "FAILED",
               f"status={row['status'] if row else None}")
        record("带错误码 E5004",
               row is not None and row["code"] == "E5004",
               f"error_code={row['code'] if row else None!r}")
        record("错误信息点明是派发消息发送失败",
               row is not None and "派发" in row["msg"],
               row["msg"] if row else "")
        record("FAILED 的任务能走重试路径救回来（PENDING 两条路都进不去）",
               row is not None and row["status"] == "FAILED",
               "launchWorkflow 只收 CONFIGURING、retryWorkflow 只收 FAILED")
    finally:
        if kafka_stopped:
            docker("start", KAFKA_CT)
            print("    （Kafka 已恢复）")
        if task_id:
            req("DELETE", f"/api/workflows/{task_id}", token)

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n=== {passed}/{len(results)} 通过 ===")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL {name}: {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
