#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SLA 闭环判据（P2-4）：指标 → 落库 → 告警。

跑一个真实的 mysql→mysql 全量+增量任务，检查第 5 批新增的那几个指标是不是真的被采集、
上报、落库，并且能被告警规则用起来。

五把尺子：

1. **绝对复制延迟有值**：`replication_lag_ms` = 源库当前时刻 − 已应用事件的源端时刻。
   它与既有 `rpo_ms` 的差别正是这把尺子的意义——源库空闲时 rpo 恒为 0（分子分母都不动），
   链路整段卡死也一样是 0；绝对延迟用源库时钟做分子，卡多久涨多久。
2. **磁盘占用有值**：任务目录字节数 > 0（长跑资源治理的可观测面）。
3. **重启次数可见**：SIGKILL 掉 capture 之后，`restart_count_10m` 必须涨上来。
   crash-loop 此前只在 agent 日志里，外部看不到。
4. **重放放大量有值**：capture 重启后 `capture_replay_bytes` 必须被写出来；
   位点持久化正常时它应该很小（一次重启只重放最后一小段）。
5. **告警闭环**：对新指标建一条阈值规则，规则引擎必须真的产生告警事件。
   指标采不到、落不了库、或 AlertRuleService 不认这个指标类型，这一把都会红。

前置：backend + agent 已用<b>本次构建</b>重启（Flyway V9 会在 backend 启动时建列）；
synctask-mysql(33306) 在跑。
用法：python3 test_scripts/fault_injection/sla_metrics.py
"""
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

SRC_DB = "sla_src"
TGT_DB = "sla_tgt"
CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def seed():
    F.sql_exec(CFG, [
        f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB} CHARACTER SET utf8mb4",
        f"DROP DATABASE IF EXISTS {TGT_DB}", f"CREATE DATABASE {TGT_DB} CHARACTER SET utf8mb4",
    ])
    F.sql_exec(CFG, [
        "CREATE TABLE t(id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(64), ts DATETIME(3))",
        "INSERT INTO t(v, ts) VALUES ('seed', NOW(3))",
    ], db=SRC_DB)


def workflow(token, task_id):
    r = F.api("GET", f"/api/workflows/{task_id}", token)
    return (r.get("data") or {})


def wait_for(fn, timeout=180, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = fn()
        if value is not None:
            return value
        time.sleep(interval)
    return None


def main():
    print("=" * 78)
    print("SLA 闭环判据（P2-4）：指标 → 落库 → 告警")
    print("=" * 78)

    token = F.login()
    seed()
    rule_id = None

    task_id = F.create_task(token, f"SLA-{int(time.time())}", "mysql", "mysql", CONN, CONN,
                            "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["t"]}}}}',
                            TGT_DB, source_db=SRC_DB)
    print(f"[任务] {task_id}")
    try:
        st = F.wait_status(token, task_id, ["INCREMENT_RUNNING"], timeout=420)
        record("任务进入增量阶段", st == "INCREMENT_RUNNING", f"status={st}")
        if st != "INCREMENT_RUNNING":
            raise SystemExit(1)

        # 持续写入，让链路有真实流量（绝对延迟需要"已应用事件"才有分母）
        for i in range(20):
            F.sql_exec(CFG, [f"INSERT INTO t(v, ts) VALUES ('row{i}', NOW(3))"], db=SRC_DB)
            time.sleep(0.2)

        # ---- 尺子 1/2：指标落库 ----
        w = wait_for(lambda: workflow(token, task_id) if
                     workflow(token, task_id).get("replication_lag_ms") is not None else None,
                     timeout=180)
        lag = (w or {}).get("replication_lag_ms")
        record("绝对复制延迟已上报（与 rpo_ms 是两个口径）", lag is not None,
               f"replication_lag_ms={lag}, rpo_ms={(w or {}).get('rpo_ms')}")

        disk = (w or {}).get("disk_usage_bytes")
        record("磁盘占用已上报", disk is not None and disk > 0, f"disk_usage_bytes={disk}")

        # ---- 尺子 3/4：杀 capture，重启次数与重放量 ----
        killed = F.signal_child(task_id, "capture", signal.SIGKILL)
        print(f"[注入] SIGKILL capture: {killed}")
        restarts = wait_for(lambda: (workflow(token, task_id).get("restart_count_10m") or 0) or None,
                            timeout=180)
        record("子进程重启次数对外可见（crash-loop 不再只存在于 agent 日志）",
               bool(restarts), f"restart_count_10m={restarts}")

        replay = workflow(token, task_id).get("capture_replay_bytes")
        record("重放放大量已上报（位点持久化正常时应很小）", replay is not None,
               f"capture_replay_bytes={replay}")

        # ---- 尺子 5：告警闭环 ----
        rule = F.api("POST", "/api/advanced/alert-rules", token, json={
            "workflowId": task_id, "ruleName": f"sla-{int(time.time())}",
            "metricType": "DISK_USAGE_BYTES", "operator": "GT", "threshold": 0,
            "notifyChannels": "WEBHOOK",
        })
        record("新指标类型可建告警规则", bool(rule.get("success")), str(rule.get("message") or ""))
        rule_id = ((rule.get("data") or {}).get("id"))

        def has_event():
            ev = F.api("GET", "/api/advanced/alert-events?page=1&pageSize=20", token)
            items = ((ev.get("data") or {}).get("list") or (ev.get("data") or {}).get("records") or [])
            for e in items:
                if e.get("workflowId") == task_id and e.get("metricType") == "DISK_USAGE_BYTES":
                    return e
            return None

        # 规则引擎每 30s 扫一次
        event = wait_for(has_event, timeout=150, interval=10)
        record("告警规则真的按新指标触发（指标→落库→告警 全链路通）", event is not None,
               (event or {}).get("message", "未产生告警事件"))
    finally:
        try:
            if rule_id:
                F.api("DELETE", f"/api/advanced/alert-rules/{rule_id}", token)
            F.stop_task(token, task_id)
            F.delete_task(token, task_id)
        except Exception as e:
            print(f"[清理] 忽略: {e}")

    print("\n" + "=" * 78)
    failed = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail else ""))
    print(f"\n合计 {len(results)} 项，失败 {len(failed)} 项")
    print("=" * 78)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
