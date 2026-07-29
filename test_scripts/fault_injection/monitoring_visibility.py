#!/usr/bin/env python3
"""
实时监控指标可见性验证：灾备(DR) / 同步(SYNC) / 订阅(SUBSCRIBE) 三类任务都应在监控页可见、
且能取到真实指标。

背景：监控页此前直连 agent:8083 取指标。agent 的只读监控端点在配置了 AGENT_API_TOKEN 后
要求 Bearer <AGENT_API_TOKEN>（服务端密钥，不能下发浏览器），页面要么不带头、要么错带用户
JWT，于是一律 401 —— 三类任务的指标卡片全是 "--"。现改为经后端代理
（/api/workflows/... ，用户 JWT 鉴权 + 属主校验，后端再持 agent token 转发）。

本脚本按监控页的真实取数路径逐个验证，用法：
  python3 monitoring_visibility.py            # 只检查当前已在跑的任务
  python3 monitoring_visibility.py --create-dr  # 顺带起一个灾备任务再验证
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sublib as S  # noqa: E402

# 监控页判定"正在跑"的状态集合（与 admin-dashboard.js 的 RUNNING_STATES 保持一致）
RUNNING_STATES = {
    "PENDING", "RECEIVED", "STARTING", "RUNNING",
    "FULL_MIGRATING", "FULL_COMPLETED", "INCREMENT_RUNNING",
    "SUBSCRIBE_RUNNING", "SWITCHING",
}


def metrics_task_list(token):
    """复刻监控页 loadMetricsTaskList()：三个数据源合并后按运行态过滤。"""
    task_map = {}

    d = S.api("GET", "/api/workflows?page=1&pageSize=200", token)
    for t in (d.get("data") or {}).get("list") or []:
        tid = t.get("id")
        if tid and tid not in task_map:
            task_map[tid] = dict(id=tid, name=t.get("name") or tid,
                                 taskType=t.get("task_type") or "SYNC",
                                 status=t.get("status") or "",
                                 drStatus=t.get("dr_status"))

    for path, in (("/api/workflows/agent-status",), ("/api/workflows/metrics/all",)):
        d = S.api("GET", path, token)
        for t in d.get("tasks") or []:
            tid = t.get("taskId")
            if tid and tid not in task_map:
                task_map[tid] = dict(id=tid, name=t.get("name") or tid,
                                     taskType=t.get("taskType") or "SYNC",
                                     status=t.get("status") or "RUNNING",
                                     drStatus=t.get("drStatus"))

    return [t for t in task_map.values() if (t["status"] or "").upper() in RUNNING_STATES]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--create-dr", action="store_true", help="先起一个灾备任务再验证")
    ap.add_argument("--create-sync", action="store_true", help="先起一个普通同步任务再验证")
    ap.add_argument("--keep", action="store_true", help="验证完保留新建的任务")
    args = ap.parse_args()

    token = S.login()
    passed, failed = [], []
    dr_tid = None
    sync_tid = None

    if args.create_sync:
        import dblib as D
        import faultlib as F
        print("[准备] 重建 pg2pg 两端并起普通同步任务 ...")
        link = D.LINKS["pg2pg"]
        src = D.make_endpoint(link["source"])
        tgt = D.make_endpoint(link["target"])
        src.reset_source()
        tgt.reset_target()
        src.seed(20000)
        sync_tid = F.create_task(token, f"MON-sync-{int(time.time())}",
                                 link["source_type"], link["target_type"],
                                 link["src_conn"], link["tgt_conn"], "fullAndIncre",
                                 link["sync_objects"], link["target"]["db"],
                                 source_db=link["source"]["db"])
        print(f"[任务] 同步 {sync_tid}")
        st = F.wait_status(token, sync_tid, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            failed.append(f"同步任务未进入 INCREMENT_RUNNING（{st}）")

    if args.create_dr:
        import drlib as R
        print("[准备] 重建灾备两端并起灾备任务 ...")
        R.drop_pg_slots("mysql2mysql")
        a, _b = R.reset_both("mysql2mysql", seed_rows=2000, seed_side="a")
        dr_tid = R.create_dr_task(token, f"MON-dr-{int(time.time())}", "mysql2mysql")
        print(f"[任务] 灾备 {dr_tid}")
        st = S.wait_status(token, dr_tid, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            failed.append(f"灾备任务未进入 INCREMENT_RUNNING（{st}）")
            return S.print_result(passed, failed)
        w = R.make_writer(a, 0.05, side="a")
        w.start()
        time.sleep(40)
        w.stop.set(); w.join(timeout=20)

    tasks = metrics_task_list(token)
    by_type = {}
    for t in tasks:
        by_type.setdefault(t["taskType"], []).append(t)

    print(f"\n[监控页任务下拉] 共 {len(tasks)} 个运行中任务")
    for t in tasks:
        print(f"    [{t['taskType']}] {t['name']}  status={t['status']}  drStatus={t.get('drStatus')}")

    # 逐个任务验证能否取到真实指标
    print("\n[逐任务指标]")
    for t in tasks:
        m = S.api("GET", f"/api/workflows/{t['id']}/metrics", token)
        if "captureRate" not in m:
            failed.append(f"[{t['taskType']}] {t['name']} 取不到指标: {str(m)[:120]}")
            print(f"    ✗ [{t['taskType']}] {t['name']}: {str(m)[:120]}")
            continue
        procs = m.get("processes") or []
        running = [p for p in procs if p.get("state") == "RUNNING"]
        print(f"    ✓ [{t['taskType']}] {t['name']}: captureRate={m.get('captureRate')} "
              f"e2eLatency={m.get('e2eLatency')} 队列={m.get('queueDepth')} "
              f"进程 {len(running)}/{len(procs)}")
        passed.append(f"[{t['taskType']}] {t['name']} 指标可读（进程 {len(running)}/{len(procs)}）")

    for want in ("DR", "SYNC", "SUBSCRIBE"):
        label = {"DR": "灾备", "SYNC": "同步", "SUBSCRIBE": "订阅"}[want]
        if by_type.get(want):
            passed.append(f"监控页可见「{label}中」的任务（{len(by_type[want])} 个）")
        else:
            print(f"    （提示：当前没有运行中的{label}任务，未覆盖该类型）")

    if not args.keep:
        if dr_tid:
            import drlib as R
            R.cleanup_task(token, dr_tid)
        if sync_tid:
            S.stop_task(token, sync_tid)
            time.sleep(3)
            S.delete_task(token, sync_tid)
    elif dr_tid or sync_tid:
        print(f"\n[保留] 灾备={dr_tid} 同步={sync_tid}（未停止未删除）")

    return S.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
