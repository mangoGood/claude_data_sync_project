#!/usr/bin/env python3
"""
双向灾备写写冲突消解的判定（mysql↔mysql 双向，dr-mysql-a/b）。

背景：双向此前只有<b>防回环</b>（__sync_origin 事务打标），没有<b>冲突策略</b>。
两端同时改同一行时，两条通道各自 ON DUPLICATE KEY UPDATE 覆盖对方，结果是
**值互换**：A 变成 B 的值、B 变成 A 的值，两端永远不一致，而且看起来还像"同步成功"——
既不报错也不告警，只有对账时才会发现。

判据：
  1. 两端同时改同一行后，最终<b>收敛到同一个值</b>（旧行为是值互换、永不收敛）；
  2. 赢家是确定的（由策略决定，不是"谁后到"）——两端各自独立裁决且结论一致；
  3. 冲突被记录下来（conflict.jsonl / /api/agent/conflicts），不是静默丢写；
  4. 旁路表 __sync_rowmeta 有该行的元数据（谁在什么源时刻写的）；
  5. 非冲突行照常双向同步（冲突消解没把正常链路弄坏）。

制造真冲突的办法：先把两个方向的 increment 都 SIGSTOP 住，两端各写各的，
再 SIGCONT 放行——否则一端的写会先同步过去，变成"顺序修改"而不是并发冲突。

用法：
    python3 test_scripts/fault_injection/bidi_conflict.py [--rows 6]
"""
import argparse
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

A = dict(host="127.0.0.1", port=33320, user="root", password="rootpassword")
B = dict(host="127.0.0.1", port=33321, user="root", password="rootpassword")
DB = "cdrtest"
TABLE = "acct"
A_CONN = "mysql://root:rootpassword@127.0.0.1:33320"
B_CONN = "mysql://root:rootpassword@127.0.0.1:33321"

DDL = f"""
CREATE TABLE `{TABLE}` (
  `id` INT NOT NULL,
  `owner` VARCHAR(32) NOT NULL,
  `val` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def create_bidi_dr_task(token, name, sync_objects):
    """建一个双向灾备任务（A→B，后端会自动配一条 B→A 的影子通道）。

    没有复用 drlib：它连带 import psycopg2（本机没装），而这个用例只跑 mysql↔mysql。
    """
    r = F.api("POST", "/api/workflows", token,
              json={"name": name, "sourceType": "mysql", "targetType": "mysql",
                    "taskType": "DR", "drMode": "BIDIRECTIONAL"})
    if not r.get("success"):
        print(f"创建灾备任务失败: {r}")
        sys.exit(1)
    task_id = r["data"]["id"]
    F.api("PUT", f"/api/workflows/{task_id}/config", token, json={
        "sourceConnection": A_CONN,
        "targetConnection": B_CONN,
        "migrationMode": "fullAndIncre",
        "syncObjects": sync_objects,
        "sourceDbName": DB,
        "targetDbName": DB,
        "sourceType": "mysql",
        "targetType": "mysql",
    })
    r = F.api("POST", f"/api/workflows/{task_id}/launch", token)
    if not r.get("success"):
        print(f"启动灾备任务失败: {r}")
        sys.exit(1)
    return task_id


def shadow_id(token, task_id):
    return (F.api("GET", f"/api/workflows/{task_id}", token).get("data") or {}).get("dr_peer_workflow_id")


def cleanup_task(token, task_id):
    F.stop_task(token, task_id)
    time.sleep(3)
    F.delete_task(token, task_id)


def reset(cfg):
    F.sql_exec(cfg, [f"DROP DATABASE IF EXISTS {DB}", f"CREATE DATABASE {DB}"])


def rows(cfg):
    return {r[0]: (r[1], r[2]) for r in
            F.sql_fetch(cfg, DB, f"SELECT id, owner, val FROM {TABLE} ORDER BY id")}


def stop_increments(task_ids, sig):
    hit = []
    for t in task_ids:
        hit += F.signal_child(t, "increment", sig)
    return hit


def conflicts_via_api(token, task_id):
    try:
        r = F.api("GET", f"/api/workflows/{task_id}/conflicts", token)
        body = r.get("data") if isinstance(r.get("data"), dict) else r
        return (body or {}).get("records") or []
    except Exception as e:
        print(f"    （冲突接口不可用: {e}）")
        return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=6, help="并发冲突的行数")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    token = F.login()
    print("✓ 登录成功")

    reset(A)
    reset(B)
    seed = ",".join(f"({i},'seed',0)" for i in range(1, args.rows + 3))
    F.sql_exec(A, [DDL, f"INSERT INTO {TABLE} (id,owner,val) VALUES {seed}"], db=DB)

    passed, failed = [], []
    task_id = shadow = None
    try:
        task_id = create_bidi_dr_task(
            token, f"cdr-{int(time.time())}",
            '{"%s": {"tables": ["%s"]}}' % (DB, TABLE))
        print(f"[任务] 正向 {task_id}")
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=480)
        if st != "INCREMENT_RUNNING":
            failed.append(f"正向任务未进入增量（{st}）")
            return F.print_result(passed, failed)

        deadline = time.time() + 180
        while time.time() < deadline:
            shadow = shadow_id(token, task_id)
            if shadow and F.child_pids(shadow, "increment"):
                break
            time.sleep(3)
        print(f"[任务] 反向影子 {shadow}")
        if not shadow:
            failed.append("双向灾备没有创建反向影子任务")
            return F.print_result(passed, failed)
        time.sleep(10)

        # ---------- 制造并发冲突 ----------
        both = [task_id, shadow]
        stopped = stop_increments(both, signal.SIGSTOP)
        print(f"\n[冲突] 已冻结两个方向的 increment: {stopped}")
        try:
            ca = F.sql_conn(A, DB); cua = ca.cursor()
            cb = F.sql_conn(B, DB); cub = cb.cursor()
            for i in range(1, args.rows + 1):
                cua.execute(f"UPDATE {TABLE} SET owner='A', val=100+{i} WHERE id={i}")
            print(f"    A 端改了 {args.rows} 行（owner=A）")
            # binlog 事件时间戳是<b>秒级</b>的：两端写入必须隔开 2s 以上，
            # 否则时间戳相同、LWW 退化成平局裁决，测不出"时间序"这条语义
            time.sleep(2.5)
            for i in range(1, args.rows + 1):
                cub.execute(f"UPDATE {TABLE} SET owner='B', val=200+{i} WHERE id={i}")
            print(f"    B 端改了同样 {args.rows} 行（owner=B，晚 2.5s）")
            # 非冲突对照行：只在 A 端改
            cua.execute(f"UPDATE {TABLE} SET owner='A-only', val=999 WHERE id={args.rows + 1}")
            cua.close(); ca.close(); cub.close(); cb.close()
        finally:
            stop_increments(both, signal.SIGCONT)
            print("[冲突] 已放行两个方向的 increment")

        # ---------- 判据1/2：收敛 ----------
        deadline = time.time() + 240
        ra = rb = {}
        while time.time() < deadline:
            ra, rb = rows(A), rows(B)
            conflict_ids = list(range(1, args.rows + 1))
            if all(ra.get(i) == rb.get(i) for i in conflict_ids) and ra:
                break
            time.sleep(3)

        mismatched = [i for i in range(1, args.rows + 1) if ra.get(i) != rb.get(i)]
        print(f"\n[判据1] 冲突行 {args.rows} 行，两端仍不一致的: {len(mismatched)}")
        for i in list(range(1, args.rows + 1))[:3]:
            print(f"    id={i}: A={ra.get(i)} B={rb.get(i)}")
        if mismatched:
            failed.append(f"{len(mismatched)}/{args.rows} 个冲突行两端不一致（值互换/未收敛）")
        else:
            passed.append(f"{args.rows} 个并发冲突行全部收敛到同一值")

        winners = {ra.get(i, (None,))[0] for i in range(1, args.rows + 1)}
        print(f"[判据2] 收敛后的 owner 取值: {winners}")
        if len(winners) == 1 and None not in winners:
            passed.append(f"赢家确定且一致（owner={winners.pop()}），不是「谁后到谁覆盖」")
        else:
            failed.append(f"不同冲突行选出了不同赢家: {winners}")

        # ---------- 判据3：冲突记录 ----------
        recs = conflicts_via_api(token, task_id) + conflicts_via_api(token, shadow)
        print(f"[判据3] 冲突记录条数: {len(recs)}")
        if recs:
            print(f"    示例: {recs[0]}")
            passed.append(f"冲突被记录（{len(recs)} 条，可在 UI 查看）")
        else:
            failed.append("没有任何冲突记录，等于静默丢写")

        # ---------- 判据4：旁路表 ----------
        meta_a = F.sql_fetch(A, DB, "SELECT COUNT(*) FROM __sync_rowmeta")[0][0]
        meta_b = F.sql_fetch(B, DB, "SELECT COUNT(*) FROM __sync_rowmeta")[0][0]
        print(f"[判据4] __sync_rowmeta 行数: A={meta_a} B={meta_b}")
        if meta_a > 0 and meta_b > 0:
            passed.append(f"两端旁路表都记下了行级写入元数据（A={meta_a} B={meta_b}）")
        else:
            failed.append(f"旁路表为空（A={meta_a} B={meta_b}），冲突消解没有依据")

        # ---------- 判据5：非冲突行照常同步 ----------
        only_id = args.rows + 1
        deadline = time.time() + 120
        while time.time() < deadline:
            rb = rows(B)
            if rb.get(only_id) == ("A-only", 999):
                break
            time.sleep(3)
        print(f"[判据5] 非冲突行 id={only_id}: A={rows(A).get(only_id)} B={rb.get(only_id)}")
        if rb.get(only_id) == ("A-only", 999):
            passed.append("非冲突行照常双向同步（冲突消解没伤到正常链路）")
        else:
            failed.append(f"非冲突行没同步过去: B={rb.get(only_id)}")

    finally:
        if not args.keep:
            for t in (task_id, shadow):
                if t:
                    cleanup_task(token, t)
            time.sleep(2)
            for cfg in (A, B):
                try:
                    F.sql_exec(cfg, [f"DROP DATABASE IF EXISTS {DB}"])
                except Exception:
                    pass

    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
