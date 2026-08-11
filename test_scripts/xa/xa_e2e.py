#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
源库 XA 事务的增量同步判据（capture → extract → increment 三进程真链路）。

核心判据只有一句：**源库 XA 提交的那一刻，目标库才能看到这批数据**——
prepare 之后、commit 之前目标库必须是干净的；源库回滚掉的分支目标库永远不能有。

判据用"水位标记"而不是 sleep 来判定：每做完一件事就插一行普通数据当标记，
等标记到达目标库，就说明它之前的所有事件都已经流过整条链路了。这时候再断言
"XA 的行不在目标库"才是有意义的，否则只是没等够。

用法：
    python3 test_scripts/xa/xa_e2e.py
"""
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xalib as X

TASK = "xa-e2e"
SRC_DB, TGT_DB, TABLE = "xa_src", "xa_tgt", "t_xa"
TASK_DIR = os.path.join(X.PROJECT_ROOT, "files", TASK)
CAP_DIR = os.path.join(TASK_DIR, "binlog_output")
THL_DIR = os.path.join(TASK_DIR, "thl_output")

results = []
marker_seq = [1000]


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def marker(pipe, what):
    """插一行普通数据当水位标记，并等它流到目标库。返回是否成功。

    标记到达 = 它之前进 binlog 的所有事件都已经走完整条链路。后续断言
    "某些行不在目标库"才站得住脚。
    """
    marker_seq[0] += 1
    mid = marker_seq[0]
    X.mysql(X.SRC_CT, f"INSERT INTO {TABLE} VALUES ({mid}, 'marker-{what}', 0)", db=SRC_DB)
    ok = pipe.wait_until(lambda: mid in X.rows_of(X.TGT_CT, TGT_DB, TABLE), 90,
                         f"水位标记 {mid}({what}) 到达目标库")
    return ok


def target_ids():
    return set(X.rows_of(X.TGT_CT, TGT_DB, TABLE).keys())


def main():
    X.require_jars()

    print("=" * 72)
    print("源库 XA 事务增量同步判据（capture → extract → increment）")
    print("=" * 72)

    X.reset_db(X.SRC_CT, SRC_DB)
    X.reset_db(X.TGT_CT, TGT_DB)
    X.create_table(X.SRC_CT, SRC_DB, TABLE)
    X.create_table(X.TGT_CT, TGT_DB, TABLE)

    # 上一轮跑挂了可能留下未决分支，会一直占着源库的锁
    for gtrid in X.xa_recover(X.SRC_CT):
        try:
            X.xa_rollback(X.SRC_CT, gtrid)
            print(f"  (清理上一轮残留的未决分支 {gtrid})")
        except Exception:
            pass

    shutil.rmtree(TASK_DIR, ignore_errors=True)
    os.makedirs(CAP_DIR, exist_ok=True)
    os.makedirs(THL_DIR, exist_ok=True)
    X.write_config(TASK_DIR, TASK, SRC_DB, TGT_DB, TABLE, CAP_DIR, THL_DIR)

    pipe = X.Pipeline(TASK, TASK_DIR)
    try:
        pipe.start_all()
        time.sleep(6)

        if not marker(pipe, "boot"):
            record("链路打通", False, "普通事务都没能同步，后面的判据无从谈起")
            return summarize()
        record("链路打通", True, "普通事务已能同步")

        # ---- 判据 1：两阶段提交，prepare 之后目标库必须干净 ----
        X.xa_prepare_branch(X.SRC_CT, SRC_DB, "xa-commit", [
            f"INSERT INTO {TABLE} VALUES (10, 'xa-commit', 1)",
            f"INSERT INTO {TABLE} VALUES (11, 'xa-commit', 2)",
        ])
        if not marker(pipe, "after-prepare"):
            record("prepare 后普通事务不受阻", False, "未决分支把后续事务堵住了")
            return summarize()
        record("prepare 后普通事务不受阻", True, "未决 XA 分支不阻塞其它事务的同步")

        after_prepare = target_ids()
        record("prepare 阶段目标库不落数据", not ({10, 11} & after_prepare),
               "源库尚未提交，目标库必须看不到这批行"
               if not ({10, 11} & after_prepare) else f"目标库提前出现了 {sorted({10, 11} & after_prepare)}")

        # ---- 判据 2：XA COMMIT 之后整段到达 ----
        X.xa_commit(X.SRC_CT, "xa-commit")
        got = pipe.wait_until(lambda: {10, 11} <= target_ids(), 90, "XA COMMIT 后数据到达目标库")
        record("XA COMMIT 后整段到达", got, "源库提交的那一刻数据才下发")

        # ---- 判据 3：XA ROLLBACK 的分支一行都不能有 ----
        X.xa_prepare_branch(X.SRC_CT, SRC_DB, "xa-rollback", [
            f"INSERT INTO {TABLE} VALUES (30, 'xa-rollback', 1)",
            f"INSERT INTO {TABLE} VALUES (31, 'xa-rollback', 2)",
        ])
        marker(pipe, "before-rollback")
        X.xa_rollback(X.SRC_CT, "xa-rollback")
        if not marker(pipe, "after-rollback"):
            record("回滚分支不留幻影行", False, "标记未到达，判据无效")
            return summarize()
        phantom = {30, 31} & target_ids()
        record("回滚分支不留幻影行", not phantom,
               "源库回滚的 XA 事务在目标库不留痕"
               if not phantom else f"目标库留下了幻影行 {sorted(phantom)}")

        # ---- 判据 4：一阶段提交（XA COMMIT … ONE PHASE）----
        X.xa_one_phase(X.SRC_CT, SRC_DB, "xa-one-phase", [
            f"INSERT INTO {TABLE} VALUES (40, 'xa-one-phase', 1)",
        ])
        got = pipe.wait_until(lambda: 40 in target_ids(), 90, "一阶段提交到达目标库")
        record("一阶段提交（ONE PHASE）", got, "binlog 里只有一个 onePhase=true 的 XA_prepare 事件")

        # ---- 判据 5：整个 XA 事务在目标库是原子的 ----
        X.xa_prepare_branch(X.SRC_CT, SRC_DB, "xa-atomic", [
            f"INSERT INTO {TABLE} VALUES ({i}, 'xa-atomic', {i})" for i in (50, 51, 52, 53)
        ])
        marker(pipe, "before-atomic-commit")
        X.xa_commit(X.SRC_CT, "xa-atomic")
        partial_seen = []
        deadline = time.time() + 90
        while time.time() < deadline:
            present = {50, 51, 52, 53} & target_ids()
            if present and len(present) < 4:
                partial_seen.append(sorted(present))
            if len(present) == 4:
                break
            time.sleep(0.05)
        atomic_ok = ({50, 51, 52, 53} <= target_ids()) and not partial_seen
        record("XA 事务在目标库原子落地", atomic_ok,
               "高频采样未观测到半个事务"
               if atomic_ok else f"观测到中间态 {partial_seen[:3]}")

        # ---- 判据 6：已 prepare 的分支跨 extract 重启仍能提交 ----
        X.xa_prepare_branch(X.SRC_CT, SRC_DB, "xa-restart", [
            f"INSERT INTO {TABLE} VALUES (60, 'xa-restart', 1)",
        ])
        marker(pipe, "before-restart")
        pipe.stop("extract")
        time.sleep(2)
        pipe.start("extract")
        time.sleep(3)
        X.xa_commit(X.SRC_CT, "xa-restart")
        got = pipe.wait_until(lambda: 60 in target_ids(), 120, "重启后 XA COMMIT 数据到达")
        record("已 prepare 分支跨 extract 重启不丢", got, "分支落盘文件在重启后被重新纳管")

        # ---- 判据 7：应用连接没有被 XA 语句打死 ----
        err = X.error_status(TASK_DIR)
        record("无 fail-stop 上报", err == "",
               "整轮没有 error_status" if err == "" else f"上报了错误: {err[:160]}")
        alive = pipe.alive()
        record("三进程全部存活", all(alive.values()), str(alive))
        record("应用连接仍可用", marker(pipe, "final"),
               "XA 语句若被原样打到目标库，连接会卡在 XA ACTIVE 态，这一步必然失败")

        # ---- 判据 8：源库没留下未决分支（判据自身的卫生）----
        left = X.xa_recover(X.SRC_CT)
        record("源库无残留未决分支", not left, str(left))

    finally:
        logs = pipe.stop_all()
        failed = [name for name, ok, _ in results if not ok]
        if failed:
            for name in ("extract", "increment"):
                tail = "".join(logs.get(name, "").splitlines(keepends=True)[-40:])
                print(f"\n---- {name} 日志尾部 ----\n{tail}")

    return summarize()


def summarize():
    print("\n" + "=" * 72)
    passed = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail else ""))
    print(f"总计: {passed}/{len(results)}")
    print("=" * 72)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
