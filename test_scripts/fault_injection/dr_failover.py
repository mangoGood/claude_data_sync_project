#!/usr/bin/env python3
"""
灾备主备倒换（failover）+ 故障注入一致性测试。

倒换语义：后端交换任务的源/目标连接，agent 停掉旧方向的全部子进程、清空 checkpoint/THL、
以 skipFullMigration=true 重启——新方向 B→A 的 capture 从 B 的**最新**位点起步。因此
倒换点必须是"两端已收敛"的时刻（计划内切换），倒换后 A 才是 B 的有效副本基线。

注入时机（--inject）：
  before  倒换前（两端已收敛后）SIGKILL 一个受守护子进程，紧接着在自愈窗口内发起倒换
  during  发起倒换后立刻 SIGKILL 新方向刚拉起的子进程（SWITCHING 窗口内崩溃）
  after   倒换完成、新方向写入过程中轮流 SIGKILL 子进程
  none    不注入（基线对照）

每种都验证：倒换后两端在倒换点仍精确一致 → 向新主库 B 持续写入 → 新方向 B→A 收敛且精确一致。
--switch-back 额外再倒换回 A→B 并复验一次。

用法：
  python3 dr_failover.py <mysql2mysql|pg2pg> [--inject before|during|after|none]
                         [--seed-rows N] [--write-seconds 60] [--switch-back]
退出码 0 = 全通过。
"""
import argparse
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drlib as DR  # noqa: E402
import faultlib as F  # noqa: E402

fmt = DR.fmt


def kill_engine(task_id, engine, tag=""):
    pids = F.signal_child(task_id, engine, signal.SIGKILL)
    stamp = time.strftime("%H:%M:%S")
    print(f"    [{stamp}] {tag}SIGKILL {engine} pid={pids if pids else '(无进程)'}")
    return pids


def write_and_maybe_inject(ep, side, task_id, link, seconds, inject, passed):
    """向某一端持续写入 seconds 秒；inject=True 时期间轮流杀受守护子进程。"""
    engines = DR.DR_LINKS[link]["engines"]
    w = DR.make_writer(ep, 0.03, side)
    w.start()
    kills = 0
    end = time.time() + seconds
    next_kill = time.time() + 25
    idx = 0
    while time.time() < end:
        time.sleep(2)
        if inject and time.time() >= next_kill:
            if kill_engine(task_id, engines[idx % len(engines)], tag="[写入中] "):
                kills += 1
            idx += 1
            next_kill = time.time() + 35
    w.stop.set(); w.join(timeout=30)
    print(f"    写入 {side.upper()} 端结束：ins={w.inserts} upd={w.updates} del={w.deletes}"
          f"{f'，注入 {kills} 次崩溃' if inject else ''}")
    if w.error:
        return kills, f"写入线程异常: {w.error}"
    if inject:
        passed.append(f"倒换后新方向写入期间注入 {kills} 次崩溃")
    return kills, None


def wait_post_failover_running(token, task_id, timeout=420):
    """等待倒换后任务回到 INCREMENT_RUNNING。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        st = F.get_status(token, task_id)
        if st != last:
            print(f"    倒换后状态: {st}")
            last = st
        if st == "INCREMENT_RUNNING":
            return st
        if st == "FAILED":
            # 倒换窗口内注入崩溃可能把任务打成 FAILED，允许 retry 自愈
            print("    倒换后 FAILED → retry")
            DR.retry(token, task_id)
        time.sleep(4)
    return last


def do_failover_round(token, task_id, link, src, tgt, src_side, tgt_side,
                      inject, write_seconds, passed, failed, round_name):
    """一轮倒换：src→tgt 变成 tgt→src。src/tgt 为倒换**前**的方向。"""
    print(f"\n=== {round_name}：倒换前静默并等待两端收敛 ===")
    ok, sfp, tfp = DR.wait_converge(src, tgt, token, task_id, timeout=900)
    print(f"    倒换点 {src_side.upper()}={fmt(sfp)} {tgt_side.upper()}={fmt(tfp)}")
    (passed if ok else failed).append(f"{round_name} 倒换前两端已收敛一致")
    if not ok:
        return False
    pre_fp = sfp

    if inject == "before":
        print("=== 注入：倒换前崩溃（自愈窗口内立即发起倒换）===")
        kill_engine(task_id, "increment", tag="[倒换前] ")
        kill_engine(task_id, "capture", tag="[倒换前] ")
        time.sleep(2)
        passed.append(f"{round_name} 倒换前已注入子进程崩溃")

    print(f"=== {round_name}：发起主备倒换 ===")
    r = DR.failover(token, task_id)
    if not r.get("success"):
        failed.append(f"{round_name} 倒换 API 调用失败: {r}")
        return False
    passed.append(f"{round_name} 倒换 API 调用成功")

    if inject == "during":
        print("=== 注入：倒换窗口内崩溃（新方向子进程刚拉起就杀）===")
        killed = 0
        for _ in range(6):
            time.sleep(5)
            for eng in ("capture", "extract", "increment"):
                if kill_engine(task_id, eng, tag="[倒换中] "):
                    killed += 1
            if killed >= 3:
                break
        passed.append(f"{round_name} 倒换窗口内注入 {killed} 次崩溃")

    st = wait_post_failover_running(token, task_id)
    ok = st == "INCREMENT_RUNNING"
    (passed if ok else failed).append(f"{round_name} 倒换后恢复增量灾备（终态 {st}）")
    if not ok:
        return False

    # 倒换后连接串应已交换
    t = DR.get_task(token, task_id)
    L = DR.DR_LINKS[link]
    # 倒换后新源应指向原目标端的 host:port（连接串可能被加密存储，只比对端点部分）
    new_src_endpoint = L[f"{tgt_side}_conn"].rsplit("@", 1)[-1]
    host_port = f"{L[tgt_side]['host']}:{L[tgt_side]['port']}"
    src_conn_now = t.get("source_connection") or ""
    swapped = host_port in src_conn_now or new_src_endpoint in src_conn_now
    (passed if swapped else failed).append(
        f"{round_name} 任务源/目标连接已交换（当前源={src_conn_now}）")

    # 倒换点数据未变：新主库仍是倒换前的那份数据
    time.sleep(3)
    now_fp = DR.fingerprint(tgt)
    same = now_fp == pre_fp
    (passed if same else failed).append(
        f"{round_name} 倒换后新主库数据与倒换点一致 {fmt(now_fp)}")

    print(f"=== {round_name}：向新主库 {tgt_side.upper()} 写入 {write_seconds}s"
          f"{'（期间注入崩溃）' if inject == 'after' else ''} ===")
    _, werr = write_and_maybe_inject(tgt, tgt_side, task_id, link, write_seconds,
                                     inject == "after", passed)
    if werr:
        failed.append(f"{round_name} {werr}")

    print(f"=== {round_name}：等待新方向 {tgt_side.upper()}→{src_side.upper()} 收敛 ===")
    ok, nfp, ofp = DR.wait_converge(tgt, src, token, task_id, timeout=900)
    print(f"    最终 {tgt_side.upper()}={fmt(nfp)} {src_side.upper()}={fmt(ofp)}")
    (passed if ok else failed).append(f"{round_name} 倒换后新方向数据一致（不丢不重）")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link", choices=list(DR.DR_LINKS.keys()))
    ap.add_argument("--inject", choices=["before", "during", "after", "none"], default="before")
    ap.add_argument("--seed-rows", type=int, default=int(os.environ.get("DR_SEED_ROWS", "200000")))
    ap.add_argument("--write-seconds", type=int, default=60)
    ap.add_argument("--switch-back", action="store_true")
    args = ap.parse_args()

    token = F.login()
    print(f"✓ 登录；单向灾备 {args.link} 主备倒换测试，注入时机={args.inject}")
    passed, failed = [], []

    DR.drop_pg_slots(args.link)
    print(f"[准备] 重建两端库并在 A 端播种 {args.seed_rows} 行 ...")
    a, b = DR.reset_both(args.link, seed_rows=args.seed_rows, seed_side="a")

    old_quota = F.get_increment_quota()
    if old_quota is not None:
        F.set_increment_quota(100000)
    task_id = DR.create_dr_task(token, f"DRFO-{args.link}-{args.inject}-{int(time.time())}",
                                args.link, "UNIDIRECTIONAL")
    print(f"[任务] 灾备任务 {task_id}")
    try:
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=1800)
        ok = st == "INCREMENT_RUNNING"
        (passed if ok else failed).append(f"全量完成并进入增量灾备（终态 {st}）")
        if not ok:
            return F.print_result(passed, failed)

        # 倒换前先在原主库 A 上写一段增量，制造真实的"运行中"状态
        print("[倒换前] 向原主库 A 写入 40s ...")
        write_and_maybe_inject(a, "a", task_id, args.link, 40, False, passed)

        ok = do_failover_round(token, task_id, args.link, a, b, "a", "b",
                               args.inject, args.write_seconds, passed, failed, "第一次倒换 A→B ⇒ B→A")

        if ok and args.switch_back:
            ok = do_failover_round(token, task_id, args.link, b, a, "b", "a",
                                   args.inject, args.write_seconds, passed, failed,
                                   "第二次倒换 B→A ⇒ A→B")
    finally:
        DR.cleanup_task(token, task_id)
        if old_quota is not None:
            F.set_increment_quota(old_quota)
    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
