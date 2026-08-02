#!/usr/bin/env python3
"""
灾备任务断点续传 + 故障注入一致性测试（mysql2mysql / pg2pg，单向 / 双向）。

灾备任务的同步模式由后端强制为 fullAndIncre，故"全量"与"全量+增量"两种覆盖表现为
**在不同阶段注入故障**：

  --phase full   全量搬运途中 SIGKILL migration-full（不受 ProcessGuard 守护 → 任务 FAILED），
                 retry 触发断点续传，最终全量落地 + 进入增量，逐指纹比对源/目标精确一致。
  --phase incre  进入增量后持续写入，其间轮流 SIGKILL capture/extract/increment（受守护，
                 自动重启 + checkpoint 续传），停写后等待追平，逐指纹比对。
  --phase both   先 full 再 incre（同一任务上连续注入两个阶段的故障）。

  --mode bidi    双向灾备：A/B 两端同时写入，正/反两条通道的子进程都注入崩溃，
                 停写后等待两端收敛到同一指纹（既验证续传一致，也验证不回环放大）。

用法：
  python3 dr_resume.py <mysql2mysql|pg2pg> [--mode uni|bidi] [--phase full|incre|both]
                       [--minutes 5] [--seed-rows N]
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


def kill_engine(task_id, engine, label=""):
    pids = F.signal_child(task_id, engine, signal.SIGKILL)
    stamp = time.strftime("%H:%M:%S")
    if pids:
        print(f"    [{stamp}] SIGKILL {label}{engine} pid={pids}")
    else:
        print(f"    [{stamp}] {label}{engine} 无进程（可能正在重启）")
    return pids


# --------------------------------------------------------------- 全量阶段故障注入

def inject_full_phase_guarded(token, task_id, tgt, passed, failed, engine, seed_rows=0, kills=2):
    """单进程引擎（Mongo）的全量阶段注入：进程受 ProcessGuard 守护，崩溃后自愈重跑。

    与 SQL 管线的关键差别：这里**不该**出现 FAILED + retry —— migration-full 不受守护所以
    崩溃即判死，而 mongo 引擎受守护，杀掉后 guard 会按退避自动拉起。全量重跑靠 upsert 幂等
    收敛（已搬的文档再写一遍是同值覆盖），因此"崩溃后任务始终不 FAILED"本身就是一条断言。
    """
    print("[全量] 等待全量搬运推进并注入崩溃（单进程引擎，受守护自愈）...")
    marks = [max(2000, int(seed_rows * f)) for f in (0.25, 0.55, 0.75)] if seed_rows else [2000, 4000, 6000]
    injected = 0
    saw_failed = False
    deadline = time.time() + 1800
    while time.time() < deadline and injected < kills:
        time.sleep(2)
        st = F.get_status(token, task_id)
        if st in ("FULL_COMPLETED", "INCREMENT_RUNNING", "COMPLETED"):
            print(f"    全量已完成（状态 {st}），停止注入（已注入 {injected} 次）")
            break
        if st == "FAILED":
            saw_failed = True
            print("    任务 FAILED（不应发生：受守护进程应自愈）→ retry 兜底")
            DR.retry(token, task_id)
            time.sleep(5)
            continue
        if tgt.count() > marks[min(injected, len(marks) - 1)]:
            kill_engine(task_id, engine, label="[全量] ")
            injected += 1
            time.sleep(6)
    (passed if injected else failed).append(
        f"全量搬运途中注入 {injected} 次崩溃" if injected else "未能在全量阶段注入崩溃（数据量太小/太快）")
    (passed if not saw_failed else failed).append("全量阶段崩溃由 ProcessGuard 自愈（任务未被判 FAILED）")

    st = None
    deadline = time.time() + 1800
    while time.time() < deadline:
        st = F.get_status(token, task_id)
        if st in ("INCREMENT_RUNNING", "COMPLETED"):
            break
        if st == "FAILED":
            DR.retry(token, task_id)
        time.sleep(4)
    ok = st in ("INCREMENT_RUNNING", "COMPLETED")
    (passed if ok else failed).append(f"全量崩溃自愈后进入增量（终态 {st}）")
    return ok


def inject_full_phase(token, task_id, tgt, passed, failed, seed_rows=0, kills=2):
    """全量搬运途中杀 migration-full（无守护 → FAILED），retry 续传。"""
    print("[全量] 等待全量搬运推进并注入崩溃 ...")
    # 按已搬运进度的比例注入，数据量大时也能打在"搬到一半"上而不是刚起步
    marks = [max(3000, int(seed_rows * f)) for f in (0.25, 0.55, 0.75)] if seed_rows else [3000, 6000, 9000]
    injected = 0
    deadline = time.time() + 3600
    while time.time() < deadline and injected < kills:
        time.sleep(2)
        st = F.get_status(token, task_id)
        if st in ("FULL_COMPLETED", "INCREMENT_RUNNING", "COMPLETED"):
            print(f"    全量已完成（状态 {st}），停止注入（已注入 {injected} 次）")
            break
        if st == "FAILED":
            print("    任务 FAILED → retry 恢复")
            DR.retry(token, task_id)
            time.sleep(5)
            continue
        cnt = tgt.count()
        if cnt > marks[min(injected, len(marks) - 1)]:
            kill_engine(task_id, "full", label="[全量] ")
            injected += 1
            time.sleep(4)
            if F.get_status(token, task_id) == "FAILED":
                print("    任务 FAILED（预期：migration-full 无守护）→ retry 恢复续传")
                DR.retry(token, task_id)
    (passed if injected else failed).append(
        f"全量搬运途中注入 {injected} 次崩溃" if injected else "未能在全量阶段注入崩溃（数据量太小/太快）")

    # retry 后可能再次 FAILED（多次崩溃），循环 retry 直到进入增量
    st = None
    deadline = time.time() + 1500
    while time.time() < deadline:
        st = F.get_status(token, task_id)
        if st in ("INCREMENT_RUNNING", "COMPLETED"):
            break
        if st == "FAILED":
            print("    仍为 FAILED → 再次 retry")
            DR.retry(token, task_id)
        time.sleep(4)
    ok = st in ("INCREMENT_RUNNING", "COMPLETED")
    (passed if ok else failed).append(f"全量断点续传后进入增量（终态 {st}）")
    return ok


# --------------------------------------------------------------- 增量阶段故障注入

def inject_incre_phase(token, task_id, link, writers, minutes, passed, failed,
                       extra_task_ids=()):
    """增量期间持续写入 + 轮流 SIGKILL 受守护子进程。extra_task_ids 用于双向的影子通道。"""
    L = DR.DR_LINKS[link]
    engines = L["engines"]
    kill_every = float(os.environ.get("FI_KILL_EVERY", "70"))
    targets = [(task_id, "正向 ")] + [(t, "反向 ") for t in extra_task_ids]

    print(f"[增量] 持续写入 {minutes} 分钟，每 {kill_every}s 崩溃一个子进程 ...")
    for w in writers:
        w.start()

    kills = 0
    idx = 0
    end = time.time() + minutes * 60
    next_kill = time.time() + kill_every
    while time.time() < end:
        time.sleep(2)
        if time.time() >= next_kill:
            tid, label = targets[idx % len(targets)]
            eng = engines[(idx // len(targets)) % len(engines)]
            idx += 1
            if kill_engine(tid, eng, label=label):
                kills += 1
            next_kill = time.time() + kill_every
            if F.get_status(token, task_id) == "FAILED":
                failed.append(f"故障注入期间灾备任务被判 FAILED（{label}{eng} 杀后未自愈）")
                break

    for w in writers:
        w.stop.set()
    for w in writers:
        w.join(timeout=30)
    for i, w in enumerate(writers):
        if w.error:
            failed.append(f"写入线程 {i} 异常: {w.error}")
    tot = [f"ins={w.inserts} upd={w.updates} del={w.deletes}" for w in writers]
    print(f"[增量] 写入结束：{' | '.join(tot)}；共注入 {kills} 次崩溃")
    passed.append(f"增量期间注入 {kills} 次子进程崩溃")

    st = F.get_status(token, task_id)
    if st != "INCREMENT_RUNNING":
        failed.append(f"故障注入后灾备任务未维持 INCREMENT_RUNNING（当前 {st}）")
    return kills


# --------------------------------------------------------------- 单向灾备

def run_uni(link, phase, minutes, seed_rows):
    L = DR.DR_LINKS[link]
    token = F.login()
    print(f"✓ 登录成功；单向灾备 {link}（{L['source_type']}→{L['target_type']}），阶段={phase}")
    passed, failed = [], []

    DR.drop_pg_slots(link)
    print(f"[准备] 重建两端库并在 A 端播种 {seed_rows} 行 ...")
    t0 = time.time()
    a, b = DR.reset_both(link, seed_rows=seed_rows, seed_side="a")
    print(f"    播种耗时 {time.time()-t0:.0f}s")

    old_quota = F.get_increment_quota()
    if old_quota is not None:
        F.set_increment_quota(100000)
    task_id = DR.create_dr_task(token, f"DR-{link}-{phase}-{int(time.time())}", link, "UNIDIRECTIONAL")
    print(f"[任务] 灾备任务 {task_id}")
    try:
        t_full = time.time()
        if phase in ("full", "both"):
            if L.get("single_process"):
                ok = inject_full_phase_guarded(token, task_id, b, passed, failed,
                                               L["engines"][0], seed_rows=seed_rows)
            else:
                ok = inject_full_phase(token, task_id, b, passed, failed, seed_rows=seed_rows)
            if not ok:
                return F.print_result(passed, failed)
        else:
            st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=1800)
            ok = st == "INCREMENT_RUNNING"
            (passed if ok else failed).append(f"全量完成并进入增量灾备（终态 {st}）")
            if not ok:
                return F.print_result(passed, failed)
        print(f"    全量阶段总耗时 {time.time()-t_full:.0f}s")

        # 全量阶段一致性（此时尚无增量写入）
        time.sleep(5)
        ok, sfp, tfp = DR.wait_converge(a, b, token, task_id, timeout=180)
        (passed if ok else failed).append(f"全量阶段数据一致 src={fmt(sfp)} tgt={fmt(tfp)}")
        print(f"  [全量校验] src={fmt(sfp)} tgt={fmt(tfp)} → {'一致' if ok else '不一致'}")

        if phase in ("incre", "both"):
            w = DR.make_writer(a, float(os.environ.get("FI_WRITE_INTERVAL", "0.04")), "a")
            inject_incre_phase(token, task_id, link, [w], minutes, passed, failed)
            print("[校验] 等待增量追平并逐指纹比对 ...")
            ok, sfp, tfp = DR.wait_converge(a, b, token, task_id, timeout=900)
            print(f"    最终 src={fmt(sfp)} tgt={fmt(tfp)}")
            (passed if ok else failed).append("增量故障注入后断点续传数据一致")
    finally:
        DR.cleanup_task(token, task_id)
        if old_quota is not None:
            F.set_increment_quota(old_quota)
    return F.print_result(passed, failed)


# --------------------------------------------------------------- 双向灾备

def run_bidi(link, minutes, seed_rows):
    L = DR.DR_LINKS[link]
    token = F.login()
    print(f"✓ 登录成功；双向灾备 {link}，两端同时写入 + 双通道故障注入")
    passed, failed = [], []

    DR.drop_pg_slots(link)
    print(f"[准备] 重建两端库并在 A 端播种 {seed_rows} 行 ...")
    a, b = DR.reset_both(link, seed_rows=seed_rows, seed_side="a")

    old_quota = F.get_increment_quota()
    if old_quota is not None:
        F.set_increment_quota(100000)
    task_id = DR.create_dr_task(token, f"DR-{link}-bidi-{int(time.time())}", link, "BIDIRECTIONAL")
    print(f"[任务] 双向灾备主任务 {task_id}")
    try:
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=1800)
        ok = st == "INCREMENT_RUNNING"
        (passed if ok else failed).append(f"正向全量完成并进入增量（终态 {st}）")
        if not ok:
            return F.print_result(passed, failed)

        ok, sfp, tfp = DR.wait_converge(a, b, token, task_id, timeout=180)
        (passed if ok else failed).append(f"全量阶段数据一致 A={fmt(sfp)} B={fmt(tfp)}")
        print(f"  [全量校验] A={fmt(sfp)} B={fmt(tfp)} → {'一致' if ok else '不一致'}")

        # 影子（反向）任务应已自动启动
        sid = DR.shadow_id(token, task_id)
        shadow_ok = False
        for _ in range(40):
            if sid and DR.get_task(token, sid).get("status") in ("INCREMENT_RUNNING", "FULL_COMPLETED"):
                shadow_ok = True
                break
            time.sleep(3)
            sid = sid or DR.shadow_id(token, task_id)
        (passed if shadow_ok else failed).append(
            f"反向影子通道自动启动（{sid}, 状态 {DR.get_task(token, sid).get('status') if sid else 'None'}）")
        print(f"[任务] 反向影子任务 {sid} 状态 {DR.get_task(token, sid).get('status') if sid else 'None'}")

        iv = float(os.environ.get("FI_WRITE_INTERVAL", "0.06"))
        wa, wb = DR.make_writer(a, iv, "a"), DR.make_writer(b, iv, "b")
        inject_incre_phase(token, task_id, link, [wa, wb], minutes, passed, failed,
                           extra_task_ids=[sid] if sid else [])

        print("[校验] 等待两端双向收敛 ...")
        ok, afp, bfp = DR.wait_converge(a, b, token, task_id, timeout=900)
        print(f"    最终 A={fmt(afp)} B={fmt(bfp)}")
        (passed if ok else failed).append("双向灾备故障注入后两端数据收敛一致")

        # 收敛后 30s 内不应继续变化（回环放大会让行数持续增长）
        if ok:
            time.sleep(20)
            a2, b2 = DR.fingerprint(a), DR.fingerprint(b)
            stable = (a2 == afp and b2 == bfp)
            (passed if stable else failed).append(
                f"收敛后 20s 稳定不回环放大 A={fmt(a2)} B={fmt(b2)}")
    finally:
        DR.cleanup_task(token, task_id)
        if old_quota is not None:
            F.set_increment_quota(old_quota)
    return F.print_result(passed, failed)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link", choices=list(DR.DR_LINKS.keys()))
    ap.add_argument("--mode", choices=["uni", "bidi"], default="uni")
    ap.add_argument("--phase", choices=["full", "incre", "both"], default="both")
    ap.add_argument("--minutes", type=float, default=5)
    # Mongo 链路的一致性指纹在 Python 侧算（Mongo 没有 BIT_XOR 之类的聚合指纹），
    # 默认播种量相应调小；SQL 链路仍用库内聚合指纹，可以跑到几十万行。
    ap.add_argument("--seed-rows", type=int, default=None)
    args = ap.parse_args()
    if args.seed_rows is None:
        default_seed = "30000" if DR.is_mongo(args.link) else "200000"
        args.seed_rows = int(os.environ.get("DR_SEED_ROWS", default_seed))
    if args.mode == "bidi":
        rc = run_bidi(args.link, args.minutes, args.seed_rows)
    else:
        rc = run_uni(args.link, args.phase, args.minutes, args.seed_rows)
    sys.exit(rc)


if __name__ == "__main__":
    main()
