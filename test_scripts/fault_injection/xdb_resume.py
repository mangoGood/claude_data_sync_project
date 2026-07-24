#!/usr/bin/env python3
"""
异构链路断点续传 + 故障注入一致性测试：pg2pg / mysql2pg / pg2mysql / mongo2mongo。

全量+增量（默认）：
  1. 源建表/集合 + 播种存量数据。
  2. 创建 fullAndIncre 任务，等到 INCREMENT_RUNNING。
  3. 校验全量阶段一致性（写入前 源==目标 快照指纹）。
  4. 启动后台写入线程，持续 N 分钟，其间每隔一段时间 SIGKILL 一个受 ProcessGuard 守护的
     子进程（SQL: capture/extract/increment 轮流；mongo: 单进程 mongo），验证自动重启+续传。
  5. 停止写入，等待增量追平，逐指纹比对源/目标一致（不丢不重）。

仅全量（--mode full）：
  播种大批量，全量搬运途中杀全量进程；SQL 的 migration-full 无守护→FAILED→retry 续传，
  mongo 单进程受守护→自动重启续传。最终逐指纹一致。

用法：
  python3 xdb_resume.py <pg2pg|mysql2pg|pg2mysql|mongo2mongo> [--minutes 5] [--mode incre|full]
退出码 0 = 全通过。
"""
import argparse
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402
import dblib as D  # noqa: E402


def fmt(fp):
    return f"(count={fp[0]}, xor={fp[1]:#010x})"


def run_incremental(link, minutes):
    L = D.LINKS[link]
    src = D.make_endpoint(L["source"])
    tgt = D.make_endpoint(L["target"])
    token = F.login()
    print(f"✓ 登录成功；链路 {link}（{L['source_type']}→{L['target_type']}），故障注入 {minutes} 分钟")
    passed, failed = [], []

    old_quota = F.get_increment_quota()
    if old_quota is not None:
        F.set_increment_quota(100000)
        print(f"[配额] 增量限速临时 {old_quota} → 100000 行/秒")
    try:
        return _incr_inner(link, L, src, tgt, token, minutes, passed, failed)
    finally:
        if old_quota is not None:
            F.set_increment_quota(old_quota)
            print(f"[配额] 已还原增量限速 → {old_quota}")


def _incr_inner(link, L, src, tgt, token, minutes, passed, failed):
    seed_rows = int(os.environ.get("FI_SEED_ROWS", "20000"))
    print(f"[准备] 源播种 {seed_rows} 行/文档 ...")
    src.reset_source()
    tgt.reset_target()
    src.seed(seed_rows)

    task_id = F.create_task(token, f"FI-{link}-incre-{int(time.time())}",
                            L["source_type"], L["target_type"], L["src_conn"], L["tgt_conn"],
                            "fullAndIncre", L["sync_objects"], L["target"]["db"],
                            source_db=L["source"]["db"])
    print(f"[任务] {task_id}")

    st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=480)
    if st != "INCREMENT_RUNNING":
        failed.append(f"未进入 INCREMENT_RUNNING（当前 {st}）")
        F.stop_task(token, task_id); F.delete_task(token, task_id)
        return F.print_result(passed, failed), task_id
    passed.append("全量完成并进入 INCREMENT_RUNNING")

    # 全量阶段一致性：写入尚未开始，源=存量
    time.sleep(3)
    sfp, tfp = src.fingerprint(), tgt.fingerprint()
    ok = sfp == tfp
    (passed if ok else failed).append(f"全量阶段一致性 src={fmt(sfp)} tgt={fmt(tfp)}")
    print(f"  [全量校验] src={fmt(sfp)} tgt={fmt(tfp)} → {'一致' if ok else '不一致'}")

    write_interval = float(os.environ.get("FI_WRITE_INTERVAL", "0.04"))
    kill_every = float(os.environ.get("FI_KILL_EVERY", "80"))
    print(f"[写入] 后台写入 {minutes} 分钟（间隔 {write_interval}s，每 {kill_every}s 崩溃一个子进程）...")
    writer = src.make_writer(write_interval)
    writer.start()

    engines = L["engines"]
    kills = []
    end = time.time() + minutes * 60
    idx = 0
    next_kill = time.time() + kill_every
    while time.time() < end:
        time.sleep(2)
        if time.time() >= next_kill:
            eng = engines[idx % len(engines)]
            idx += 1
            pids = F.signal_child(task_id, eng, signal.SIGKILL)
            stamp = time.strftime("%H:%M:%S")
            if pids:
                kills.append((eng, pids))
                print(f"    [{stamp}] SIGKILL {eng} pid={pids} "
                      f"(ins={writer.inserts} upd={writer.updates} del={writer.deletes})")
            else:
                print(f"    [{stamp}] {eng} 无进程（可能正在重启）")
            next_kill = time.time() + kill_every
            if F.get_status(token, task_id) == "FAILED":
                failed.append(f"故障注入期间任务被判 FAILED（{eng} 杀后未自愈）")
                break

    writer.stop.set()
    writer.join(timeout=30)
    if writer.error:
        failed.append(f"写入线程异常: {writer.error}")
    print(f"[写入] 结束：ins={writer.inserts} upd={writer.updates} del={writer.deletes}；"
          f"共注入 {len(kills)} 次崩溃")

    st = F.get_status(token, task_id)
    if st not in ("INCREMENT_RUNNING",):
        failed.append(f"故障注入后任务未维持 INCREMENT_RUNNING（当前 {st}）")

    print("[校验] 等待增量追平并逐指纹比对（最多 10 分钟）...")
    ok = False
    sfp = src.fingerprint()
    for _ in range(200):
        time.sleep(3)
        tfp = tgt.fingerprint()
        sfp = src.fingerprint()
        if sfp == tfp:
            ok = True
            break
        if F.get_status(token, task_id) == "FAILED":
            failed.append("追平期间任务被判 FAILED（崩溃自愈失败或看门狗误报）")
            break
    print(f"    最终 src={fmt(sfp)} tgt={fmt(tfp)}")
    (passed if ok else failed).append(
        f"故障注入后断点续传数据一致（{len(kills)} 次崩溃自愈）")

    F.stop_task(token, task_id)
    time.sleep(3)
    F.delete_task(token, task_id)
    return F.print_result(passed, failed), task_id


def run_full(link):
    L = D.LINKS[link]
    src = D.make_endpoint(L["source"])
    tgt = D.make_endpoint(L["target"])
    token = F.login()
    is_mongo = L["source_type"] == "mongodb"
    print(f"✓ 登录成功；链路 {link} 仅全量断点续传（{'单进程受守护' if is_mongo else 'migration-full 无守护+retry'}）")
    passed, failed = [], []

    seed_rows = int(os.environ.get("FI_FULL_ROWS", "200000"))
    print(f"[准备] 源播种 {seed_rows} 行/文档 ...")
    src.reset_source()
    tgt.reset_target()
    src.seed(seed_rows)

    task_id = F.create_task(token, f"FI-{link}-full-{int(time.time())}",
                            L["source_type"], L["target_type"], L["src_conn"], L["tgt_conn"],
                            "full", L["sync_objects"], L["target"]["db"],
                            source_db=L["source"]["db"])
    print(f"[任务] {task_id}")

    full_engine = "mongo" if is_mongo else "full"
    killed = False
    end = time.time() + 240
    while time.time() < end:
        time.sleep(2)
        st = F.get_status(token, task_id)
        if st in ("FULL_COMPLETED", "COMPLETED", "FAILED"):
            break
        cnt = tgt.count()
        if not killed and cnt > 5000:
            pids = F.signal_child(task_id, full_engine, signal.SIGKILL)
            print(f"    全量搬运中（目标已 {cnt} 行），SIGKILL {full_engine} pid={pids}")
            killed = True
    passed.append("全量搬运途中已注入崩溃" if killed else "未能在搬运途中注入（数据太小/太快）")

    st = F.get_status(token, task_id)
    print(f"    杀后状态: {st}")
    if not is_mongo and st == "FAILED":
        print("    任务 FAILED（预期，全量进程无守护）→ retry 恢复续传")
        F.api("POST", f"/api/workflows/{task_id}/retry", token)

    st = F.wait_status(token, task_id, {"FULL_COMPLETED", "COMPLETED"}, timeout=900)
    ok = st in ("FULL_COMPLETED", "COMPLETED")
    (passed if ok else failed).append(f"全量断点续传后完成（终态 {st}）")

    if ok:
        time.sleep(3)
        sfp, tfp = src.fingerprint(), tgt.fingerprint()
        c_ok = sfp == tfp
        (passed if c_ok else failed).append(f"全量续传数据一致 src={fmt(sfp)} tgt={fmt(tfp)}")
        print(f"    最终 src={fmt(sfp)} tgt={fmt(tfp)}")

    F.stop_task(token, task_id)
    time.sleep(2)
    F.delete_task(token, task_id)
    return F.print_result(passed, failed), task_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link", choices=list(D.LINKS.keys()))
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--mode", choices=["incre", "full"], default="incre")
    args = ap.parse_args()
    if args.mode == "full":
        rc, _ = run_full(args.link)
    else:
        rc, _ = run_incremental(args.link, args.minutes)
    sys.exit(rc)


if __name__ == "__main__":
    main()
