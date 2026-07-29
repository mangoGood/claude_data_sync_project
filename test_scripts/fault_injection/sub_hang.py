#!/usr/bin/env python3
"""
订阅任务线程僵死检测：SIGSTOP 冻结某个子进程（进程仍 alive、但一步不走），
验证监控进程能发现这种"假活"并把任务上报为 FAILED。

崩溃（SIGKILL）看 isAlive 就能发现，僵死不行——冻结后进程状态仍是 alive，
只有靠"活性文件是否还在刷新"才能识别。本用例逐个冻结 capture / extract / subscribe。

用法：
  python3 sub_hang.py mysql subscribe
  python3 sub_hang.py pg     capture
  python3 sub_hang.py oracle extract
"""
import argparse
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sublib as S  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=list(S.SOURCES.keys()))
    # mongo 订阅是单进程引擎，唯一可冻结的就是 mongo 本身
    ap.add_argument("engine", nargs="?", default=None,
                    choices=["capture", "extract", "subscribe", "mongo"])
    ap.add_argument("--seed-rows", type=int, default=2000)
    ap.add_argument("--rate", type=int, default=20)
    ap.add_argument("--wait", type=int, default=420, help="等待上报 FAILED 的秒数")
    args = ap.parse_args()
    engines = S.SUB_ENGINES[args.kind]
    if args.engine is None:
        args.engine = engines[0]
    if args.engine not in engines:
        print(f"源 {args.kind} 的订阅链路没有 {args.engine} 进程，可选: {engines}")
        return 2

    src = S.SOURCES[args.kind]()
    prefix = f"fihang{args.kind}"
    passed, failed = [], []

    token = S.login()
    print(f"✓ 登录；订阅僵死检测 源={args.kind} 冻结={args.engine}")

    S.delete_topics(prefix)
    src.reset()
    src.seed(args.seed_rows)

    tid = S.create_subscribe_task(token, f"FI-subhang-{args.kind}-{int(time.time())}",
                                  src.source_type, src.src_conn_str(),
                                  src.sync_objects(), src.db, prefix)
    print(f"[任务] {tid}")
    st = S.wait_status(token, tid, {"SUBSCRIBE_RUNNING"}, timeout=420)
    if st != "SUBSCRIBE_RUNNING":
        failed.append(f"未进入 SUBSCRIBE_RUNNING（{st}）")
        S.stop_task(token, tid)
        return S.print_result(passed, failed)

    time.sleep(20)
    w = src.make_writer(start_id=10_000_000, rate=args.rate)
    w._start_id = 10_000_000
    w.start()
    time.sleep(30)

    pids = S.signal_child(tid, args.engine, signal.SIGSTOP)
    if not pids:
        failed.append(f"找不到 {args.engine} 进程，无法冻结")
        w.stop.set()
        S.stop_task(token, tid)
        return S.print_result(passed, failed)
    print(f"[注入] SIGSTOP 冻结 {args.engine} pid={pids}，等待监控上报 FAILED（最多 {args.wait}s）...")

    t0 = time.time()
    detected = False
    try:
        while time.time() - t0 < args.wait:
            time.sleep(5)
            st = S.get_status(token, tid)
            if st == "FAILED":
                detected = True
                break
        elapsed = int(time.time() - t0)
        task = S.get_task(token, tid)
        if detected:
            # 后端任务对象是 snake_case（error_code/error_message）
            code = task.get("error_code") or task.get("errorCode")
            msg = task.get("error_message") or task.get("errorMessage")
            print(f"[结果] {elapsed}s 后上报 FAILED：code={code} msg={msg}")
            ok_code = code == "E3005"
            (passed if ok_code else failed).append(
                f"冻结 {args.engine} 后 {elapsed}s 检出僵死并上报 FAILED（错误码 {code}，期望 E3005）")
        else:
            print(f"[结果] {elapsed}s 内未上报失败，当前状态={st}（僵死未被发现）")
            failed.append(f"冻结 {args.engine} 后 {args.wait}s 未检出僵死（状态仍 {st}）")
    finally:
        # 解冻，避免留下僵尸进程
        for p in pids:
            try:
                os.kill(p, signal.SIGCONT)
            except ProcessLookupError:
                pass
        w.stop.set()
        w.join(timeout=30)
        S.stop_task(token, tid)
        time.sleep(3)
        S.delete_task(token, tid)

    return S.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
