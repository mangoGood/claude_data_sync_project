#!/usr/bin/env python3
"""
灾备任务的线程僵死检测：把灾备通道的一个受守护子进程 SIGSTOP（进程仍 alive 但完全冻结、
不推进），验证监控进程能发现「假活」并把灾备任务上报为 FAILED。

单向灾备冻结正向通道；双向灾备可用 --shadow 冻结反向影子通道，验证影子失败会透传到主任务。

用法：
  python3 dr_hang.py <mysql2mysql|pg2pg> [engine] [--bidi] [--shadow]
    engine 默认 increment，可选 capture/extract/increment
  FI_HANG_WAIT=220 控制冻结后观察时长（秒；需 > monitor.stall.threshold.ms=90s）
退出码 0 = 监控在观察窗口内把任务判为 FAILED。
"""
import argparse
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drlib as DR  # noqa: E402
import faultlib as F  # noqa: E402


def _alive(pids):
    if not pids:
        return False
    out = subprocess.run(["ps", "-o", "pid=", "-p", ",".join(map(str, pids))],
                         capture_output=True, text=True).stdout
    return bool(out.strip())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link", choices=list(DR.DR_LINKS.keys()))
    ap.add_argument("engine", nargs="?", default="increment",
                    choices=["capture", "extract", "increment"])
    ap.add_argument("--bidi", action="store_true", help="双向灾备")
    ap.add_argument("--shadow", action="store_true", help="冻结反向影子通道的子进程（隐含 --bidi）")
    args = ap.parse_args()
    bidi = args.bidi or args.shadow
    wait_s = int(os.environ.get("FI_HANG_WAIT", "220"))

    token = F.login()
    mode = "双向" if bidi else "单向"
    chan = "反向影子" if args.shadow else "正向"
    print(f"✓ 登录；{mode}灾备 {args.link}，冻结{chan}通道的 {args.engine}，观察 {wait_s}s 内是否上报 FAILED")

    DR.drop_pg_slots(args.link)
    old = F.get_increment_quota()
    if old is not None:
        F.set_increment_quota(100000)
    stopped_pids = []
    tid = None
    try:
        a, b = DR.reset_both(args.link, seed_rows=5000, seed_side="a")
        tid = DR.create_dr_task(token, f"DRHANG-{args.link}-{args.engine}-{int(time.time())}",
                                args.link, "BIDIRECTIONAL" if bidi else "UNIDIRECTIONAL")
        print(f"[任务] {tid}")
        st = F.wait_status(token, tid, {"INCREMENT_RUNNING"}, timeout=600)
        if st != "INCREMENT_RUNNING":
            print(f"未进入 INCREMENT_RUNNING（{st}），中止")
            return 2

        target_tid = tid
        if args.shadow:
            sid = None
            for _ in range(40):
                sid = DR.shadow_id(token, tid)
                if sid and DR.get_task(token, sid).get("status") == "INCREMENT_RUNNING":
                    break
                time.sleep(3)
            if not sid:
                print("反向影子任务未启动，中止")
                return 2
            target_tid = sid
            print(f"[影子任务] {sid}")

        # 持续写入，保证被冻结进程本应有活干（僵死才有可观测后果）
        w = DR.make_writer(b if args.shadow else a, 0.02, "b" if args.shadow else "a")
        w.start()
        time.sleep(10)

        stopped_pids = F.signal_child(target_tid, args.engine, signal.SIGSTOP)
        if not stopped_pids:
            print(f"未找到 {args.engine} 子进程，无法注入僵死")
            w.stop.set()
            return 2
        print(f"  [{time.strftime('%H:%M:%S')}] SIGSTOP {args.engine} pid={stopped_pids}（已冻结，进程仍存活）")

        detected = False
        t0 = time.time()
        last = None
        while time.time() < t0 + wait_s:
            time.sleep(5)
            stt = F.get_status(token, target_tid)
            main_stt = F.get_status(token, tid) if args.shadow else stt
            if (stt, main_stt) != last:
                print(f"  +{int(time.time()-t0)}s status={stt}"
                      f"{f' 主任务={main_stt}' if args.shadow else ''} frozenAlive={_alive(stopped_pids)}")
                last = (stt, main_stt)
            if stt == "FAILED":
                detected = True
                break

        w.stop.set(); w.join(timeout=10)
        if detected and args.shadow:
            err = DR.get_task(token, tid).get("error_message") or ""
            print(f"  主任务 errorMessage: {err[:160]}")
        print(f"\n结论：监控{'已' if detected else '未'}在 {wait_s}s 内发现 {args.engine} 僵死并上报 FAILED")
        return 0 if detected else 1
    finally:
        for pid in stopped_pids:
            try:
                os.kill(pid, signal.SIGCONT)  # 兜底解冻，否则残留冻结进程
            except ProcessLookupError:
                pass
        if tid:
            DR.cleanup_task(token, tid)
        if old is not None:
            F.set_increment_quota(old)


if __name__ == "__main__":
    sys.exit(main())
