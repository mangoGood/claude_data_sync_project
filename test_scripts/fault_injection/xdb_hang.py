#!/usr/bin/env python3
"""
线程僵死检测：把一个受守护子进程 SIGSTOP（进程仍 alive 但完全冻结、不推进），验证监控能
发现「假活」并把任务上报为 FAILED。SIGKILL 会让 isAlive()=false（易检出），SIGSTOP 下
isAlive() 仍 true——只看存活的健康检查是盲区，这正是要考验的。

用法：
  python3 xdb_hang.py <pg2pg|mysql2pg|pg2mysql|mongo2mongo> [engine]
    engine 默认：SQL 链路=increment，mongo 链路=mongo
  FI_HANG_WAIT=200 控制冻结后观察时长（秒；需 > monitor.stall.threshold.ms=90s）
退出码 0 = 监控在观察窗口内把任务判为 FAILED（能发现僵死）。
"""
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402
import dblib as D  # noqa: E402


def _alive(pids):
    if not pids:
        return False
    out = subprocess.run(["ps", "-o", "pid=", "-p", ",".join(map(str, pids))],
                         capture_output=True, text=True).stdout
    return bool(out.strip())


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in D.LINKS:
        print("用法: xdb_hang.py <pg2pg|mysql2pg|pg2mysql|mongo2mongo> [engine]")
        return 2
    link = sys.argv[1]
    L = D.LINKS[link]
    default_engine = "mongo" if L["source_type"] == "mongodb" else "increment"
    engine = sys.argv[2] if len(sys.argv) > 2 else default_engine
    wait_s = int(os.environ.get("FI_HANG_WAIT", "200"))

    src = D.make_endpoint(L["source"])
    tgt = D.make_endpoint(L["target"])
    token = F.login()
    print(f"✓ 登录；链路 {link}，将冻结(SIGSTOP)受守护子进程 {engine}，观察 {wait_s}s 内是否上报 FAILED")

    old = F.get_increment_quota()
    if old is not None:
        F.set_increment_quota(100000)
    stopped_pids = []
    try:
        src.reset_source(); tgt.reset_target(); src.seed(5000)
        tid = F.create_task(token, f"HANG-{link}-{engine}-{int(time.time())}",
                            L["source_type"], L["target_type"], L["src_conn"], L["tgt_conn"],
                            "fullAndIncre", L["sync_objects"], L["target"]["db"],
                            source_db=L["source"]["db"])
        print(f"[任务] {tid}")
        st = F.wait_status(token, tid, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            print(f"未进入 INCREMENT_RUNNING（{st}），中止")
            F.stop_task(token, tid); F.delete_task(token, tid)
            return 2

        # 持续写入，保证被冻结进程本应有活干（僵死才有可观测后果）
        w = src.make_writer(0.02)
        w.start()
        time.sleep(8)

        stopped_pids = F.signal_child(tid, engine, signal.SIGSTOP)
        if not stopped_pids:
            print(f"未找到 {engine} 子进程，无法注入僵死")
            w.stop.set(); F.stop_task(token, tid); F.delete_task(token, tid)
            return 2
        print(f"  [{time.strftime('%H:%M:%S')}] SIGSTOP {engine} pid={stopped_pids}（已冻结，进程仍存活）")

        detected = False
        t0 = time.time()
        deadline = t0 + wait_s
        last = None
        while time.time() < deadline:
            time.sleep(5)
            stt = F.get_status(token, tid)
            if stt != last:
                print(f"  +{int(time.time()-t0)}s status={stt} frozenAlive={_alive(stopped_pids)}")
                last = stt
            if stt == "FAILED":
                detected = True
                break

        w.stop.set(); w.join(timeout=10)
        for pid in stopped_pids:
            try:
                os.kill(pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        print(f"\n结论：监控{'已' if detected else '未'}在 {wait_s}s 内发现 {engine} 僵死并上报 FAILED")
        F.stop_task(token, tid)
        time.sleep(2)
        F.delete_task(token, tid)
        return 0 if detected else 1
    finally:
        for pid in stopped_pids:
            try:
                os.kill(pid, signal.SIGCONT)  # 兜底解冻
            except ProcessLookupError:
                pass
        if old is not None:
            F.set_increment_quota(old)


if __name__ == "__main__":
    sys.exit(main())
