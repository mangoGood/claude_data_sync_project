#!/usr/bin/env python3
"""
线程僵死检测测试：把一个受守护的子进程 SIGSTOP（进程仍 alive 但完全冻结、不再推进），
验证监控能否发现“假活”并把任务上报为 FAILED。

SIGKILL（崩溃）会让 process.isAlive()=false，ProcessGuard 能发现并重启；
SIGSTOP（僵死）下 isAlive() 仍为 true，只看存活的健康检查会永远认为一切正常——
这正是要考验的盲区。

用法：
  python3 hang_detect.py [capture|extract|increment]   # 默认 increment
  FI_HANG_WAIT=180 python3 hang_detect.py               # 冻结后观察时长（秒）
退出码 0 = 监控在观察窗口内把任务判为 FAILED（能发现僵死）。
"""
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402
from sql_resume import LINKS, TABLE, seed, drop_db, Writer  # noqa: E402


def main():
    engine = sys.argv[1] if len(sys.argv) > 1 else "increment"
    wait_s = int(os.environ.get("FI_HANG_WAIT", "180"))
    L = LINKS["mysql"]
    token = F.login()
    print(f"✓ 登录；将冻结(SIGSTOP)受守护子进程 {engine}，观察 {wait_s}s 内监控是否上报 FAILED")

    old = F.get_increment_quota()
    F.set_increment_quota(100000)
    stopped_pids = []
    try:
        seed(L["src_cfg"], L["src_db"], 5000)
        drop_db(L["tgt_cfg"], L["tgt_db"])
        so = f'{{"{L["src_db"]}": {{"tables": ["{TABLE}"]}}}}'
        tid = F.create_task(token, f"HANG-{engine}-{int(time.time())}", "mysql", "mysql",
                            L["src_conn"], L["tgt_conn"], "fullAndIncre", so, L["tgt_db"],
                            source_db=L["src_db"])
        print(f"[任务] {tid}")
        st = F.wait_status(token, tid, {"INCREMENT_RUNNING"}, timeout=300)
        if st != "INCREMENT_RUNNING":
            print(f"未进入 INCREMENT_RUNNING（{st}），中止")
            F.stop_task(token, tid)
            return 2

        # 持续写入，保证被冻结的进程本应有活干（僵死才有可观测后果）
        w = Writer(L["src_cfg"], L["src_db"], interval=0.02)
        w.start()
        time.sleep(8)

        stopped_pids = F.signal_child(tid, engine, signal.SIGSTOP)
        if not stopped_pids:
            print(f"未找到 {engine} 子进程，无法注入僵死")
            w.stop.set()
            F.stop_task(token, tid)
            return 2
        print(f"  [{time.strftime('%H:%M:%S')}] SIGSTOP {engine} pid={stopped_pids}（已冻结，进程仍存活）")

        # 观察任务状态
        detected = False
        deadline = time.time() + wait_s
        last = None
        while time.time() < deadline:
            time.sleep(5)
            stt = F.get_status(token, tid)
            alive = _alive(stopped_pids)
            if stt != last:
                print(f"  +{int(time.time() - (deadline - wait_s))}s status={stt} frozenAlive={alive}")
                last = stt
            if stt == "FAILED":
                detected = True
                break

        w.stop.set()
        w.join(timeout=10)

        # 解冻并清理
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
                os.kill(pid, signal.SIGCONT)  # 兜底解冻，避免残留僵死进程
            except ProcessLookupError:
                pass
        if old is not None:
            F.set_increment_quota(old)


def _alive(pids):
    import subprocess
    out = subprocess.run(["ps", "-o", "pid=", "-p", ",".join(map(str, pids))],
                         capture_output=True, text=True).stdout
    return bool(out.strip())


if __name__ == "__main__":
    sys.exit(main())
