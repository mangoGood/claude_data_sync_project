#!/usr/bin/env python3
"""
Redis→Redis 断点续传 / 故障注入测试。

Redis 同步是单进程引擎（migration-redis，受 ProcessGuard 守护）：全量 PSYNC 拉 RDB、
增量转发复制命令流。崩溃后 guard 重启引擎，重新 PSYNC 全量（RESTORE replace 幂等）+ 增量。

覆盖：
  A. 崩溃自愈一致性：增量过程中持续写入源库，SIGKILL 引擎若干次，验证最终目标==源。
  B. 僵死检测：SIGSTOP 引擎（进程存活但冻结），验证监控在阈值内上报 FAILED。

前置：docker compose -f docker-compose-synctask-redis.yml up -d
用法：
  python3 redis_resume.py resume   [--minutes 5]
  python3 redis_resume.py hang     [--wait 150]
"""
import argparse
import os
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PW = "syncredis"
SRC_CONN = f"redis://default:{PW}@127.0.0.1:6390"
TGT_CONN = f"redis://default:{PW}@127.0.0.1:6391"
SRC_C, TGT_C = "synctask-redis-a", "synctask-redis-b"
DB = 0


def rc(container, *args):
    return subprocess.run(
        ["docker", "exec", container, "redis-cli", "-a", PW, "--no-auth-warning", *args],
        capture_output=True, text=True).stdout.strip()


def src(*a):
    return rc(SRC_C, "-n", str(DB), *a)


def tgt(*a):
    return rc(TGT_C, "-n", str(DB), *a)


def keyspace_digest(container):
    """整库 (键→值) 指纹：对所有 string 键的 name+value 排序后 md5。种子/写入均用 string。"""
    script = (
        "local ks=redis.call('KEYS','*'); table.sort(ks); "
        "local out={}; for i=1,#ks do out[#out+1]=ks[i]..'='..(redis.call('GET',ks[i]) or ''); end; "
        "return table.concat(out,'\\n')"
    )
    dump = rc(container, "-n", str(DB), "EVAL", script, "0")
    import hashlib
    return rc(container, "-n", str(DB), "DBSIZE"), hashlib.md5(dump.encode()).hexdigest()


def seed(n):
    rc(SRC_C, "FLUSHALL")
    rc(TGT_C, "FLUSHALL")
    # 批量种子：pipeline 写入 n 个 string 键
    cmds = "\n".join(f"SET seed:{i} v{i}" for i in range(n)) + "\n"
    subprocess.run(["docker", "exec", "-i", SRC_C, "redis-cli", "-a", PW, "--no-auth-warning", "-n", str(DB)],
                   input=cmds, capture_output=True, text=True)


class RedisWriter(threading.Thread):
    def __init__(self, interval=0.01):
        super().__init__(daemon=True)
        self.interval = interval
        self.stop = threading.Event()
        self.sets = 0
        self.dels = 0

    def run(self):
        seq = 0
        # 用一个常驻管道进程持续写，降低 docker exec 开销
        while not self.stop.is_set():
            seq += 1
            src("SET", f"live:{seq}", f"val-{seq}")
            self.sets += 1
            if seq % 7 == 0:
                src("SET", f"live:{seq - 6}", f"upd-{seq}")  # 覆盖更新
            if seq % 13 == 0:
                src("DEL", f"live:{seq - 11}")
                self.dels += 1
            time.sleep(self.interval)


def create_redis_task(token, name, mode):
    return F.create_task(token, name, "redis", "redis", SRC_CONN, TGT_CONN,
                         mode, '{"db":{"dbLevel":true}}', "")


def run_resume(minutes):
    token = F.login()
    print(f"✓ 登录；Redis→Redis 崩溃自愈一致性，故障注入 {minutes} 分钟")
    passed, failed = [], []

    print("[准备] 源播种 8000 个键 ...")
    seed(8000)
    tid = create_redis_task(token, f"FIR-resume-{int(time.time())}", "fullAndIncre")
    print(f"[任务] {tid}")
    st = F.wait_status(token, tid, {"INCREMENT_RUNNING"}, timeout=300)
    if st != "INCREMENT_RUNNING":
        failed.append(f"未进入 INCREMENT_RUNNING（{st}）")
        F.stop_task(token, tid)
        return F.print_result(passed, failed)
    passed.append("全量完成并进入 INCREMENT_RUNNING")

    w = RedisWriter(interval=0.008)
    w.start()
    kills = []
    end = time.time() + minutes * 60
    next_kill = time.time() + 90
    while time.time() < end:
        time.sleep(2)
        if time.time() >= next_kill:
            pids = F.signal_child(tid, "redis", signal.SIGKILL)
            if pids:
                kills.append(pids)
                print(f"    [{time.strftime('%H:%M:%S')}] SIGKILL redis 引擎 pid={pids} (sets={w.sets})")
            next_kill = time.time() + 90
            if F.get_status(token, tid) == "FAILED":
                failed.append("故障注入期间任务 FAILED（自愈失败）")
                break
    w.stop.set()
    w.join(timeout=15)
    print(f"[写入] 结束 sets={w.sets} dels={w.dels}；注入 {len(kills)} 次崩溃")

    print("[校验] 等待增量追平并整库指纹比对 ...")
    ok = False
    for _ in range(80):
        time.sleep(3)
        s = keyspace_digest(SRC_C)
        t = keyspace_digest(TGT_C)
        if s == t:
            ok = True
            break
        if F.get_status(token, tid) == "FAILED":
            failed.append("追平期间任务 FAILED")
            break
    print(f"    最终 src={s} tgt={t}")
    (passed if ok else failed).append(f"崩溃自愈后整库一致（{len(kills)} 次崩溃）")

    F.stop_task(token, tid)
    time.sleep(2)
    F.delete_task(token, tid)
    return F.print_result(passed, failed)


def run_hang(wait_s):
    token = F.login()
    print(f"✓ 登录；Redis 引擎僵死检测，观察 {wait_s}s")
    passed, failed = [], []
    seed(3000)
    tid = create_redis_task(token, f"FIR-hang-{int(time.time())}", "fullAndIncre")
    print(f"[任务] {tid}")
    st = F.wait_status(token, tid, {"INCREMENT_RUNNING"}, timeout=300)
    if st != "INCREMENT_RUNNING":
        failed.append(f"未进入 INCREMENT_RUNNING（{st}）")
        F.stop_task(token, tid)
        return F.print_result(passed, failed)

    w = RedisWriter(interval=0.02)
    w.start()
    time.sleep(6)
    pids = F.signal_child(tid, "redis", signal.SIGSTOP)
    print(f"  [{time.strftime('%H:%M:%S')}] SIGSTOP redis 引擎 pid={pids}（冻结，进程仍存活）")
    detected = False
    deadline = time.time() + wait_s
    while time.time() < deadline:
        time.sleep(5)
        if F.get_status(token, tid) == "FAILED":
            detected = True
            break
    w.stop.set()
    for pid in pids:
        try:
            os.kill(pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
    (passed if detected else failed).append(f"监控{'已' if detected else '未'}在 {wait_s}s 内发现 redis 引擎僵死并上报 FAILED")
    F.stop_task(token, tid)
    time.sleep(2)
    F.delete_task(token, tid)
    return F.print_result(passed, failed)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["resume", "hang"])
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--wait", type=int, default=150)
    args = ap.parse_args()
    rc_ = run_resume(args.minutes) if args.mode == "resume" else run_hang(args.wait)
    sys.exit(rc_)


if __name__ == "__main__":
    main()
