#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 6 验收：受限堆下的大字段<b>增量</b>同步端到端（capture → extract → increment）。

三个进程各自 -Xmx144m 独立运行，覆盖：
  1) INSERT 一行大 LONGBLOB → 目标端逐字节一致（MD5 服务端比对）
  2) UPDATE 大字段 → 目标端跟着变
  3) UPDATE 非大字段列 → 大字段内容不受影响（前镜像丢弃不能误伤后镜像）
  4) DELETE → 目标端行消失
  5) 三个进程峰值 RSS 都 < 256MB
  6) 位点推进后落盘文件被回收（不留 GB 级垃圾）

用法：
  python3 test_scripts/lob/lob_incr_e2e.py               # 默认 32MB（快）
  python3 test_scripts/lob/lob_incr_e2e.py --size 1G     # 题设规模
"""
import argparse
import glob
import os
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loblib as L  # noqa: E402

TASK = "lob-incr-e2e"
SRC_DB, TGT_DB, TABLE = "lob_incr_src", "lob_incr_tgt", "t_lob"
TASK_DIR = os.path.join(L.PROJECT_ROOT, "files", TASK)
CAP_DIR = os.path.join(TASK_DIR, "capture_out")
THL_DIR = os.path.join(TASK_DIR, "thl_output")
LOB_DIR = os.path.join(TASK_DIR, "lob")

JARS = {
    "capture": os.path.join(L.PROJECT_ROOT, "migration-capture", "target", "migration-capture-1.0.0.jar"),
    "extract": os.path.join(L.PROJECT_ROOT, "migration-extract", "target", "migration-extract-1.0.0.jar"),
    "increment": os.path.join(L.PROJECT_ROOT, "migration-increment", "target", "migration-increment-1.0.0.jar"),
}
MAINS = {
    "capture": "com.migration.capture.CaptureMain",
    "extract": "com.migration.extract.ContinuousExtractMain",
    "increment": "com.migration.increment.ContinuousIncrementMain",
}
JVM = ["-Xmx144m", "-XX:MaxMetaspaceSize=64m", "-XX:MaxDirectMemorySize=24m",
       "-XX:+ExitOnOutOfMemoryError"]

results = []
procs = {}
peaks = {}
stop_flag = [False]


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def parse_size(s):
    s = s.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    return int(float(s[:-1]) * mult[s[-1]]) if s[-1] in mult else int(s)


def write_config():
    os.makedirs(TASK_DIR, exist_ok=True)
    src_url = f"jdbc:mysql://localhost:{L.SRC_PORT}/?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    tgt_url = f"jdbc:mysql://localhost:{L.TGT_PORT}/{TGT_DB}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    cfg = f"""task.id={TASK}
source.db.type=mysql
source.db.host=localhost
source.db.port={L.SRC_PORT}
source.db.username=root
source.db.password={L.PWD}
source.db.database={SRC_DB}
source.db.jdbc.url={src_url}
target.db.type=mysql
target.db.host=localhost
target.db.port={L.TGT_PORT}
target.db.username=root
target.db.password={L.PWD}
target.db.database={TGT_DB}
target.db.jdbc.url={tgt_url}
capture.output.dir={CAP_DIR}
capture.server.id=99321
capture.gtid.enabled=false
capture.max.events.per.file=100000
capture.position.health.enabled=false
extract.input.dir={CAP_DIR}
extract.output.dir={THL_DIR}
extract.scan.interval=1000
increment.thl.dir={THL_DIR}
increment.scan.interval=1000
migration.included.databases={SRC_DB}
migration.included.tables={SRC_DB}.{TABLE}
migration.sync.objects={{"{SRC_DB}":{{"tables":["{TABLE}"],"targetDb":"{TGT_DB}"}}}}
schema.mapping.db.{SRC_DB}={TGT_DB}
migration.lob.spill.enabled=true
migration.lob.spill.threshold.bytes=1048576
migration.lob.spill.dir={LOB_DIR}
"""
    with open(os.path.join(TASK_DIR, "config.properties"), "w") as f:
        f.write(cfg)


logs_buf = {}


def start(name):
    proc = subprocess.Popen(
        ["java"] + JVM + ["-Dtask.id=" + TASK, "-cp", JARS[name], MAINS[name],
                          "--config", os.path.join(TASK_DIR, "config.properties")],
        cwd=L.PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    procs[name] = proc
    peaks[name] = [0]
    logs_buf[name] = []

    # 必须<b>持续</b>把子进程 stdout 读走。管道缓冲只有 64KB，写满之后子进程会阻塞在
    # 写日志上——表现为"进程还在、CPU 也不高、就是不干活"，极难归因。
    def drain():
        for line in proc.stdout:
            logs_buf[name].append(line)
            if len(logs_buf[name]) > 4000:
                del logs_buf[name][:2000]

    threading.Thread(target=drain, daemon=True).start()

    def sample():
        while not stop_flag[0] and proc.poll() is None:
            try:
                out = subprocess.run(["ps", "-o", "rss=", "-p", str(proc.pid)],
                                     capture_output=True, text=True, timeout=5).stdout.strip()
                if out:
                    peaks[name][0] = max(peaks[name][0], int(out))
            except Exception:
                pass
            time.sleep(0.3)

    threading.Thread(target=sample, daemon=True).start()
    return proc


def stop_all():
    stop_flag[0] = True
    for proc in procs.values():
        proc.terminate()
    time.sleep(3)
    for proc in procs.values():
        if proc.poll() is None:
            proc.kill()
    time.sleep(1)
    return {name: "".join(lines) for name, lines in logs_buf.items()}


def wait_until(fn, timeout, desc):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        for name, proc in procs.items():
            if proc.poll() is not None:
                print(f"    !! {name} 进程已退出 rc={proc.returncode}")
                return False
        time.sleep(2)
    print(f"    !! 等待超时: {desc}")
    return False


def tgt_md5():
    return L.scalar(L.TGT_CT, f"SELECT MD5(c_blob) FROM {TABLE} WHERE id=1", TGT_DB)


def src_md5():
    return L.scalar(L.SRC_CT, f"SELECT MD5(c_blob) FROM {TABLE} WHERE id=1", SRC_DB)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="32M")
    args = ap.parse_args()
    blob_bytes = parse_size(args.size)

    for name, jar in JARS.items():
        L.require_jar(jar, f"mvn -pl migration-{name} -am package -DskipTests")

    print("=" * 72)
    print(f"步骤6 增量 E2E：单值 {L.human(blob_bytes)}，三进程各 -Xmx144m")
    print("=" * 72)

    L.tune_server(L.SRC_CT)
    L.tune_server(L.TGT_CT)
    shutil.rmtree(TASK_DIR, ignore_errors=True)
    os.makedirs(CAP_DIR, exist_ok=True)
    os.makedirs(THL_DIR, exist_ok=True)

    L.reset_db(L.SRC_CT, SRC_DB)
    L.create_table(L.SRC_CT, SRC_DB, TABLE)
    L.reset_db(L.TGT_CT, TGT_DB)
    L.create_table(L.TGT_CT, TGT_DB, TABLE)
    write_config()

    print("[1/6] 启动 capture / extract / increment …")
    start("capture")
    time.sleep(6)
    start("extract")
    start("increment")
    time.sleep(6)

    try:
        print(f"[2/6] 源库 INSERT 一行 {L.human(blob_bytes)} …")
        L.seed_rows(L.SRC_CT, SRC_DB, TABLE, 1, blob_bytes, progress=False)
        want = src_md5()
        ok = wait_until(lambda: tgt_md5() == want, 1800, "INSERT 同步到目标")
        record("INSERT 大字段同步一致", ok, f"src={want} tgt={tgt_md5()}")

        print("[3/6] UPDATE 大字段 …")
        L.mysql(L.SRC_CT, f"UPDATE {TABLE} SET c_blob = CONCAT(c_blob, REPEAT('Z', 1048576)) WHERE id=1;", db=SRC_DB)
        want2 = src_md5()
        ok2 = wait_until(lambda: tgt_md5() == want2, 1800, "UPDATE 大字段同步")
        record("UPDATE 大字段同步一致", ok2, f"src={want2} tgt={tgt_md5()}")

        print("[4/6] UPDATE 非大字段列（大字段内容不能被误伤）…")
        L.mysql(L.SRC_CT, f"UPDATE {TABLE} SET c_vc='changed-only-vc' WHERE id=1;", db=SRC_DB)
        ok3 = wait_until(
            lambda: L.scalar(L.TGT_CT, f"SELECT c_vc FROM {TABLE} WHERE id=1", TGT_DB) == "changed-only-vc",
            600, "非大字段列同步")
        record("改非大字段列后大字段内容不变", ok3 and tgt_md5() == want2,
               f"tgt={tgt_md5()} 期望={want2}")

        print("[5/6] DELETE …")
        L.mysql(L.SRC_CT, f"DELETE FROM {TABLE} WHERE id=1;", db=SRC_DB)
        ok4 = wait_until(lambda: L.scalar(L.TGT_CT, f"SELECT COUNT(*) FROM {TABLE}", TGT_DB) == "0",
                         600, "DELETE 同步")
        record("DELETE 同步", ok4)

        print("[6/6] 落盘文件回收 …")
        time.sleep(15)
        left = glob.glob(os.path.join(LOB_DIR, "*.lob"))
        total_left = sum(os.path.getsize(f) for f in left)
        # 位点推进后旧的落盘文件应被回收；最新一批可能还没过位点，留一点是正常的
        record("落盘文件已按位点回收（未堆积）", total_left <= blob_bytes * 2,
               f"残留 {len(left)} 个 / {L.human(total_left)}")
    finally:
        logs = stop_all()

    for name in ("capture", "extract", "increment"):
        mb = peaks[name][0] / 1024
        record(f"{name} 峰值 RSS < 256MB", mb < 256, f"{mb:.0f}MB")
    for name, log in logs.items():
        if "OutOfMemoryError" in log:
            record(f"{name} 无 OOM", False, "日志里有 OutOfMemoryError")
    if all("OutOfMemoryError" not in log for log in logs.values()):
        record("三个进程都没有 OOM", True)

    print("=" * 72)
    ok_all = all(r[1] for r in results)
    print(f"  步骤6 {'通过' if ok_all else '未通过'}：{sum(1 for r in results if r[1])}/{len(results)}")
    if not ok_all:
        for name, log in logs.items():
            tail = [l for l in log.splitlines() if "ERROR" in l or "Exception" in l][-6:]
            if tail:
                print(f"  ---- {name} 错误尾部 ----")
                for line in tail:
                    print("   " + line[:180])
    print("=" * 72)
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
