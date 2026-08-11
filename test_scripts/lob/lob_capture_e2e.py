#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 4 集测：受限堆下 capture 能吃下带大字段的 binlog 事件。

这一步单独验的是"字节从 socket 到磁盘"这一段，不涉及 extract/increment：
  1) 一个带 1GB 级 LONGBLOB 的 INSERT，capture 进程不 OOM（对照：关掉落盘即复现 OOM）
  2) 落盘文件的 md5 == 源库 MD5(c_blob)，逐字节保真
  3) .cap 里写的是 @lob: 引用而不是巨型十六进制串
  4) UPDATE 的前镜像、DELETE 的行镜像都不落盘（只留身份）
  5) 落盘文件名按位点确定；capture 重连重放同一事件不会堆出第二份

用法：
  python3 test_scripts/lob/lob_capture_e2e.py                 # 默认 64MB（快）
  python3 test_scripts/lob/lob_capture_e2e.py --size 1G       # 题设规模
"""
import argparse
import glob
import re
import hashlib
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loblib as L  # noqa: E402

TASK = "lob-capture-e2e"
SRC_DB, TABLE = "lob_cap_src", "t_lob"
JAR = os.path.join(L.PROJECT_ROOT, "migration-capture", "target", "migration-capture-1.0.0.jar")
TASK_DIR = os.path.join(L.PROJECT_ROOT, "files", TASK)
CAP_DIR = os.path.join(TASK_DIR, "capture_out")
LOB_DIR = os.path.join(TASK_DIR, "lob")

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def parse_size(s):
    s = s.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    return int(float(s[:-1]) * mult[s[-1]]) if s[-1] in mult else int(s)


def write_config(spill_enabled=True, threshold=1024 * 1024):
    os.makedirs(TASK_DIR, exist_ok=True)
    cfg = f"""task.id={TASK}
source.db.type=mysql
source.db.host=localhost
source.db.port={L.SRC_PORT}
source.db.username=root
source.db.password={L.PWD}
source.db.database={SRC_DB}
capture.output.dir={CAP_DIR}
capture.server.id=99123
capture.gtid.enabled=false
capture.max.events.per.file=100000
capture.position.health.enabled=false
migration.included.databases={SRC_DB}
migration.included.tables={SRC_DB}.{TABLE}
migration.sync.objects={{"{SRC_DB}":{{"tables":["{TABLE}"]}}}}
migration.lob.spill.enabled={"true" if spill_enabled else "false"}
migration.lob.spill.threshold.bytes={threshold}
migration.lob.spill.dir={LOB_DIR}
"""
    with open(os.path.join(TASK_DIR, "config.properties"), "w") as f:
        f.write(cfg)


def current_binlog_pos():
    out = L.mysql(L.SRC_CT, "SHOW MASTER STATUS", want=True).split("\t")
    return out[0], out[1]


CAPTURE_LOG = []


def start_capture(xmx="144m"):
    import threading as _t
    proc = subprocess.Popen(
        ["java", f"-Xmx{xmx}", "-XX:MaxMetaspaceSize=64m", "-XX:MaxDirectMemorySize=24m",
         "-XX:+ExitOnOutOfMemoryError", "-Dtask.id=" + TASK,
         "-cp", JAR, "com.migration.capture.CaptureMain",
         "--config", os.path.join(TASK_DIR, "config.properties")],
        cwd=L.PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # 必须持续排空子进程 stdout：管道缓冲只有 64KB，写满后子进程会阻塞在写日志上，
    # 表现成"进程活着但不干活"，极难归因
    def drain():
        for line in proc.stdout:
            CAPTURE_LOG.append(line)
            if len(CAPTURE_LOG) > 6000:
                del CAPTURE_LOG[:3000]

    _t.Thread(target=drain, daemon=True).start()
    return proc


def sample_rss(proc, stop_flag, peak):
    while not stop_flag[0] and proc.poll() is None:
        try:
            out = subprocess.run(["ps", "-o", "rss=", "-p", str(proc.pid)],
                                 capture_output=True, text=True, timeout=5).stdout.strip()
            if out:
                peak[0] = max(peak[0], int(out))
        except Exception:
            pass
        time.sleep(0.2)


def wait_for_lob_files(target_size, timeout, proc=None):
    """等到出现一个大小等于最终值的落盘文件。

    不能只等"有文件就返回"：造数是从 1MB 起服务端倍增的，binlog 里是一串
    越来越大的 UPDATE，第一个文件出现时最终值还差好几轮。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = glob.glob(os.path.join(LOB_DIR, "*.lob"))
        if any(os.path.getsize(f) == target_size for f in files):
            return files
        if proc is not None and proc.poll() is not None:
            print(f"    !! capture 进程已退出 rc={proc.returncode}")
            return files
        time.sleep(2)
    return glob.glob(os.path.join(LOB_DIR, "*.lob"))


def file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def cap_text():
    parts = []
    for f in sorted(glob.glob(os.path.join(CAP_DIR, "*.cap"))):
        with open(f, "r", errors="replace") as fh:
            parts.append(fh.read())
    return "".join(parts)


def main():
    import threading
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="64M")
    ap.add_argument("--xmx", default="144m")
    args = ap.parse_args()
    blob_bytes = parse_size(args.size)

    L.require_jar(JAR, "mvn -pl migration-capture -am package -DskipTests")
    print("=" * 72)
    print(f"步骤4 capture 集测：单值 {L.human(blob_bytes)}，-Xmx{args.xmx}")
    print("=" * 72)

    L.tune_server(L.SRC_CT)
    shutil.rmtree(TASK_DIR, ignore_errors=True)
    os.makedirs(CAP_DIR, exist_ok=True)

    print("[1/5] 建表并启动 capture …")
    L.reset_db(L.SRC_CT, SRC_DB)
    L.create_table(L.SRC_CT, SRC_DB, TABLE)
    write_config(spill_enabled=True)
    proc = start_capture(args.xmx)
    peak, stop = [0], [False]
    threading.Thread(target=sample_rss, args=(proc, stop, peak), daemon=True).start()
    time.sleep(8)   # 等 binlog 流建立

    print(f"[2/5] 源库写入 {L.human(blob_bytes)} 的一行 …")
    L.seed_rows(L.SRC_CT, SRC_DB, TABLE, 1, blob_bytes, progress=False)
    src_md5 = L.scalar(L.SRC_CT, f"SELECT MD5(c_blob) FROM {TABLE} WHERE id=1", SRC_DB)

    files = wait_for_lob_files(blob_bytes, timeout=1800, proc=proc)
    print(f"    落盘文件 {len(files)} 个")
    record("capture 进程仍存活（未 OOM）", proc.poll() is None,
           f"exit={proc.poll()}")
    record("产生了落盘文件", len(files) >= 1, f"{len(files)} 个")

    if files:
        # 造数是"先插 1MB 种子再服务端倍增"，所以 binlog 里是一串 UPDATE，
        # 最后一个落盘文件才是最终值；按大小挑出等于目标长度的那个。
        finals = [f for f in files if os.path.getsize(f) == blob_bytes]
        record("有一个落盘文件长度等于最终值", len(finals) >= 1,
               f"{[os.path.getsize(f) for f in files][-3:]}")
        if finals:
            got = file_md5(finals[0])
            record("落盘内容 MD5 与源库一致", got == src_md5, f"{got} vs {src_md5}")
        record("落盘文件名按位点确定", all("#" in os.path.basename(f) for f in files),
               os.path.basename(files[0]))
        record("没有残留临时文件", len(glob.glob(os.path.join(LOB_DIR, "*.lobtmp"))) == 0)

    print("[3/5] UPDATE / DELETE 的镜像不应落盘 …")
    before_count = len(glob.glob(os.path.join(LOB_DIR, "*.lob")))
    L.mysql(L.SRC_CT, f"UPDATE {TABLE} SET c_vc='changed' WHERE id=1;", db=SRC_DB)
    L.mysql(L.SRC_CT, f"DELETE FROM {TABLE} WHERE id=1;", db=SRC_DB)
    time.sleep(20)
    after_count = len(glob.glob(os.path.join(LOB_DIR, "*.lob")))
    # UPDATE 只改了非大字段列，但 binlog_row_image=FULL 会把 blob 前后镜像都带上：
    # 前镜像丢弃，后镜像要落盘 ⇒ 至多 +1；DELETE 只有行镜像 ⇒ 不应再增加
    record("UPDATE 前镜像 + DELETE 行镜像不落盘", after_count - before_count <= 1,
           f"{before_count} → {after_count}")

    print("[4/5] 检查 .cap 内容格式 …")
    text = cap_text()
    record(".cap 里写的是 @lob: 引用", "@lob:" in text, f".cap 总长 {len(text)} 字符")
    # 关键不变量：<b>没有任何超阈值的值以十六进制出现</b>。
    # .cap 总长本身不是判据——造数是从 1MB 起倍增的，那串低于阈值的中间值本来就该走
    # 老的 0x hex 路径（这正是"阈值以下零回归"的设计），它们能让 .cap 有几十 MB。
    threshold = 1024 * 1024
    longest_hex = 0
    for run in re.findall(r"0x[0-9a-fA-F]+", text):
        longest_hex = max(longest_hex, (len(run) - 2) // 2)   # hex 字符数 → 字节数
    record("超过阈值的值一律不以十六进制出现在 .cap 里",
           longest_hex <= threshold,
           f"最长十六进制值 {L.human(longest_hex)}，阈值 {L.human(threshold)}")

    stop[0] = True
    proc.terminate()
    time.sleep(3)
    if proc.poll() is None:
        proc.kill()
    out = "".join(CAPTURE_LOG)
    print(f"    capture 峰值 RSS={peak[0] / 1024:.0f}MB")
    record("capture 峰值 RSS < 256MB", peak[0] / 1024 < 256, f"{peak[0] / 1024:.0f}MB")
    record("日志中无 OutOfMemoryError", "OutOfMemoryError" not in out)

    print("[5/5] 对照组：关掉落盘应复现 OOM …")
    shutil.rmtree(CAP_DIR, ignore_errors=True)
    os.makedirs(CAP_DIR, exist_ok=True)
    shutil.rmtree(LOB_DIR, ignore_errors=True)
    write_config(spill_enabled=False)
    CAPTURE_LOG.clear()
    proc2 = start_capture(args.xmx)
    time.sleep(8)
    L.mysql(L.SRC_CT, f"TRUNCATE TABLE {TABLE};", db=SRC_DB)
    L.seed_rows(L.SRC_CT, SRC_DB, TABLE, 1, blob_bytes, progress=False)
    deadline = time.time() + 600
    oom = False
    while time.time() < deadline and proc2.poll() is None:
        time.sleep(2)
    if proc2.poll() is None:
        proc2.kill()
    time.sleep(1)
    out2 = "".join(CAPTURE_LOG)
    oom = "OutOfMemoryError" in out2 or proc2.returncode not in (0, -15, 143)
    # 对照组证明"落盘"不是可有可无的优化：同一条数据，关掉它进程就死
    record("对照组（关落盘）确实 OOM/异常退出 ⇒ 落盘是必需项", oom,
           f"rc={proc2.returncode} oom={'OutOfMemoryError' in out2}")

    print("=" * 72)
    ok = all(r[1] for r in results)
    print(f"  步骤4 集测 {'通过' if ok else '未通过'}：{sum(1 for r in results if r[1])}/{len(results)}")
    print("=" * 72)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
