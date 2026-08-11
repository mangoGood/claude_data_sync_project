#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 2 验收（真库探针）：受限堆下把一个大字段从 dr-mysql-a 流式搬到 dr-mysql-b。

要坐实的假设只有一个，但它是整个方案的地基：
    setBinaryStream + useServerPrepStmts=true ⇒ 驱动走 COM_STMT_SEND_LONG_DATA 分片推送，
    客户端内存与字段大小无关。
若这个假设不成立（驱动把流读进内存再组包），后面 3~6 步全都建在沙子上，
所以这里额外跑一遍 useServerPrepStmts=false 做对照——它应该 OOM。

用法：
  python3 test_scripts/lob/lob_writer_probe.py            # 默认 256MB
  python3 test_scripts/lob/lob_writer_probe.py --size 1G  # 题设规模
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loblib as L  # noqa: E402

SRC_DB, TGT_DB, TABLE = "lob_probe_src", "lob_probe_tgt", "t_lob"
JAR = os.path.join(L.PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
PROBE_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "LobCopyProbe.java")

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def parse_size(s):
    s = s.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    return int(float(s[:-1]) * mult[s[-1]]) if s[-1] in mult else int(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="256M")
    ap.add_argument("--xmx", default="144m")
    ap.add_argument("--skip-seed", action="store_true")
    args = ap.parse_args()
    blob_bytes = parse_size(args.size)

    L.require_jar(JAR, "mvn -pl migration-full -am package -DskipTests")

    print("=" * 72)
    print(f"步骤2 探针：1 行 × {L.human(blob_bytes)}，-Xmx{args.xmx}")
    print("=" * 72)

    L.tune_server(L.SRC_CT)
    L.tune_server(L.TGT_CT)

    if not args.skip_seed:
        print(f"[1/4] 造数 …")
        L.reset_db(L.SRC_CT, SRC_DB)
        L.create_table(L.SRC_CT, SRC_DB, TABLE)
        L.seed_rows(L.SRC_CT, SRC_DB, TABLE, 1, blob_bytes)
    L.reset_db(L.TGT_CT, TGT_DB)
    L.create_table(L.TGT_CT, TGT_DB, TABLE)

    print("[2/4] 编译探针 …")
    workdir = tempfile.mkdtemp(prefix="lobprobe-")
    try:
        p = subprocess.run(["javac", "-cp", JAR, "-d", workdir, PROBE_SRC],
                           capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stdout + p.stderr)
            return 2
        cp = f"{JAR}:{workdir}"
        src_url = f"jdbc:mysql://localhost:{L.SRC_PORT}/?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        tgt_url = f"jdbc:mysql://localhost:{L.TGT_PORT}/?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        base = ["-cp", cp, "LobCopyProbe", src_url, SRC_DB, tgt_url, TGT_DB, TABLE, "id", "1", "c_blob"]
        jvm = [f"-Xmx{args.xmx}", "-XX:MaxMetaspaceSize=64m", "-XX:MaxDirectMemorySize=24m",
               "-XX:+ExitOnOutOfMemoryError"]

        print(f"[3/4] 流式搬运（useServerPrepStmts=true）…")
        ok_run = L.run_java(base + ["true"], jvm_opts=jvm, timeout=3600)
        for line in ok_run.output.splitlines():
            if line.startswith("[probe]"):
                print("    " + line)
        print(f"    退出码={ok_run.returncode} 峰值RSS={ok_run.peak_rss_mb:.0f}MB 耗时={ok_run.seconds:.0f}s")

        src_md5 = L.scalar(L.SRC_CT, f"SELECT MD5(c_blob) FROM {TABLE} WHERE id=1", SRC_DB)
        tgt_md5 = L.scalar(L.TGT_CT, f"SELECT MD5(c_blob) FROM {TABLE} WHERE id=1", TGT_DB)
        tgt_len = L.scalar(L.TGT_CT, f"SELECT OCTET_LENGTH(c_blob) FROM {TABLE} WHERE id=1", TGT_DB)

        record("流式搬运成功退出", ok_run.returncode == 0, f"rc={ok_run.returncode}")
        record("目标端长度一致", tgt_len == str(blob_bytes), f"{tgt_len} vs {blob_bytes}")
        record("目标端 MD5 与源一致", bool(src_md5) and src_md5 == tgt_md5, f"{src_md5} vs {tgt_md5}")
        record(f"峰值 RSS < 256MB", ok_run.peak_rss_mb < 256, f"{ok_run.peak_rss_mb:.0f}MB")
        record("未发生 OOM", not ok_run.oom())

        print(f"[4/4] 对照组（useServerPrepStmts=false，预期失败）…")
        L.mysql(L.TGT_CT, f"TRUNCATE TABLE {TABLE};", db=TGT_DB)
        bad_run = L.run_java(base + ["false"], jvm_opts=jvm, timeout=1800)
        print(f"    退出码={bad_run.returncode} 峰值RSS={bad_run.peak_rss_mb:.0f}MB oom={bad_run.oom()}")
        # 对照组证明 useServerPrepStmts 是必要条件而不是可选优化：
        # 客户端预编译下驱动要把整个流读进内存组包，同样的代码就会炸。
        record("对照组（无 server prep）确实搬不动 ⇒ useServerPrepStmts 是必要条件",
               bad_run.returncode != 0 or bad_run.oom(),
               f"rc={bad_run.returncode} oom={bad_run.oom()}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print("=" * 72)
    ok = all(r[1] for r in results)
    print(f"  步骤2 探针 {'通过' if ok else '未通过'}：{sum(1 for r in results if r[1])}/{len(results)}")
    print("=" * 72)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
