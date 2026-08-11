#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大字段同步的总判据脚本：把题设场景整条跑一遍。

题设：mysql→mysql，表有 10 个字段其中一个 LONGBLOB，单值 1GB、共 10 行（10GB），
各进程运行时内存上限 256MB，要求全量与增量都能搬完且不 OOM。

按顺序跑：
  1) 前置体检（磁盘、两端 max_allowed_packet、binlog 参数）
  2) 全量：lob_full_e2e（含幂等与块级续传）
  3) 增量：lob_incr_e2e（含 INSERT/UPDATE/DELETE 与落盘回收）
  4) capture 单链路：lob_capture_e2e（含关落盘的 OOM 对照）

用法：
  python3 test_scripts/lob/lob_acceptance.py                    # 冒烟规模（快）
  python3 test_scripts/lob/lob_acceptance.py --rows 10 --size 1G  # 题设满规模
"""
import argparse
import os
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loblib as L  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def parse_size(s):
    s = s.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    return int(float(s[:-1]) * mult[s[-1]]) if s[-1] in mult else int(s)


def preflight(rows, blob_bytes):
    """跑之前先算清楚盘够不够——10×1GB 的场景里，磁盘是最先撞上的墙。"""
    print("[前置体检]")
    ok = True

    # 源表 + 目标表 + 两端 binlog + 落盘文件，粗估 5 倍
    need = rows * blob_bytes * 5
    free = shutil.disk_usage(L.PROJECT_ROOT).free
    vol = subprocess.run(["docker", "exec", L.SRC_CT, "df", "-k", "/var/lib/mysql"],
                         capture_output=True, text=True).stdout.strip().splitlines()
    vol_free = int(vol[-1].split()[3]) * 1024 if len(vol) > 1 else 0
    print(f"  预计需要 ~{L.human(need)}；宿主机可用 {L.human(free)}，数据库卷可用 {L.human(vol_free)}")
    if free < need or vol_free < need:
        print("  !! 磁盘可能不够。可以先降规模（--rows/--size），或清理历史 binlog 后重跑")
        ok = False

    for name, ct in (("源", L.SRC_CT), ("目标", L.TGT_CT)):
        v = L.tune_server(ct)
        packet = v["max_allowed_packet"]
        print(f"  {name}端 max_allowed_packet={packet} redo={L.human(v['redo_capacity'])}")
        if packet < blob_bytes:
            print(f"  !! {name}端 max_allowed_packet 小于单值 {blob_bytes}，"
                  f"该参数同时限制 CONCAT 结果上限，分块追加也绕不过")
            ok = False
    if blob_bytes > 1073741824:
        print("  !! 单值超过 1GB：MySQL 的 max_allowed_packet 上限就是 1GB，无法通过 SQL 协议写入")
        ok = False
    return ok


def run(script, args, label):
    print("\n" + "=" * 72)
    print(f"  {label}")
    print("=" * 72)
    p = subprocess.run([sys.executable, "-u", os.path.join(HERE, script)] + args,
                       cwd=L.PROJECT_ROOT)
    return p.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2)
    ap.add_argument("--size", default="256M")
    ap.add_argument("--skip-preflight", action="store_true")
    args = ap.parse_args()
    blob_bytes = parse_size(args.size)

    print("=" * 72)
    print(f"大字段同步总判据：{args.rows} 行 × {L.human(blob_bytes)}，各进程 -Xmx144m")
    print("=" * 72)

    if not args.skip_preflight and not preflight(args.rows, blob_bytes):
        print("\n前置体检未通过，先处理上面的问题再跑。")
        return 2

    outcomes = []
    outcomes.append(("全量", run("lob_full_e2e.py",
                                 ["--rows", str(args.rows), "--size", args.size], "全量（含幂等 + 块级续传）")))
    outcomes.append(("增量", run("lob_incr_e2e.py",
                                 ["--size", args.size], "增量（capture→extract→increment）")))
    outcomes.append(("capture", run("lob_capture_e2e.py",
                                    ["--size", args.size], "capture 单链路（含关落盘的 OOM 对照）")))

    print("\n" + "=" * 72)
    for name, ok in outcomes:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    all_ok = all(ok for _, ok in outcomes)
    print(f"  总判据 {'通过' if all_ok else '未通过'}")
    print("=" * 72)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
