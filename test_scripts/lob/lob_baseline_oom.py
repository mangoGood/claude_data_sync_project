#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 1 验收：把"单进程内存上界"这条约束落到实处，并复现改造前的 OOM 基线。

跑两件事：
  1) ChildJvmOptions/-Xmx 透传是否真的到了子进程命令行上（单测已覆盖，这里再做一次运行时确认）；
  2) 现状（未改造的全量链路）在受限堆下搬运大 LONGBLOB 时必然 OOM ——
     这是后续每一步的对照基线，没有它就无法证明改造有效。

默认用 256MB × 2 行（够炸、够快）。跑满题设规模用 --rows 10 --size 1G，
但那要先给两端把 max_allowed_packet 调到 1G，且耗时以十分钟计。

用法：
  python3 test_scripts/lob/lob_baseline_oom.py                 # 默认 256MB×2
  python3 test_scripts/lob/lob_baseline_oom.py --size 1G --rows 1
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loblib as L  # noqa: E402

TASK = "lob-baseline"
SRC_DB, TGT_DB, TABLE = "lob_src", "lob_tgt", "t_lob"
JAR = os.path.join(L.PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def parse_size(s):
    s = s.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    if s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2)
    ap.add_argument("--size", default="256M")
    ap.add_argument("--xmx", default="144m")
    ap.add_argument("--skip-seed", action="store_true", help="复用上次造好的数据")
    args = ap.parse_args()
    blob_bytes = parse_size(args.size)

    L.require_jar(JAR, "mvn -pl migration-full -am package -DskipTests")

    print("=" * 72)
    print(f"LOB 基线：{args.rows} 行 × {L.human(blob_bytes)} LONGBLOB，堆上限 -Xmx{args.xmx}")
    print("=" * 72)

    print("[1/4] 调优两端实例 …")
    src_vars = L.tune_server(L.SRC_CT)
    tgt_vars = L.tune_server(L.TGT_CT)
    print(f"    源 max_allowed_packet={src_vars['max_allowed_packet']} "
          f"redo={L.human(src_vars['redo_capacity'])}")
    print(f"    目标 max_allowed_packet={tgt_vars['max_allowed_packet']} "
          f"redo={L.human(tgt_vars['redo_capacity'])}")
    # 1GB 的值加上其余 9 列一定超过 max_allowed_packet 的 1GB 硬上限——
    # 这条边界决定了写入侧必须有"分块追加"路径，先在基线里把它记录下来。
    record("max_allowed_packet 已达 MySQL 上限 1G",
           src_vars["max_allowed_packet"] >= 1073741824,
           f"src={src_vars['max_allowed_packet']}")

    if not args.skip_seed:
        print(f"[2/4] 造数（{args.rows} 行 × {L.human(blob_bytes)}，服务端倍增）…")
        L.reset_db(L.SRC_CT, SRC_DB)
        L.create_table(L.SRC_CT, SRC_DB, TABLE)
        L.seed_rows(L.SRC_CT, SRC_DB, TABLE, args.rows, blob_bytes)
    else:
        print("[2/4] 跳过造数（--skip-seed）")
    L.reset_db(L.TGT_CT, TGT_DB)

    total = int(L.scalar(L.SRC_CT, f"SELECT SUM(OCTET_LENGTH(c_blob)) FROM {TABLE}", SRC_DB) or 0)
    print(f"    源表 LOB 合计 {L.human(total)}")

    print(f"[3/4] 跑现状全量（-Xmx{args.xmx}）…")
    L.write_full_config(TASK, SRC_DB, TGT_DB, TABLE)
    res = L.run_java(["-cp", JAR, "com.migration.full.Main", "--task-id", TASK],
                     jvm_opts=[f"-Xmx{args.xmx}", "-XX:MaxMetaspaceSize=64m",
                               "-XX:MaxDirectMemorySize=24m", "-XX:+ExitOnOutOfMemoryError"],
                     timeout=1800)
    print(f"    退出码={res.returncode} 峰值RSS={res.peak_rss_mb:.0f}MB 耗时={res.seconds:.0f}s")

    tail = "\n".join(res.output.strip().splitlines()[-12:])
    print("    ---- 尾部日志 ----")
    for line in tail.splitlines():
        print("    " + line[:160])

    print("[4/4] 判定基线 …")
    # 基线要证明的就是"现状搬不动"：要么 OOM，要么数据没搬对。
    moved = 0
    try:
        moved = int(L.scalar(L.TGT_CT, f"SELECT COUNT(*) FROM {TABLE}", TGT_DB) or 0)
    except Exception:
        moved = 0
    record("现状在受限堆下无法完成大 LOB 全量（OOM 或数据不全）",
           res.oom() or res.returncode != 0 or moved != args.rows,
           f"oom={res.oom()} rc={res.returncode} 目标行数={moved}/{args.rows}")
    record("峰值 RSS 未超 256MB（-Xmx 确实生效，不是靠堆变大绕过去的）",
           res.peak_rss_mb < 256, f"{res.peak_rss_mb:.0f}MB")

    print("=" * 72)
    ok = all(r[1] for r in results)
    print(f"  基线 {'建立' if ok else '未建立'}：{sum(1 for r in results if r[1])}/{len(results)}")
    print("=" * 72)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
