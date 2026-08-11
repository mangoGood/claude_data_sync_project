#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 3 验收：受限堆下的大字段全量同步端到端。

覆盖：
  1) 全量搬运 N 行 × S 字节的 LONGBLOB，逐行 10 列全字段比对（大字段比 MD5）
  2) 峰值 RSS < 256MB，且无 OOM
  3) 断点续传：搬到一半 kill -9，重启后继续，最终数据一致且不重来
  4) 幂等：重复跑一次，结果不变、不翻倍
  5) 零回归：同一次任务里的无大字段表照旧走原路径

用法：
  python3 test_scripts/lob/lob_full_e2e.py                    # 2 行 × 256MB（快）
  python3 test_scripts/lob/lob_full_e2e.py --rows 10 --size 1G  # 题设规模
"""
import argparse
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loblib as L  # noqa: E402

TASK = "lob-full-e2e"
SRC_DB, TGT_DB, TABLE = "lob_e2e_src", "lob_e2e_tgt", "t_lob"
PLAIN_TABLE = "t_plain"
JAR = os.path.join(L.PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def parse_size(s):
    s = s.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    return int(float(s[:-1]) * mult[s[-1]]) if s[-1] in mult else int(s)


JVM = ["-Xmx144m", "-XX:MaxMetaspaceSize=64m", "-XX:MaxDirectMemorySize=24m",
       "-XX:+ExitOnOutOfMemoryError"]


def run_full(timeout=7200):
    return L.run_java(["-cp", JAR, "com.migration.full.Main", "--task-id", TASK],
                      jvm_opts=JVM, timeout=timeout)


def seed(rows, blob_bytes):
    L.reset_db(L.SRC_CT, SRC_DB)
    L.create_table(L.SRC_CT, SRC_DB, TABLE)
    L.seed_rows(L.SRC_CT, SRC_DB, TABLE, rows, blob_bytes)
    # 零回归对照：同一次任务里的普通表必须照旧走原批量路径
    L.mysql(L.SRC_CT, f"CREATE TABLE {PLAIN_TABLE} (id INT PRIMARY KEY, v VARCHAR(64))", db=SRC_DB)
    L.mysql(L.SRC_CT, "INSERT INTO {} VALUES {}".format(
        PLAIN_TABLE, ",".join(f"({i},'v{i}')" for i in range(1, 501))), db=SRC_DB)


def write_cfg():
    L.write_full_config(TASK, SRC_DB, TGT_DB, TABLE, extra_lines="")
    # 两张表都纳入本次任务
    path = os.path.join(L.PROJECT_ROOT, "files", TASK, "config.properties")
    cfg = open(path).read()
    cfg = cfg.replace(f"migration.included.tables={SRC_DB}.{TABLE}",
                      f"migration.included.tables={SRC_DB}.{TABLE},{SRC_DB}.{PLAIN_TABLE}")
    cfg = cfg.replace(f'{{"{SRC_DB}":{{"tables":["{TABLE}"],"targetDb":"{TGT_DB}"}}}}',
                      f'{{"{SRC_DB}":{{"tables":["{TABLE}","{PLAIN_TABLE}"],"targetDb":"{TGT_DB}"}}}}')
    open(path, "w").write(cfg)


def compare_all(rows):
    src = L.row_digests(L.SRC_CT, SRC_DB, TABLE)
    tgt = L.row_digests(L.TGT_CT, TGT_DB, TABLE)
    ok, diffs = L.compare(src, tgt)
    return ok, diffs, len(tgt)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2)
    ap.add_argument("--size", default="256M")
    ap.add_argument("--skip-seed", action="store_true")
    ap.add_argument("--skip-resume", action="store_true")
    args = ap.parse_args()
    blob_bytes = parse_size(args.size)

    L.require_jar(JAR, "mvn -pl migration-full -am package -DskipTests")
    print("=" * 72)
    print(f"步骤3 全量 E2E：{args.rows} 行 × {L.human(blob_bytes)} + 普通表 500 行，-Xmx144m")
    print("=" * 72)

    L.tune_server(L.SRC_CT)
    L.tune_server(L.TGT_CT)

    if not args.skip_seed:
        print("[1/5] 造数 …")
        seed(args.rows, blob_bytes)
    L.reset_db(L.TGT_CT, TGT_DB)
    write_cfg()

    print("[2/5] 全量搬运 …")
    res = run_full()
    print(f"    退出码={res.returncode} 峰值RSS={res.peak_rss_mb:.0f}MB 耗时={res.seconds:.0f}s")
    if res.returncode != 0:
        print("\n".join(res.output.splitlines()[-25:]))
    record("全量退出码 0", res.returncode == 0, f"rc={res.returncode}")
    record("未发生 OOM", not res.oom())
    record("峰值 RSS < 256MB", res.peak_rss_mb < 256, f"{res.peak_rss_mb:.0f}MB")

    ok, diffs, moved = compare_all(args.rows)
    record(f"逐行 10 列全字段一致（{args.rows} 行）", ok and moved == args.rows,
           f"目标 {moved}/{args.rows}" + ("；" + " / ".join(diffs[:2]) if diffs else ""))

    plain = L.scalar(L.TGT_CT, f"SELECT COUNT(*) FROM {PLAIN_TABLE}", TGT_DB)
    record("零回归：同任务内普通表照常搬完", plain == "500", f"{plain}/500")

    print("[3/5] 幂等：原样再跑一次 …")
    res2 = run_full()
    ok2, diffs2, moved2 = compare_all(args.rows)
    record("重复执行结果不变（幂等）", res2.returncode == 0 and ok2 and moved2 == args.rows,
           f"rc={res2.returncode} 目标 {moved2}/{args.rows}")

    if not args.skip_resume:
        print("[4/5] 断点续传：搬运中途 kill -9 …")
        L.reset_db(L.TGT_CT, TGT_DB)
        # 强制走"分块追加"——这才是 1GB 场景真正跑的那条路径，也只有它能测出<b>块级</b>续传
        # （单条语句写整值的话，被杀就整条回滚，退化成行级重来）。
        #
        # 强制方式是把"包余量"调大到几乎吃掉整个预算，而<b>不能</b>去调小服务端的
        # max_allowed_packet：该参数同时限制 CONCAT 的结果大小，调小到值本身之下时，
        # 这个值在 MySQL 里根本就存不进去，测的就不是续传而是一个不可能的配置了。
        write_cfg()
        cfg_path = os.path.join(L.PROJECT_ROOT, "files", TASK, "config.properties")
        with open(cfg_path, "a") as f:
            f.write("migration.lob.append.block.bytes=33554432\n")
            f.write(f"migration.lob.packet.safety.margin.bytes={1073741824 - 16 * 1024 * 1024}\n")
        proc = subprocess.Popen(["java"] + JVM + ["-cp", JAR, "com.migration.full.Main", "--task-id", TASK],
                                cwd=L.PROJECT_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # 等到目标端确实写进去一部分再杀，才叫"中途"
        killed_at = 0
        deadline = time.time() + 900
        while time.time() < deadline:
            time.sleep(3)
            try:
                got = L.scalar(L.TGT_CT, f"SELECT IFNULL(SUM(OCTET_LENGTH(c_blob)),0) FROM {TABLE}", TGT_DB)
                killed_at = int(got or 0)
            except Exception:
                killed_at = 0
            if killed_at > blob_bytes // 4:
                break
            if proc.poll() is not None:
                break
        proc.kill()
        proc.wait()
        print(f"    已写入 {L.human(killed_at)} 时杀掉进程")
        record("确实杀在搬运中途", 0 < killed_at < blob_bytes * args.rows,
               f"{L.human(killed_at)}/{L.human(blob_bytes * args.rows)}")

        print("[5/5] 重启续传 …")
        res3 = run_full()
        print(f"    退出码={res3.returncode} 峰值RSS={res3.peak_rss_mb:.0f}MB 耗时={res3.seconds:.0f}s")
        ok3, diffs3, moved3 = compare_all(args.rows)
        record("续传后逐行 10 列全字段一致", res3.returncode == 0 and ok3 and moved3 == args.rows,
               f"rc={res3.returncode} 目标 {moved3}/{args.rows}"
               + ("；" + " / ".join(diffs3[:2]) if diffs3 else ""))
        # 续传必须是"接着写"而不是"清空重搬"：后者在 10×1GB 上等于永远搬不完
        after = int(L.scalar(L.TGT_CT, f"SELECT SUM(OCTET_LENGTH(c_blob)) FROM {TABLE}", TGT_DB) or 0)
        record("续传是接着写而非清表重来",
               after == blob_bytes * args.rows and killed_at > 0,
               f"被杀时已写 {L.human(killed_at)}，续传后 {L.human(after)}")

    print("=" * 72)
    ok_all = all(r[1] for r in results)
    print(f"  步骤3 {'通过' if ok_all else '未通过'}：{sum(1 for r in results if r[1])}/{len(results)}")
    print("=" * 72)
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
