#!/usr/bin/env python3
"""
多行事件的目标端写放大判定（mysql→mysql）。

背景：extract 把一个 N 行的 binlog 行事件拆成 N 个"每行一条"的 THL 事件，拆分时逐键复制
metadata。若逐行元数据（rows_typed / rows_before_typed / rows_data_before）被整份复制，
拆出来的 N 个事件<b>每一个都带着全部 N 行</b>——而增量端的类型化值管道读的正是 rows_typed，
于是每个事件各自生成 N 条 SQL：

    N 行的源事件 → N 个 THL 事件 × 每个 N 条 SQL = N² 次目标写入

判据（两把独立的尺子）：

1. **目标库 binlog 行事件计数（确定性，不依赖日志格式）**——源侧写入 R 行，
   数目标库 binlog 里落在目标表上的行事件数 A。放大率 = A / R。
   修复后应 ≈1；未修复时等于"每批行数"的量级（一次 200 行的批量 INSERT → 放大 200 倍）。

2. **增量日志里的实际执行条数**——数 `执行参数化SQL` 行数，与尺子1 相互印证。

用法：
    python3 test_scripts/fault_injection/write_amplification.py [--batch 200] [--batches 5]
"""
import argparse
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
SRC_DB = "amp_src"
TGT_DB = "amp_tgt"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

DDL = """
CREATE TABLE `bulk` (
  `id` INT NOT NULL,
  `grp` INT NOT NULL,
  `payload` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# 放大率判定阈值：一行变更最多允许 1.5 次目标写入（幂等 upsert 本身是 1 次）
AMPLIFICATION_LIMIT = 1.5


def setup_source():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}"])
    F.sql_exec(CFG, [DDL], db=SRC_DB)
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {TGT_DB}"])


def master_pos():
    rows = F.sql_fetch(CFG, None, "SHOW MASTER STATUS")
    return rows[0][0], rows[0][1]


def count_target_row_events(start_file, start_pos):
    """数目标库 binlog 里落在 TGT_DB.bulk 上的行事件数（从 start 到当前末尾）。"""
    c = F.sql_conn(CFG, None)
    cur = c.cursor()
    row_events = 0
    f, pos = start_file, start_pos
    marked = False
    while True:
        cur.execute(f"SHOW BINLOG EVENTS IN '{f}' FROM {pos} LIMIT 20000")
        rows = cur.fetchall()
        if not rows:
            break
        last_pos = pos
        for (logname, p, etype, sid, endpos, info) in rows:
            last_pos = endpos
            if etype == "Table_map":
                marked = f"({TGT_DB}.bulk)" in info
            elif etype in ("Write_rows", "Update_rows", "Delete_rows") and marked:
                row_events += 1
            elif etype == "Xid":
                marked = False
        if last_pos <= pos:
            break
        pos = last_pos
    cur.close()
    c.close()
    return row_events


def count_increment_applies(task_id, since_line):
    """增量日志里<b>真正落库的</b> DML 条数（从 since_line 行之后算起）。

    只数 `执行参数化SQL`（类型化管道，恒为一条 DML）与文本路径里非事务控制的 `执行SQL`——
    文本路径会把源事务的 XID 记成 `执行SQL (seqno=N): COMMIT;` 再在执行时跳过，
    那不是写入，计进来会把放大率抬高一截（实测 300 条 DML 旁边有 278 条 COMMIT 日志）。
    """
    # 任务级 logback 把三个子进程的日志都写进 files/<taskId>/logs/migration.log
    log = os.path.join(PROJECT_DIR, "files", task_id, "logs", "migration.log")
    if not os.path.exists(log):
        return None, since_line
    n = 0
    total = 0
    with open(log, "r", errors="ignore") as fh:
        for i, line in enumerate(fh):
            total = i + 1
            if i < since_line:
                continue
            if "执行参数化SQL" in line:
                n += 1
            elif "执行SQL (seqno=" in line:
                stmt = line.split("):", 1)[-1].strip().rstrip(";").upper()
                if stmt not in ("COMMIT", "BEGIN", "ROLLBACK"):
                    n += 1
    return n, total


def log_line_count(task_id):
    _, total = count_increment_applies(task_id, 1 << 30)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=200, help="单条 INSERT 的行数")
    ap.add_argument("--batches", type=int, default=5, help="批数")
    ap.add_argument("--converge", type=int, default=600)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    total_rows = args.batch * args.batches

    token = F.login()
    print("✓ 登录成功")

    setup_source()
    print(f"[准备] 源库 {SRC_DB}.bulk（空表）")

    task_id = F.create_task(
        token, f"write-amp-{int(time.time())}", "mysql", "mysql", CONN, CONN,
        "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["bulk"]}}}}', TGT_DB, source_db=SRC_DB)
    print(f"[任务] {task_id}")

    failures = []
    quota = None
    try:
        # 限速会让"追平耗时"失真，也会拖长用例；临时放开，finally 还原
        quota = F.get_increment_quota()
        F.set_increment_quota(100000)

        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            print(f"✗ 任务未进入增量阶段: {st}")
            return 1
        time.sleep(5)

        start_f, start_p = master_pos()
        log_start = log_line_count(task_id)
        print(f"[基线] 目标 binlog 起点 {start_f}:{start_p}")

        # 每批一条多行 INSERT：一个 binlog 行事件带 args.batch 行
        c = F.sql_conn(CFG, SRC_DB)
        cur = c.cursor()
        for b in range(args.batches):
            base = b * args.batch
            values = ",".join(f"({base + i},{b},'p{base + i}')" for i in range(args.batch))
            cur.execute(f"INSERT INTO bulk (id,grp,payload) VALUES {values}")
            c.commit()
            time.sleep(0.2)
        cur.close()
        c.close()
        print(f"[写入] {args.batches} 条多行 INSERT，每条 {args.batch} 行，共 {total_rows} 行")

        deadline = time.time() + args.converge
        tgt_count = 0
        while time.time() < deadline:
            try:
                tgt_count = F.sql_fetch(CFG, TGT_DB, "SELECT COUNT(*) FROM bulk")[0][0]
                if tgt_count >= total_rows:
                    break
            except Exception:
                pass
            time.sleep(2)
        elapsed = args.converge - max(0, deadline - time.time())
        print(f"[追平] 目标行数 {tgt_count}/{total_rows}，耗时 ~{elapsed:.0f}s")
        if tgt_count != total_rows:
            failures.append(f"目标行数 {tgt_count} != 源行数 {total_rows}")

        time.sleep(3)
        applied_events = count_target_row_events(start_f, start_p)
        applied_sql, _ = count_increment_applies(task_id, log_start)

        ratio = applied_events / total_rows if total_rows else 0

        print("\n" + "=" * 68)
        print("尺子1｜目标库 binlog 行事件计数（确定性）")
        print(f"  源侧写入行数        : {total_rows}（{args.batches} 个事件 × {args.batch} 行）")
        print(f"  目标库行事件数      : {applied_events}")
        print(f"  放大率              : {ratio:.2f}×")
        print(f"  未修复时的理论值    : {args.batch}×（N 个事件各写 N 行 = ΣN² = {total_rows * args.batch}）")
        if ratio > AMPLIFICATION_LIMIT:
            print(f"  → 判定: 写放大 {ratio:.1f}×，多行事件的逐行元数据【未按行切片】")
            failures.append(f"目标端写放大 {ratio:.2f}×，超过阈值 {AMPLIFICATION_LIMIT}×")
        else:
            print("  → 判定: 放大率≈1，逐行元数据【已按行切片】")

        print("\n尺子2｜增量日志实际执行的 SQL 条数")
        if applied_sql is None:
            print("  （未找到增量日志，跳过）")
        else:
            print(f"  执行SQL 条数        : {applied_sql}")
            print(f"  期望（≈源行数）     : {total_rows}")
            if applied_sql > total_rows * AMPLIFICATION_LIMIT:
                print(f"  → 判定: 实际执行 {applied_sql / total_rows:.1f} 倍于行数")
                failures.append(f"增量执行 SQL {applied_sql} 条，远超源行数 {total_rows}")
            else:
                print("  → 判定: 与源行数同量级")

        print("\n尺子3｜数据一致性")
        src = F.sql_fetch(CFG, SRC_DB, "SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS(',',id,grp,payload))) FROM bulk")[0]
        tgt = F.sql_fetch(CFG, TGT_DB, "SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS(',',id,grp,payload))) FROM bulk")[0]
        print(f"  源 = {src}")
        print(f"  目标 = {tgt}")
        if src != tgt:
            failures.append(f"指纹不一致: 源={src} 目标={tgt}")
        else:
            print("  → 判定: 两端指纹相等")
        print("=" * 68)

    finally:
        if quota is not None:
            try:
                F.set_increment_quota(quota)
            except Exception:
                pass
        if not args.keep:
            F.stop_task(token, task_id)
            time.sleep(3)
            F.delete_task(token, task_id)
            try:
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}",
                                 f"DROP DATABASE IF EXISTS {TGT_DB}"])
            except Exception:
                pass

    return F.print_result([] if failures else ["write_amplification"], failures)


if __name__ == "__main__":
    sys.exit(main())
