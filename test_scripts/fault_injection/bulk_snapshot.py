#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量批量装载（P2-2）+ 一致性快照（P2-3）判据。

直接驱动 migration-full（com.migration.full.Main，读 files/<taskId>/config.properties），
在 synctask-mysql(33306) 上做 bulk_src -> bulk_tgt 全量同步。

五把尺子：

1. **值保真**：批量通道走的仍是 PreparedStatement 绑定（只是驱动把一批合并成一条多值 INSERT），
   逐行 MD5 汇总必须与源端逐字节一致。这是不选 LOAD DATA / COPY 文本通道的理由——
   文本通道会绕开类型绑定，本项目增量链路为此踩过 5 类值保真缺陷。
2. **成功计数不塌**：批量语句被重写后 executeBatch 返回 SUCCESS_NO_INFO(-2) 而非逐行影响数。
   若沿用"负数即失败"的老口径，全量会把<b>全部行报成失败</b>。日志里的"成功: N"必须等于真实行数。
3. **吞吐**：同一份数据分别按 bulk 开/关跑一遍，开启后应明显更快（本机 docker MySQL 实测 3~5 倍）。
4. **快照位点落盘**：默认 GTID_ONLY 模式要产出 files/<task>/full_snapshot_position（GTID 或 binlog 坐标）。
5. **快照隔离**：CONSISTENT 模式下，全量<b>开始之后</b>写入源库的行不得出现在本次全量结果里
   —— 这正是"全量结束点 = 某个一致快照"的含义。对照组（NONE）允许漏进来，用来证明尺子有效。

前置：synctask-mysql 容器在跑；migration-full fat jar 已 package（mvn -pl migration-full -am install -DskipTests）。
用法：python3 test_scripts/fault_injection/bulk_snapshot.py [--rows 200000]
"""
import argparse
import os
import re
import subprocess
import sys
import threading
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-full", "target", "migration-full-1.0.0.jar")
CT = "synctask-mysql"
SRC_DB = "bulk_src"
TGT_DB = "bulk_tgt"
TASK = "bulk-snapshot"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def mysql(sql, db=None, want=False, timeout=600):
    args = ["docker", "exec", "-i", CT, "mysql", "-uroot", "-prootpassword",
            "--default-character-set=utf8mb4"]
    if want:
        args.append("-N")
    if db:
        args += ["-D", db]
    p = subprocess.run(args, input=sql, capture_output=True, text=True, timeout=timeout)
    return (p.stdout or "").strip()


def scalar(sql, db=None):
    return mysql(sql, db=db, want=True).strip()


def write_config(bulk_enabled, snapshot_mode):
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)
    url = "jdbc:mysql://localhost:33306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    cfg = f"""source.db.type=mysql
source.db.host=localhost
source.db.port=33306
source.db.username=root
source.db.password=rootpassword
source.db.database=
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url={url}
target.db.type=mysql
target.db.host=localhost
target.db.port=33306
target.db.username=root
target.db.password=rootpassword
target.db.database=
target.db.jdbc.driver=com.mysql.cj.jdbc.Driver
target.db.jdbc.url={url}
target.db.quote.char=`
migration.included.databases={SRC_DB}
migration.included.tables={SRC_DB}.t
migration.sync.objects={{"{SRC_DB}":{{"tables":["t"],"targetDb":"{TGT_DB}"}}}}
schema.mapping.db.{SRC_DB}={TGT_DB}
migration.full.parallelism=1
migration.full.shard.enabled=false
migration.record.checkpoint=false
migration.enable.resume=false
migration.full.bulk.enabled={'true' if bulk_enabled else 'false'}
migration.full.snapshot.mode={snapshot_mode}
"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)


def seed(rows):
    mysql(f"DROP DATABASE IF EXISTS {SRC_DB}; CREATE DATABASE {SRC_DB} CHARACTER SET utf8mb4;")
    mysql("""CREATE TABLE t(
      id INT PRIMARY KEY,
      n BIGINT,
      d DECIMAL(20,4),
      ts DATETIME(3),
      b VARBINARY(32),
      s VARCHAR(128)
    ) CHARACTER SET utf8mb4;""", db=SRC_DB)
    # 分批插入，避免单条 SQL 撞 max_allowed_packet
    step = 5000
    for start in range(1, rows + 1, step):
        end = min(start + step - 1, rows)
        vals = ",".join(
            f"({i},{i*7},{i}.1234,'2026-01-01 00:00:00.123',UNHEX('{i:08x}'),'行{i}·unicode📦')"
            for i in range(start, end + 1))
        mysql(f"INSERT INTO t VALUES {vals};", db=SRC_DB)


def reset_target():
    mysql(f"DROP DATABASE IF EXISTS {TGT_DB}; CREATE DATABASE {TGT_DB} CHARACTER SET utf8mb4;")


def fingerprint(db):
    """逐行 MD5 汇总：任何一个字节的差异都会改变结果（NULL 与空串也可区分）。"""
    return scalar(
        "SELECT IFNULL(MD5(GROUP_CONCAT(x ORDER BY x SEPARATOR '')),'-') FROM ("
        "SELECT MD5(CONCAT_WS('|',id,n,d,ts,HEX(b),s)) AS x FROM t) g", db=db)


def run_full():
    t0 = time.time()
    p = subprocess.run(["java", "-Dh2.bindAddress=127.0.0.1", "-cp", JAR,
                        "com.migration.full.Main", "--task-id", TASK],
                       cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=1800)
    return p.returncode == 0, (p.stdout or "") + (p.stderr or ""), time.time() - t0


def reported_success(log):
    """日志里最后一条 '表 t 数据迁移完成，成功: N, 失败: M'。"""
    hits = re.findall(r"表 t 数据迁移完成，成功: (\d+), 失败: (\d+)", log)
    return (int(hits[-1][0]), int(hits[-1][1])) if hits else (None, None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200000)
    args = ap.parse_args()

    if not os.path.exists(JAR):
        print(f"缺少 fat jar：{JAR}\n先执行：mvn -pl migration-full -am install -DskipTests")
        sys.exit(2)

    print("=" * 78)
    print(f"全量批量装载 + 一致性快照判据：{SRC_DB} -> {TGT_DB} @ synctask-mysql(33306)，{args.rows} 行")
    print("=" * 78)

    print(f"\n[准备] 造 {args.rows} 行源数据 ...")
    seed(args.rows)
    src_rows = int(scalar("SELECT COUNT(*) FROM t", SRC_DB))
    src_fp = fingerprint(SRC_DB)
    record(f"源表 {args.rows} 行就绪", src_rows == args.rows, f"count={src_rows}")

    # ---------- 尺子 1/2/3/4：批量装载 ----------
    print("\n[1/3] 批量装载 关闭（基线）...")
    reset_target()
    write_config(bulk_enabled=False, snapshot_mode="GTID_ONLY")
    ok_off, log_off, sec_off = run_full()
    rows_off = int(scalar("SELECT COUNT(*) FROM t", TGT_DB) or 0)
    fp_off = fingerprint(TGT_DB)
    print(f"      基线耗时 {sec_off:.1f}s，{rows_off} 行")

    print("\n[2/3] 批量装载 开启 ...")
    reset_target()
    write_config(bulk_enabled=True, snapshot_mode="GTID_ONLY")
    ok_on, log_on, sec_on = run_full()
    rows_on = int(scalar("SELECT COUNT(*) FROM t", TGT_DB) or 0)
    fp_on = fingerprint(TGT_DB)
    print(f"      批量耗时 {sec_on:.1f}s，{rows_on} 行")

    record("两种模式都跑成功", ok_off and ok_on, f"off={ok_off}, on={ok_on}")
    record("行数一致", rows_off == src_rows and rows_on == src_rows,
           f"src={src_rows}, off={rows_off}, on={rows_on}")
    record("值保真：逐行 MD5 汇总与源端一致（类型绑定未被文本通道绕开）",
           fp_on == src_fp and fp_off == src_fp,
           f"src={src_fp[:12]}, off={fp_off[:12]}, on={fp_on[:12]}")

    succ_on, fail_on = reported_success(log_on)
    record("成功计数不塌：SUCCESS_NO_INFO 必须算成功",
           succ_on == src_rows and fail_on == 0,
           f"日志成功={succ_on}, 失败={fail_on}, 期望成功={src_rows}")

    speedup = (sec_off / sec_on) if sec_on > 0 else 0
    record("吞吐提升 ≥ 1.5×", speedup >= 1.5,
           f"{sec_off:.1f}s -> {sec_on:.1f}s = {speedup:.2f}×  "
           f"({src_rows/sec_off:.0f} -> {src_rows/sec_on:.0f} 行/秒)")

    pos_file = os.path.join(PROJECT_ROOT, "files", TASK, "full_snapshot_position")
    pos = open(pos_file).read().strip() if os.path.exists(pos_file) else ""
    record("快照位点落盘（GTID / binlog 坐标）",
           ("gtid:" in pos or "binlog:" in pos), pos[:100] or "文件不存在")

    # ---------- 尺子 5：一致性快照隔离 ----------
    # 全量跑起来之后往源库继续插入新行；CONSISTENT 模式下这些行不该出现在本次全量结果里。
    print("\n[3/3] 一致性快照隔离（全量期间并发写入源库）...")
    # 判据必须以"快照建立时刻"为界，而不是"进程启动时刻"：JVM 启动 + 建表要一两秒，
    # 这段时间里写进源库的行本来就属于快照之内，理应被搬走。位点文件第一段就是快照时刻（epoch ms）。
    # 持续写入而不是写一次：批量装载后全量只要几秒，写一次很容易整个错过搬运窗口。
    def late_writer(stop_evt, batches):
        next_id = args.rows + 1
        while not stop_evt.is_set():
            vals = ",".join(
                f"({i},{i*7},{i}.1234,'2026-01-01 00:00:00.123',UNHEX('{i:08x}'),'late{i}')"
                for i in range(next_id, next_id + 50))
            started = time.time() * 1000
            mysql(f"INSERT INTO t VALUES {vals};", db=SRC_DB)
            # (起始id, 结束id, 本批开始写入的时刻, 本批写完的时刻)
            batches.append((next_id, next_id + 49, started, time.time() * 1000))
            next_id += 50
            stop_evt.wait(0.2)

    pos_path = os.path.join(PROJECT_ROOT, "files", TASK, "full_snapshot_position")

    def run_with_late_writes(mode):
        mysql(f"DELETE FROM t WHERE id > {args.rows}", db=SRC_DB)
        reset_target()
        if os.path.exists(pos_path):
            os.remove(pos_path)
        write_config(bulk_enabled=True, snapshot_mode=mode)
        stop_evt = threading.Event()
        batches = []
        th = threading.Thread(target=late_writer, args=(stop_evt, batches), daemon=True)
        th.start()
        ok, log, sec = run_full()
        stop_evt.set()
        th.join(timeout=10)

        snap_ms = None
        if os.path.exists(pos_path):
            snap_ms = float(open(pos_path).read().strip().split("|")[0])
        got = set(int(x) for x in mysql(
            f"SELECT id FROM t WHERE id > {args.rows}", db=TGT_DB, want=True).split() if x.strip())

        # 违规 = 本批<b>开始写入</b>时快照已经建立（这些行在快照点上确定不存在），却出现在目标端
        after, missing_before = 0, 0
        for lo, hi, t_start, t_end in batches:
            ids = set(range(lo, hi + 1))
            if snap_ms is not None and t_start > snap_ms:
                after += len(ids & got)
            elif snap_ms is not None and t_end < snap_ms:
                # 快照建立前就已提交的行，必须搬到（否则是快照取早了 = 丢数据）
                missing_before += len(ids - got)
        return ok, len(got), len(batches) * 50, after, missing_before

    ok_snap, got_snap, written_snap, after_snap, missing_snap = run_with_late_writes("CONSISTENT")
    record("CONSISTENT：快照点之后写入的行不得进入本次全量",
           ok_snap and after_snap == 0,
           f"并发写入 {written_snap} 行，其中 {got_snap} 行进了目标端，快照点之后的有 {after_snap} 行（期望 0）")
    record("CONSISTENT：快照点之前已提交的行必须全部搬到（快照不能取早）",
           ok_snap and missing_snap == 0, f"缺失 {missing_snap} 行")

    # 对照组用 GTID_ONLY 而不是 NONE：它同样记位点（判据需要快照时刻），但不隔离读取，
    # 正好是"只知道位点、数据却不属于那个位点"的旧状态。
    ok_none, got_none, written_none, after_none, _ = run_with_late_writes("GTID_ONLY")
    # 对照组证明尺子有效：不隔离读取时目标端确实会混进"位点之后"的行。
    # 没漏不判失败——只说明这一轮时序没赶上，但那样上面两把尺子就没被真正验证过，需要提示。
    print(f"      对照组（GTID_ONLY，只记位点不隔离）：并发写入 {written_none} 行，进目标端 {got_none} 行，"
          f"其中快照点之后的 {after_none} 行 " +
          ("→ 尺子有效（无快照确实在拼接不同时刻的数据）" if after_none > 0
           else "→ 本轮时序没赶上，快照尺子未被真正验证，建议加大 --rows 重跑"))

    print("\n" + "=" * 78)
    failed = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail else ""))
    print(f"\n合计 {len(results)} 项，失败 {len(failed)} 项")
    print("=" * 78)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
