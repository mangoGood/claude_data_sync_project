#!/usr/bin/env python3
"""
长跑资源治理的判定（mysql→mysql）：日志体积 / 行值合规 / 表延迟文件有界 / 终态可清理。

背景（三处无界增长，跑几天必然把磁盘吃光）：

1. **日志**：per-task logback 把 `com.migration` 设成 DEBUG，增量还对<b>每一行</b>打 INFO
   （`执行SQL (seqno=…): <拼好值的完整 DML>`）。实测 10 分钟任务产出 283MB；单任务
   totalSizeCap 10GB，N 个任务就是 N×10GB。顺带还是合规问题——行数据明文落盘，
   DTS/DMS 默认都不记录行值。
2. **表级延迟文件**：`recordTableLatency()` 每个事件往 `table_latency/<表>.tsv` 追加一行，
   <b>从不裁剪</b>；读侧还每次整文件读进内存。实测单个测试任务 46,268 行 / 2.6MB，
   按 5000 行/秒估算约 17GB/天/表。
3. **任务目录**：任务删了也没人清 `files/<taskId>`，本机实测累积 12GB / 204 个目录。

判据：
  1. 默认策略下日志里<b>没有行值</b>（不出现逐行的"执行参数化SQL/执行SQL"明文语句）；
  2. 每 1000 行变更产生的日志体积低于阈值（旧行为是它的几十倍）；
  3. 表延迟 tsv 行数被压在上限的 2 倍以内，且热力图接口仍能出数（裁剪没把功能弄坏）；
  4. 任务删除后任务目录带上 `.terminal` 终态标记（保留期到点由 agent 巡检清理）。

用法：
    python3 test_scripts/fault_injection/resource_governance.py [--rows 3000]
"""
import argparse
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
SRC_DB = "res_src"
TGT_DB = "res_tgt"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

DDL = """
CREATE TABLE `wide` (
  `id` INT NOT NULL,
  `name` VARCHAR(64) NOT NULL,
  `secret` VARCHAR(64) NOT NULL,
  `amount` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# 行值里塞一个好找的标记：只要它出现在日志里，就说明行数据被明文落盘了
SECRET_MARK = "PII-4f9c2a"

TSV_MAX_LINES = 200
# 每 1000 行变更允许的日志字节数上限。修复前逐行 INFO 打完整 DML，量级在 MB/千行。
LOG_BYTES_PER_1K_LIMIT = 400 * 1024


def restart_agent(extra_env=None):
    env = dict(os.environ)
    env.update(extra_env or {})
    r = subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR, env=env,
                       capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"重启 agent 失败: {r.stdout}\n{r.stderr}")
    print(f"[agent] 已重启（{'表延迟上限=' + str(TSV_MAX_LINES) if extra_env else '默认参数'}）")


def log_path(task_id):
    return os.path.join(PROJECT_DIR, "files", task_id, "logs", "migration.log")


def log_size(task_id):
    p = log_path(task_id)
    return os.path.getsize(p) if os.path.exists(p) else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=3000)
    ap.add_argument("--converge", type=int, default=420)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    restart_agent({"INCREMENT_TABLE_LATENCY_MAX_LINES": str(TSV_MAX_LINES)})
    token = F.login()
    print("✓ 登录成功")

    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}"])
    F.sql_exec(CFG, [DDL], db=SRC_DB)

    task_id = F.create_task(
        token, f"res-gov-{int(time.time())}", "mysql", "mysql", CONN, CONN,
        "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["wide"]}}}}', TGT_DB, source_db=SRC_DB)
    print(f"[任务] {task_id}")

    passed, failed = [], []
    quota = None
    try:
        quota = F.get_increment_quota()
        F.set_increment_quota(100000)

        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            failed.append(f"任务未进入增量阶段（{st}）")
            return F.print_result(passed, failed)
        time.sleep(5)

        base_log = log_size(task_id)
        print(f"[基线] 日志 {base_log / 1024:.0f}KB")

        c = F.sql_conn(CFG, SRC_DB)
        cur = c.cursor()
        for i in range(args.rows):
            cur.execute("INSERT INTO wide (id,name,secret,amount) VALUES (%s,%s,%s,%s)",
                        (i, f"name-{i}", f"{SECRET_MARK}-{i}", i * 13))
        cur.close()
        c.close()
        print(f"[写入] {args.rows} 行（每行含标记 {SECRET_MARK}）")

        deadline = time.time() + args.converge
        tgt_rows = 0
        while time.time() < deadline:
            try:
                tgt_rows = F.sql_fetch(CFG, TGT_DB, "SELECT COUNT(*) FROM wide")[0][0]
                if tgt_rows >= args.rows:
                    break
            except Exception:
                pass
            time.sleep(2)
        print(f"[追平] 目标 {tgt_rows}/{args.rows} 行")
        if tgt_rows != args.rows:
            failed.append(f"目标行数 {tgt_rows} != 源 {args.rows}（治理不能以丢数据为代价）")
        time.sleep(5)

        # ---------- 判据1：日志里不含行值 ----------
        leaked = 0
        per_row_lines = 0
        with open(log_path(task_id), "r", errors="ignore") as fh:
            for line in fh:
                if SECRET_MARK in line:
                    leaked += 1
                if "执行参数化SQL (seqno=" in line or "执行SQL (seqno=" in line:
                    per_row_lines += 1
        print(f"\n[判据1] 含行值明文的日志行: {leaked}；逐行 SQL 日志行: {per_row_lines}")
        if leaked:
            failed.append(f"{leaked} 行日志里出现了行数据明文（合规风险）")
        else:
            passed.append("默认策略下日志不含行值明文")

        # ---------- 判据2：日志体积 ----------
        grown = log_size(task_id) - base_log
        per_1k = grown / max(1, args.rows) * 1000
        print(f"[判据2] 本轮日志增长 {grown / 1024:.0f}KB，折合 {per_1k / 1024:.0f}KB/千行"
              f"（阈值 {LOG_BYTES_PER_1K_LIMIT / 1024:.0f}KB/千行）")
        if per_1k > LOG_BYTES_PER_1K_LIMIT:
            failed.append(f"日志 {per_1k / 1024:.0f}KB/千行，超过阈值")
        else:
            passed.append(f"日志体积 {per_1k / 1024:.0f}KB/千行，在阈值内")

        # ---------- 判据3：表延迟 tsv 有界 ----------
        tsv_dir = os.path.join(PROJECT_DIR, "files", task_id, "binlog_output", "table_latency")
        tsv_files = [os.path.join(tsv_dir, f) for f in os.listdir(tsv_dir)] if os.path.isdir(tsv_dir) else []
        worst = 0
        for p in tsv_files:
            with open(p, "r", errors="ignore") as fh:
                worst = max(worst, sum(1 for _ in fh))
        print(f"[判据3] 表延迟 tsv {len(tsv_files)} 个，最大行数 {worst}（上限 {TSV_MAX_LINES}，容忍 2×）")
        if not tsv_files:
            failed.append("没有表延迟文件，热力图数据链路可能被改坏了")
        elif worst > TSV_MAX_LINES * 2:
            failed.append(f"表延迟文件 {worst} 行，超过上限 2 倍——仍是从不裁剪的老行为")
        else:
            passed.append(f"表延迟文件被压在 {worst} 行（≤ 上限 2 倍）")

        # 后端是"原样透传 agent 响应"，没有 data 包装；兼容两种形状
        heat = F.api("GET", f"/api/workflows/{task_id}/table-latency", token)
        heat_body = heat.get("data") if isinstance(heat.get("data"), dict) else heat
        heat_tables = (heat_body or {}).get("tables") or []
        print(f"[判据3b] 热力图接口返回 {len(heat_tables)} 张表")
        if heat_tables:
            passed.append("裁剪后热力图接口仍能出数")
        else:
            failed.append("热力图接口没有数据，裁剪把功能弄坏了")

        # ---------- 判据4：终态标记 ----------
        F.stop_task(token, task_id)
        time.sleep(3)
        F.delete_task(token, task_id)
        marker = os.path.join(PROJECT_DIR, "files", task_id, ".terminal")
        ok = False
        for _ in range(20):
            if os.path.exists(marker):
                ok = True
                break
            time.sleep(1)
        print(f"[判据4] 终态标记 {marker}: {'已生成' if ok else '缺失'}")
        if ok:
            with open(marker) as fh:
                print(f"    内容: {fh.read().strip()}")
            passed.append("任务删除后目录被打上终态标记（保留期后由 agent 清理）")
        else:
            failed.append("任务删除后没有终态标记，files/<taskId> 将永远没人清")

    finally:
        if quota is not None:
            try:
                F.set_increment_quota(quota)
            except Exception:
                pass
        if not args.keep:
            F.stop_task(token, task_id)
            time.sleep(2)
            F.delete_task(token, task_id)
            try:
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}",
                                 f"DROP DATABASE IF EXISTS {TGT_DB}"])
            except Exception:
                pass
            try:
                restart_agent()
            except Exception as e:
                print(f"[警告] 还原 agent 默认参数失败: {e}")

    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
