#!/usr/bin/env python3
"""
Oracle→PostgreSQL 断点续传 / 故障注入测试（best-effort）。

Oracle 源与其它 SQL 源共用 capture+extract+increment 三段管线与 ProcessGuard 守护，
断点续传机制完全相同（capture=OracleRedoCapture/LogMiner）。本用例验证：增量过程中
SIGKILL 受守护子进程后，管线自愈、目标 PG 最终与源 Oracle 行数/主键校验一致。

前置：oracle_db(1521/FREEPDB1, app_user) + postgres_db(5432/myapp_db, app_user) 均在跑，
且 Oracle 已开归档日志+补充日志（LogMiner 增量前提，通常由环境预置）。
用法：python3 oracle_resume.py [--minutes 3]
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

ORA_C, PG_C = "oracle_db", "postgres_db"
SRC_CONN = "oracle://app_user:userpassword@127.0.0.1:1521/FREEPDB1"
TGT_CONN = "postgresql://app_user:userpassword@127.0.0.1:5432/myapp_db"
SRC_DB, TGT_DB = "FREEPDB1", "myapp_db"
SCHEMA = "APP_USER"
TABLE = "FI_ORA"


def ora(sql):
    script = f"SET HEADING OFF\nSET FEEDBACK OFF\nSET PAGESIZE 0\nSET TAB OFF\nSET ECHO OFF\n{sql}\nEXIT\n"
    out = subprocess.run(
        ["docker", "exec", "-i", ORA_C, "sqlplus", "-s",
         f"app_user/userpassword@localhost:1521/{SRC_DB}"],
        input=script, capture_output=True, text=True).stdout
    # 取最后一个非空行作为结果（规避 sqlplus 回显/告警行）
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    return lines[-1] if lines else ""


def pg(sql):
    return subprocess.run(
        ["docker", "exec", PG_C, "psql", "-U", "app_user", "-d", TGT_DB, "-tAc", sql],
        capture_output=True, text=True).stdout.strip()


def ora_setup(seed_rows):
    # sqlplus：独立 SQL 语句需以 ; 结尾，PL/SQL 块以 / 结尾
    ora(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {TABLE}'; EXCEPTION WHEN OTHERS THEN NULL; END;\n/")
    ora(f"CREATE TABLE {TABLE} (ID NUMBER(10) PRIMARY KEY, VAL VARCHAR2(128));")
    ora(f"BEGIN FOR i IN 1..{seed_rows} LOOP "
        f"INSERT INTO {TABLE} VALUES (i, 'seed-'||i); END LOOP; COMMIT; END;\n/")
    cnt = ora(f"SELECT COUNT(*) FROM {TABLE};")
    print(f"[准备] 源建表并播种，当前行数={cnt}")


def pg_target_table():
    """定位 PG 目标表所在 schema（oracle→pg 落到源 schema 同名的小写 schema）。"""
    s = pg("SELECT table_schema||'.'||table_name FROM information_schema.tables "
           f"WHERE lower(table_name)=lower('{TABLE}') LIMIT 1")
    return s or None


def counts():
    src = ora(f"SELECT COUNT(*)||','||NVL(SUM(ID),0) FROM {TABLE};")
    tgt_tbl = pg_target_table()
    if not tgt_tbl:
        return src, None
    tg = pg(f"SELECT COUNT(*)||','||COALESCE(SUM(id),0) FROM {tgt_tbl}")
    return src, tg


class OraWriter(threading.Thread):
    def __init__(self, start_id):
        super().__init__(daemon=True)
        self.next_id = start_id
        self.stop = threading.Event()
        self.inserts = 0

    def run(self):
        while not self.stop.is_set():
            base = self.next_id
            # 每轮插 20 行 + 提交，再歇 1s——控制在 ~20 行/秒，避免压垮 LogMiner 增量捕获
            stmts = "".join(f"INSERT INTO {TABLE} VALUES ({base + k}, 'live-{base + k}');"
                            for k in range(20))
            ora(f"BEGIN {stmts} COMMIT; END;\n/")
            self.next_id += 20
            self.inserts += 20
            time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=3)
    args = ap.parse_args()
    token = F.login()
    print(f"✓ 登录；Oracle→PG 断点续传，故障注入 {args.minutes} 分钟（best-effort）")
    passed, failed = [], []

    old = F.get_increment_quota()
    F.set_increment_quota(100000)
    try:
        print("[准备] Oracle 源播种 3000 行 ...")
        ora_setup(3000)
        # 清目标（若存在）
        tt = pg_target_table()
        if tt:
            pg(f"TRUNCATE {tt}")

        sync_objects = f'{{"{SCHEMA}": {{"tables": ["{TABLE}"]}}}}'
        tid = F.create_task(token, f"FI-ora-{int(time.time())}", "oracle", "postgresql",
                            SRC_CONN, TGT_CONN, "fullAndIncre", sync_objects, TGT_DB, source_db=SRC_DB)
        print(f"[任务] {tid}")
        st = F.wait_status(token, tid, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            failed.append(f"未进入 INCREMENT_RUNNING（{st}）——可能 Oracle LogMiner 未就绪")
            F.stop_task(token, tid)
            return F.print_result(passed, failed)
        passed.append("全量完成并进入 INCREMENT_RUNNING")

        src, tgt = counts()
        (passed if src == tgt else failed).append(f"全量阶段一致 src={src} tgt={tgt}")

        w = OraWriter(start_id=100000)
        w.start()
        kills = []
        end = time.time() + args.minutes * 60
        next_kill = time.time() + 60
        while time.time() < end:
            time.sleep(3)
            if time.time() >= next_kill:
                eng = ["capture", "extract", "increment"][len(kills) % 3]
                pids = F.signal_child(tid, eng, signal.SIGKILL)
                if pids:
                    kills.append((eng, pids))
                    print(f"    [{time.strftime('%H:%M:%S')}] SIGKILL {eng} pid={pids} (inserts={w.inserts})")
                next_kill = time.time() + 60
                if F.get_status(token, tid) == "FAILED":
                    failed.append(f"故障注入期间 FAILED（{eng} 杀后未自愈）")
                    break
        w.stop.set()
        w.join(timeout=30)
        print(f"[写入] 结束 inserts={w.inserts}；注入 {len(kills)} 次崩溃")

        print("[校验] 等待增量追平并比对行数/主键和（最多 10 分钟，LogMiner+多次崩溃重放较慢）...")
        ok = False
        for _ in range(200):
            time.sleep(3)
            src, tgt = counts()
            if src == tgt and tgt is not None:
                ok = True
                break
            if F.get_status(token, tid) == "FAILED":
                failed.append("追平期间 FAILED")
                break
        print(f"    最终 src={src} tgt={tgt}")
        (passed if ok else failed).append(f"崩溃自愈后一致（{len(kills)} 次崩溃）")

        F.stop_task(token, tid)
        time.sleep(2)
        F.delete_task(token, tid)
        return F.print_result(passed, failed)
    finally:
        if old is not None:
            F.set_increment_quota(old)


if __name__ == "__main__":
    sys.exit(main())
