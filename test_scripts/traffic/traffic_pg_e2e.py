#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""流量复制与回放：PostgreSQL 引擎级判据。

与 traffic_e2e.py（MySQL）同构，钉的是 PG 特有的那批边界。每一条都对应一个
**实测踩出来的坑**，不是照着文档写的：

  * 参数不替换进 SQL —— PG 记 `execute <unnamed>: SELECT $1` + 另一份 Parameters；
  * `$n` 占位符 pgjdbc 不认，不改写成 `?` 会报"栏位数：0"；
  * NULL 绑定渲染成裸 NULL，与带引号的 'NULL' 是两回事；
  * PG **明文**把口令写进日志（MySQL/Oracle 都由库自己抹），必须我们脱敏；
  * `log_min_duration_statement=0` 下一次扩展协议执行记三行（parse/bind/execute），
    照单全收就是同一条语句回放三遍；
  * `ALTER SYSTEM` 会被命令行参数静默压过，不回读校验就录出空文件；
  * PG 是三家里唯一有真位点的，续录通常**不产生时间轴空洞**。

前置：docker compose -f docker-compose-synctask-traffic.yml up -d
      （trf-pg-src:55480 源、trf-pg-tgt:55481 目标，两者必须是不同实例）

用法:
    python3 test_scripts/traffic/traffic_pg_e2e.py            # 全跑
    python3 test_scripts/traffic/traffic_pg_e2e.py -k bind    # 只跑名字含 bind 的用例
"""
import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
JAR = os.path.join(ROOT, "migration-traffic", "target", "migration-traffic-1.0.0.jar")
SRC = dict(ctr="trf-pg-src", port=55480)
TGT = dict(ctr="trf-pg-tgt", port=55481)
DB = "trfdb"
PASS = "trafficpass"

RESULTS = []


# ------------------------------------------------------------------ 基础设施
def psql(cfg, sql, db=DB):
    p = subprocess.run(
        ["docker", "exec", cfg["ctr"], "psql", "-U", "postgres", "-d", db, "-tAq", "-c", sql],
        capture_output=True, text=True)
    return p.stdout.strip()


def psql_script(cfg, script, db=DB):
    p = subprocess.run(
        ["docker", "exec", "-i", cfg["ctr"], "psql", "-U", "postgres", "-d", db, "-q"],
        input=script, capture_output=True, text=True)
    return p.stdout.strip()


def check(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", name, ("  — " + str(detail)) if detail else ""))
    return ok


def task_dir(task_id):
    return os.path.join(ROOT, "files", task_id)


def write_config(task_id, extra):
    d = task_dir(task_id)
    os.makedirs(d, exist_ok=True)
    lines = ["task.id=%s" % task_id, "task.name=%s" % task_id]
    lines += ["%s=%s" % (k, v) for k, v in extra.items()]
    with open(os.path.join(d, "config.properties"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return os.path.join(d, "config.properties")


def capture_config(task_id, **over):
    cfg = {
        "source.db.host": "127.0.0.1", "source.db.port": SRC["port"],
        "source.db.username": "postgres", "source.db.password": PASS,
        "source.db.database": DB, "source.db.type": "postgresql",
        "traffic.capture.classes": "SELECT,DML,DDL,DCL",
        "traffic.capture.pg.poll.ms": 400,
        "traffic.capture.pg.group.wait.ms": 800,
    }
    cfg.update(over)
    return write_config(task_id, cfg)


def replay_config(task_id, recording_dir, **over):
    cfg = {
        "target.db.host": "127.0.0.1", "target.db.port": TGT["port"],
        "target.db.username": "postgres", "target.db.password": PASS,
        "target.db.database": DB, "target.db.type": "postgresql",
        "traffic.replay.recording.dir": recording_dir,
        "traffic.replay.classes": "SELECT,DML,DDL",
    }
    cfg.update(over)
    return write_config(task_id, cfg)


class Engine:
    """起一个 migration-traffic 子进程，并**持续排空** stdout（不排空会阻塞在写日志上）。"""

    def __init__(self, task_id, mode, config):
        self.task_id = task_id
        self.lines = []
        self.proc = subprocess.Popen(
            ["java", "-Dtask.id=" + task_id, "-jar", JAR, "--mode", mode, "--config", config],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.pump = threading.Thread(target=self._drain, daemon=True)
        self.pump.start()

    def _drain(self):
        for line in self.proc.stdout:
            self.lines.append(line.rstrip())

    def wait_log(self, needle, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(needle in l for l in self.lines):
                return True
            if self.proc.poll() is not None:
                return any(needle in l for l in self.lines)
            time.sleep(0.3)
        return False

    def stop(self, timeout=60):
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.pump.join(timeout=5)

    def kill9(self):
        self.proc.kill()
        self.proc.wait(timeout=10)
        self.pump.join(timeout=5)

    def wait_exit(self, timeout=180):
        try:
            self.proc.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            return False


def read_records(recording_dir):
    out = []
    if not os.path.isdir(recording_dir):
        return out
    for name in sorted(os.listdir(recording_dir)):
        if not name.startswith("seg-") or not name.endswith(".trf.gz"):
            continue
        try:
            with gzip.open(os.path.join(recording_dir, name), "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            out.append(json.loads(line))
                        except ValueError:
                            pass
        except (EOFError, OSError):
            pass
    return out


def read_manifest(recording_dir):
    p = os.path.join(recording_dir, "manifest.json")
    if not os.path.isfile(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def reset(task_id):
    shutil.rmtree(task_dir(task_id), ignore_errors=True)


def rec_dir(task_id):
    return os.path.join(task_dir(task_id), "traffic")


def source_log_state():
    return psql(SRC, "SELECT setting || '/' || (SELECT setting FROM pg_settings "
                     "WHERE name='log_destination') FROM pg_settings WHERE name='log_statement'")


def reset_tables(cfg):
    psql(cfg, "DROP TABLE IF EXISTS t1; DROP TABLE IF EXISTS b1;")


def wait_pg_ready(ctr, timeout=120):
    """等一次性容器真正能查询为止。

    只看 pg_isready 不够：镜像初始化脚本还会重启一次实例，那个窗口里 isready 是通过的，
    随后连接又会被拒。判据因此偶发失败，且失败信息（Connection refused）跟用例想验证的
    东西毫无关系，极易被误判成产品缺陷。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = subprocess.run(["docker", "exec", ctr, "psql", "-U", "postgres", "-tAc", "SELECT 1"],
                           capture_output=True, text=True)
        if p.returncode == 0 and "1" in p.stdout:
            time.sleep(1)
            return True
        time.sleep(1)
    return False


def restore_source():
    """把源库开关强行掰回默认值。

    每个用例开头都要做：上一轮如果留下了 log_statement=all（比如判据本身跑挂了），
    这一轮的捕获会把它当成"原值"记下来，收尾时忠实地还原成 all —— 于是源库再也回不去了。
    这正是产品侧要防的那件事，判据自己更不能踩。
    """
    psql(SRC, "ALTER SYSTEM RESET log_statement")
    psql(SRC, "ALTER SYSTEM RESET log_destination")
    psql(SRC, "ALTER SYSTEM RESET log_min_duration_statement")
    psql(SRC, "ALTER SYSTEM RESET log_duration")
    psql(SRC, "SELECT pg_reload_conf()")
    time.sleep(1.5)


def jdbc_load(java_src, cls):
    """用 fat jar 里的 pgjdbc 跑一段单文件 Java —— psql 的 \\bind 送不出真 NULL。"""
    path = os.path.join(task_dir("trf-pg-tmp"), cls + ".java")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(java_src)
    p = subprocess.run(["java", "-cp", JAR, path], capture_output=True, text=True, cwd=ROOT)
    return p.stdout.strip() + p.stderr.strip()


# ------------------------------------------------------------------ 用例
def case_timeline_and_types():
    """核心：用户样例（DDL→+5s DML→+5s SELECT）+ 类别覆盖 + 4 字节 UTF-8 + 源端报错。"""
    tid = "trf-pg-timeline"
    restore_source()
    reset(tid)
    reset_tables(SRC)
    eng = Engine(tid, "capture", capture_config(tid, **{"traffic.capture.enrich": "true"}))
    check("捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))

    psql(SRC, "CREATE TABLE t1(id int primary key, v text)")
    time.sleep(5)
    psql(SRC, "INSERT INTO t1 VALUES (1,'中文🚀emoji')")
    time.sleep(5)
    psql(SRC, "SELECT * FROM t1")
    psql(SRC, "SELECT * FROM no_such_table_pg")           # 源端报错
    psql(SRC, "CREATE USER trfleak WITH PASSWORD 'S3cretPass'")
    psql(SRC, "DROP USER trfleak")
    time.sleep(3)
    eng.stop()

    recs = read_records(rec_dir(tid))
    mani = read_manifest(rec_dir(tid))
    check("manifest 标了引擎与通道",
          mani and mani.get("engine") == "postgresql" and mani.get("captureBackend") == "PG_JSONLOG",
          "%s / %s" % (mani and mani.get("engine"), mani and mani.get("captureBackend")))
    check("录制格式为 v2（v1 装不下绑定参数）",
          mani and mani.get("format") == "synctask-traffic/2", mani and mani.get("format"))

    def find(sub):
        return [r for r in recs if sub in (r.get("q") or "")]

    ddl = find("CREATE TABLE t1")
    dml = find("INSERT INTO t1")
    sel = [r for r in recs if (r.get("q") or "").startswith("SELECT * FROM t1")]
    check("DDL 录到了", len(ddl) == 1, len(ddl))
    check("DML 录到了", len(dml) == 1, len(dml))
    check("SELECT 录到了（binlog/WAL 里没有 SELECT，这是本功能存在的理由）", len(sel) == 1, len(sel))

    if ddl and dml and sel:
        d1 = (dml[0]["t"] - ddl[0]["t"]) / 1e6
        d2 = (sel[0]["t"] - dml[0]["t"]) / 1e6
        check("录制里 DDL→DML 间隔 ≈5s", 4.0 < d1 < 6.5, "%.3fs" % d1)
        check("录制里 DML→SELECT 间隔 ≈5s", 4.0 < d2 < 6.5, "%.3fs" % d2)

    check("4 字节 UTF-8 原样保留", any("🚀" in (r.get("q") or "") for r in recs))

    err = find("no_such_table_pg")
    check("源端报错语句带 SQLSTATE（PG 的富化是免费的）",
          bool(err) and (err[0].get("e") or {}).get("st") == "42P01",
          err and (err[0].get("e") or {}).get("st"))
    check("失败语句只录一条（statement 行 + ERROR 行不能各记一条）", len(err) == 1, len(err))

    dur = [r for r in recs if (r.get("e") or {}).get("us", 0) > 0]
    check("开了富化就有源端耗时", len(dur) > 0, "%d 条带耗时" % len(dur))

    leak = find("PASSWORD")
    check("PG 明文记的口令已在落盘前脱敏",
          bool(leak) and all("S3cretPass" not in (r.get("q") or "") for r in recs),
          leak and leak[0].get("q"))
    check("脱敏过的语句标记为不可回放", bool(leak) and leak[0].get("rd") is True)

    noise = [r for r in recs if "pg_read_binary_file" in (r.get("q") or "")
             or "pg_current_logfile" in (r.get("q") or "")]
    check("采集连接自身无自噪声（会话级 SET log_statement=none）", not noise, len(noise))
    check("源库开关已还原", source_log_state().startswith("none/"), source_log_state())
    return rec_dir(tid)


def case_binds_and_null():
    """绑定参数：$n 改写、NULL 与空串、整数列上绑文本（stringtype=unspecified）。"""
    tid = "trf-pg-bind"
    restore_source()
    reset(tid)
    reset_tables(SRC)
    reset_tables(TGT)
    psql(SRC, "CREATE TABLE b1(id int primary key, v text, n int)")
    psql(TGT, "CREATE TABLE b1(id int primary key, v text, n int)")

    eng = Engine(tid, "capture", capture_config(tid))
    check("绑定用例：捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))

    out = jdbc_load("""
import java.sql.*;
public class PgBindE2E {
    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:%d/%s", "postgres", "%s")) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO b1 VALUES (?,?,?)")) {
                ps.setInt(1, 1); ps.setString(2, "中文🚀emoji"); ps.setInt(3, 42); ps.executeUpdate();
                ps.setInt(1, 2); ps.setNull(2, Types.VARCHAR); ps.setNull(3, Types.INTEGER); ps.executeUpdate();
                ps.setInt(1, 3); ps.setString(2, ""); ps.setInt(3, 0); ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT count(*) FROM b1 WHERE id = ? OR id = ?")) {
                ps.setInt(1, 1); ps.setInt(2, 2);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); System.out.println("src=" + rs.getInt(1)); }
            }
        }
    }
}
""" % (SRC["port"], DB, PASS), "PgBindE2E")
    check("源端灌数成功", "src=2" in out, out[-120:])
    time.sleep(3)
    eng.stop()

    recs = read_records(rec_dir(tid))
    ins = [r for r in recs if (r.get("q") or "").startswith("INSERT INTO b1")]
    check("扩展协议只录一条（parse/bind/execute 三行不能各记一条）", len(ins) == 3, len(ins))
    if len(ins) == 3:
        check("参数录下来了（PG 不把参数替换进 SQL）", ins[0].get("b") == ["1", "中文🚀emoji", "42"], ins[0].get("b"))
        check("NULL 绑定是 null 而不是空串或字符串 'NULL'",
              ins[1].get("b") == ["2", None, None], ins[1].get("b"))
        check("空串还是空串（与 NULL 严格区分）", ins[2].get("b") == ["3", "", "0"], ins[2].get("b"))

    # 回放
    rid = tid + "-r"
    reset(rid)
    r = Engine(rid, "replay", replay_config(rid, rec_dir(tid)))
    check("绑定用例：回放正常结束", r.wait_exit(180) and r.proc.returncode == 0,
          "returncode=%s" % r.proc.returncode)
    r.stop()

    got = psql(TGT, "SELECT string_agg(id || ':' || COALESCE(v,'<NULL>') || ':' "
                    "|| COALESCE(n::text,'<NULL>'), '|' ORDER BY id) FROM b1")
    want = "1:中文🚀emoji:42|2:<NULL>:<NULL>|3::0"
    check("回放后目标库与源库逐值一致（NULL/空串/4 字节 UTF-8 各归各位）", got == want, got)

    errs = 0
    ep = os.path.join(task_dir(rid), "traffic", "replay_errors.jsonl")
    if os.path.isfile(ep):
        errs = sum(1 for _ in open(ep, encoding="utf-8"))
    check("回放零错误（$n 没改写成 ? 的话这里全是『栏位数：0』）", errs == 0, errs)


def case_ordering():
    """顺序保真：单会话自增，回放后目标值必须与源值相等。"""
    tid = "trf-pg-order"
    restore_source()
    reset(tid)
    psql(SRC, "DROP TABLE IF EXISTS ord1")
    psql(TGT, "DROP TABLE IF EXISTS ord1")
    psql(SRC, "CREATE TABLE ord1(id int primary key, n int)")
    psql(TGT, "CREATE TABLE ord1(id int primary key, n int)")
    psql(SRC, "INSERT INTO ord1 VALUES (1,0)")
    psql(TGT, "INSERT INTO ord1 VALUES (1,0)")

    eng = Engine(tid, "capture", capture_config(tid))
    check("顺序用例：捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))
    psql_script(SRC, "".join("UPDATE ord1 SET n=n+1 WHERE id=1;\n" for _ in range(300)))
    src_n = psql(SRC, "SELECT n FROM ord1 WHERE id=1")
    check("源库自增到 300", src_n == "300", src_n)
    time.sleep(4)
    eng.stop()

    rid = tid + "-r"
    reset(rid)
    r = Engine(rid, "replay", replay_config(rid, rec_dir(tid), **{"traffic.replay.speed": "20"}))
    check("顺序用例：回放正常结束", r.wait_exit(240) and r.proc.returncode == 0,
          "returncode=%s" % r.proc.returncode)
    r.stop()
    tgt_n = psql(TGT, "SELECT n FROM ord1 WHERE id=1")
    check("回放后目标值 == 源值（少一条或乱序都不会相等）", tgt_n == src_n,
          "目标 %s / 源 %s" % (tgt_n, src_n))


def case_transaction():
    """事务边界：BEGIN…ROLLBACK 的中间 DML 不能落到目标库。"""
    tid = "trf-pg-txn"
    restore_source()
    reset(tid)
    psql(SRC, "DROP TABLE IF EXISTS tx1")
    psql(TGT, "DROP TABLE IF EXISTS tx1")
    psql(SRC, "CREATE TABLE tx1(id int primary key)")
    psql(TGT, "CREATE TABLE tx1(id int primary key)")

    eng = Engine(tid, "capture", capture_config(tid))
    check("事务用例：捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))
    psql_script(SRC, "BEGIN;\nINSERT INTO tx1 VALUES (1);\nCOMMIT;\n"
                     "BEGIN;\nINSERT INTO tx1 VALUES (2);\nROLLBACK;\n")
    time.sleep(4)
    eng.stop()

    rid = tid + "-r"
    reset(rid)
    r = Engine(rid, "replay", replay_config(rid, rec_dir(tid)))
    r.wait_exit(180)
    r.stop()
    got = psql(TGT, "SELECT COALESCE(string_agg(id::text, ',' ORDER BY id), '') FROM tx1")
    check("提交的落地、回滚的不落地", got == "1", got)


def case_dangerous_and_guards():
    """危险语句黑名单 + 同实例互锁 + 跨引擎互锁。"""
    tid = "trf-pg-danger"
    restore_source()
    reset(tid)
    psql(SRC, "DROP TABLE IF EXISTS keep1")
    psql(TGT, "DROP TABLE IF EXISTS keep1")

    eng = Engine(tid, "capture", capture_config(tid))
    check("危险语句用例：捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))
    psql(SRC, "CREATE TABLE keep1(id int)")
    # COPY … FROM PROGRAM 会在数据库服务器上执行 shell 命令，必须拦
    psql(SRC, "COPY keep1 FROM PROGRAM 'echo 1'")
    psql(SRC, "ALTER SYSTEM SET log_min_messages='warning'")
    time.sleep(4)
    eng.stop()
    psql(SRC, "ALTER SYSTEM RESET log_min_messages; SELECT pg_reload_conf()")

    rid = tid + "-r"
    reset(rid)
    r = Engine(rid, "replay", replay_config(rid, rec_dir(tid)))
    r.wait_exit(180)
    r.stop()
    blocked = [l for l in open(os.path.join(task_dir(rid), "traffic", "replay_errors.jsonl"),
                               encoding="utf-8")] if os.path.isfile(
        os.path.join(task_dir(rid), "traffic", "replay_errors.jsonl")) else []
    reasons = " ".join(blocked)
    check("COPY … FROM PROGRAM 被拦下（它能在目标库机器上执行任意命令）",
          "PROGRAM" in reasons, reasons[:120])
    check("ALTER SYSTEM 被拦下（改的是目标实例的持久化配置）",
          "ALTER SYSTEM" in reasons, reasons[:120])

    # 回放到源库自己
    sid = tid + "-same"
    reset(sid)
    s = Engine(sid, "replay", replay_config(sid, rec_dir(tid), **{"target.db.port": SRC["port"]}))
    s.wait_exit(180)
    s.stop()
    check("回放到录制源库自己被拒绝（system_identifier 互锁）",
          any("同一个 PostgreSQL 实例" in l for l in s.lines) and s.proc.returncode != 0,
          "returncode=%s" % s.proc.returncode)

    # 跨引擎
    xid = tid + "-xeng"
    reset(xid)
    x = Engine(xid, "replay", replay_config(xid, rec_dir(tid), **{"target.db.type": "mysql"}))
    x.wait_exit(120)
    x.stop()
    check("跨引擎回放被拒绝（SQL 方言不可能自动翻译）",
          any("录制来自 PostgreSQL" in l for l in x.lines) and x.proc.returncode != 0,
          "returncode=%s" % x.proc.returncode)


def case_switch_ineffective():
    """ALTER SYSTEM 被命令行参数压过时必须以 E3128 失败，而不是录出空文件。"""
    tid = "trf-pg-ineffective"
    reset(tid)
    ctr = "trf-pg-pinned"
    subprocess.run(["docker", "rm", "-f", ctr], capture_output=True)
    # 命令行里钉死 log_statement=none —— ALTER SYSTEM 改不动它，且不会有任何报错
    subprocess.run(["docker", "run", "-d", "--name", ctr,
                    "-e", "POSTGRES_PASSWORD=" + PASS, "-e", "POSTGRES_DB=" + DB,
                    "-p", "55482:5432", "docker.1ms.run/postgres:16",
                    "postgres", "-c", "logging_collector=on", "-c", "log_statement=none"],
                   capture_output=True)
    try:
        wait_pg_ready(ctr)
        eng = Engine(tid, "capture", capture_config(tid, **{"source.db.port": 55482}))
        eng.wait_exit(120)
        eng.stop()
        hit = any("已下发但未生效" in l for l in eng.lines)
        check("开关被命令行压过时以失败告终，而不是录出空文件（E3128）",
              hit and eng.proc.returncode != 0, "returncode=%s" % eng.proc.returncode)
        # 错误码走既有那条管道：files/<taskId>/binlog_output/error_status
        status = os.path.join(task_dir(tid), "binlog_output", "error_status")
        code = open(status, encoding="utf-8").read() if os.path.isfile(status) else ""
        check("上报了 E3128", "E3128" in code, code.replace("\n", " ")[:90])
    finally:
        subprocess.run(["docker", "rm", "-f", ctr], capture_output=True)


def case_no_collector():
    """logging_collector=off：预检级别的硬前置，必须以 E3127 拒绝启动。"""
    tid = "trf-pg-nocollector"
    reset(tid)
    ctr = "trf-pg-nocol"
    subprocess.run(["docker", "rm", "-f", ctr], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", ctr,
                    "-e", "POSTGRES_PASSWORD=" + PASS, "-e", "POSTGRES_DB=" + DB,
                    "-p", "55483:5432", "docker.1ms.run/postgres:16"],
                   capture_output=True)
    try:
        wait_pg_ready(ctr)
        eng = Engine(tid, "capture", capture_config(tid, **{"source.db.port": 55483}))
        eng.wait_exit(120)
        eng.stop()
        check("logging_collector=off 时拒绝启动（E3127），而不是录 0 条",
              any("logging_collector" in l for l in eng.lines) and eng.proc.returncode != 0,
              "returncode=%s" % eng.proc.returncode)
        recs = read_records(rec_dir(tid))
        check("没有产出任何录制内容", not recs, len(recs))
    finally:
        subprocess.run(["docker", "rm", "-f", ctr], capture_output=True)


def case_resume_no_gap():
    """PG 有真位点：捕获<b>崩溃</b>后重启能把停摆期间的语句补回来，不产生时间轴空洞。

    这里必须用 kill -9 而不是正常停止，而且这不是为了"制造难度"——
    正常停止会把源库的 log_statement 还原成 none，源库<b>从那一刻起就不再记日志了</b>，
    停摆期间的语句压根没被写下来，谁也补不回来。位点的价值只在"捕获死了、
    源库开关还开着"这种情形下才成立，也正是它要救的那种情形。
    """
    tid = "trf-pg-resume"
    restore_source()
    reset(tid)
    psql(SRC, "DROP TABLE IF EXISTS rs1")
    psql(SRC, "CREATE TABLE rs1(id int)")

    eng = Engine(tid, "capture", capture_config(tid))
    check("续录用例：捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))
    psql(SRC, "INSERT INTO rs1 VALUES (1) /*BEFORE_STOP*/")
    time.sleep(3)
    eng.kill9()

    # 崩溃后源库开关还开着，停摆期间的语句照样落进日志文件
    check("崩溃后源库仍在记日志（位点能救回来的前提）",
          source_log_state().startswith("all/"), source_log_state())
    psql(SRC, "INSERT INTO rs1 VALUES (2) /*DURING_GAP*/")
    time.sleep(2)

    eng2 = Engine(tid, "capture", capture_config(tid))
    check("续录：捕获重启", eng2.wait_log("流量复制已启动", 90))
    resumed = any("从位点续读" in l for l in eng2.lines)
    psql(SRC, "INSERT INTO rs1 VALUES (3) /*AFTER_RESUME*/")
    time.sleep(4)
    eng2.stop()

    recs = read_records(rec_dir(tid))
    texts = " ".join(r.get("q") or "" for r in recs)
    check("按位点续读（这是 PG 相对另外两家最实在的优势）", resumed)
    check("停摆前的语句在", "BEFORE_STOP" in texts)
    check("停摆期间的语句<b>补回来了</b>（MySQL/Oracle 做不到这一点）", "DURING_GAP" in texts)
    check("续录后的语句也在", "AFTER_RESUME" in texts)
    mani = read_manifest(rec_dir(tid))
    check("续读成功就不记时间轴空洞", mani is not None and not mani.get("gaps"),
          mani and mani.get("gaps"))
    check("续录用例收尾时源库开关已还原", source_log_state().startswith("none/"), source_log_state())


def case_restore_on_kill():
    """kill -9 后源库开关必须靠状态文件兜底还原，并且回读校验。"""
    tid = "trf-pg-kill"
    restore_source()
    reset(tid)
    eng = Engine(tid, "capture", capture_config(tid))
    check("兜底还原用例：捕获启动", eng.wait_log("PG 语句日志捕获已开启", 90))
    psql(SRC, "SELECT 1")
    time.sleep(2)
    eng.kill9()

    check("kill -9 后源库开关仍是开的（子进程没机会还原）",
          source_log_state().startswith("all/"), source_log_state())
    state = os.path.join(rec_dir(tid), "source_state.properties")
    check("兜底还原状态文件已落盘", os.path.isfile(state))
    if os.path.isfile(state):
        body = open(state, encoding="utf-8").read()
        check("状态文件标了引擎", "source.engine=postgresql" in body)
        check("四个 GUC 的原值与来源都记了",
              all(("restore.pg." + g) in body for g in
                  ("log_statement", "log_destination", "log_min_duration_statement", "log_duration")))
        check("口令不明文落盘", PASS not in body)

    r = subprocess.run(["java", "-jar", JAR, "--mode", "restore", "--task", tid],
                       cwd=ROOT, capture_output=True, text=True)
    ok = r.returncode == 0 and source_log_state().startswith("none/")
    check("兜底还原成功且已回读校验", ok, source_log_state())
    check("还原后状态文件被清掉", not os.path.isfile(state))


CASES = [
    ("timeline", case_timeline_and_types),
    ("binds", case_binds_and_null),
    ("ordering", case_ordering),
    ("transaction", case_transaction),
    ("dangerous", case_dangerous_and_guards),
    ("switch_ineffective", case_switch_ineffective),
    ("no_collector", case_no_collector),
    ("resume", case_resume_no_gap),
    ("restore_kill", case_restore_on_kill),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", dest="filter", default="", help="只跑名字含该串的用例")
    args = ap.parse_args()

    if not os.path.isfile(JAR):
        print("缺少 fat jar: %s\n请先跑: mvn -pl migration-traffic -am install -Dmaven.test.skip=true" % JAR)
        return 2
    if subprocess.run(["docker", "inspect", SRC["ctr"]], capture_output=True).returncode != 0:
        print("缺少测试容器，请先跑: docker compose -f docker-compose-synctask-traffic.yml up -d")
        return 2

    for name, fn in CASES:
        if args.filter and args.filter not in name:
            continue
        print("\n" + "=" * 66)
        print("用例: %s" % name)
        print("=" * 66)
        try:
            fn()
        except Exception as e:                      # noqa: BLE001 判据脚本要把异常也算成失败
            check("用例 %s 未跑完" % name, False, repr(e))

    print("\n" + "=" * 66)
    ok = sum(1 for _, o, _ in RESULTS if o)
    print("合计 %d 项，通过 %d，失败 %d" % (len(RESULTS), ok, len(RESULTS) - ok))
    for n, o, d in RESULTS:
        if not o:
            print("  ✗ %s  %s" % (n, d))
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
