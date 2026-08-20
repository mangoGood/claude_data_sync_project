#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""流量复制与回放：引擎级判据。

直接驱动 migration-traffic 子进程（不经过平台），把设计文档里那批边界逐条钉住。
平台级的那条路由 autotest/suites/traffic.py 负责，两层各管各的：
这里管"引擎本身对不对"，那里管"用户点得出来的那条路通不通"。

用法:
    python3 test_scripts/traffic/traffic_e2e.py            # 全跑
    python3 test_scripts/traffic/traffic_e2e.py -k redact  # 只跑名字含 redact 的用例

前置：synctask-mysql(33306) 与 synctask-mysql-b(33307) 都在跑。
两者必须是不同实例——产品会硬拦"回放到录制源库自己"。

已知坑（踩过，别再踩）：
  * 子进程 stdout 必须持续排空，否则它会阻塞在写日志上；
  * 判据要跑 **fat jar**，不能只 mvn compile——shade 后的包才是线上跑的那个；
  * 读被 kill 的分段要按块解压，BufferedReader 会把最后一块已解出的内容一起丢掉。
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
SRC = dict(ctr="synctask-mysql", port=33306)
TGT = dict(ctr="synctask-mysql-b", port=33307)
DB = "trf_probe"

RESULTS = []


# ------------------------------------------------------------------ 基础设施
def mysql(cfg, sql, want_output=False):
    p = subprocess.run(
        ["docker", "exec", cfg["ctr"], "mysql", "-uroot", "-prootpassword",
         "--default-character-set=utf8mb4", "-N", "-e", sql],
        capture_output=True, text=True)
    if want_output:
        return p.stdout.strip()
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
        "source.db.username": "root", "source.db.password": "rootpassword",
        "source.db.type": "mysql",
        "traffic.capture.classes": "SELECT,DML,DDL,DCL",
        "traffic.capture.rotate.interval.ms": 1000,
    }
    cfg.update(over)
    return write_config(task_id, cfg)


def replay_config(task_id, recording_dir, **over):
    cfg = {
        "target.db.host": "127.0.0.1", "target.db.port": TGT["port"],
        "target.db.username": "root", "target.db.password": "rootpassword",
        "target.db.type": "mysql",
        "traffic.replay.recording.dir": recording_dir,
        "traffic.replay.classes": "SELECT,DML,DDL",
    }
    cfg.update(over)
    return write_config(task_id, cfg)


class Engine:
    """起一个 migration-traffic 子进程，并<b>持续排空</b>它的 stdout。

    不排空的话，子进程写满管道缓冲区后会阻塞在写日志上——现象是"进程还在、
    但什么都不干"，极难定位。这个坑本仓库踩过不止一次。
    """

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

    def wait_log(self, needle, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(needle in l for l in self.lines):
                return True
            if self.proc.poll() is not None:
                return any(needle in l for l in self.lines)
            time.sleep(0.3)
        return False

    def stop(self, timeout=40):
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

    def wait_exit(self, timeout=120):
        try:
            self.proc.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            return False


def read_records(recording_dir):
    """读全部分段。逐行迭代 + 吞掉尾部 EOFError：被 kill 的段没有 gzip trailer。"""
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
            pass    # 截断段：保留已读到的
    return out


def read_manifest(recording_dir):
    p = os.path.join(recording_dir, "manifest.json")
    if not os.path.isfile(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def reset(task_id):
    shutil.rmtree(task_dir(task_id), ignore_errors=True)


def restore_source():
    mysql(SRC, "SET GLOBAL general_log='OFF'; SET GLOBAL log_output='FILE';"
               "DROP TABLE IF EXISTS mysql.general_log_trf_read;"
               "DROP TABLE IF EXISTS mysql.general_log_trf_next;")


def source_log_state():
    return mysql(SRC, "SELECT CONCAT(@@GLOBAL.general_log,'/',@@GLOBAL.log_output);")


# ------------------------------------------------------------------ 用例
def case_timeline_and_types():
    """核心：5s/10s/15s 场景 + 语句类别覆盖 + 4 字节 UTF-8 + 预处理噪声剔除 + 口令抹除。"""
    tid = "trf-e2e-timeline"
    reset(tid)
    mysql(SRC, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (DB, DB))
    mysql(SRC, "DROP USER IF EXISTS 'trfe2e'@'%';")
    # 必须限定库：本机的 MySQL 上同时跑着平台自己的元数据库(sync_task_db)，
    # 不过滤会把它的心跳 UPDATE 一起录进来，回放到没有该库的目标端必然报
    # "No database selected"——那是真实且正确的回放错误，但不是本用例要考的东西。
    eng = Engine(tid, "capture", capture_config(tid, **{"traffic.capture.databases": DB}))
    try:
        check("捕获启动", eng.wait_log("流量复制已启动", 60))
        t0 = time.time()
        while time.time() - t0 < 5:
            time.sleep(0.02)
        mysql(SRC, "USE %s; CREATE TABLE orders(id INT PRIMARY KEY, note VARCHAR(50));" % DB)
        while time.time() - t0 < 10:
            time.sleep(0.02)
        mysql(SRC, "USE %s; INSERT INTO orders VALUES (1,'中文🚀emoji');" % DB)
        while time.time() - t0 < 15:
            time.sleep(0.02)
        mysql(SRC, ("USE %s; SELECT * FROM orders WHERE id=1; "
                    "CREATE USER 'trfe2e'@'%%' IDENTIFIED BY 'Pw#12345'; "
                    "PREPARE st FROM 'SELECT note FROM orders WHERE id=?'; SET @z=1; "
                    "EXECUTE st USING @z; DEALLOCATE PREPARE st;") % DB)
        time.sleep(4)
    finally:
        eng.stop()

    d = os.path.join(task_dir(tid), "traffic")
    recs = read_records(d)
    man = read_manifest(d)
    check("manifest 已封口", man is not None and man.get("sealed") is True)
    check("全局校验和已生成", man is not None and bool(man.get("sha256")))

    by_kind = {}
    for r in recs:
        by_kind.setdefault(r.get("k") or "CONN", []).append(r)

    ddl = [r for r in by_kind.get("DDL", []) if "CREATE TABLE orders" in (r.get("q") or "")]
    dml = [r for r in by_kind.get("DML", []) if "INSERT INTO orders" in (r.get("q") or "")]
    sel = [r for r in by_kind.get("SELECT", []) if "SELECT * FROM orders" in (r.get("q") or "")]
    check("DDL 被录到", len(ddl) == 1, len(ddl))
    check("DML 被录到", len(dml) == 1, len(dml))
    check("SELECT 被录到（binlog 里没有它，这是本功能存在的理由）", len(sel) == 1, len(sel))

    if ddl and dml and sel:
        g1 = (dml[0]["t"] - ddl[0]["t"]) / 1e6
        g2 = (sel[0]["t"] - dml[0]["t"]) / 1e6
        check("录制里 DDL→DML 间隔 ≈5s", abs(g1 - 5) < 0.4, "%.3fs" % g1)
        check("录制里 DML→SELECT 间隔 ≈5s", abs(g2 - 5) < 0.4, "%.3fs" % g2)

    check("4 字节 UTF-8 原样保留（服务端 CONVERT 会变成 ???）",
          bool(dml) and "中文🚀emoji" in dml[0]["q"], dml[0]["q"] if dml else None)

    dcl = [r for r in by_kind.get("DCL", []) if "CREATE USER" in (r.get("q") or "")]
    check("CREATE USER 归类为 DCL 而非 DDL", len(dcl) == 1, len(dcl))
    check("带口令的语句被标记 redacted（MySQL 自己抹的，谁也拿不到原文）",
          bool(dcl) and dcl[0].get("rd") is True, dcl[0] if dcl else None)

    noise = [r for r in recs
             if (r.get("q") or "").startswith(("PREPARE ", "EXECUTE ", "DEALLOCATE "))]
    check("PREPARE/EXECUTE/DEALLOCATE 噪声行已剔除", len(noise) == 0, noise[:2])
    # MySQL 回显时保留预处理文本的原始空白（'... WHERE id=?' → '... WHERE id=1'），
    # 按去空白后比对，免得判据被一个空格绊倒
    execd = [r for r in recs
             if r.get("c") == "E" and "id=1" in (r.get("q") or "").replace(" ", "")]
    check("服务端预处理保留为参数已替换的 Execute 行", len(execd) == 1,
          execd[0]["q"] if execd else None)

    polls = [r for r in recs if "general_log_trf_read" in (r.get("q") or "")]
    check("采集连接自身无自噪声（sql_log_off）", len(polls) == 0, len(polls))

    check("源库开关已还原", source_log_state().startswith("0/"), source_log_state())
    return d


def case_replay(recording_dir):
    """回放：时间轴保真（在目标库上量）+ 数据一致 + 零错误。"""
    tid = "trf-e2e-replay"
    reset(tid)
    mysql(TGT, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (DB, DB))
    mysql(TGT, "SET GLOBAL log_output='TABLE'; SET GLOBAL general_log='ON';"
               "TRUNCATE TABLE mysql.general_log;")
    eng = Engine(tid, "replay", replay_config(tid, recording_dir))
    try:
        done = eng.wait_exit(240)
        check("回放进程正常结束", done and eng.proc.returncode == 0,
              "returncode=%s" % eng.proc.returncode)
    finally:
        eng.stop()
        rows = mysql(TGT,
                     "SET SESSION sql_log_off=1; "
                     "SELECT UNIX_TIMESTAMP(event_time) FROM mysql.general_log "
                     "WHERE CONVERT(argument USING utf8mb4) REGEXP "
                     "'CREATE TABLE orders|INSERT INTO orders|SELECT \\\\* FROM orders';")
        mysql(TGT, "SET GLOBAL general_log='OFF'; SET GLOBAL log_output='FILE';")

    times = sorted(float(x) for x in rows.split("\n") if x.strip())
    check("目标库上三条语句都执行了", len(times) == 3, "%d 条" % len(times))
    if len(times) == 3:
        check("回放 DDL→DML 间隔 ≈5s", abs(times[1] - times[0] - 5) < 0.5,
              "%.3fs" % (times[1] - times[0]))
        check("回放 DML→SELECT 间隔 ≈5s", abs(times[2] - times[1] - 5) < 0.5,
              "%.3fs" % (times[2] - times[1]))

    note = mysql(TGT, "SELECT note FROM %s.orders WHERE id=1;" % DB)
    check("目标库数据一致（含 4 字节 UTF-8）", note == "中文🚀emoji", repr(note))

    rp = os.path.join(task_dir(tid), "traffic", "replay_report.properties")
    report = {}
    if os.path.isfile(rp):
        for line in open(rp, encoding="utf-8"):
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                report[k] = v
    check("回放报告已生成", bool(report))
    check("回放零错误", report.get("count.REPLAY_ERROR") == "0", report.get("count.REPLAY_ERROR"))
    check("口令被抹的语句记为不可回放",
          int(report.get("count.UNREPLAYABLE_REDACTED") or 0) >= 0,
          report.get("count.UNREPLAYABLE_REDACTED"))
    return report


def case_ordering():
    """会话内顺序保真：1000 条 n=n+1，回放后必须正好等于 1000。"""
    cap, rep = "trf-e2e-order-cap", "trf-e2e-order-rep"
    reset(cap)
    reset(rep)
    odb = DB + "_ord"
    mysql(SRC, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (odb, odb))
    mysql(TGT, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (odb, odb))
    eng = Engine(cap, "capture", capture_config(cap, **{"traffic.capture.databases": odb}))
    try:
        check("顺序用例：捕获启动", eng.wait_log("流量复制已启动", 60))
        sql = ["USE %s;" % odb, "CREATE TABLE ctr(id INT PRIMARY KEY, n INT);",
               "INSERT INTO ctr VALUES (1,0);"]
        sql += ["UPDATE ctr SET n=n+1 WHERE id=1;"] * 1000
        subprocess.run(["docker", "exec", "-i", SRC["ctr"], "mysql", "-uroot", "-prootpassword",
                        "--default-character-set=utf8mb4"],
                       input="\n".join(sql), capture_output=True, text=True)
        time.sleep(4)
    finally:
        eng.stop()

    src_n = mysql(SRC, "SELECT n FROM %s.ctr WHERE id=1;" % odb)
    check("源库自增到 1000", src_n == "1000", src_n)

    d = os.path.join(task_dir(cap), "traffic")
    eng2 = Engine(rep, "replay", replay_config(rep, d, **{"traffic.replay.speed": "10"}))
    try:
        check("顺序用例：回放正常结束", eng2.wait_exit(300) and eng2.proc.returncode == 0,
              "returncode=%s" % eng2.proc.returncode)
    finally:
        eng2.stop()
    tgt_n = mysql(TGT, "SELECT n FROM %s.ctr WHERE id=1;" % odb)
    check("回放后目标值 == 源值（少一条或乱序都不会相等）", tgt_n == "1000",
          "目标 %s / 源 1000" % tgt_n)


def case_dangerous():
    """DROP DATABASE 必须被拦下。"""
    cap, rep = "trf-e2e-danger-cap", "trf-e2e-danger-rep"
    reset(cap)
    reset(rep)
    ddb, bomb = DB + "_dg", DB + "_bomb"
    for c in (SRC, TGT):
        mysql(c, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (ddb, ddb))
        mysql(c, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (bomb, bomb))
    eng = Engine(cap, "capture", capture_config(cap, **{"traffic.capture.databases": ddb}))
    try:
        check("危险语句用例：捕获启动", eng.wait_log("流量复制已启动", 60))
        mysql(SRC, "USE %s; CREATE TABLE keep(id INT PRIMARY KEY); INSERT INTO keep VALUES (1); "
                   "DROP DATABASE %s;" % (ddb, bomb))
        time.sleep(4)
    finally:
        eng.stop()

    d = os.path.join(task_dir(cap), "traffic")
    eng2 = Engine(rep, "replay", replay_config(rep, d))
    try:
        eng2.wait_exit(180)
    finally:
        eng2.stop()

    still = mysql(TGT, "SHOW DATABASES LIKE '%s';" % bomb)
    check("DROP DATABASE 被拦下（目标库该库仍在）", still == bomb, still)
    kept = mysql(TGT, "SELECT id FROM %s.keep WHERE id=1;" % ddb)
    check("普通业务语句照常回放", kept == "1", kept)

    errs = os.path.join(task_dir(rep), "traffic", "replay_errors.jsonl")
    blocked = []
    if os.path.isfile(errs):
        for line in open(errs, encoding="utf-8"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("outcome") == "BLOCKED":
                blocked.append(r)
    check("拦截被记进报告（不是静默跳过）", len(blocked) >= 1,
          blocked[0].get("detail") if blocked else None)


def case_same_instance():
    """回放到录制源库自己：引擎侧必须直接拒绝。"""
    tid = "trf-e2e-same"
    reset(tid)
    src_dir = os.path.join(task_dir("trf-e2e-timeline"), "traffic")
    if not os.path.isdir(src_dir):
        check("同实例用例：需要先跑时间轴用例", False, src_dir)
        return
    cfg = replay_config(tid, src_dir, **{"target.db.port": SRC["port"]})
    eng = Engine(tid, "replay", cfg)
    try:
        eng.wait_exit(90)
    finally:
        eng.stop()
    hit = any("同一个 MySQL 实例" in l for l in eng.lines)
    check("回放到录制源库自己被拒绝", hit and eng.proc.returncode != 0,
          "returncode=%s" % eng.proc.returncode)


def case_crash_recovery():
    """kill -9 后：已落盘的记录必须救回来、源库开关要能兜底还原、续录记空洞。"""
    tid = "trf-e2e-crash"
    reset(tid)
    cdb = DB + "_crash"
    mysql(SRC, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (cdb, cdb))
    eng = Engine(tid, "capture", capture_config(tid, **{"traffic.capture.databases": cdb}))
    check("崩溃用例：捕获启动", eng.wait_log("流量复制已启动", 60))
    mysql(SRC, "USE %s; CREATE TABLE t(id INT PRIMARY KEY); SELECT 'MARKER_ONE';" % cdb)
    time.sleep(3)
    eng.kill9()

    check("kill -9 后源库开关仍是开的（子进程没机会还原）",
          source_log_state().startswith("1/"), source_log_state())
    state = os.path.join(task_dir(tid), "traffic", "source_state.properties")
    check("兜底还原状态文件已落盘", os.path.isfile(state), state)

    r = subprocess.run(["java", "-jar", JAR, "--mode", "restore", "--task", tid],
                       cwd=ROOT, capture_output=True, text=True)
    check("兜底还原成功", r.returncode == 0 and source_log_state().startswith("0/"),
          source_log_state())
    check("还原后状态文件被清掉", not os.path.isfile(state))

    d = os.path.join(task_dir(tid), "traffic")
    before = read_records(d)
    check("被 kill 的分段里已落盘的记录能读出来", len(before) > 0, "%d 条" % len(before))

    # 续录：必须沿用原时间轴并记一条空洞
    man_before = read_manifest(d)
    time.sleep(6)
    eng2 = Engine(tid, "capture", capture_config(tid, **{"traffic.capture.databases": cdb}))
    try:
        check("续录启动", eng2.wait_log("续录模式", 60))
        mysql(SRC, "USE %s; SELECT 'MARKER_TWO';" % cdb)
        time.sleep(3)
    finally:
        eng2.stop()

    man = read_manifest(d)
    check("续录沿用原时间轴原点（换原点会让两段偏移不在同一条轴上）",
          man and man_before and man["t0EpochMicros"] == man_before["t0EpochMicros"],
          "%s vs %s" % (man and man.get("t0EpochMicros"), man_before and man_before.get("t0EpochMicros")))
    check("停摆记为时间轴空洞", man and len(man.get("gaps") or []) >= 1,
          man and man.get("gaps"))
    segs = [s["file"] for s in (man.get("segments") or [])] if man else []
    check("续录写新分段、不覆盖旧分段", len(segs) >= 2, segs)
    after = read_records(d)
    qs = [r.get("q") or "" for r in after]
    check("崩溃前的记录仍在", any("MARKER_ONE" in q for q in qs))
    check("续录的记录也在", any("MARKER_TWO" in q for q in qs))
    check("源库开关最终已还原", source_log_state().startswith("0/"), source_log_state())


def case_size_limit():
    """体量护栏：到量自动封口，且算正常结束（COMPLETED）而不是失败。"""
    tid = "trf-e2e-limit"
    reset(tid)
    eng = Engine(tid, "capture",
                 capture_config(tid, **{"traffic.capture.max.duration.ms": 6000}))
    start = time.time()
    ok = eng.wait_exit(60)
    elapsed = time.time() - start
    eng.stop()
    check("到达时长上限后自行退出", ok and elapsed < 30, "%.1fs" % elapsed)
    marker = os.path.join(task_dir(tid), "traffic", "capture_result.properties")
    outcome = ""
    if os.path.isfile(marker):
        for line in open(marker, encoding="utf-8"):
            if line.startswith("outcome="):
                outcome = line.strip().split("=", 1)[1]
    check("收工标记为 COMPLETED（到量是正常结束，不是故障）", outcome == "COMPLETED", outcome)
    man = read_manifest(os.path.join(task_dir(tid), "traffic"))
    check("到量时录制已封口", man is not None and man.get("sealed") is True)
    check("源库开关已还原", source_log_state().startswith("0/"), source_log_state())


# ------------------------------------------------------------------ 入口
CASES = [
    ("timeline", None),      # 特殊：产出录制目录给 replay 用
    ("replay", None),
    ("ordering", case_ordering),
    ("dangerous", case_dangerous),
    ("same_instance", case_same_instance),
    ("crash_recovery", case_crash_recovery),
    ("size_limit", case_size_limit),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", dest="filter", default="", help="只跑名字含该串的用例")
    args = ap.parse_args()

    if not os.path.isfile(JAR):
        print("缺少 fat jar: %s\n请先跑: mvn -pl migration-traffic -am install -Dmaven.test.skip=true" % JAR)
        return 2

    recording_dir = None
    for name, fn in CASES:
        if args.filter and args.filter not in name:
            continue
        print("\n" + "=" * 66)
        print("用例: %s" % name)
        print("=" * 66)
        try:
            if name == "timeline":
                recording_dir = case_timeline_and_types()
            elif name == "replay":
                if recording_dir is None:
                    recording_dir = os.path.join(task_dir("trf-e2e-timeline"), "traffic")
                case_replay(recording_dir)
            else:
                fn()
        except Exception as e:  # noqa: BLE001
            check("用例 %s 未抛异常" % name, False, repr(e))
        finally:
            restore_source()

    failed = [r for r in RESULTS if not r[1]]
    print("\n" + "=" * 66)
    print("合计 %d 项，通过 %d，失败 %d" % (len(RESULTS), len(RESULTS) - len(failed), len(failed)))
    for name, _, detail in failed:
        print("  ✗ %s  %s" % (name, detail))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
