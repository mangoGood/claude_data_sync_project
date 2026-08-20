#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""流量复制与回放（TRAFFIC_CAPTURE / TRAFFIC_REPLAY）端到端用例。

全程走平台 REST API，不直接驱动引擎——这一层要证明的是"用户点得出来的那条路能跑通"。

三条用例：
  1. **时间轴保真**（核心）：源库按 5s / 10s / 15s 三个时刻各执行一条 DDL/DML/SELECT，
     回放后在目标库上用 general_log 量出实际执行时刻，要求<b>间隔</b>与录制一致。
     这是本功能唯一真正的验收标准：录得下来不算数，按原节奏放得出来才算。
  2. **顺序保真**：单会话 500 条 `n=n+1`，回放后目标值必须等于 500。
     少一条、乱一次序，结果都不会等于 500——这比逐条比对更能一眼看出问题。
  3. **安全边界**：DROP DATABASE 必须被拦下；回放到录制源库自己必须被预检硬拦。

源库开关的还原在每条用例结束时都会核一次：不还原会让源库把
mysql.general_log 一路写到磁盘满，是本功能最大的运维风险。
"""
import time

from ..framework import config as C
from ..framework import endpoints as E
from ..framework.case import suite

P = C.PREFIX
DB = "%s_traffic" % P


def _src():
    return E.SqlEndpoint(C.MYSQL, DB)


def _tgt():
    return E.SqlEndpoint(C.MYSQL_B, DB)


def _requires():
    """两端必须是<b>不同实例</b>：回放到录制源库自己会被产品硬拦（这正是设计要的）。"""
    try:
        src, tgt = _src(), _tgt()
    except Exception as e:  # noqa: BLE001
        return False, "端点不可用: %s" % e
    try:
        with src.conn() as c1, tgt.conn() as c2:
            u1 = _scalar(c1, "SELECT @@server_uuid")
            u2 = _scalar(c2, "SELECT @@server_uuid")
    except Exception as e:  # noqa: BLE001
        return False, "MySQL 连接失败: %s" % e
    if u1 == u2:
        return False, "MYSQL 与 MYSQL_B 是同一个实例，无法验证回放（产品会硬拦同实例回放）"
    return True, ""


def _scalar(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


_NO_DB = object()


def _exec(ep, sql, db=_NO_DB):
    """默认连到 endpoint 自己的库。

    框架的 {@code conn(db=None)} 把 None 原样传给驱动 = <b>不选库</b>，
    不显式给就会撞上 "No database selected"。建库/删库这类语句传 db=""。
    """
    target = ep.db if db is _NO_DB else (db or None)
    with ep.conn(db=target) as c:
        cur = c.cursor()
        for stmt in [x for x in sql.split(";") if x.strip()]:
            cur.execute(stmt)
            # SELECT 的结果不取走就关游标 → mysql-connector 抛 "Unread result found"。
            # 而本套用例<b>必须</b>发 SELECT（流量复制的价值就在于 binlog 里没有它）。
            if cur.with_rows:
                cur.fetchall()
        cur.close()


# ----------------------------------------------------------------- 公共流程
def _new_capture(ctx, name, databases, extra_cfg=None):
    api = ctx.api
    # 上一条用例的捕获进程刚停，源库上的会话锁要等连接被服务端回收才释放。
    # 引擎侧已经会等（LOCK_WAIT_MS），这里再让一让，避免把等待算进用例耗时。
    time.sleep(3)
    wid = api.create_workflow(name, "mysql", "mysql", task_type="TRAFFIC_CAPTURE")
    ctx.reg_task(wid)
    api.config_workflow(wid, {
        "sourceConnection": _src().conn_str(),
        "sourceType": "mysql", "targetType": "mysql",
        "sourceDbName": databases, "migrationMode": "trafficCapture",
    })
    cfg = {"captureDatabases": databases, "captureClasses": "SELECT,DML,DDL"}
    cfg.update(extra_cfg or {})
    d = api.put("/api/traffic/config/%s" % wid, cfg)
    ctx.require("流量复制配置保存", d.get("success"), d.get("message"))
    api.launch(wid)
    ok, st = api.wait_status(wid, "TRAFFIC_CAPTURING", timeout=120)
    ctx.require("进入 TRAFFIC_CAPTURING", ok, "实际=%s" % st)
    return wid


def _stop_and_sync(ctx, wid):
    api = ctx.api
    api.stop(wid)
    api.wait_status(wid, {"COMPLETED", "PAUSED", "FAILED"}, timeout=120, fail_fast=False)
    # 停止后子进程才封口，给它一点时间
    time.sleep(6)
    d = api.post("/api/traffic/recordings/sync/%s" % wid, {})
    ctx.require("录制元数据同步", d.get("success"), d.get("message"))
    rec = d.get("data") or {}
    ctx.check("录制已封口", rec.get("sealed") is True, rec.get("sealed"))
    ctx.check("录制条数 > 0", (rec.get("recordCount") or 0) > 0, rec.get("recordCount"))

    # 源库开关必须回到原值，否则源库会一直写日志表直到磁盘满
    with _src().conn() as c:
        gl = str(_scalar(c, "SELECT @@GLOBAL.general_log"))
    ctx.check("源库 general_log 已还原", gl in ("0", "OFF"), "实际=%s" % gl)
    return rec


def _new_replay(ctx, name, recording_id, classes="SELECT,DML,DDL", extra_cfg=None):
    api = ctx.api
    wid = api.create_workflow(name, "mysql", "mysql", task_type="TRAFFIC_REPLAY")
    ctx.reg_task(wid)
    api.config_workflow(wid, {
        "targetConnection": _tgt().conn_str(),
        "sourceType": "mysql", "targetType": "mysql",
        "targetDbName": DB, "migrationMode": "trafficReplay",
    })
    cfg = {"replayRecordingId": recording_id, "replayClasses": classes,
           "replayMaxSessions": 100}
    cfg.update(extra_cfg or {})
    d = api.put("/api/traffic/config/%s" % wid, cfg)
    ctx.require("回放配置保存", d.get("success"), d.get("message"))
    return wid


def _report(ctx, wid):
    d = ctx.api.get("/api/traffic/replay-report/%s" % wid)
    return d.get("data") or {}


# ----------------------------------------------------------------- 用例 1
def run_timeline(ctx):
    """录制 5s/10s/15s 三条语句，回放后在目标库上量实际间隔。"""
    api = ctx.api
    src, tgt = _src(), _tgt()
    ctx.reg_sql_db("MYSQL", DB)
    ctx.reg_sql_db("MYSQL_B", DB)
    _exec(src, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s" % (DB, DB), db="")
    _exec(tgt, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s" % (DB, DB), db="")

    ctx.step("启动流量复制")
    cap = _new_capture(ctx, "%s-流量复制-时间轴" % P, DB)

    ctx.step("源库按 5s / 10s / 15s 执行 DDL / DML / SELECT")
    t0 = time.time()
    _wait_until(t0, 5)
    _exec(src, "CREATE TABLE orders(id INT PRIMARY KEY, note VARCHAR(50))")
    _wait_until(t0, 10)
    _exec(src, "INSERT INTO orders VALUES (1,'中文🚀emoji')")
    _wait_until(t0, 15)
    _exec(src, "SELECT * FROM orders WHERE id=1")
    time.sleep(4)

    rec = _stop_and_sync(ctx, cap)

    ctx.step("在目标库上打开语句日志，用它量回放的实际执行时刻")
    _exec(tgt, "SET GLOBAL log_output='TABLE'", db="")
    _exec(tgt, "SET GLOBAL general_log='ON'", db="")
    _exec(tgt, "TRUNCATE TABLE mysql.general_log", db="")
    try:
        rep = _new_replay(ctx, "%s-流量回放-时间轴" % P, rec["id"])
        api.launch(rep)
        ok, st = api.wait_status(rep, {"COMPLETED", "FAILED"}, timeout=180, fail_fast=False)
        ctx.check("回放跑到 COMPLETED", st == "COMPLETED", "实际=%s" % st)
        times = _measure(tgt)
    finally:
        _exec(tgt, "SET GLOBAL general_log='OFF'", db="")
        _exec(tgt, "SET GLOBAL log_output='FILE'", db="")

    ctx.check("目标库三条语句都执行到了", len(times) == 3, "实际 %d 条: %s" % (len(times), times))
    if len(times) == 3:
        gap1 = times[1] - times[0]
        gap2 = times[2] - times[1]
        # 源端两个间隔各 5s；允许 ±0.5s（调度抖动 + 语句本身耗时）
        ctx.check("DDL→DML 间隔保真", abs(gap1 - 5.0) < 0.5, "实际 %.3fs（期望 5s）" % gap1)
        ctx.check("DML→SELECT 间隔保真", abs(gap2 - 5.0) < 0.5, "实际 %.3fs（期望 5s）" % gap2)

    ctx.step("校验目标库数据")
    with tgt.conn() as c:
        note = _scalar(c, "SELECT note FROM %s.orders WHERE id=1" % DB)
    ctx.check("4 字节 UTF-8 原样落地", note == "中文🚀emoji", repr(note))

    r = _report(ctx, rep)
    ctx.check("回放零错误", r.get("count.REPLAY_ERROR") == "0", r.get("count.REPLAY_ERROR"))
    ctx.step("回放报告：偏差 P50/P95/P99 = %s/%s/%s ms" %
             (r.get("skew.p50Ms"), r.get("skew.p95Ms"), r.get("skew.p99Ms")))


def _wait_until(t0, secs):
    while time.time() - t0 < secs:
        time.sleep(0.02)


def _measure(tgt):
    """从目标库的 general_log 里取三条标志语句的执行时刻（epoch 秒，带微秒）。"""
    sql = ("SELECT UNIX_TIMESTAMP(event_time) FROM mysql.general_log "
           "WHERE CONVERT(argument USING utf8mb4) REGEXP "
           "'CREATE TABLE orders|INSERT INTO orders|SELECT \\\\* FROM orders'")
    out = []
    with tgt.conn() as c:
        cur = c.cursor()
        cur.execute("SET SESSION sql_log_off=1")
        cur.execute(sql)
        for row in cur.fetchall():
            out.append(float(row[0]))
        cur.close()
    return sorted(out)


# ----------------------------------------------------------------- 用例 2
def run_ordering(ctx):
    """单会话 500 条自增，回放后目标值必须等于 500 —— 少一条或乱一次序都不会相等。"""
    api = ctx.api
    src, tgt = _src(), _tgt()
    ctx.reg_sql_db("MYSQL", DB)
    ctx.reg_sql_db("MYSQL_B", DB)
    _exec(src, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s" % (DB, DB), db="")
    _exec(tgt, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s" % (DB, DB), db="")

    cap = _new_capture(ctx, "%s-流量复制-顺序" % P, DB)

    ctx.step("单会话连续 500 条 n=n+1")
    with src.conn(db=DB) as c:
        cur = c.cursor()
        cur.execute("CREATE TABLE ctr(id INT PRIMARY KEY, n INT)")
        cur.execute("INSERT INTO ctr VALUES (1,0)")
        for _ in range(500):
            cur.execute("UPDATE ctr SET n=n+1 WHERE id=1")
        cur.close()
    time.sleep(4)

    with src.conn(db=DB) as c:
        src_n = _scalar(c, "SELECT n FROM ctr WHERE id=1")
    ctx.require("源库自增到 500", src_n == 500, src_n)

    rec = _stop_and_sync(ctx, cap)

    # 高倍速回放：压过目标库的处理能力，正好检验"落后也绝不乱序"
    rep = _new_replay(ctx, "%s-流量回放-顺序" % P, rec["id"], extra_cfg={"replaySpeed": 10})
    api.launch(rep)
    ok, st = api.wait_status(rep, {"COMPLETED", "FAILED"}, timeout=300, fail_fast=False)
    ctx.check("回放跑到 COMPLETED", st == "COMPLETED", "实际=%s" % st)

    with tgt.conn() as c:
        tgt_n = _scalar(c, "SELECT n FROM %s.ctr WHERE id=1" % DB)
    ctx.check("目标库自增结果与源库一致", tgt_n == 500,
              "目标 %s / 源 500（不等即说明回放丢了语句或乱了序）" % tgt_n)

    r = _report(ctx, rep)
    ctx.check("回放零错误", r.get("count.REPLAY_ERROR") == "0", r.get("count.REPLAY_ERROR"))


# ----------------------------------------------------------------- 用例 3
def run_safety(ctx):
    """危险语句必须被拦下；回放到录制源库自己必须被预检硬拦。"""
    api = ctx.api
    src, tgt = _src(), _tgt()
    bomb = "%s_traffic_bomb" % P
    ctx.reg_sql_db("MYSQL", DB)
    ctx.reg_sql_db("MYSQL_B", DB)
    ctx.reg_sql_db("MYSQL", bomb)
    ctx.reg_sql_db("MYSQL_B", bomb)
    for ep in (src, tgt):
        _exec(ep, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s" % (DB, DB), db="")
        _exec(ep, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s" % (bomb, bomb), db="")

    cap = _new_capture(ctx, "%s-流量复制-安全" % P, DB)

    ctx.step("源库执行一条 DROP DATABASE（回放时必须被拦）")
    _exec(src, "CREATE TABLE keep(id INT PRIMARY KEY)")
    _exec(src, "INSERT INTO keep VALUES (1)")
    _exec(src, "DROP DATABASE %s" % bomb)
    time.sleep(4)

    rec = _stop_and_sync(ctx, cap)

    ctx.step("回放到<b>录制源库自己</b>：预检必须硬拦（E3124）")
    same = api.create_workflow("%s-流量回放-同实例" % P, "mysql", "mysql",
                               task_type="TRAFFIC_REPLAY")
    ctx.reg_task(same)
    api.config_workflow(same, {
        "targetConnection": _src().conn_str(), "sourceType": "mysql",
        "targetType": "mysql", "migrationMode": "trafficReplay"})
    api.put("/api/traffic/config/%s" % same, {"replayRecordingId": rec["id"]})
    pc = (api.post("/api/traffic/precheck/%s" % same, {}).get("data") or {})
    iso = next((c for c in (pc.get("checks") or []) if c.get("checkName") == "目标实例隔离"), None)
    ctx.check("同实例被预检判 FAIL", iso is not None and iso.get("status") == "FAIL", iso)
    d = api.post("/api/workflows/%s/launch" % same, {})
    ctx.check("同实例回放被拒绝启动", not d.get("success"), d.get("message"))

    ctx.step("回放到另一实例：DROP DATABASE 必须被拦下，业务语句照常执行")
    rep = _new_replay(ctx, "%s-流量回放-安全" % P, rec["id"])
    api.launch(rep)
    ok, st = api.wait_status(rep, {"COMPLETED", "FAILED"}, timeout=180, fail_fast=False)
    ctx.check("回放跑到 COMPLETED", st == "COMPLETED", "实际=%s" % st)

    with tgt.conn() as c:
        still = _scalar(c, "SHOW DATABASES LIKE '%s'" % bomb)
        kept = _scalar(c, "SELECT id FROM %s.keep WHERE id=1" % DB)
    ctx.check("DROP DATABASE 被拦下，目标库该库仍在", still == bomb, still)
    ctx.check("普通业务语句照常回放", kept == 1, kept)

    r = _report(ctx, rep)
    ctx.check("报告里记了拦截", int(r.get("count.BLOCKED") or 0) >= 1, r.get("count.BLOCKED"))


# ----------------------------------------------------------------- 注册
@suite("traffic_timeline", "MySQL 流量复制→回放（时间轴保真）", "traffic",
       est_secs=240, requires=_requires, tags=("traffic", "mysql"))
def _t1(ctx):
    return run_timeline(ctx)


@suite("traffic_ordering", "MySQL 流量回放（会话内顺序保真）", "traffic",
       est_secs=200, requires=_requires, tags=("traffic", "mysql"))
def _t2(ctx):
    return run_ordering(ctx)


@suite("traffic_safety", "MySQL 流量回放（危险语句拦截 + 同实例硬拦）", "traffic",
       est_secs=200, requires=_requires, tags=("traffic", "mysql"))
def _t3(ctx):
    return run_safety(ctx)
