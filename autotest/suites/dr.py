#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""灾备（DR）链路端到端用例：单向灾备 + 主备倒换不丢数据，双向灾备（双写）互同步。

与普通同步任务的差别，决定了这套用例的形状：
  - taskType=DR，migrationMode 被后端强制 fullAndIncre；
  - 源/目标必须是**不同实例**（后端预校验强制），两端库名必须一致（倒换后直接对调）；
  - 双向灾备会额外建一个隐藏影子任务（DR_SHADOW，B→A 仅增量），主方向进入增量后自动启动；
  - 双向灾备**不允许**倒换（两端本就都可读写），用例把这条也当判据验一遍。

主键一律显式指定并按端分段（A 段 10,000,000+ / B 段 20,000,000+）：
两端各自的自增序列会生成相同 id，那是天然写写冲突，是用例设计缺陷而非产品缺陷。
"""
import time

from ..framework import config as C
from ..framework import endpoints as E
from ..framework.case import suite

P = C.PREFIX
DB = "%s_dr" % P
A_BASE = 10_000_000
B_BASE = 20_000_000


# ----------------------------------------------------------------- 链路定义
DR_LINKS = {
    "mysql": dict(
        title="MySQL", src_type="mysql", tgt_type="mysql",
        a_spec=C.DR_MYSQL_A, b_spec=C.DR_MYSQL_B, a_name="DR_MYSQL_A", b_name="DR_MYSQL_B",
        kind="sql", planned_switchover=True,
    ),
    "pg": dict(
        title="PostgreSQL", src_type="postgresql", tgt_type="postgresql",
        a_spec=C.DR_PG_A, b_spec=C.DR_PG_B, a_name="DR_PG_A", b_name="DR_PG_B",
        kind="sql", planned_switchover=True,
    ),
    "mongo": dict(
        title="MongoDB", src_type="mongodb", tgt_type="mongodb",
        a_spec=C.MONGO_A, b_spec=C.MONGO_B, a_name="MONGO_A", b_name="MONGO_B",
        kind="mongo", planned_switchover=False,   # Mongo 走计划外接管（drain 只实现了 SQL 两端）
    ),
}


def _eps(link):
    if link["kind"] == "mongo":
        return E.MongoEndpoint(link["a_spec"], DB), E.MongoEndpoint(link["b_spec"], DB)
    return E.SqlEndpoint(link["a_spec"], DB), E.SqlEndpoint(link["b_spec"], DB)


def _conn(ep, link):
    return ep.conn_str()


def _requires(link_key):
    def check():
        link = DR_LINKS[link_key]
        a, b = _eps(link)
        # 探活连的是维护库，用例库这会儿还不存在
        if link["kind"] == "mongo":
            pa, pb = E.MongoEndpoint(link["a_spec"], "admin"), E.MongoEndpoint(link["b_spec"], "admin")
        else:
            m = "postgres" if link["a_spec"]["kind"] == "pg" else "mysql"
            pa = E.SqlEndpoint(link["a_spec"], m)
            pb = E.SqlEndpoint(link["b_spec"], m)
        if not pa.alive():
            return False, "灾备 A 端不可达 %s:%s（DR 环境请先 docker compose -f " \
                          "docker-compose-synctask-dr.yml up -d）" % (link["a_spec"]["host"],
                                                                     link["a_spec"]["port"])
        if not pb.alive():
            return False, "灾备 B 端不可达 %s:%s" % (link["b_spec"]["host"], link["b_spec"]["port"])
        return True, ""
    return check


def _reset_both(ctx, link):
    """两端全部重建。B 端也建表：倒换/双向时 B 要能当源（目标表会被同步引擎覆盖重建）。"""
    a, b = _eps(link)
    if link["kind"] == "sql":
        # 上一轮如果在倒换后没解围栏，这里会连建库都被拒——先无条件解一次
        E.set_read_only(a, False)
        E.set_read_only(b, False)
        ctx.register({"type": "sql_unfence", "spec": link["a_name"]})
        ctx.register({"type": "sql_unfence", "spec": link["b_name"]})
    a.reset_source()
    b.reset_source()
    reg = ctx.reg_sql_db if link["kind"] == "sql" else ctx.reg_mongo_db
    reg(link["a_name"], DB)
    reg(link["b_name"], DB)
    return a, b


def _create_dr(ctx, link, name, dr_mode, a, b, swap=False):
    src, tgt = (b, a) if swap else (a, b)
    tid = ctx.api.create_workflow(name, link["src_type"], link["tgt_type"],
                                  task_type="DR", dr_mode=dr_mode)
    ctx.reg_task(tid)
    ctx.api.config_workflow(tid, {
        "sourceConnection": src.conn_str(), "targetConnection": tgt.conn_str(),
        "migrationMode": "fullAndIncre",
        "sourceType": link["src_type"], "targetType": link["tgt_type"],
        "sourceDbName": DB, "targetDbName": DB,
        "syncObjects": src.sync_objects(),
    })
    ctx.api.launch(tid)
    ctx.log("    任务 id: %s" % tid)
    return tid


# ----------------------------------------------------------------- 单向灾备 + 主备倒换
def run_dr_unidirectional(ctx, link_key):
    link = DR_LINKS[link_key]
    api = ctx.api

    ctx.step("重建灾备两端库 %s（A/B 为不同实例）" % DB)
    a, b = _reset_both(ctx, link)

    ctx.step("A 端播种存量 %d 行" % C.SEED_ROWS)
    a.seed(C.SEED_ROWS, base=1)

    ctx.step("创建单向灾备任务 A → B")
    tid = _create_dr(ctx, link, "autotest-dr-%s-uni-%d" % (link_key, int(time.time())),
                     "UNIDIRECTIONAL", a, b)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("灾备任务进入运行态", ok, "当前状态=%s" % st)

    ok, sfp, tfp = ctx.wait_converge(a, b, task_id=tid)
    ctx.require("灾备全量：备端与主端一致", ok, "主 %s / 备 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    ctx.require("灾备任务进入增量（灾备中）", ok, "当前状态=%s" % st)

    ctx.step("主端持续写入增量")
    a.write(C.INCR_ROWS, base=A_BASE, tag="a-pre")
    ok, sfp, tfp = ctx.wait_converge(a, b, task_id=tid)
    ctx.check("灾备增量：备端追平主端", ok, "主 %s / 备 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    # ---- 主备倒换：核心判据是**不丢数据** ----
    # 倒换前再打一批写入且**不等它追平**，倒换流程必须自己把这批排干；
    # 等追平了再倒换，等于把最容易丢数据的那段窗口测掉了。
    ctx.step("倒换前再写一批（不等追平，倒换流程必须自己排干）")
    a.write(max(C.INCR_ROWS // 2, 10), base=A_BASE + 500_000, tag="a-tail")

    if link["planned_switchover"]:
        ctx.step("发起计划内主备切换（停旧主写入 → 等追平 → 才切）")
        r = api.switchover(tid, drain_timeout_ms=C.CONVERGE_TIMEOUT * 1000)
        ctx.require("计划内主备切换被接受", bool(r.get("success")), str(r.get("message"))[:200])
    else:
        ctx.step("静默主端写入并等追平后，发起计划外接管（Mongo 链路无 SQL 停写通道）")
        ok, sfp, tfp = ctx.wait_converge(a, b, task_id=tid)
        ctx.require("接管前链路已追平", ok, "主 %s / 备 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))
        r = api.failover(tid)
        ctx.require("主备倒换被接受", bool(r.get("success")), str(r.get("message"))[:200])

    # 倒换后旧主不再接受业务写入，它的指纹就是"倒换那一刻主库的全部数据"
    time.sleep(5)
    a_fp = a.fingerprint()
    ok, b_fp = False, None
    deadline = time.time() + C.CONVERGE_TIMEOUT
    while time.time() < deadline:
        b_fp = b.fingerprint()
        if b_fp == a_fp:
            ok = True
            break
        time.sleep(C.POLL_INTERVAL)
    ctx.check("主备倒换不丢数据：新主(B)数据 == 倒换时旧主(A)的全部数据", ok,
              "旧主 %s / 新主 %s" % (E.fmt_fp(a_fp), E.fmt_fp(b_fp or (-1, 0))))

    ok, st = api.wait_status(tid, {"INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.check("倒换后任务回到灾备中（新方向 B → A）", ok, "当前状态=%s" % st)

    wf = api.workflow(tid)
    swapped = str(wf.get("source_connection") or "").find(str(b.port)) >= 0
    ctx.check("倒换后任务的源/目标已对调", swapped,
              "source=%s" % str(wf.get("source_connection"))[:80])

    if link["kind"] == "sql":
        ctx.check("倒换后旧主已被置为只读（防双写分叉）", E.is_read_only(a),
                  "read_only=%s" % E.is_read_only(a))
        ctx.step("模拟运维解除旧主只读，验证反向同步真的能落库")
        E.set_read_only(a, False)

    ctx.step("向新主 B 写入，验证反向同步链路生效")
    b.write(max(C.INCR_ROWS // 2, 10), base=B_BASE, tag="b-post")
    ok, sfp, tfp = ctx.wait_converge(b, a, task_id=tid)
    ctx.check("倒换后反向同步生效：新备(A)追平新主(B)", ok,
              "新主 %s / 新备 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    api.stop(tid)
    return {"task_id": tid}


# ----------------------------------------------------------------- 双向灾备（双写）
def run_dr_bidirectional(ctx, link_key):
    link = DR_LINKS[link_key]
    api = ctx.api

    ctx.step("重建灾备两端库 %s" % DB)
    a, b = _reset_both(ctx, link)

    ctx.step("A 端播种存量 %d 行" % C.SEED_ROWS)
    a.seed(C.SEED_ROWS, base=1)

    ctx.step("创建双向灾备任务 A ⇄ B")
    tid = _create_dr(ctx, link, "autotest-dr-%s-bidi-%d" % (link_key, int(time.time())),
                     "BIDIRECTIONAL", a, b)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("双向灾备任务进入运行态", ok, "当前状态=%s" % st)

    ok, sfp, tfp = ctx.wait_converge(a, b, task_id=tid)
    ctx.require("双向灾备全量：B 端与 A 端一致", ok, "A %s / B %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    ctx.require("双向灾备进入增量", ok, "当前状态=%s" % st)

    # 影子任务（B→A，仅增量）由后端在主方向进入增量后自动创建并启动
    shadow = None
    deadline = time.time() + 120
    while time.time() < deadline:
        shadow = (api.workflow(tid) or {}).get("dr_peer_workflow_id")
        if shadow:
            break
        time.sleep(C.POLL_INTERVAL)
    if shadow:
        ctx.reg_task(shadow)
    ctx.require("反向影子任务已自动创建（DR_SHADOW: B → A）", bool(shadow), "shadow=%s" % shadow)

    ok, st = api.wait_status(shadow, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    ctx.check("反向影子任务已进入增量", ok, "当前状态=%s" % st)

    # ---- 双写：两端各写各的 id 段，必须互相同步且不产生回环放大 ----
    ctx.step("A 端写入（id 段 %d+）" % A_BASE)
    sa = a.write(C.INCR_ROWS, base=A_BASE, tag="a-side")
    ok, sfp, tfp = ctx.wait_converge(a, b, task_id=tid)
    ctx.check("双写方向一：A 端写入同步到 B 端", ok, "A %s / B %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ctx.step("B 端写入（id 段 %d+）" % B_BASE)
    sb = b.write(C.INCR_ROWS, base=B_BASE, tag="b-side")
    ok, sfp, tfp = ctx.wait_converge(b, a, task_id=shadow)
    ctx.check("双写方向二：B 端写入同步到 A 端", ok, "B %s / A %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    # 防回环：两端行数必须精确相等且等于"存量 + 两端净增"。
    # 回环放大的典型表现是行数持续增长/两端反复互相覆盖——所以这里要静置一段再看第二次。
    expect = C.SEED_ROWS + len(sa["inserted"]) + len(sb["inserted"])
    time.sleep(10)
    ca, cb = a.count(), b.count()
    ctx.check("双向灾备防回环：两端行数相等且等于存量+两端净增（%d）" % expect,
              ca == cb == expect, "A=%d B=%d" % (ca, cb))

    time.sleep(10)
    ca2, cb2 = a.count(), b.count()
    ctx.check("双向灾备防回环：静置后行数不再变化（无无限回环）",
              (ca2, cb2) == (ca, cb), "静置前 A=%d B=%d，静置后 A=%d B=%d" % (ca, cb, ca2, cb2))

    fa, fb = a.fingerprint(), b.fingerprint()
    ctx.check("双向灾备最终一致：两端指纹相等", fa == fb,
              "A %s / B %s" % (E.fmt_fp(fa), E.fmt_fp(fb)))

    # 产品规则：双向灾备两端都可读写，倒换无意义，必须被拒
    r = api.failover(tid)
    ctx.check("双向灾备拒绝主备倒换（两端均可读写，倒换无意义）", not r.get("success"),
              str(r.get("message"))[:120])

    api.stop(tid)
    if shadow:
        api.stop(shadow)
    return {"task_id": tid, "shadow": shadow}


# ----------------------------------------------------------------- 注册
# 预估耗时（秒），实测值 + 余量。PG 两条偏高是因为当前会走满等待超时（见 README 的已知问题）。
_UNI_EST = {"mysql": 120, "pg": 260, "mongo": 110}
_BIDI_EST = {"mysql": 140, "pg": 440, "mongo": 130}

for _k, _l in DR_LINKS.items():
    def _mk_uni(k=_k, l=_l):
        @suite("dr_%s_uni" % k, "%s 单向灾备 + 主备倒换（不丢数据）" % l["title"], "dr",
               est_secs=_UNI_EST[k], requires=_requires(k), tags=("dr", k))
        def _run(ctx, _k=k):
            return run_dr_unidirectional(ctx, _k)

    def _mk_bidi(k=_k, l=_l):
        @suite("dr_%s_bidi" % k, "%s 双向灾备（双写互同步 + 防回环）" % l["title"], "dr",
               est_secs=_BIDI_EST[k], requires=_requires(k), tags=("dr", k))
        def _run(ctx, _k=k):
            return run_dr_bidirectional(ctx, _k)

    _mk_uni()
    _mk_bidi()
