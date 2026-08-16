#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据订阅（SUBSCRIBE）端到端用例：源库 DML → Kafka CDC 事件。

订阅任务只有增量、没有全量：任务启动**之后**源库的每一次 DML 才应该出现在 Kafka 里。
因此先播种存量（不该进 CDC），再起任务，再增删改，最后用两把尺子判定：
  1. **不丢**：写入端记录的每一条 (id, val, n) 真值，都要能在 Kafka 事件里找到；
  2. **可收敛**：按投递顺序回放 c/u/d，last-write-wins 得到的最终状态必须与源表逐行相等。
     只满足 1) 不满足 2) 说明重投顺序错乱，下游照样是脏数据。

下游 Kafka 用独立的一套（39092），与控制面 Kafka(29092) 隔离：
几十万条 CDC 挤在控制面上会把任务下发/状态上报拖死，那是环境问题不是产品问题。
"""
import time

from ..framework import config as C
from ..framework import endpoints as E
from ..framework import kafka_util as K
from ..framework.case import suite

P = C.PREFIX
INCR_BASE = 5_000_000


# ----------------------------------------------------------------- 源端定义
def _mysql_src():
    return E.SqlEndpoint(C.MYSQL, "%s_sub_my" % P)


def _pg_src():
    return E.SqlEndpoint(C.PG, "%s_sub_pg" % P)


def _tidb_src():
    return E.SqlEndpoint(C.TIDB, "%s_sub_tidb" % P)


def _mongo_src():
    return E.MongoEndpoint(C.MONGO_A, "%s_sub_mgo" % P)


def _oracle_src():
    return E.OracleEndpoint(C.ORACLE)


SOURCES = {
    "mysql": dict(title="MySQL", type="mysql", make=_mysql_src, spec="MYSQL", kind="sql"),
    "pg": dict(title="PostgreSQL", type="postgresql", make=_pg_src, spec="PG", kind="sql"),
    "tidb": dict(title="TiDB", type="tidb", make=_tidb_src, spec="TIDB", kind="sql"),
    "mongo": dict(title="MongoDB", type="mongodb", make=_mongo_src, spec="MONGO_A", kind="mongo"),
    "oracle": dict(title="Oracle", type="oracle", make=_oracle_src, spec="ORACLE", kind="oracle"),
}


def _requires(key):
    def check():
        s = SOURCES[key]
        try:
            ep = s["make"]()
        except Exception as e:  # noqa: BLE001
            return False, "源端构造失败: %s" % e
        # alive() 连的是维护库（mysql/postgres/admin），用例库这会儿还不存在
        if not ep.alive():
            return False, "订阅源端不可达 %s:%s" % (ep.host, ep.port)
        try:
            K.list_topics()
        except Exception as e:  # noqa: BLE001
            return False, "订阅下游 Kafka(%s) 不可达: %s" % (C.SUB_KAFKA, str(e)[:80])
        return True, ""
    return check


# ----------------------------------------------------------------- 通用流程
def run_subscribe(ctx, key):
    s = SOURCES[key]
    api = ctx.api
    src = s["make"]()
    prefix = "%s_sub_%s" % (P, key)

    ctx.step("清理上一轮遗留 topic（%s*）" % prefix)
    K.delete_topics(prefix)
    ctx.reg_topics(prefix)

    ctx.step("重建源端 %s 并播种存量（存量不应进 CDC）" % src.db)
    src.reset_source()
    if s["kind"] == "sql":
        ctx.reg_sql_db(s["spec"], src.db)
    elif s["kind"] == "mongo":
        ctx.reg_mongo_db(s["spec"], src.db)
    else:
        ctx.reg_oracle_table()
    src.seed(C.SEED_ROWS, base=1)

    ctx.step("创建并启动订阅任务 → Kafka(%s)" % C.SUB_KAFKA)
    tid = api.create_workflow("autotest-sub-%s-%d" % (key, int(time.time())),
                              s["type"], "kafka", task_type="SUBSCRIBE")
    ctx.reg_task(tid)
    api.config_workflow(tid, {
        "sourceConnection": src.conn_str(),
        "targetConnection": "kafka://%s" % C.SUB_KAFKA,
        "migrationMode": "subscribe",
        "sourceType": s["type"], "targetType": "kafka",
        # Oracle 的 sourceDbName 是**服务名/PDB**（FREEPDB1），不是 schema；
        # 填成 schema 会让 checkpoint 初始化取不到 SCN，任务直接 FAILED(E2005)。
        "sourceDbName": getattr(src, "service", None) or src.db,
        "targetDbName": getattr(src, "service", None) or src.db,
        "syncObjects": src.sync_objects(),
        "kafkaBootstrapServers": C.SUB_KAFKA,
        "kafkaTopicPrefix": prefix,
        "kafkaTopicStrategy": "TABLE",
        "subscribeFormat": "DEBEZIUM_JSON",
    })
    api.launch(tid)
    ctx.log("    任务 id: %s" % tid)

    ok, st = api.wait_status(tid, "SUBSCRIBE_RUNNING", C.LAUNCH_TIMEOUT)
    ctx.require("订阅任务进入订阅中", ok, "当前状态=%s" % st)

    # 位点建立需要一点时间；起早了写的 DML 会落在订阅起始位点之前（那是环境节奏，不是缺陷）
    time.sleep(10)

    ctx.step("源端写入 DML（INSERT/UPDATE/DELETE）")
    stat = src.write(C.INCR_ROWS, base=INCR_BASE, tag="sub")
    ctx.log("    写入：插入%d 更新%d 删除%d"
            % (len(stat["inserted"]), stat["updates"], stat["deletes"]))

    ctx.step("消费 Kafka 事件并回放")
    records = []
    deadline = time.time() + C.CONVERGE_TIMEOUT
    expect_ids = set(stat["inserted"])
    replayed = {}
    while time.time() < deadline:
        records = K.consume_all(prefix, idle_timeout=8)
        replayed = K.replay(records)
        got = {r for r in replayed if r >= INCR_BASE}
        if got == expect_ids:
            break
        time.sleep(C.POLL_INTERVAL)

    ctx.require("Kafka 中收到订阅事件", len(records) > 0, "事件数=%d" % len(records))

    topics = K.list_topics(prefix)
    ctx.check("订阅 topic 已按表创建（前缀 %s）" % prefix, len(topics) > 0, "topics=%s" % topics)

    # 1) 不丢：本次写入后仍存活的 id，回放后必须都在
    got_ids = {r for r in replayed if r >= INCR_BASE}
    missing = expect_ids - got_ids
    extra = got_ids - expect_ids
    ctx.check("订阅不丢数据：存活行全部出现在事件流中", not missing,
              "缺失 %d 条：%s" % (len(missing), sorted(missing)[:8]))
    ctx.check("订阅删除生效：已删除的行回放后不残留", not extra,
              "多出 %d 条：%s" % (len(extra), sorted(extra)[:8]))

    # 2) 可收敛：回放最终状态 == 源表当前状态（只比增量 id 段，存量本就不进 CDC）
    src_rows = {r[0]: r for r in src.fetch_rows() if r[0] >= INCR_BASE}
    diffs = []
    for rid, row in src_rows.items():
        got = replayed.get(rid)
        if got is None:
            diffs.append("%s 缺失" % rid)
        elif E.canon_key(got) != E.canon_key(row):
            diffs.append("%s 值不符(源=%s 回放=%s)" % (rid, row[2], got[2]))
        if len(diffs) >= 5:
            break
    ctx.check("订阅可收敛：按投递顺序回放的最终状态与源表逐行相等", not diffs,
              "; ".join(diffs) if diffs else "比对 %d 行" % len(src_rows))

    api.stop(tid)
    return {"task_id": tid}


_EST = {"mysql": 150, "pg": 170, "tidb": 190, "mongo": 150, "oracle": 220}

for _k in SOURCES:
    def _mk(k=_k):
        @suite("subscribe_%s" % k, "%s → Kafka 数据订阅（不丢 + 可收敛）" % SOURCES[k]["title"],
               "subscribe", est_secs=_EST[k], requires=_requires(k), tags=("subscribe", k))
        def _run(ctx, _k=k):
            return run_subscribe(ctx, _k)
    _mk()
