#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""同步链路（SYNC）端到端用例：全量 → 增量 → 数据对比 → 差异检出 → 一键修复。

每条链路跑同一套判据，改一处所有链路都受益：
  1. 全量：目标端行数/指纹 == 源端存量
  2. 增量：源端 INSERT/UPDATE/DELETE 后目标端追平（指纹相等，对删除同样敏感）
  3. 行数对比（ROW_COUNT）：结论一致、无差异表
  4. 内容对比（CONTENT）：结论一致、无差异表
  5. **对比功能本身是否可信**：在目标端人为改坏一行，内容对比必须抓到差异
     （只跑"一致"的正向用例，等于没测对比——它永远返回一致也能过）
  6. 一键修复：修完复核一致，且目标端指纹重新等于源端
"""
import json
import time

from ..framework import config as C
from ..framework import endpoints as E
from ..framework.api import compare_tables
from ..framework.case import suite

P = C.PREFIX


# ----------------------------------------------------------------- 链路定义
def sql_link(key, src_spec, src_spec_name, tgt_spec, tgt_spec_name,
             src_type, tgt_type, tgt_schema=None):
    """构造一条 SQL→SQL 链路描述。"""
    return dict(key=key, src_spec=src_spec, src_spec_name=src_spec_name,
                tgt_spec=tgt_spec, tgt_spec_name=tgt_spec_name,
                src_type=src_type, tgt_type=tgt_type, tgt_schema=tgt_schema)


def _endpoints(link, run_tag):
    src_db = "%s_%s_src" % (P, link["key"])
    tgt_db = "%s_%s_tgt" % (P, link["key"])
    src = E.SqlEndpoint(link["src_spec"], src_db)
    # mysql→pg：同步把表落在「源库名」schema 下（不是 public），比对要连对 schema
    schema = src_db if link["tgt_schema"] == "SOURCE_DB" else None
    tgt = E.SqlEndpoint(link["tgt_spec"], tgt_db, schema=schema)
    return src, tgt


def _task_config(link, src, tgt, mode="fullAndIncre"):
    return {
        "sourceConnection": src.conn_str(),
        "targetConnection": tgt.conn_str(),
        "migrationMode": mode,
        "sourceType": link["src_type"],
        "targetType": link["tgt_type"],
        "sourceDbName": src.db,
        "targetDbName": tgt.db,
        "syncObjects": src.sync_objects(),
    }



ENGINE_FAMILY = {"tidb": "mysql"}


def _family(t):
    return ENGINE_FAMILY.get(t, t)


def _diff_detail(res):
    """把对比结果里的差异表摘出来，报告里只写 failed=1 没法排查。"""
    bad = [t for t in compare_tables(res)
           if t.get("sourceRowCount") != t.get("targetRowCount") or t.get("diffCount")]
    if not bad:
        return str(res.get("errorMessage") or res.get("error_message") or "")[:160]
    return "; ".join("%s 源%s/目标%s" % (t.get("sourceTable"), t.get("sourceRowCount"),
                                        t.get("targetRowCount")) for t in bad[:4])


# ----------------------------------------------------------------- 通用流程
def run_sync_link(ctx, link):
    api = ctx.api
    src, tgt = _endpoints(link, ctx.key)

    ctx.step("重建源/目标库：%s → %s" % (src.db, tgt.db))
    src.reset_source()
    tgt.reset_target()
    ctx.reg_sql_db(link["src_spec_name"], src.db)
    ctx.reg_sql_db(link["tgt_spec_name"], tgt.db)

    ctx.step("播种存量 %d 行" % C.SEED_ROWS)
    src.seed(C.SEED_ROWS, base=1)
    seed_fp = src.fingerprint()

    ctx.step("创建并启动同步任务")
    name = "autotest-%s-%d" % (link["key"], int(time.time()))
    tid = api.create_workflow(name, link["src_type"], link["tgt_type"])
    ctx.reg_task(tid)
    api.config_workflow(tid, _task_config(link, src, tgt))
    api.launch(tid)
    ctx.log("    任务 id: %s" % tid)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("任务进入运行态（全量完成/增量中）", ok, "当前状态=%s" % st)

    tgt.resolve_schema()

    # ---- 1. 全量 ----
    ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
    ctx.require("全量同步：目标端与源端一致", ok,
                "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    # ---- 2. 增量 ----
    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    ctx.require("任务进入增量同步", ok, "当前状态=%s" % st)

    ctx.step("源端写入增量（INSERT/UPDATE/DELETE 各若干）")
    stat = src.write(C.INCR_ROWS, base=1_000_000, tag="incr")
    ctx.log("    增量：插入%d 更新%d 删除%d" % (len(stat["inserted"]), stat["updates"], stat["deletes"]))
    ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
    ctx.check("增量同步：目标端追平源端（含更新与删除）", ok,
              "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    # ---- 3/4. 数据对比：一致场景 ----
    vid, res = api.run_compare(tid, "ROW_COUNT")
    ctx.register({"type": "validation", "id": vid})
    ctx.check("行数对比：结论一致、无差异表",
              res.get("status") == "COMPLETED" and (res.get("failedTables") or 0) == 0,
              "status=%s failed=%s %s" % (res.get("status"), res.get("failedTables"),
                                          _diff_detail(res)))

    # TiDB 在产品内部就归一成 mysql（source.db.type=mysql + flavor=tidb），
    # 因此 tidb→mysql 属于**同引擎**，内容对比与一键修复都该支持。
    same_engine = _family(link["src_type"]) == _family(link["tgt_type"])
    if not same_engine:
        # 异构链路产品上不做内容对比（列类型语义不同，逐行比会满屏假差异）。
        # 这条规则本身也要验：它必须是**被明确拒绝**，而不是悄悄返回"一致"。
        try:
            api.create_validation(tid, "CONTENT")
            ctx.check("异构链路的内容对比被明确拒绝（而非静默返回一致）", False, "竟然创建成功了")
        except Exception as e:  # noqa: BLE001
            ctx.check("异构链路的内容对比被明确拒绝（而非静默返回一致）",
                      "相同类型" in str(e) or "不支持" in str(e), str(e)[:120])
        ctx.step("停止任务")
        api.stop(tid)
        return {"task_id": tid, "seed_fp": seed_fp}

    vid_c, res_c = api.run_compare(tid, "CONTENT")
    ctx.register({"type": "validation", "id": vid_c})
    ctx.check("内容对比：结论一致、无差异表",
              res_c.get("status") == "COMPLETED" and (res_c.get("failedTables") or 0) == 0,
              "status=%s failed=%s mismatched=%s"
              % (res_c.get("status"), res_c.get("failedTables"), res_c.get("mismatchedRows")))

    # ---- 5. 数据对比：差异必须被抓到（反向用例）----
    victim = sorted(r[0] for r in src.fetch_rows())[0]
    ctx.step("在目标端人为改坏 id=%s，验证内容对比能抓到差异" % victim)
    changed = tgt.corrupt_row(victim)
    if changed != 1:
        ctx.check("反向用例：目标端制造差异", False, "改坏 %d 行（期望 1）" % changed)
    else:
        vid_b, res_b = api.run_compare(tid, "CONTENT")
        ctx.register({"type": "validation", "id": vid_b})
        detected = ((res_b.get("failedTables") or 0) > 0
                    or (res_b.get("mismatchedRows") or 0) > 0)
        ctx.check("内容对比：能检出人为制造的差异（对比功能可信）", detected,
                  "status=%s failed=%s mismatched=%s"
                  % (res_b.get("status"), res_b.get("failedTables"), res_b.get("mismatchedRows")))

        # ---- 6. 一键修复 ----
        if detected and res_b.get("status") == "COMPLETED":
            rep = api.repair_validation(vid_b)
            after = api.validation(vid_b)
            ctx.check("差异一键修复：修复后复核一致", after.get("repairStatus") == "REPAIRED",
                      "repairStatus=%s msg=%s"
                      % (after.get("repairStatus"), str(rep.get("message"))[:120]))
            ok, sfp, tfp = ctx.wait_converge(src, tgt, timeout=60, task_id=tid)
            ctx.check("差异修复后：目标端指纹重新等于源端", ok,
                      "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ctx.step("停止任务")
    api.stop(tid)
    return {"task_id": tid, "seed_fp": seed_fp}


# ----------------------------------------------------------------- 各链路注册
LINKS = [
    sql_link("mysql2mysql", C.MYSQL, "MYSQL", C.MYSQL, "MYSQL", "mysql", "mysql"),
    sql_link("mysql2pg", C.MYSQL, "MYSQL", C.PG, "PG", "mysql", "postgresql",
             tgt_schema="SOURCE_DB"),
    sql_link("pg2pg", C.PG, "PG", C.PG, "PG", "postgresql", "postgresql"),
    sql_link("pg2mysql", C.PG, "PG", C.MYSQL, "MYSQL", "postgresql", "mysql"),
    sql_link("tidb2mysql", C.TIDB, "TIDB", C.MYSQL, "MYSQL", "tidb", "mysql"),
]

# 预估耗时（秒）：按实测值 + 余量。估太高会被总预算误跳，估太低会让整轮超预算。
SYNC_EST = {"mysql2mysql": 90, "mysql2pg": 90, "pg2pg": 90, "pg2mysql": 90, "tidb2mysql": 90}

TITLES = {
    "mysql2mysql": "MySQL → MySQL 同步（全量+增量+对比+修复）",
    "mysql2pg": "MySQL → PostgreSQL 同步（全量+增量+对比+修复）",
    "pg2pg": "PostgreSQL → PostgreSQL 同步（全量+增量+对比+修复）",
    "pg2mysql": "PostgreSQL → MySQL 同步（全量+增量+对比+修复）",
    "tidb2mysql": "TiDB → MySQL 同步（全量+增量+对比+修复）",
}


def _requires_sql(link):
    def check():
        # alive() 连的是维护库（mysql/postgres），用例库这会儿还不存在
        src = E.SqlEndpoint(link["src_spec"], "_probe")
        tgt = E.SqlEndpoint(link["tgt_spec"], "_probe")
        if not src.alive():
            return False, "源端不可达 %s:%s" % (link["src_spec"]["host"], link["src_spec"]["port"])
        if not tgt.alive():
            return False, "目标端不可达 %s:%s" % (link["tgt_spec"]["host"], link["tgt_spec"]["port"])
        return True, ""
    return check


def _register(link):
    @suite("sync_" + link["key"], TITLES[link["key"]], "sync",
           est_secs=SYNC_EST.get(link["key"], 120), requires=_requires_sql(link),
           tags=(link["key"],))
    def _run(ctx, _link=link):
        return run_sync_link(ctx, _link)


for _l in LINKS:
    _register(_l)


# ----------------------------------------------------------------- MongoDB → MongoDB
def _mongo_alive():
    a = E.MongoEndpoint(C.MONGO_A, "admin")
    b = E.MongoEndpoint(C.MONGO_B, "admin")
    if not a.alive():
        return False, "mongo-a(27117) 不可达"
    if not b.alive():
        return False, "mongo-b(27118) 不可达"
    return True, ""


@suite("sync_mongo2mongo", "MongoDB → MongoDB 同步（全量+增量+对比）", "sync",
       est_secs=70, requires=_mongo_alive, tags=("mongo2mongo",))
def sync_mongo(ctx):
    api = ctx.api
    # migration-mongo 是「同名命名空间镜像」：目标库名必须与源库名相同（引擎忽略 targetDbName），
    # 源/目标是两个独立实例故不冲突。写成不同库名会去错误的库比到 0 文档。
    db = "%s_mongo" % P
    src = E.MongoEndpoint(C.MONGO_A, db)
    tgt = E.MongoEndpoint(C.MONGO_B, db)

    ctx.step("重建源/目标库 %s" % db)
    src.reset_source()
    tgt.reset_target()
    ctx.reg_mongo_db("MONGO_A", db)
    ctx.reg_mongo_db("MONGO_B", db)

    ctx.step("播种存量 %d 文档" % C.SEED_ROWS)
    src.seed(C.SEED_ROWS, base=1)

    name = "autotest-mongo2mongo-%d" % int(time.time())
    tid = api.create_workflow(name, "mongodb", "mongodb")
    ctx.reg_task(tid)
    api.config_workflow(tid, {
        "sourceConnection": src.conn_str(), "targetConnection": tgt.conn_str(),
        "migrationMode": "fullAndIncre", "sourceType": "mongodb", "targetType": "mongodb",
        "sourceDbName": db, "targetDbName": db, "syncObjects": src.sync_objects(),
    })
    api.launch(tid)
    ctx.log("    任务 id: %s" % tid)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("任务进入运行态", ok, "当前状态=%s" % st)

    ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
    ctx.require("全量同步：目标端与源端一致", ok,
                "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    ctx.require("任务进入增量同步（Change Streams）", ok, "当前状态=%s" % st)

    ctx.step("源端写入增量文档")
    stat = src.write(C.INCR_ROWS, base=1_000_000, tag="incr")
    ctx.log("    增量：插入%d 更新%d 删除%d" % (len(stat["inserted"]), stat["updates"], stat["deletes"]))
    ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
    ctx.check("增量同步：目标端追平源端（含更新与删除）", ok,
              "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    vid, res = api.run_compare(tid, "ROW_COUNT")
    ctx.register({"type": "validation", "id": vid})
    ctx.check("行数对比：结论一致、无差异表",
              res.get("status") == "COMPLETED" and (res.get("failedTables") or 0) == 0,
              "status=%s failed=%s" % (res.get("status"), res.get("failedTables")))

    # Mongo 内容对比不支持一键修复（产品既定），这里只验证差异能被检出
    victim = sorted(r[0] for r in src.fetch_rows())[0]
    if tgt.corrupt_row(victim) == 1:
        vid_b, res_b = api.run_compare(tid, "CONTENT")
        ctx.register({"type": "validation", "id": vid_b})
        detected = ((res_b.get("failedTables") or 0) > 0
                    or (res_b.get("mismatchedRows") or 0) > 0)
        ctx.check("内容对比：能检出人为制造的差异", detected,
                  "status=%s failed=%s mismatched=%s"
                  % (res_b.get("status"), res_b.get("failedTables"), res_b.get("mismatchedRows")))

    api.stop(tid)
    return {"task_id": tid}


# ----------------------------------------------------------------- Redis → Redis
def _redis_alive():
    if not E.RedisEndpoint(C.REDIS_A).alive():
        return False, "redis-a(6390) 不可达"
    if not E.RedisEndpoint(C.REDIS_B).alive():
        return False, "redis-b(6391) 不可达"
    return True, ""


@suite("sync_redis2redis", "Redis → Redis 同步（全量+增量）", "sync",
       est_secs=180, requires=_redis_alive, tags=("redis2redis",))
def sync_redis(ctx):
    api = ctx.api
    src = E.RedisEndpoint(C.REDIS_A)
    tgt = E.RedisEndpoint(C.REDIS_B)

    ctx.step("清空源/目标 Redis")
    src.reset_source()
    tgt.reset_target()
    ctx.reg_redis("REDIS_A")
    ctx.reg_redis("REDIS_B")

    ctx.step("播种存量 key")
    src.seed(C.SEED_ROWS, base=1)

    tid = api.create_workflow("autotest-redis2redis-%d" % int(time.time()), "redis", "redis")
    ctx.reg_task(tid)
    api.config_workflow(tid, {
        "sourceConnection": src.conn_str(), "targetConnection": tgt.conn_str(),
        "migrationMode": "fullAndIncre", "sourceType": "redis", "targetType": "redis",
        "sourceDbName": "0", "targetDbName": "0", "syncObjects": src.sync_objects(),
    })
    api.launch(tid)
    ctx.log("    任务 id: %s" % tid)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("任务进入运行态", ok, "当前状态=%s" % st)

    ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
    ctx.require("全量同步：目标端 keyspace 与源端一致", ok,
                "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    if ctx.check("任务进入增量同步", ok, "当前状态=%s" % st):
        ctx.step("源端写入增量 key（含 DEL）")
        src.write(C.INCR_ROWS, base=1_000_000, tag="incr")
        ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
        ctx.check("增量同步：目标端 keyspace 追平（含删除）", ok,
                  "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    api.stop(tid)
    return {"task_id": tid}


# ----------------------------------------------------------------- MySQL → Elasticsearch
def _es_alive():
    if not E.EsEndpoint(C.ES, "_").alive():
        return False, "ES(9200) 不可达"
    if not E.SqlEndpoint(C.MYSQL, "mysql").alive():
        return False, "MySQL(33306) 不可达"
    return True, ""


@suite("sync_mysql2es", "MySQL → Elasticsearch 同步（全量+增量）", "sync",
       est_secs=70, requires=_es_alive, tags=("mysql2es",))
def sync_es(ctx):
    api = ctx.api
    db = "%s_es_src" % P
    src = E.SqlEndpoint(C.MYSQL, db)
    # 索引名按引擎约定：源库名_表名（小写）
    tgt = E.EsEndpoint(C.ES, "%s_%s" % (db, C.TABLE))

    ctx.step("重建源库 %s，清理目标索引" % db)
    src.reset_source()
    tgt.reset_target()
    ctx.reg_sql_db("MYSQL", db)
    ctx.reg_es_index(db)

    src.seed(C.SEED_ROWS, base=1)

    tid = api.create_workflow("autotest-mysql2es-%d" % int(time.time()), "mysql", "elasticsearch")
    ctx.reg_task(tid)
    api.config_workflow(tid, {
        "sourceConnection": src.conn_str(), "targetConnection": tgt.conn_str(),
        "migrationMode": "fullAndIncre", "sourceType": "mysql", "targetType": "elasticsearch",
        "sourceDbName": db, "targetDbName": db, "syncObjects": src.sync_objects(),
    })
    api.launch(tid)
    ctx.log("    任务 id: %s" % tid)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("任务进入运行态", ok, "当前状态=%s" % st)

    ok, n = ctx.wait_count(tgt, C.SEED_ROWS)
    ctx.require("全量同步：ES 文档数 == 源端行数（%d）" % C.SEED_ROWS, ok, "实际 %s" % n)

    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    if ctx.check("任务进入增量同步（binlog 直读）", ok, "当前状态=%s" % st):
        stat = src.write(C.INCR_ROWS, base=1_000_000, tag="incr")
        expect = C.SEED_ROWS + len(stat["inserted"])
        ok, n = ctx.wait_count(tgt, expect)
        ctx.check("增量同步：ES 文档数追平（含删除后的净值 %d）" % expect, ok, "实际 %s" % n)
        ok, sfp, tfp = ctx.wait_converge(src, tgt, timeout=60, task_id=tid)
        ctx.check("增量同步：ES 文档内容与源端逐行一致", ok,
                  "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    api.stop(tid)
    return {"task_id": tid}


# ----------------------------------------------------------------- Oracle → MySQL
def _oracle_alive():
    if not E.OracleEndpoint(C.ORACLE).alive():
        return False, "Oracle(1521) 不可达或 oracledb 驱动缺失"
    if not E.SqlEndpoint(C.PG, "_probe").alive():
        return False, "PostgreSQL(5432) 不可达"
    return True, ""


@suite("sync_oracle2pg", "Oracle → PostgreSQL 同步（全量+增量+对比）", "sync",
       est_secs=120, requires=_oracle_alive, tags=("oracle2pg",))
def sync_oracle(ctx):
    api = ctx.api
    src = E.OracleEndpoint(C.ORACLE)
    tgt_db = "%s_ora_tgt" % P
    # 目标表实际落在哪个 schema 由引擎决定，全量完成后用 resolve_schema() 现查
    tgt = E.SqlEndpoint(C.PG, tgt_db)

    ctx.step("重建 Oracle 源表 %s.%s，重建 PG 目标库 %s" % (src.db, src.table, tgt_db))
    src.reset_source()
    tgt.reset_target()
    ctx.reg_oracle_table()
    ctx.reg_sql_db("PG", tgt_db)

    src.seed(C.SEED_ROWS, base=1)

    tid = api.create_workflow("autotest-oracle2pg-%d" % int(time.time()), "oracle", "postgresql")
    ctx.reg_task(tid)
    api.config_workflow(tid, {
        "sourceConnection": src.conn_str(), "targetConnection": tgt.conn_str(),
        "migrationMode": "fullAndIncre", "sourceType": "oracle", "targetType": "postgresql",
        # Oracle 的 sourceDbName 是**服务名/PDB**（FREEPDB1），不是 schema；
        # syncObjects 的 key 才是 schema（APP_USER）。填成 schema 会让 checkpoint 初始化取不到 SCN。
        "sourceDbName": src.service, "targetDbName": tgt_db, "syncObjects": src.sync_objects(),
    })
    api.launch(tid)
    ctx.log("    任务 id: %s" % tid)

    ok, st = api.wait_status(tid, {"FULL_COMPLETED", "INCREMENT_RUNNING"}, C.LAUNCH_TIMEOUT)
    ctx.require("任务进入运行态", ok, "当前状态=%s" % st)

    tgt.resolve_schema()

    ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
    ctx.require("全量同步：目标端与源端一致", ok,
                "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    ok, st = api.wait_status(tid, "INCREMENT_RUNNING", C.LAUNCH_TIMEOUT)
    if ctx.check("任务进入增量同步（LogMiner）", ok, "当前状态=%s" % st):
        stat = src.write(C.INCR_ROWS, base=1_000_000, tag="incr")
        ctx.log("    增量：插入%d 更新%d 删除%d"
                % (len(stat["inserted"]), stat["updates"], stat["deletes"]))
        ok, sfp, tfp = ctx.wait_converge(src, tgt, task_id=tid)
        ctx.check("增量同步：目标端追平源端（含更新与删除）", ok,
                  "源 %s / 目标 %s" % (E.fmt_fp(sfp), E.fmt_fp(tfp)))

    vid, res = api.run_compare(tid, "ROW_COUNT")
    ctx.register({"type": "validation", "id": vid})
    ctx.check("行数对比：结论一致、无差异表",
              res.get("status") == "COMPLETED" and (res.get("failedTables") or 0) == 0,
              "status=%s failed=%s %s" % (res.get("status"), res.get("failedTables"),
                                          _diff_detail(res)))

    api.stop(tid)
    return {"task_id": tid}
