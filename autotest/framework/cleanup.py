#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按登记的资源描述符做清理。

描述符是**可序列化**的，所以主进程死掉之后 autotest_stop 也能照着状态文件收尾。
每一种清理都吞掉自身异常：清理失败不该把测试结论从 PASS 翻成 FAIL，
但会把失败原因回报给调用方打印出来。
"""
from . import config as C
from . import endpoints as E
from .state import SPECS


def _spec(name):
    return SPECS[name]


def clean_one(api, d):
    """执行单个清理描述符，返回 (ok, 描述文本)。"""
    t = d.get("type")
    try:
        if t == "task":
            # 先 stop 让子进程退出，再 delete；只 delete 会留下跑着的管线继续写目标库
            api.stop(d["id"])
            api.remove(d["id"])
            return True, "任务 %s" % d["id"]

        if t == "validation":
            api.delete_validation(d["id"])
            return True, "对比任务 %s" % d["id"]

        if t == "sql_db":
            E.SqlEndpoint(_spec(d["spec"]), d["db"]).drop_db()
            return True, "%s 库 %s" % (d["spec"], d["db"])

        if t == "mongo_db":
            E.MongoEndpoint(_spec(d["spec"]), d["db"]).drop_db()
            return True, "%s 库 %s" % (d["spec"], d["db"])

        if t == "es_index":
            E.EsEndpoint(C.ES, d["index"]).drop_db()
            return True, "ES 索引 %s*" % d["index"]

        if t == "redis_flush":
            E.RedisEndpoint(_spec(d["spec"])).drop_db()
            return True, "Redis %s" % d["spec"]

        if t == "sql_unfence":
            E.set_read_only(E.SqlEndpoint(_spec(d["spec"]), "postgres"
                                          if _spec(d["spec"])["kind"] == "pg" else "mysql"), False)
            return True, "解除 %s 只读围栏" % d["spec"]

        if t == "oracle_table":
            E.OracleEndpoint(C.ORACLE).drop_db()
            return True, "Oracle 表 %s" % C.TABLE.upper()

        if t == "kafka_topics":
            from .kafka_util import delete_topics
            delete_topics(d["prefix"])
            return True, "Kafka topic %s*" % d["prefix"]

        return False, "未知描述符 %s" % t
    except Exception as e:  # noqa: BLE001
        return False, "%s 清理失败: %s" % (t, e)


def clean_resources(api, resources, log=print):
    """按登记的**逆序**清理：任务先停，再删库（顺序反了会让还活着的管线把库重建回来）。"""
    tasks = [r for r in resources if r.get("type") in ("task", "validation")]
    unfence = [r for r in resources if r.get("type") == "sql_unfence"]
    others = [r for r in resources
              if r.get("type") not in ("task", "validation", "sql_unfence")]
    ok_all = True
    for d in tasks + unfence + others:
        ok, msg = clean_one(api, d)
        if not ok:
            ok_all = False
            log("    ! " + msg)
    return ok_all


def clean_state(api, state, only_status=None, log=print):
    """清理状态文件里登记的资源。only_status 限定只清某种结论的 suite。"""
    n = 0
    for key, s in (state.data.get("suites") or {}).items():
        if only_status and s.get("status") not in only_status:
            continue
        res = s.get("resources") or []
        if not res:
            continue
        log("  清理 %s（%d 项）..." % (key, len(res)))
        clean_resources(api, res, log=log)
        s["resources"] = []
        n += 1
    state.save()
    return n
