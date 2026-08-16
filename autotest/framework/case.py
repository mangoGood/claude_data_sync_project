#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用例上下文与 suite 注册表。

一个 **suite** 是一条链路的完整端到端场景（建库 → 建任务 → 同步 → 校验 → 清理），
内部包含多个 **check**（断言点）。续跑的粒度是 suite —— 一个跑了一半的实时同步场景
从中间接着跑没有意义，必须从头重来。
"""
import time

from . import config as C


class SuiteAbort(Exception):
    """前置断言失败：本 suite 后续步骤已无意义，直接收尾（不是框架错误）。"""


class SuiteSkip(Exception):
    """环境不满足：本 suite 记 SKIPPED，不算失败。"""


class Ctx:
    """跑一个 suite 时传给它的上下文：断言、资源登记、进度输出。"""

    def __init__(self, api, state, key, log, keep_on_fail=True):
        self.api = api
        self.state = state
        self.key = key
        self.log = log
        self.keep_on_fail = keep_on_fail
        self.failed = 0
        self.passed = 0
        self.t0 = time.time()

    # -------------------------------------------------- 断言
    def check(self, name, ok, detail=""):
        ok = bool(ok)
        self.passed += ok
        self.failed += (not ok)
        self.state.add_check(self.key, name, ok, detail)
        self.log("    [%s] %s%s" % ("PASS" if ok else "FAIL", name,
                                    (" — %s" % detail) if detail else ""))
        return ok

    def require(self, name, ok, detail=""):
        """失败即中止本 suite（后面的断言依赖它成立，继续跑只会刷屏假失败）。"""
        if not self.check(name, ok, detail):
            raise SuiteAbort("%s: %s" % (name, detail))
        return True

    def skip(self, reason):
        raise SuiteSkip(reason)

    def step(self, msg):
        self.log("  · %s" % msg)

    # -------------------------------------------------- 资源登记
    def register(self, descriptor):
        self.state.register(self.key, descriptor)
        return descriptor

    def reg_task(self, tid):
        self.register({"type": "task", "id": tid})
        return tid

    def reg_sql_db(self, spec_name, db):
        return self.register({"type": "sql_db", "spec": spec_name, "db": db})

    def reg_mongo_db(self, spec_name, db):
        return self.register({"type": "mongo_db", "spec": spec_name, "db": db})

    def reg_es_index(self, index):
        return self.register({"type": "es_index", "index": index})

    def reg_redis(self, spec_name):
        return self.register({"type": "redis_flush", "spec": spec_name})

    def reg_oracle_table(self):
        return self.register({"type": "oracle_table"})

    def reg_topics(self, prefix):
        return self.register({"type": "kafka_topics", "prefix": prefix})

    # -------------------------------------------------- 常用等待
    def wait_converge(self, src, tgt, timeout=None, task_id=None, label="目标端追平源端"):
        """轮询到两端指纹相等。任务中途 FAILED 就别再干等了。"""
        deadline = time.time() + (timeout or C.CONVERGE_TIMEOUT)
        sfp = tfp = (-1, 0)
        while time.time() < deadline:
            sfp, tfp = src.fingerprint(), tgt.fingerprint()
            if sfp == tfp:
                return True, sfp, tfp
            if task_id and self.api.status(task_id) == "FAILED":
                self.log("    ! 任务已 FAILED，停止等待追平")
                break
            time.sleep(C.POLL_INTERVAL)
        return False, sfp, tfp

    def wait_count(self, ep, expect, timeout=None):
        deadline = time.time() + (timeout or C.CONVERGE_TIMEOUT)
        n = -1
        while time.time() < deadline:
            n = ep.count()
            if n == expect:
                return True, n
            time.sleep(C.POLL_INTERVAL)
        return False, n


# ----------------------------------------------------------------- 注册表
SUITES = {}


class Suite:
    def __init__(self, key, title, group, run, est_secs=180, requires=None, tags=()):
        self.key = key
        self.title = title
        self.group = group          # sync | compare | dr | subscribe | feature
        self.run = run
        self.est_secs = est_secs    # 预估耗时，用于总预算调度
        self.requires = requires    # () -> (ok, reason)
        self.tags = tuple(tags)

    def preflight(self):
        if not self.requires:
            return True, ""
        try:
            return self.requires()
        except Exception as e:  # noqa: BLE001
            return False, "前置检查异常: %s" % e


def suite(key, title, group, est_secs=180, requires=None, tags=()):
    """装饰器：把一个 run(ctx) 函数注册成 suite。"""
    def deco(fn):
        SUITES[key] = Suite(key, title, group, fn, est_secs, requires, tags)
        return fn
    return deco
