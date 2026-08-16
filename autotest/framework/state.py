#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""断点续跑状态 + 测试资源登记表。

两个职责：
  1. **续跑**：每个 suite 跑完立刻落盘结论，中途崩溃/被 kill 之后 `--resume` 能跳过已通过的。
  2. **资源登记**：所有本轮创建的任务 / 库 / 索引 / topic 都登记成**可序列化的清理描述符**，
     这样即使主进程已经死了，autotest_stop 也能照着状态文件把残留清干净。
     失败的 suite 不清理（保留现场供排查），其 task id 会进测试报告。
"""
import json
import os
import time

from . import config as C

SPECS = {
    "MYSQL": C.MYSQL, "PG": C.PG, "MONGO_A": C.MONGO_A, "MONGO_B": C.MONGO_B,
    "REDIS_A": C.REDIS_A, "REDIS_B": C.REDIS_B, "ES": C.ES, "TIDB": C.TIDB,
    "ORACLE": C.ORACLE, "DR_MYSQL_A": C.DR_MYSQL_A, "DR_MYSQL_B": C.DR_MYSQL_B,
    "DR_PG_A": C.DR_PG_A, "DR_PG_B": C.DR_PG_B,
}

PASSED, FAILED, SKIPPED, RUNNING = "PASSED", "FAILED", "SKIPPED", "RUNNING"


class State:
    def __init__(self, path=None):
        self.path = path or C.STATE_FILE
        self.data = {"run_id": None, "started_at": None, "profile": None,
                     "args": None, "suites": {}}

    # -------------------------------------------------- 持久化
    def load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    self.data = json.load(f)
            except (ValueError, OSError):
                pass
        return self

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def reset(self, run_id, profile, args):
        self.data = {"run_id": run_id, "started_at": time.time(), "profile": profile,
                     "args": args, "suites": {}}
        self.save()

    # -------------------------------------------------- suite 结论
    def suite(self, key):
        return self.data.setdefault("suites", {}).setdefault(
            key, {"status": None, "checks": [], "resources": [], "task_ids": [],
                  "duration": 0.0, "error": None, "started_at": None})

    def is_passed(self, key):
        return self.data.get("suites", {}).get(key, {}).get("status") == PASSED

    def mark(self, key, status, **kw):
        s = self.suite(key)
        s["status"] = status
        s.update(kw)
        self.save()

    def register(self, key, descriptor):
        """登记一个待清理资源。descriptor 见 cleanup.py。"""
        s = self.suite(key)
        if descriptor not in s["resources"]:
            s["resources"].append(descriptor)
        if descriptor.get("type") == "task":
            tid = descriptor.get("id")
            if tid and tid not in s["task_ids"]:
                s["task_ids"].append(tid)
        self.save()

    def add_check(self, key, name, ok, detail=""):
        self.suite(key)["checks"].append(
            {"name": name, "ok": bool(ok), "detail": str(detail)[:800], "ts": time.time()})

    # -------------------------------------------------- 汇总
    def summary(self):
        out = []
        for k, v in self.data.get("suites", {}).items():
            out.append((k, v.get("status"), v.get("task_ids", []), v.get("checks", [])))
        return out
