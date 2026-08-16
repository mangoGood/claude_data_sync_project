#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""后端 REST 客户端（仅标准库 urllib，不引第三方依赖）。

只封装用例真正会用到的端点；返回值一律是后端的 ApiResponse dict。
"""
import json
import time
import urllib.error
import urllib.request

from . import config as C


class ApiError(RuntimeError):
    pass


class Api:
    def __init__(self, base_url=None, user=None, password=None):
        self.base = (base_url or C.BASE_URL).rstrip("/")
        self.user = user or C.USER
        self.password = password or C.PASSWORD
        self.token = None

    # -------------------------------------------------- 底层
    def raw(self, method, path, body=None, timeout=60):
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                text = resp.read().decode()
        except urllib.error.HTTPError as e:
            text = e.read().decode()
        except Exception as e:  # 连接层错误：网络/后端没起来
            raise ApiError("%s %s 请求失败: %s" % (method, path, e))
        try:
            return json.loads(text) if text else {}
        except ValueError:
            return {"success": False, "message": text[:500]}

    def get(self, p, **kw):
        return self.raw("GET", p, **kw)

    def post(self, p, body=None, **kw):
        return self.raw("POST", p, body, **kw)

    def put(self, p, body=None, **kw):
        return self.raw("PUT", p, body, **kw)

    def delete(self, p, **kw):
        return self.raw("DELETE", p, **kw)

    # -------------------------------------------------- 鉴权
    def login(self):
        d = self.raw("POST", "/api/auth/login",
                     {"username": self.user, "password": self.password})
        tok = d.get("token") or (d.get("data") or {}).get("token")
        if not tok:
            raise ApiError("登录失败: %s" % d)
        self.token = tok
        return tok

    # -------------------------------------------------- 任务生命周期
    def create_workflow(self, name, source_type, target_type,
                        task_type="SYNC", dr_mode=None):
        body = {"name": name, "sourceType": source_type, "targetType": target_type,
                "taskType": task_type}
        if dr_mode:
            body["drMode"] = dr_mode
        d = self.post("/api/workflows", body)
        wid = (d.get("data") or {}).get("id")
        if not wid:
            raise ApiError("创建任务失败: %s" % d)
        return wid

    def config_workflow(self, wid, cfg):
        d = self.put("/api/workflows/%s/config" % wid, cfg)
        if not d.get("success"):
            raise ApiError("配置任务失败: %s" % d)
        return d

    def route_config(self, wid, route_config):
        d = self.put("/api/workflows/%s/route-config" % wid, {"routeConfig": route_config})
        if not d.get("success"):
            raise ApiError("路由配置失败: %s" % d)
        return d

    def launch(self, wid):
        d = self.post("/api/workflows/%s/launch" % wid, {})
        if not d.get("success"):
            raise ApiError("启动任务失败: %s" % d)
        return d

    def workflow(self, wid):
        return self.get("/api/workflows/%s" % wid).get("data") or {}

    def status(self, wid):
        return self.workflow(wid).get("status")

    def stop(self, wid):
        return self.post("/api/workflows/%s/stop" % wid, {})

    def remove(self, wid):
        return self.delete("/api/workflows/%s" % wid)

    def failover(self, wid):
        return self.post("/api/workflows/%s/failover" % wid, {})

    def switchover(self, wid, drain_timeout_ms=180000, fence=True):
        return self.post("/api/workflows/%s/switchover" % wid,
                         {"drainTimeoutMs": drain_timeout_ms, "fence": fence},
                         timeout=drain_timeout_ms // 1000 + 60)

    def list_workflows(self, page_size=200):
        d = self.get("/api/workflows?page=1&pageSize=%d" % page_size)
        return ((d.get("data") or {}).get("list")) or []

    # -------------------------------------------------- 等待
    def wait_status(self, wid, wanted, timeout=None, fail_fast=True):
        """等任务到达 wanted（可以是单个状态或状态集合）。

        返回 (ok, last_status)。fail_fast=True 时任务 FAILED 直接返回（除非 FAILED 就是目标）。
        """
        wanted = {wanted} if isinstance(wanted, str) else set(wanted)
        deadline = time.time() + (timeout or C.LAUNCH_TIMEOUT)
        st = None
        while time.time() < deadline:
            st = self.status(wid)
            if st in wanted:
                return True, st
            if fail_fast and st == "FAILED" and "FAILED" not in wanted:
                return False, st
            time.sleep(C.POLL_INTERVAL)
        return False, st

    # -------------------------------------------------- 数据对比
    def create_validation(self, wid, compare_type="ROW_COUNT"):
        d = self.post("/api/validation-tasks",
                      {"workflowId": wid, "compareType": compare_type})
        if not d.get("success"):
            raise ApiError("创建对比任务失败(%s): %s" % (compare_type, d.get("message")))
        return (d.get("data") or {}).get("id")

    def validation(self, vid):
        return self.get("/api/validation-tasks/%s" % vid).get("data") or {}

    def wait_validation(self, vid, timeout=None):
        deadline = time.time() + (timeout or C.COMPARE_TIMEOUT)
        d = {}
        while time.time() < deadline:
            d = self.validation(vid)
            if d.get("status") in ("COMPLETED", "FAILED", "PARTIAL"):
                return d
            time.sleep(C.POLL_INTERVAL)
        d["status"] = d.get("status") or "TIMEOUT"
        return d

    def run_compare(self, wid, compare_type="ROW_COUNT", timeout=None):
        """建对比任务并等出结论，返回 (vid, result_dict)。"""
        vid = self.create_validation(wid, compare_type)
        return vid, self.wait_validation(vid, timeout)

    def repair_validation(self, vid):
        return self.post("/api/validation-tasks/%s/repair" % vid, {}, timeout=300)

    def delete_validation(self, vid):
        return self.delete("/api/validation-tasks/%s" % vid)


def compare_tables(result):
    """对比明细：compareResult 是 JSON 串 {"tables":[...]}。"""
    raw = result.get("compareResult")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return []
    return (raw or {}).get("tables", []) if isinstance(raw, dict) else []
