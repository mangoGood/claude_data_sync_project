#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端冒烟测试 harness（CI 可跑，零第三方依赖）。

把本项目反复手跑的"起任务 → 打数据 → 验结果 → 清理"固化为可重复、可作 CI 门禁的脚本。
仅用 Python 3 标准库（urllib）+ docker exec 访问 MySQL，无需 pip 安装。

前置：基础设施 + agent + backend 已启动（./create_env.sh 后 ./start.sh）。
用法：
    python3 test_scripts/e2e_smoke.py                # 跑全部场景
    python3 test_scripts/e2e_smoke.py multidb_compare # 只跑指定场景
环境变量覆盖（CI）：
    E2E_BASE_URL(默认 http://localhost:38080) E2E_USER/E2E_PASS(admin/admin123)
    E2E_MYSQL_CONTAINER(synctask-mysql) E2E_MYSQL_USER/E2E_MYSQL_PASS(root/rootpassword)
    E2E_SRC_CONN/E2E_TGT_CONN(mysql://root:rootpassword@localhost:33306)
退出码：全部通过 0，任一失败非 0。
"""
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error

BASE_URL = os.environ.get("E2E_BASE_URL", "http://localhost:38080")
USER = os.environ.get("E2E_USER", "admin")
PASS = os.environ.get("E2E_PASS", "admin123")
MYSQL_CONTAINER = os.environ.get("E2E_MYSQL_CONTAINER", "synctask-mysql")
MYSQL_USER = os.environ.get("E2E_MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("E2E_MYSQL_PASS", "rootpassword")
SRC_CONN = os.environ.get("E2E_SRC_CONN", "mysql://root:rootpassword@localhost:33306")
TGT_CONN = os.environ.get("E2E_TGT_CONN", "mysql://root:rootpassword@localhost:33306")

STATUS_WAIT_SECS = int(os.environ.get("E2E_STATUS_WAIT", "240"))
PROPAGATE_SECS = int(os.environ.get("E2E_PROPAGATE", "15"))


# ----------------------------- HTTP -----------------------------
def _req(method, path, token=None, body=None):
    url = BASE_URL + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
    except Exception as e:
        raise RuntimeError("请求 %s %s 失败: %s" % (method, path, e))
    try:
        return json.loads(raw)
    except ValueError:
        return {"raw": raw}


def login():
    d = _req("POST", "/api/auth/login", body={"username": USER, "password": PASS})
    tok = d.get("token") or (d.get("data") or {}).get("token")
    if not tok:
        raise RuntimeError("登录失败: %s" % d)
    return tok


# ----------------------------- MySQL -----------------------------
def mysql(sql):
    cmd = ["docker", "exec", "-i", MYSQL_CONTAINER, "mysql",
           "-u" + MYSQL_USER, "-p" + MYSQL_PASS, "--default-character-set=utf8mb4",
           "-N", "-e", sql]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0 and "Using a password" not in p.stderr:
        raise RuntimeError("MySQL 执行失败: %s\n%s" % (sql[:120], p.stderr))
    return p.stdout.strip()


def mysql_scalar(sql):
    out = mysql(sql)
    return out.splitlines()[0].strip() if out else ""


# ----------------------------- workflow 生命周期 -----------------------------
def create_workflow(token, name, src_type="MySQL", tgt_type="MySQL"):
    d = _req("POST", "/api/workflows", token,
             {"name": name, "sourceType": src_type, "targetType": tgt_type})
    wid = (d.get("data") or {}).get("id") or d.get("id")
    if not wid:
        raise RuntimeError("创建任务失败: %s" % d)
    return wid


def config_workflow(token, wid, sync_objects, mode="fullAndIncre"):
    d = _req("PUT", "/api/workflows/%s/config" % wid, token, {
        "sourceConnection": SRC_CONN, "targetConnection": TGT_CONN,
        "migrationMode": mode, "syncObjects": sync_objects})
    if not d.get("success"):
        raise RuntimeError("配置任务失败: %s" % d)


def launch_workflow(token, wid):
    d = _req("POST", "/api/workflows/%s/launch" % wid, token)
    if not d.get("success"):
        raise RuntimeError("启动任务失败: %s" % d)


def workflow_status(token, wid):
    d = _req("GET", "/api/workflows/%s" % wid, token)
    return (d.get("data") or {}).get("status")


def wait_status(token, wid, target, timeout=STATUS_WAIT_SECS):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = workflow_status(token, wid)
        if st == target:
            return True
        if st == "FAILED" and target != "FAILED":
            raise RuntimeError("任务意外 FAILED，期望 %s" % target)
        time.sleep(3)
    raise RuntimeError("等待状态 %s 超时（当前 %s）" % (target, workflow_status(token, wid)))


def delete_workflow(token, wid):
    try:
        _req("POST", "/api/workflows/%s/stop" % wid, token)
        time.sleep(3)
    except Exception:
        pass
    # 已完成/失败/配置中才可删；先降级状态再删，尽力而为
    try:
        _req("DELETE", "/api/workflows/%s" % wid, token)
    except Exception:
        pass


# ----------------------------- 断言 -----------------------------
class Fail(Exception):
    pass


def expect(cond, msg):
    if not cond:
        raise Fail(msg)


# ============================================================
# 场景 1：mysql→mysql 全量+增量，库名映射
# 覆盖：启动全链路、per-db 目标库解析（P0 串库修复）、全量快照、增量 I/U/D
# ============================================================
def scenario_mysql_mapping(token):
    src, tgt = "e2e_src1", "e2e_tgt1"
    mysql("DROP DATABASE IF EXISTS %s; DROP DATABASE IF EXISTS %s" % (src, tgt))
    mysql("CREATE DATABASE %s" % src)
    mysql("CREATE TABLE %s.t1 (id INT PRIMARY KEY, val VARCHAR(50))" % src)
    mysql("INSERT INTO %s.t1 VALUES (1,'a'),(2,'b')" % src)  # 全量快照
    wid = create_workflow(token, "e2e-mapping-%d" % int(time.time()))
    try:
        sync = '{"%s":{"tables":["t1"],"targetDb":"%s"}}' % (src, tgt)
        config_workflow(token, wid, sync)
        launch_workflow(token, wid)
        wait_status(token, wid, "INCREMENT_RUNNING")
        # 全量：映射到 tgt 库
        expect(mysql_scalar("SELECT COUNT(*) FROM %s.t1" % tgt) == "2", "全量应搬 2 行到映射库")
        # 增量 INSERT / UPDATE / DELETE
        mysql("INSERT INTO %s.t1 VALUES (3,'c')" % src)
        mysql("UPDATE %s.t1 SET val='B' WHERE id=2" % src)
        mysql("DELETE FROM %s.t1 WHERE id=1" % src)
        time.sleep(PROPAGATE_SECS)
        expect(mysql_scalar("SELECT COUNT(*) FROM %s.t1" % tgt) == "2", "增量后应为 2 行(3新增/1删除)")
        expect(mysql_scalar("SELECT val FROM %s.t1 WHERE id=2" % tgt) == "B", "增量 UPDATE 未生效")
        expect(mysql_scalar("SELECT val FROM %s.t1 WHERE id=3" % tgt) == "c", "增量 INSERT 未生效")
        expect(mysql_scalar("SELECT COUNT(*) FROM %s.t1 WHERE id=1" % tgt) == "0", "增量 DELETE 未生效")
        return "全量2行+增量I/U/D 均按库名映射(%s→%s)传播" % (src, tgt)
    finally:
        delete_workflow(token, wid)
        mysql("DROP DATABASE IF EXISTS %s; DROP DATABASE IF EXISTS %s" % (src, tgt))


# ============================================================
# 场景 2：多库对比链路（数据链路#1）
# 覆盖：per-db 目标库映射贯通对比链路、行数/内容对比、投毒差异检出
# ============================================================
def scenario_multidb_compare(token):
    s1, s2, t1, t2 = "e2e_cs1", "e2e_cs2", "e2e_ct1", "e2e_ct2"
    for db in (s1, s2, t1, t2):
        mysql("DROP DATABASE IF EXISTS %s" % db)
    for s, t, n in ((s1, t1, 2), (s2, t2, 1)):
        mysql("CREATE DATABASE %s; CREATE DATABASE %s" % (s, t))
        mysql("CREATE TABLE %s.t1 (id INT PRIMARY KEY, val VARCHAR(50))" % s)
        mysql("CREATE TABLE %s.t1 (id INT PRIMARY KEY, val VARCHAR(50))" % t)
        vals = ",".join("(%d,'v%d')" % (i, i) for i in range(1, n + 1))
        mysql("INSERT INTO %s.t1 VALUES %s" % (s, vals))
        mysql("INSERT INTO %s.t1 VALUES %s" % (t, vals))  # 两端先一致
    wid = create_workflow(token, "e2e-cmp-%d" % int(time.time()))
    try:
        sync = ('{"%s":{"tables":["t1"],"targetDb":"%s"},'
                '"%s":{"tables":["t1"],"targetDb":"%s"}}') % (s1, t1, s2, t2)
        config_workflow(token, wid, sync)
        # 对比前置：任务须增量中。走正常 launch 到 INCREMENT_RUNNING
        launch_workflow(token, wid)
        wait_status(token, wid, "INCREMENT_RUNNING")

        # 行数对比：两库都应 MATCH
        r = run_validation(token, wid, "ROW_COUNT")
        expect(r["passed"] == 2 and r["failed"] == 0, "行数对比应全 MATCH，实际 %s" % r)

        # 投毒一个映射目标库 → 内容对比应检出差异
        mysql("UPDATE %s.t1 SET val='POISON' WHERE id=1" % t2)
        r2 = run_validation(token, wid, "CONTENT")
        expect(r2["failed"] >= 1, "投毒后内容对比应检出差异，实际 %s" % r2)
        return "多库映射行数对比全 MATCH；投毒映射库后内容对比检出差异(failed=%d)" % r2["failed"]
    finally:
        delete_workflow(token, wid)
        for db in (s1, s2, t1, t2):
            mysql("DROP DATABASE IF EXISTS %s" % db)


def run_validation(token, wid, compare_type, wait=45):
    d = _req("POST", "/api/validation-tasks", token,
             {"workflowId": wid, "compareType": compare_type})
    vid = (d.get("data") or {}).get("id")
    if not vid:
        raise Fail("创建 %s 对比任务失败: %s" % (compare_type, d.get("message") or d))
    deadline = time.time() + wait
    while time.time() < deadline:
        vd = (_req("GET", "/api/validation-tasks/%s" % vid, token).get("data") or {})
        if vd.get("status") in ("COMPLETED", "FAILED"):
            res = {"passed": vd.get("passedTables") or 0, "failed": vd.get("failedTables") or 0,
                   "total": vd.get("totalTables") or 0, "vid": vid}
            _req("DELETE", "/api/validation-tasks/%s" % vid, token)
            return res
        time.sleep(3)
    raise Fail("%s 对比任务执行超时" % compare_type)


# ----------------------------- runner -----------------------------
SCENARIOS = {
    "mysql_mapping": scenario_mysql_mapping,
    "multidb_compare": scenario_multidb_compare,
}


def preflight():
    try:
        code = urllib.request.urlopen(BASE_URL + "/login.html", timeout=5).status
    except Exception as e:
        print("✗ 后端不可达 (%s): %s\n  请先 ./start.sh 启动栈" % (BASE_URL, e))
        sys.exit(2)


def main():
    preflight()
    wanted = sys.argv[1:] or list(SCENARIOS.keys())
    unknown = [w for w in wanted if w not in SCENARIOS]
    if unknown:
        print("未知场景: %s\n可选: %s" % (unknown, ", ".join(SCENARIOS)))
        sys.exit(2)
    token = login()
    print("=== e2e 冒烟测试 @ %s（%d 个场景）===" % (BASE_URL, len(wanted)))
    results = []
    for name in wanted:
        t0 = time.time()
        try:
            detail = SCENARIOS[name](token)
            results.append((name, True, detail, time.time() - t0))
            print("✓ %-18s %.0fs  %s" % (name, time.time() - t0, detail))
        except Exception as e:
            results.append((name, False, str(e), time.time() - t0))
            print("✗ %-18s %.0fs  %s" % (name, time.time() - t0, e))
    passed = sum(1 for _, ok, _, _ in results if ok)
    print("=== 结果：%d/%d 通过 ===" % (passed, len(results)))
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
