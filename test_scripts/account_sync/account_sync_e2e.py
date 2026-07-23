#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
账号同步 端到端测试（mysql→mysql，跨实例）。

覆盖用户要求的三个场景：
  1. 存量账号：全量阶段随表结构同步（含普通账号 + 超级账号权限过滤）
  2. 增量期新建账号：CREATE USER + GRANT 在增量过程中同步
  3. 增量期改口令 / 改权限：ALTER USER 改密码、GRANT 加权限在增量过程中同步
  外加：不同步超级权限时，全局 GRANT ALL / GRANT SUPER 被过滤

拓扑：源=synctask-mysql(localhost:33306, binlog on)，目标=acct-sync-target(localhost:33307)。
账号是服务器级对象，必须用两个独立实例才能验证"从源同步到目标"。

前置：backend(38080) + agent 已带新代码启动；目标实例 33307 已起。
用法：python3 test_scripts/account_sync/account_sync_e2e.py
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
MYSQL = os.environ.get("MYSQL_BIN", "/opt/anaconda3/bin/mysql")

SRC_PORT = 33306
TGT_PORT = 33307
SRC_CONN = "mysql://root:rootpassword@localhost:%d" % SRC_PORT
TGT_CONN = "mysql://root:rootpassword@localhost:%d" % TGT_PORT
DB = "acct_e2e_db"

# 测试账号（避开系统账号 root/mysql.*）
APP = "acct_full_app"        # 存量普通账号
SUPER = "acct_full_super"    # 存量超级账号（验证超级权限过滤）
INC = "acct_inc_new"         # 增量新建账号
INC_TMP = "acct_inc_tmp"     # 增量新建后删除
ALL_TEST_USERS = [APP, SUPER, INC, INC_TMP]

STATUS_WAIT = int(os.environ.get("E2E_STATUS_WAIT", "300"))
PROP_WAIT = int(os.environ.get("E2E_PROP_WAIT", "90"))


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


# ----------------------------- MySQL (本地 client, 两个实例) -----------------------------
def _mysql(port, sql):
    cmd = [MYSQL, "-h127.0.0.1", "-P%d" % port, "-uroot", "-prootpassword",
           "--default-character-set=utf8mb4", "-N", "-e", sql]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0 and "Using a password" not in p.stderr:
        raise RuntimeError("MySQL(%d) 执行失败: %s\n%s" % (port, sql[:160], p.stderr))
    return p.stdout.strip()


def src(sql):
    return _mysql(SRC_PORT, sql)


def tgt(sql):
    return _mysql(TGT_PORT, sql)


def src1(sql):
    o = src(sql)
    return o.splitlines()[0].strip() if o else ""


def tgt1(sql):
    o = tgt(sql)
    return o.splitlines()[0].strip() if o else ""


def grants_on(port, user):
    try:
        return _mysql(port, "SHOW GRANTS FOR '%s'@'%%'" % user)
    except RuntimeError:
        return ""


def user_exists(port, user):
    return _mysql(port, "SELECT 1 FROM mysql.user WHERE user='%s' AND host='%%'" % user).strip() == "1"


def auth_string(port, user):
    return _mysql(port, "SELECT authentication_string FROM mysql.user WHERE user='%s' AND host='%%'" % user).strip()


# ----------------------------- workflow 生命周期 -----------------------------
def create_workflow(token, name):
    d = _req("POST", "/api/workflows", token,
             {"name": name, "sourceType": "MySQL", "targetType": "MySQL"})
    wid = (d.get("data") or {}).get("id") or d.get("id")
    if not wid:
        raise RuntimeError("创建任务失败: %s" % d)
    return wid


def config_workflow(token, wid, sync_objects, sync_account, sync_super):
    d = _req("PUT", "/api/workflows/%s/config" % wid, token, {
        "sourceConnection": SRC_CONN, "targetConnection": TGT_CONN,
        "migrationMode": "fullAndIncre", "syncObjects": sync_objects,
        "sourceType": "mysql", "targetType": "mysql",
        "syncAccount": sync_account, "syncAccountSuperPrivilege": sync_super})
    if not d.get("success"):
        raise RuntimeError("配置任务失败: %s" % d)


def launch_workflow(token, wid):
    d = _req("POST", "/api/workflows/%s/launch" % wid, token)
    if not d.get("success"):
        raise RuntimeError("启动任务失败: %s" % d)


def workflow_status(token, wid):
    d = _req("GET", "/api/workflows/%s" % wid, token)
    return (d.get("data") or {}).get("status")


def wait_status(token, wid, target, timeout=STATUS_WAIT):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        st = workflow_status(token, wid)
        if st != last:
            print("   ... 任务状态: %s" % st)
            last = st
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
    try:
        _req("DELETE", "/api/workflows/%s" % wid, token)
    except Exception:
        pass


# ----------------------------- 断言 / 轮询 -----------------------------
PASSED = []
FAILED = []


def check(name, cond, detail=""):
    if cond:
        PASSED.append(name)
        print("   ✓ %s %s" % (name, detail))
    else:
        FAILED.append("%s %s" % (name, detail))
        print("   ✗ %s %s" % (name, detail))


def wait_until(fn, timeout=PROP_WAIT, interval=3):
    """轮询直到 fn() 为真或超时；返回最终布尔。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    try:
        return bool(fn())
    except Exception:
        return False


def cleanup_accounts():
    for port in (SRC_PORT, TGT_PORT):
        for u in ALL_TEST_USERS:
            try:
                _mysql(port, "DROP USER IF EXISTS '%s'@'%%'" % u)
            except Exception:
                pass


def setup_source():
    src("DROP DATABASE IF EXISTS %s" % DB)
    src("CREATE DATABASE %s" % DB)
    src("CREATE TABLE %s.t1 (id INT PRIMARY KEY, val VARCHAR(50))" % DB)
    src("INSERT INTO %s.t1 VALUES (1,'a'),(2,'b')" % DB)
    # 存量账号（全量前建好）
    src("CREATE USER '%s'@'%%' IDENTIFIED BY 'pw_app'" % APP)
    src("GRANT SELECT, INSERT ON %s.* TO '%s'@'%%'" % (DB, APP))
    src("CREATE USER '%s'@'%%' IDENTIFIED BY 'pw_super'" % SUPER)
    src("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION" % SUPER)
    src("GRANT SELECT ON %s.* TO '%s'@'%%'" % (DB, SUPER))


def main():
    token = login()
    print("=== 账号同步 E2E（源 33306 → 目标 33307，syncAccount=on, super=off）===")

    # 清理目标库/账号 + 源测试账号
    tgt("DROP DATABASE IF EXISTS %s" % DB)
    cleanup_accounts()
    setup_source()

    wid = create_workflow(token, "acct-sync-e2e-%d" % int(time.time()))
    try:
        sync = '{"%s":{"tables":["t1"]}}' % DB
        config_workflow(token, wid, sync, sync_account=True, sync_super=False)
        launch_workflow(token, wid)

        print("[1] 等待任务进入增量（全量阶段会同步存量账号）...")
        wait_status(token, wid, "INCREMENT_RUNNING")
        time.sleep(5)

        # ---------- 场景1：存量账号（全量同步） ----------
        print("[2] 校验存量账号（全量阶段同步）")
        check("存量普通账号已同步", wait_until(lambda: user_exists(TGT_PORT, APP)),
              "-> %s@%% 存在于目标" % APP)
        g_app = grants_on(TGT_PORT, APP)
        check("存量普通账号授权已同步", "SELECT" in g_app and "INSERT" in g_app and DB in g_app,
              "-> %s" % g_app.replace("\n", " | "))
        check("存量普通账号口令哈希一致(免明文迁移)",
              auth_string(SRC_PORT, APP) == auth_string(TGT_PORT, APP) and auth_string(TGT_PORT, APP) != "")

        # 超级账号：账号本身应创建，但全局超级权限被过滤
        check("存量超级账号已创建", wait_until(lambda: user_exists(TGT_PORT, SUPER)),
              "-> %s@%% 存在于目标" % SUPER)
        g_super = grants_on(TGT_PORT, SUPER).upper()
        check("超级账号未被授予全局 SUPER/ALL/GRANT OPTION（super=off 过滤生效）",
              ("ALL PRIVILEGES ON *.*" not in g_super) and (" SUPER" not in g_super)
              and ("WITH GRANT OPTION" not in g_super),
              "-> %s" % grants_on(TGT_PORT, SUPER).replace("\n", " | "))
        check("超级账号的库级授权仍保留",
              ("SELECT" in g_super and DB.upper() in g_super),
              "-> 库级 SELECT 应保留")

        # ---------- 场景2：增量期新建账号 ----------
        print("[3] 增量期新建账号（CREATE USER + GRANT）")
        src("CREATE USER '%s'@'%%' IDENTIFIED BY 'inc_pw'" % INC)
        src("GRANT SELECT ON %s.* TO '%s'@'%%'" % (DB, INC))
        check("增量新建账号已同步", wait_until(lambda: user_exists(TGT_PORT, INC)),
              "-> %s@%% 出现在目标" % INC)
        check("增量新建账号授权已同步",
              wait_until(lambda: "SELECT" in grants_on(TGT_PORT, INC) and DB in grants_on(TGT_PORT, INC)),
              "-> %s" % grants_on(TGT_PORT, INC).replace("\n", " | "))

        # ---------- 场景3：增量期改口令 ----------
        print("[4] 增量期修改账号口令（ALTER USER IDENTIFIED BY）")
        src("ALTER USER '%s'@'%%' IDENTIFIED BY 'inc_pw_changed'" % INC)
        new_src_hash = auth_string(SRC_PORT, INC)
        check("增量改口令后目标口令哈希跟随源端更新",
              wait_until(lambda: auth_string(TGT_PORT, INC) == new_src_hash and new_src_hash != ""),
              "-> 目标 authentication_string 已与源一致")

        # ---------- 场景3b：增量期改权限 ----------
        print("[5] 增量期修改账号权限（GRANT 加库级权限 / GRANT 全局超级权限被过滤）")
        src("GRANT INSERT, UPDATE ON %s.* TO '%s'@'%%'" % (DB, INC))
        check("增量新增库级权限已同步",
              wait_until(lambda: "INSERT" in grants_on(TGT_PORT, INC) and "UPDATE" in grants_on(TGT_PORT, INC)),
              "-> %s" % grants_on(TGT_PORT, INC).replace("\n", " | "))
        # 增量期授予全局 SUPER，super=off 时应被过滤
        src("GRANT SUPER ON *.* TO '%s'@'%%'" % INC)
        time.sleep(12)
        g_inc = grants_on(TGT_PORT, INC).upper()
        check("增量期全局 SUPER 授权被过滤（super=off）", " SUPER" not in g_inc,
              "-> %s" % grants_on(TGT_PORT, INC).replace("\n", " | "))

        # ---------- 场景4：增量期删除账号 ----------
        print("[6] 增量期新建并删除账号（DROP USER）")
        src("CREATE USER '%s'@'%%' IDENTIFIED BY 'tmp'" % INC_TMP)
        check("增量临时账号已同步", wait_until(lambda: user_exists(TGT_PORT, INC_TMP)))
        src("DROP USER '%s'@'%%'" % INC_TMP)
        check("增量删除账号已同步", wait_until(lambda: not user_exists(TGT_PORT, INC_TMP)),
              "-> %s 已从目标删除" % INC_TMP)

    finally:
        print("[cleanup] 删除任务与测试数据...")
        delete_workflow(token, wid)
        cleanup_accounts()
        try:
            src("DROP DATABASE IF EXISTS %s" % DB)
            tgt("DROP DATABASE IF EXISTS %s" % DB)
        except Exception:
            pass

    print("\n=== 结果：%d 通过 / %d 失败 ===" % (len(PASSED), len(FAILED)))
    for f in FAILED:
        print("  ✗ %s" % f)
    sys.exit(0 if not FAILED else 1)


if __name__ == "__main__":
    main()
