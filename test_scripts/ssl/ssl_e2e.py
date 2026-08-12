#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSL/TLS 判据套件。

设计原则只有一条：**一律从服务端视角取证，不看平台自己的日志**。
配置写了 REQUIRED 不等于连接真的加密了——驱动版本不对会静默忽略未知参数，
PREFERRED 在服务端不支持时按定义就退回明文。只信数据库自己的说法：

  MySQL/TiDB   SHOW STATUS LIKE 'Ssl_cipher'  +  performance_schema.threads
  PostgreSQL   pg_stat_ssl

最强的一条判据是**账号级强制**：把测试账号建成 REQUIRE SSL，服务端会拒绝任何
明文连接——于是"任务能跑完"本身就等价于"管线里没有任何一条腿是明文"，
比逐条去查可靠得多（B3 就是靠它逼出了漏掉的 CheckpointManager 那一跳）。

用法:
    python3 test_scripts/ssl/ssl_e2e.py                # 全部
    python3 test_scripts/ssl/ssl_e2e.py --only conn    # 只跑连接判据
"""
import argparse
import json
import subprocess
import sys
import time
import os
import ssl as ssllib
import urllib.error
import urllib.request


def _detect_backend():
    """后端可能是 http 也可能是 https（SYNCTASK_TLS_ALL=1 时）。探一下，别让判据依赖人手改常量。"""
    env = os.environ.get("SSL_E2E_BACKEND")
    if env:
        return env
    for base in ("https://localhost:38080", "http://localhost:38080"):
        try:
            ctx = ssllib._create_unverified_context() if base.startswith("https") else None
            urllib.request.urlopen(base + "/api/health", timeout=5, context=ctx)
            return base
        except urllib.error.HTTPError:
            return base          # 有响应即说明协议对上了
        except Exception:
            continue
    return "http://localhost:38080"


BACKEND = _detect_backend()
# 自签证书：判据里不校验后端证书本身（那是 B5 单独验的事），只要能连上就行
_SSL_CTX = ssllib._create_unverified_context()
SRC = {"container": "dr-mysql-a", "port": "33320"}
TGT = {"container": "dr-mysql-b", "port": "33321"}
DB = "ssl_e2e"
SSL_USER = "ssluser"
SSL_PASS = "SslPass2026"
USER = {"username": "ssl_criteria", "password": "SslCrit@2026", "email": "ssl_criteria@example.com"}

passed, failed = [], []


def ok(name, detail=""):
    passed.append(name)
    print(f"  ✓ {name}" + (f" — {detail}" if detail else ""))


def bad(name, detail=""):
    failed.append(name)
    print(f"  ✗ {name}" + (f" — {detail}" if detail else ""))


def mysql(container, sql, user="root", pw="rootpassword"):
    """在容器内跑 SQL，返回去掉 Warning 的裸输出。"""
    r = subprocess.run(
        ["docker", "exec", container, "mysql", f"-u{user}", f"-p{pw}", "-N", "-e", sql],
        capture_output=True, text=True)
    return "\n".join(l for l in r.stdout.splitlines() if "Warning" not in l).strip()


def api(path, method="GET", body=None, token=None, files=None):
    url = BACKEND + path
    headers = {}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60, context=_SSL_CTX) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {"success": False, "message": f"HTTP {e.code}"}
    except Exception as e:
        return {"success": False, "message": str(e)}


def login():
    api("/api/auth/register", "POST", USER)
    r = api("/api/auth/login", "POST", {"username": USER["username"], "password": USER["password"]})
    return r.get("token")


def upload_cert(token, name, ca_path):
    """multipart 上传，避免引第三方库，手工拼 body。"""
    boundary = "----synctaskssl"
    with open(ca_path, "rb") as f:
        ca = f.read()
    parts = []
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\n{name}\r\n".encode())
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"caCert\"; filename=\"ca.pem\"\r\n"
                 f"Content-Type: application/x-pem-file\r\n\r\n".encode() + ca + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    body = b"".join(parts)
    req = urllib.request.Request(BACKEND + "/api/certificates", data=body, method="POST",
                                 headers={"Authorization": "Bearer " + token,
                                          "Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(req, timeout=60, context=_SSL_CTX) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


# ==================== 判据 ====================

def criteria_accounts():
    """REQUIRE SSL 账号：服务端必须拒绝明文连接。这是整套判据的地基。"""
    print("\n[1] REQUIRE SSL 账号强制")
    for side in (SRC, TGT):
        mysql(side["container"], f"""
            DROP USER IF EXISTS '{SSL_USER}'@'%';
            CREATE USER '{SSL_USER}'@'%' IDENTIFIED WITH mysql_native_password
                BY '{SSL_PASS}' REQUIRE SSL;
            GRANT ALL PRIVILEGES ON *.* TO '{SSL_USER}'@'%' WITH GRANT OPTION;
            FLUSH PRIVILEGES;""")
        t = mysql(side["container"], f"SELECT ssl_type FROM mysql.user WHERE user='{SSL_USER}';")
        if t == "ANY":
            ok(f"{side['container']} 账号 REQUIRE SSL 已生效")
        else:
            bad(f"{side['container']} 账号 REQUIRE SSL", f"ssl_type={t!r}")


def criteria_connection(token, cert_a, cert_b):
    """测连：正向加密、反向拒绝、误判防护。"""
    print("\n[2] 连接判据（服务端回报的协商结果）")

    def tc(port, mode=None, cert=None):
        b = {"host": "127.0.0.1", "port": port, "username": SSL_USER, "password": SSL_PASS,
             "type": "mysql", "dbType": "mysql"}
        if mode:
            b["sslMode"] = mode
        if cert:
            b["sslCertId"] = cert
        return api("/api/metadata/test-connection", "POST", b, token).get("data", {})

    d = tc(SRC["port"], "VERIFY_CA", cert_a)
    if d.get("connected") and d.get("encrypted") and d.get("tlsVersion"):
        ok("源库 VERIFY_CA", f"{d['tlsVersion']}/{d.get('tlsCipher')}")
    else:
        bad("源库 VERIFY_CA", str(d)[:140])

    d = tc(TGT["port"], "VERIFY_CA", cert_b)
    if d.get("connected") and d.get("encrypted"):
        ok("目标库 VERIFY_CA", f"{d['tlsVersion']}/{d.get('tlsCipher')}")
    else:
        bad("目标库 VERIFY_CA", str(d)[:140])

    # 反向判据：账号是 REQUIRE SSL 的，因此**绝不允许出现"连上了而且是明文"**。
    #
    # 注意不能简单断言"必须连不上"：部署级 CONTROL_PLANE_DB_SSL_MODE 是一条**下限**——
    # 任务没配加密时会回落到它。SYNCTASK_TLS_ALL=1 的环境下那个下限是 REQUIRED，
    # 于是连接照样是加密的、服务端也照样接受。两种结果都对，错的只有"明文却连上了"。
    d = tc(SRC["port"])
    if d.get("connected") and d.get("encrypted") is False:
        bad("反向：竟然以明文连上了 REQUIRE SSL 账号（存在静默降级）")
    elif not d.get("connected"):
        ok("反向：未配加密时被服务端拒绝", d.get("errorType"))
    else:
        ok("反向：未配加密时回落到控制面下限并加密",
           f"{d.get('tlsVersion')}/{d.get('tlsCipher')}")

    # 误判防护：口令错、地址错都不能被报成 SSL 问题
    b = {"host": "127.0.0.1", "port": SRC["port"], "username": SSL_USER, "password": "WRONG",
         "type": "mysql", "dbType": "mysql", "sslMode": "REQUIRED"}
    d = api("/api/metadata/test-connection", "POST", b, token).get("data", {})
    if d.get("errorType") == "AUTH_FAILED":
        ok("误判防护：口令错仍报 AUTH_FAILED")
    else:
        bad("误判防护：口令错", f"errorType={d.get('errorType')}")

    b = {"host": "127.0.0.1", "port": "39999", "username": SSL_USER, "password": SSL_PASS,
         "type": "mysql", "dbType": "mysql", "sslMode": "REQUIRED"}
    d = api("/api/metadata/test-connection", "POST", b, token).get("data", {})
    if d.get("errorType") == "NETWORK_ERROR":
        ok("误判防护：地址错仍报 NETWORK_ERROR")
    else:
        bad("误判防护：地址错", f"errorType={d.get('errorType')}")


def criteria_task(token, cert_a, cert_b):
    """全流程：全量 + 增量，跑完即证明没有明文腿（账号 REQUIRE SSL）。"""
    print("\n[3] 全流程（全量 + 增量）")
    mysql(SRC["container"], f"""
        DROP DATABASE IF EXISTS {DB};
        CREATE DATABASE {DB} CHARACTER SET utf8mb4;
        CREATE TABLE {DB}.orders (id INT PRIMARY KEY AUTO_INCREMENT, customer VARCHAR(64),
            amount DECIMAL(10,2), note TEXT);
        INSERT INTO {DB}.orders (customer, amount, note) VALUES
            ('alice',100.50,'full-1'),('bob',200.75,'full-2'),('carol',300.00,'full-3');""")
    mysql(TGT["container"], f"DROP DATABASE IF EXISTS {DB};")

    r = api("/api/workflows", "POST",
            {"name": f"ssl-criteria-{int(time.time())}", "sourceType": "mysql",
             "targetType": "mysql", "consistencyMode": "EVENTUAL"}, token)
    if not r.get("success"):
        bad("建任务", str(r)[:140])
        return None
    wf = r["data"]["id"]

    r = api(f"/api/workflows/{wf}/config", "PUT", {
        "sourceConnection": f"mysql://{SSL_USER}:{SSL_PASS}@127.0.0.1:{SRC['port']}",
        "targetConnection": f"mysql://{SSL_USER}:{SSL_PASS}@127.0.0.1:{TGT['port']}",
        "migrationMode": "fullAndIncre",
        "syncObjects": json.dumps({DB: {"tables": ["orders"]}}),
        "sourceDbName": DB, "targetDbName": DB,
        "sourceType": "mysql", "targetType": "mysql",
        "sourceSslMode": "VERIFY_CA", "sourceSslCertId": cert_a,
        "targetSslMode": "VERIFY_CA", "targetSslCertId": cert_b}, token)
    if not r.get("success"):
        bad("保存配置", str(r)[:140])
        return wf

    r = api(f"/api/workflows/{wf}/launch?force=true", "POST", {}, token)
    if not r.get("success"):
        bad("启动任务", str(r)[:140])
        return wf

    status = ""
    for _ in range(90):
        d = api(f"/api/workflows/{wf}", token=token).get("data", {})
        status = d.get("status", "")
        if status in ("INCREMENT_RUNNING", "COMPLETED", "FAILED"):
            break
        time.sleep(4)
    if status == "FAILED":
        d = api(f"/api/workflows/{wf}", token=token).get("data", {})
        bad("任务启动", f"{status} {d.get('error_message')}")
        return wf
    ok(f"任务进入 {status}")

    n = mysql(TGT["container"], f"SELECT COUNT(*) FROM {DB}.orders;")
    if n == "3":
        ok("全量 3 行")
    else:
        bad("全量", f"目标行数={n!r}")

    mysql(SRC["container"], f"""
        INSERT INTO {DB}.orders (customer, amount, note) VALUES ('dave',444.44,'inc-1');
        UPDATE {DB}.orders SET amount=999.99, note='inc-upd' WHERE customer='alice';
        DELETE FROM {DB}.orders WHERE customer='bob';""")
    md5sql = ("SELECT MD5(GROUP_CONCAT(CONCAT_WS('|',id,customer,amount,note) ORDER BY id)) "
              f"FROM {DB}.orders;")
    a = b = ""
    for _ in range(40):
        a = mysql(SRC["container"], md5sql)
        b = mysql(TGT["container"], md5sql)
        if a and a == b:
            break
        time.sleep(3)
    if a and a == b:
        ok("增量 INSERT/UPDATE/DELETE 后源目标 MD5 一致")
    else:
        bad("增量一致性", f"src={a[:12]} tgt={b[:12]}")
    return wf


def criteria_server_side():
    """服务端取证：所有 ssluser 连接都必须有 Ssl_cipher，含 binlog 复制流。"""
    print("\n[4] 服务端取证（performance_schema）")
    q = ("SELECT CONCAT(t.PROCESSLIST_COMMAND,'|',COALESCE(sv.VARIABLE_VALUE,'')) "
         "FROM performance_schema.threads t "
         "LEFT JOIN performance_schema.status_by_thread sv "
         "  ON sv.THREAD_ID=t.THREAD_ID AND sv.VARIABLE_NAME='Ssl_cipher' "
         f"WHERE t.PROCESSLIST_USER='{SSL_USER}';")
    for side, label in ((SRC, "源库"), (TGT, "目标库")):
        rows = [r for r in mysql(side["container"], q).splitlines() if r.strip()]
        if not rows:
            bad(f"{label} 未找到 {SSL_USER} 连接（任务可能没在跑）")
            continue
        plain = [r for r in rows if r.split("|", 1)[1] == ""]
        if plain:
            bad(f"{label} 存在明文连接", "; ".join(plain[:3]))
        else:
            ok(f"{label} 全部 {len(rows)} 条连接均已加密")
        binlog = [r for r in rows if "Binlog Dump" in r]
        if label == "源库":
            if binlog and binlog[0].split("|", 1)[1]:
                ok("binlog 复制流已加密", binlog[0].split("|", 1)[1])
            elif binlog:
                bad("binlog 复制流是明文", binlog[0])
            else:
                bad("源库未找到 Binlog Dump 连接")


def cleanup(token, wf):
    if wf:
        api(f"/api/workflows/{wf}/stop", "POST", {}, token)
        time.sleep(2)
        api(f"/api/workflows/{wf}", "DELETE", token=token)
    for c in api("/api/certificates", token=token).get("data", []) or []:
        api(f"/api/certificates/{c['id']}", "DELETE", token=token)
    for side in (SRC, TGT):
        mysql(side["container"], f"DROP DATABASE IF EXISTS {DB}; DROP USER IF EXISTS '{SSL_USER}'@'%';")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["accounts", "conn", "task", "server"], help="只跑某一组")
    ap.add_argument("--keep", action="store_true", help="跑完不清理（便于手工排查）")
    args = ap.parse_args()

    print("=" * 62)
    print("  SSL/TLS 判据套件 —— 一律以服务端视角取证")
    print(f"  后端: {BACKEND}")
    print("=" * 62)

    token = login()
    if not token:
        print("✗ 无法登录后端，先确认 ./start.sh 已跑起来")
        return 1

    wf = None
    try:
        if args.only in (None, "accounts", "conn", "task", "server"):
            criteria_accounts()
        ca_a = subprocess.run(["docker", "exec", SRC["container"], "cat", "/var/lib/mysql/ca.pem"],
                              capture_output=True, text=True).stdout
        ca_b = subprocess.run(["docker", "exec", TGT["container"], "cat", "/var/lib/mysql/ca.pem"],
                              capture_output=True, text=True).stdout
        import tempfile, os
        pa = os.path.join(tempfile.gettempdir(), "ssl_ca_a.pem")
        pb = os.path.join(tempfile.gettempdir(), "ssl_ca_b.pem")
        open(pa, "w").write(ca_a)
        open(pb, "w").write(ca_b)
        ra = upload_cert(token, f"criteria-src-{int(time.time())}", pa)
        rb = upload_cert(token, f"criteria-tgt-{int(time.time())}", pb)
        cert_a = ra.get("data", {}).get("id")
        cert_b = rb.get("data", {}).get("id")
        if not cert_a or not cert_b:
            bad("上传证书", f"{ra.get('message')} / {rb.get('message')}")
            raise SystemExit(1)
        ok("上传源/目标 CA 证书")

        if args.only in (None, "conn"):
            criteria_connection(token, cert_a, cert_b)
        if args.only in (None, "task"):
            wf = criteria_task(token, cert_a, cert_b)
        if args.only in (None, "server"):
            criteria_server_side()
    finally:
        if not args.keep:
            cleanup(token, wf)

    print("\n" + "=" * 62)
    print(f"  通过 {len(passed)} / 失败 {len(failed)}")
    if failed:
        for f in failed:
            print(f"    ✗ {f}")
    print("=" * 62)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
