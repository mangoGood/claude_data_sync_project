#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""流量复制与回放：Oracle 引擎级判据。

钉的是 Oracle 统一审计这条通道特有的那批边界，每一条都是实测踩出来的：

  * `SQL_TEXT` 结尾带一个 NUL，不剥掉回放就是 ORA-00911，而录制文件里看不出来；
  * `SQL_BINDS` 形如 `#1(1):2 #2(20):has space and # hash`，**必须按声明长度切片**，
    按 `#` 或空格切会在第一个带空格的值上错位；`#n(0):` 是 NULL 不是空串；
  * `ACTIONS ALL` **抓不到多表 SELECT**，要靠对象级策略补；而对象级策略会让
    一条语句按对象数出多行（`ENTRY_ID` 不同、`STATEMENT_ID` 相同），不去重就回放两遍；
  * 两者**不能合成一条策略**，必须两条同时启用；
  * `NOAUDIT` 与 `AUDIT` 不对称：`BY` 要原样带、`EXCEPT` 带上直接 ORA-46352，
    带错了策略停不掉，审计一直写 AUDSYS；
  * 我们自己的策略 DDL 是 Oracle 的**强制审计**动作，`EXCEPT` 挡不住，要按用户名过滤。

前置：
  * `oracle_db` 容器在跑，FREEPDB1（源）与 MYAPP_DB（目标）两个 PDB 都可用；
  * 采集账号 `trfcap`（AUDIT_ADMIN / AUDIT_VIEWER / SELECT_CATALOG_ROLE）已建好，见 SETUP_SQL；
  * **引擎跑在容器里**：本机 host→oracle_db 的 1521 数据面是坏的（TCP 连得上、
    字节到不了监听器，监听器日志里连一条尝试都没有；同网段容器客户端一切正常）。
    这是本机 Docker 端口转发的环境故障，不是产品问题 —— 所以判据经
    test_scripts/traffic/orajava.sh 在 oracle_db_network 里起容器跑引擎。

用法:
    python3 test_scripts/traffic/traffic_oracle_e2e.py
    python3 test_scripts/traffic/traffic_oracle_e2e.py -k capture
"""
import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
JAR = os.path.join(ROOT, "migration-traffic", "target", "migration-traffic-1.0.0.jar")
ORAJAVA = os.path.join(ROOT, "test_scripts", "traffic", "orajava.sh")
CTR = "oracle_db"
ORACLE_HOME = "/opt/oracle/product/26ai/dbhomeFree"
SRC_SVC = "FREEPDB1"
TGT_SVC = "MYAPP_DB"
APP_USER = "app_user"
APP_PASS = "userpassword"
CAP_USER = "trfcap"
CAP_PASS = "TrfCap123"
NUL = chr(0)

SETUP_SQL = """CREATE USER trfcap IDENTIFIED BY "TrfCap123" QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, AUDIT_ADMIN, AUDIT_VIEWER, SELECT_CATALOG_ROLE TO trfcap;
GRANT SELECT ON V_$DATABASE TO trfcap;
GRANT SELECT ON V_$VERSION TO trfcap;
GRANT SELECT ON V_$OPTION TO trfcap;
GRANT EXECUTE ON DBMS_AUDIT_MGMT TO trfcap;"""

RESULTS = []


# ------------------------------------------------------------------ 基础设施
def check(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", name, ("  — " + str(detail)) if detail else ""))
    return ok


def sqlplus(conn, script, timeout=180):
    """在 oracle_db 容器里跑一段 SQL。conn 形如 user/pass@localhost:1521/service。"""
    body = script if script.rstrip().endswith("exit") else script + "\nexit\n"
    cmd = ("export ORACLE_HOME=%s; export PATH=$ORACLE_HOME/bin:$PATH; "
           "cat > /tmp/_trf.sql <<'__SQL__'\n%s\n__SQL__\n"
           "timeout %d sqlplus -s %s @/tmp/_trf.sql") % (ORACLE_HOME, body, timeout, conn)
    p = subprocess.run(["docker", "exec", "-i", CTR, "bash", "-lc", cmd],
                       capture_output=True, text=True)
    return (p.stdout or "") + (p.stderr or "")


def app(script, svc=SRC_SVC):
    return sqlplus("%s/%s@localhost:1521/%s" % (APP_USER, APP_PASS, svc), script)


def sysdba(script, pdb=SRC_SVC):
    return sqlplus("/ as sysdba", "alter session set container=%s;\nset feedback off\n%s" % (pdb, script))


def scalar(script, svc=SRC_SVC):
    out = app("set pages 0 feedback off lines 300\n" + script, svc)
    for line in out.splitlines():
        line = line.strip()
        if line and not line.startswith("ORA-") and "SQL>" not in line and not line.startswith("Help:"):
            return line
    return ""


def task_dir(task_id):
    return os.path.join(ROOT, "files", task_id)


def rec_dir(task_id):
    return os.path.join(task_dir(task_id), "traffic")


def reset(task_id):
    shutil.rmtree(task_dir(task_id), ignore_errors=True)


def write_config(task_id, extra):
    d = task_dir(task_id)
    os.makedirs(d, exist_ok=True)
    lines = ["task.id=%s" % task_id, "task.name=%s" % task_id]
    lines += ["%s=%s" % (k, v) for k, v in extra.items()]
    with open(os.path.join(d, "config.properties"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return "files/%s/config.properties" % task_id


def oracle_ip():
    p = subprocess.run(["docker", "inspect", CTR, "--format",
                        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"],
                       capture_output=True, text=True)
    return p.stdout.strip()


def capture_config(task_id, **over):
    cfg = {
        "source.db.host": oracle_ip(), "source.db.port": 1521,
        "source.db.username": CAP_USER, "source.db.password": CAP_PASS,
        "source.db.database": SRC_SVC, "source.db.type": "oracle",
        "traffic.capture.classes": "SELECT,DML,DDL,DCL",
        "traffic.capture.databases": APP_USER.upper(),
        # 只录业务账号：审计策略随之变成 BY APP_USER（顺带覆盖 NOAUDIT 必须带 BY 的那条不对称规则），
        # 判据自己用 sysdba 跑的 ALTER SESSION SET CONTAINER 也就不会被录进去
        "traffic.capture.users": APP_USER.upper(),
        "traffic.capture.oracle.poll.ms": 500,
        "traffic.capture.oracle.lag.seconds": 1,
        "traffic.capture.oracle.purge": "false",
    }
    cfg.update(over)
    return write_config(task_id, cfg)


def replay_config(task_id, recording_dir, **over):
    cfg = {
        "target.db.host": oracle_ip(), "target.db.port": 1521,
        "target.db.username": APP_USER, "target.db.password": APP_PASS,
        "target.db.database": TGT_SVC, "target.db.type": "oracle",
        "traffic.replay.recording.dir": recording_dir,
        "traffic.replay.classes": "SELECT,DML,DDL",
    }
    cfg.update(over)
    return write_config(task_id, cfg)


class Engine:
    """在 oracle_db_network 里起一个容器跑 migration-traffic（见文件头的说明）。"""

    IMAGE = "docker.1ms.run/library/flink:scala_2.12-java21"

    def __init__(self, task_id, mode, config):
        self.task_id = task_id
        self.name = "trf-ora-run-" + task_id.replace("_", "-")
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", self.name, "--network", "oracle_db_network",
             "-v", ROOT + ":/work", "-w", "/work", "--entrypoint", "java", self.IMAGE,
             "-Dtask.id=" + task_id, "-jar",
             "migration-traffic/target/migration-traffic-1.0.0.jar",
             "--mode", mode, "--config", config],
            capture_output=True, text=True)

    @property
    def lines(self):
        p = subprocess.run(["docker", "logs", self.name], capture_output=True, text=True)
        return ((p.stdout or "") + (p.stderr or "")).splitlines()

    def wait_log(self, needle, timeout=150):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(needle in l for l in self.lines):
                return True
            time.sleep(1)
        return False

    def wait_exit(self, timeout=300):
        deadline = time.time() + timeout
        while time.time() < deadline:
            p = subprocess.run(["docker", "inspect", self.name, "--format", "{{.State.Running}}"],
                               capture_output=True, text=True)
            if p.stdout.strip() == "false":
                return True
            time.sleep(1)
        return False

    @property
    def returncode(self):
        p = subprocess.run(["docker", "inspect", self.name, "--format", "{{.State.ExitCode}}"],
                           capture_output=True, text=True)
        try:
            return int(p.stdout.strip())
        except ValueError:
            return -1

    def stop(self, timeout=90):
        """SIGTERM + 等收尾。子进程要还原审计策略，给足时间。"""
        subprocess.run(["docker", "stop", "-t", str(timeout), self.name], capture_output=True)

    def kill9(self):
        subprocess.run(["docker", "kill", self.name], capture_output=True)

    def cleanup(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


def read_records(recording_dir):
    out = []
    if not os.path.isdir(recording_dir):
        return out
    for name in sorted(os.listdir(recording_dir)):
        if not name.startswith("seg-") or not name.endswith(".trf.gz"):
            continue
        try:
            with gzip.open(os.path.join(recording_dir, name), "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            out.append(json.loads(line))
                        except ValueError:
                            pass
        except (EOFError, OSError):
            pass
    return out


def read_manifest(recording_dir):
    p = os.path.join(recording_dir, "manifest.json")
    if not os.path.isfile(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def enabled_policies():
    out = sysdba("set pages 0 feedback off\n"
                 "SELECT COUNT(*) FROM audit_unified_enabled_policies "
                 "WHERE policy_name LIKE 'SYNCTASK_TRF%';")
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return -1


def cleanup_policies():
    sysdba("""BEGIN
  FOR p IN (SELECT policy_name FROM audit_unified_enabled_policies WHERE policy_name LIKE 'SYNCTASK_TRF%') LOOP
    BEGIN EXECUTE IMMEDIATE 'NOAUDIT POLICY ' || p.policy_name; EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;
  FOR p IN (SELECT policy_name FROM audit_unified_policies WHERE policy_name LIKE 'SYNCTASK_TRF%') LOOP
    BEGIN EXECUTE IMMEDIATE 'DROP AUDIT POLICY ' || p.policy_name; EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;
END;
/""")


def prepare_tables(svc):
    app("""BEGIN EXECUTE IMMEDIATE 'DROP TABLE oa PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ob PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE oc PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE oa (id NUMBER PRIMARY KEY, v VARCHAR2(200));
CREATE TABLE ob (id NUMBER PRIMARY KEY, w VARCHAR2(200));""", svc)


# ------------------------------------------------------------------ 用例
def case_capture():
    """捕获：类别覆盖 + 多表 SELECT + 绑定值 + NULL 绑定 + 源端错误码 + 自噪声 + 还原。"""
    tid = "trf-ora-cap"
    reset(tid)
    cleanup_policies()
    prepare_tables(SRC_SVC)

    eng = Engine(tid, "capture", capture_config(tid))
    try:
        check("捕获启动", eng.wait_log("Oracle 统一审计捕获已开启", 180))
        check("为已选 schema 建了对象级 SELECT 策略（不建就抓不到 join）",
              any("对象级 SELECT 审计策略" in l for l in eng.lines))
        check("两条策略都已启用（合成一条抓不到 join）", enabled_policies() == 2, enabled_policies())

        app("""set feedback off
INSERT INTO oa VALUES (1, 'zh中文emoji');
INSERT INTO ob VALUES (1, 'joined');
COMMIT;
SELECT /*E2EJOIN*/ a.v, b.w FROM oa a JOIN ob b ON a.id = b.id;
SELECT /*E2EPLAIN*/ * FROM oa;
VARIABLE q1 VARCHAR2(60)
EXEC :q1 := 'has space and # hash';
INSERT INTO oa VALUES (2, :q1);
VARIABLE q2 VARCHAR2(10)
EXEC :q2 := NULL;
INSERT INTO oa VALUES (3, :q2);
COMMIT;
UPDATE oa SET v='updated' WHERE id=1;
COMMIT;
INSERT INTO oa VALUES (4, 'rolled-back');
ROLLBACK;
CREATE TABLE oc (id NUMBER);
INSERT INTO oc VALUES (7);
COMMIT;
SELECT * FROM no_such_table_ora;""")
        time.sleep(12)
        eng.stop()
        check("审计策略已还原并回查归零",
              any("审计策略已还原并回查归零" in l for l in eng.lines) and enabled_policies() == 0,
              enabled_policies())
    finally:
        eng.cleanup()

    recs = read_records(rec_dir(tid))
    mani = read_manifest(rec_dir(tid))
    check("manifest 标了引擎与通道",
          bool(mani) and mani.get("engine") == "oracle"
          and mani.get("captureBackend") == "ORA_UNIFIED_AUDIT",
          mani and (mani.get("engine"), mani.get("captureBackend")))

    def find(sub):
        return [r for r in recs if sub in (r.get("q") or "")]

    check("多表 SELECT（join）正好录一条：0=ACTIONS ALL 漏了、2=对象级出多行没去重",
          len(find("E2EJOIN")) == 1, len(find("E2EJOIN")))
    check("单表 SELECT 录到", len(find("E2EPLAIN")) == 1, len(find("E2EPLAIN")))
    check("DDL 录到", len(find("CREATE TABLE oc")) == 1, len(find("CREATE TABLE oc")))
    check("DML 录到", len(find("INSERT INTO oa VALUES (1")) == 1)
    check("事务控制录到", len([r for r in recs if r.get("k") == "TCL"]) >= 4,
          len([r for r in recs if r.get("k") == "TCL"]))

    ins = find("INSERT INTO oa VALUES (2, :q1)")
    check("绑定值按声明长度切片（值里有空格和 #）",
          bool(ins) and ins[0].get("b") == ["has space and # hash"], ins and ins[0].get("b"))
    insn = find("INSERT INTO oa VALUES (3, :q2)")
    check("#n(0): 读回来是 NULL 而不是空串", bool(insn) and insn[0].get("b") == [None],
          insn and insn[0].get("b"))

    check("SQL 文本没有尾部 NUL（带着它回放就是 ORA-00911，而文件里看不出来）",
          all(NUL not in (r.get("q") or "") for r in recs))

    err = find("no_such_table_ora")
    check("源端错误码（RETURN_CODE）录到了", bool(err) and (err[0].get("e") or {}).get("errno") == 942,
          err and (err[0].get("e") or {}).get("errno"))

    noise = [r for r in recs if "AUDIT POLICY" in (r.get("q") or "").upper()]
    check("我们自己的审计策略 DDL 没被录进去（强制审计动作，EXCEPT 挡不住）", not noise, len(noise))
    check("多字节值原样保留", any("中文" in (r.get("q") or "") for r in recs))


def case_replay():
    """回放到另一个 PDB：数据逐值一致 + 回滚不落地 + DDL 生效 + 零回放错误。"""
    tid = "trf-ora-cap"
    if not os.path.isdir(rec_dir(tid)):
        check("回放用例：需要先跑 capture 用例", False, rec_dir(tid))
        return
    prepare_tables(TGT_SVC)
    app("BEGIN EXECUTE IMMEDIATE 'DROP TABLE oc PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;\n/", TGT_SVC)

    rid = "trf-ora-rep"
    reset(rid)
    eng = Engine(rid, "replay", replay_config(rid, "files/%s/traffic" % tid))
    try:
        check("回放正常结束", eng.wait_exit(360) and eng.returncode == 0, "exit=%s" % eng.returncode)
    finally:
        eng.cleanup()

    q = "SELECT LISTAGG(id || ':' || NVL(v,'<NULL>'), '|') WITHIN GROUP (ORDER BY id) FROM oa;"
    src = scalar(q, SRC_SVC)
    tgt = scalar(q, TGT_SVC)
    check("目标库与源库逐值一致（含 NULL 与多字节）", src == tgt and src != "",
          "源=%s 目标=%s" % (src, tgt))
    check("ROLLBACK 的那条没落地（autocommit 关掉了才有这个结果）", "rolled-back" not in tgt, tgt)
    check("DDL 也回放了", scalar("SELECT COUNT(*) FROM oc;", TGT_SVC) == "1")

    ep = os.path.join(rec_dir(rid), "replay_errors.jsonl")
    errs = [json.loads(l) for l in open(ep, encoding="utf-8")] if os.path.isfile(ep) else []
    real = [e for e in errs if e.get("outcome") == "REPLAY_ERROR"]
    check("零回放错误", not real, [e.get("detail") for e in real][:2])
    skipped = [e for e in errs if e.get("outcome") == "UNREPLAYABLE_NO_BINDS"]
    check("EXEC :v := … 这类赋值块单独记一类，不算回放错误", len(skipped) >= 1, len(skipped))


def case_guards():
    """同实例互锁 + 跨引擎互锁。"""
    tid = "trf-ora-cap"
    if not os.path.isdir(rec_dir(tid)):
        check("互锁用例：需要先跑 capture 用例", False)
        return

    sid = "trf-ora-same"
    reset(sid)
    eng = Engine(sid, "replay", replay_config(sid, "files/%s/traffic" % tid,
                                              **{"target.db.database": SRC_SVC}))
    try:
        eng.wait_exit(300)
        check("回放到录制源 PDB 自己被拒绝（DBID + 容器名一起比）",
              any("同一个 Oracle 实例" in l for l in eng.lines) and eng.returncode != 0,
              "exit=%s" % eng.returncode)
    finally:
        eng.cleanup()

    xid = "trf-ora-xeng"
    reset(xid)
    eng2 = Engine(xid, "replay", replay_config(xid, "files/%s/traffic" % tid,
                                               **{"target.db.type": "mysql"}))
    try:
        eng2.wait_exit(240)
        check("跨引擎回放被拒绝", any("录制来自 Oracle" in l for l in eng2.lines) and eng2.returncode != 0,
              "exit=%s" % eng2.returncode)
    finally:
        eng2.cleanup()


def case_restore_on_kill():
    """kill -9 后审计策略必须靠状态文件兜底还原，并回查归零。"""
    tid = "trf-ora-kill"
    reset(tid)
    cleanup_policies()

    eng = Engine(tid, "capture", capture_config(tid))
    try:
        check("兜底还原用例：捕获启动", eng.wait_log("Oracle 统一审计捕获已开启", 180))
        app("SELECT 1 FROM dual;")
        time.sleep(4)
        eng.kill9()
    finally:
        eng.cleanup()

    check("kill -9 后审计策略仍启用（子进程没机会还原）", enabled_policies() > 0, enabled_policies())
    state = os.path.join(rec_dir(tid), "source_state.properties")
    check("兜底还原状态文件已落盘", os.path.isfile(state))
    if os.path.isfile(state):
        body = open(state, encoding="utf-8").read()
        check("状态文件标了引擎", "source.engine=oracle" in body)
        check("两条策略名与启用范围都记了",
              "restore.ora.policy=" in body and "restore.ora.policy.object=" in body
              and "restore.ora.scope=" in body)
        check("口令不明文落盘", CAP_PASS not in body)

    p = subprocess.run([ORAJAVA, "--mode", "restore", "--task", tid],
                       capture_output=True, text=True, cwd=ROOT)
    left = enabled_policies()
    check("兜底还原成功且回查归零", p.returncode == 0 and left == 0,
          "rc=%s policies=%s" % (p.returncode, left))
    check("还原后状态文件被清掉", not os.path.isfile(state))


CASES = [
    ("capture", case_capture),
    ("replay", case_replay),
    ("guards", case_guards),
    ("restore_kill", case_restore_on_kill),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", dest="filter", default="", help="只跑名字含该串的用例")
    args = ap.parse_args()

    if not os.path.isfile(JAR):
        print("缺少 fat jar，请先跑: mvn -pl migration-traffic -am install -Dmaven.test.skip=true")
        return 2
    if subprocess.run(["docker", "inspect", CTR], capture_output=True).returncode != 0:
        print("缺少 %s 容器" % CTR)
        return 2
    probe = sqlplus("%s/%s@localhost:1521/%s" % (CAP_USER, CAP_PASS, SRC_SVC),
                    "set pages 0 feedback off\nSELECT 'probe_ok' FROM dual;")
    if "probe_ok" not in probe:
        print("采集账号 %s 不可用，请先在 %s 里执行:\n%s" % (CAP_USER, SRC_SVC, SETUP_SQL))
        return 2

    for name, fn in CASES:
        if args.filter and args.filter not in name:
            continue
        print("\n" + "=" * 66)
        print("用例: %s" % name)
        print("=" * 66)
        try:
            fn()
        except Exception as e:                      # noqa: BLE001
            check("用例 %s 未跑完" % name, False, repr(e))

    cleanup_policies()
    print("\n" + "=" * 66)
    ok = sum(1 for _, o, _ in RESULTS if o)
    print("合计 %d 项，通过 %d，失败 %d" % (len(RESULTS), ok, len(RESULTS) - ok))
    for n, o, d in RESULTS:
        if not o:
            print("  ✗ %s  %s" % (n, d))
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
