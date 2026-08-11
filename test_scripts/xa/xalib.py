#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
源库 XA 事务同步测试的公共脚手架。

拓扑：`dr-mysql-a(33320)` 作源、`dr-mysql-b(33321)` 作目标——两个独立实例，
不占用 `synctask-mysql`（上面常有别的任务在跑）。

关于 XA 会话的两个必须知道的点（都是实测出来的，写脚本时很容易踩）：

  * `XA START … XA PREPARE` 必须在**同一个会话**里，所以这几条要拼成一次
    `mysql -e` 调用；而 `XA COMMIT` / `XA ROLLBACK` 可以在**任意会话**发出——
    分支一旦 prepare 就与会话解绑了，这正是"两阶段"能跨进程协调的原因。
  * 分支 prepare 之后没提交前，源库里那张表的相关行是被锁住的。判据脚本里
    任何对同一行的旁路查询都要用 `READ UNCOMMITTED` 或者干脆避开，否则会卡到锁超时。
"""
import os
import subprocess
import threading
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SRC_CT, SRC_PORT = "dr-mysql-a", 33320
TGT_CT, TGT_PORT = "dr-mysql-b", 33321
PWD = "rootpassword"

TABLE_DDL = """CREATE TABLE {t} (
  id   INT PRIMARY KEY,
  tag  VARCHAR(64),
  amt  INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""

JARS = {
    "capture": os.path.join(PROJECT_ROOT, "migration-capture", "target", "migration-capture-1.0.0.jar"),
    "extract": os.path.join(PROJECT_ROOT, "migration-extract", "target", "migration-extract-1.0.0.jar"),
    "increment": os.path.join(PROJECT_ROOT, "migration-increment", "target", "migration-increment-1.0.0.jar"),
}
MAINS = {
    "capture": "com.migration.capture.CaptureMain",
    "extract": "com.migration.extract.ContinuousExtractMain",
    "increment": "com.migration.increment.ContinuousIncrementMain",
}


# ---------------------------------------------------------------- mysql 客户端

def mysql(container, sql, db=None, want=False, timeout=120):
    args = ["docker", "exec", "-i", container, "mysql", "-uroot", "-p" + PWD,
            "--default-character-set=utf8mb4"]
    if want:
        args.append("-N")
    if db:
        args += ["-D", db]
    p = subprocess.run(args, input=sql, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        err = (p.stderr or "").strip()
        if err and "Using a password" not in err:
            raise RuntimeError(f"[{container}] SQL 失败: {err}\nSQL: {sql[:300]}")
    return (p.stdout or "").strip()


def scalar(container, sql, db=None):
    return mysql(container, sql, db=db, want=True).strip()


def reset_db(container, db):
    mysql(container, f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db} DEFAULT CHARSET utf8mb4;")


def create_table(container, db, table):
    mysql(container, TABLE_DDL.format(t=table), db=db)


def rows_of(container, db, table):
    """目标/源表内容快照：{id: (tag, amt)}。行少，直接全量取。"""
    out = mysql(container, f"SELECT id, tag, amt FROM {table} ORDER BY id", db=db, want=True)
    snapshot = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            snapshot[int(parts[0])] = (parts[1], int(parts[2]))
    return snapshot


def xa_recover(container):
    """源库里还挂着的未决 XA 分支（gtrid 列表）。跑完判据要确保这里是空的。"""
    out = mysql(container, "XA RECOVER", want=True)
    return [line.split("\t")[-1] for line in out.splitlines() if line.strip()]


# ---------------------------------------------------------------- XA 语句

def xa_prepare_branch(container, db, gtrid, statements):
    """在**一个会话**里跑完 XA START → 业务语句 → XA END → XA PREPARE。

    返回后分支处于 prepared 状态：数据已经进了 binlog（行事件 + XA_prepare 事件），
    但在源库里还不可见，也还没有提交决议。
    """
    body = "\n".join(s if s.rstrip().endswith(";") else s + ";" for s in statements)
    mysql(container, f"""XA START '{gtrid}';
{body}
XA END '{gtrid}';
XA PREPARE '{gtrid}';""", db=db)


def xa_commit(container, gtrid):
    mysql(container, f"XA COMMIT '{gtrid}';")


def xa_rollback(container, gtrid):
    mysql(container, f"XA ROLLBACK '{gtrid}';")


def xa_one_phase(container, db, gtrid, statements):
    """一阶段提交：binlog 里落的是一个 onePhase=true 的 XA_prepare 事件，没有独立的 XA COMMIT。"""
    body = "\n".join(s if s.rstrip().endswith(";") else s + ";" for s in statements)
    mysql(container, f"""XA START '{gtrid}';
{body}
XA END '{gtrid}';
XA COMMIT '{gtrid}' ONE PHASE;""", db=db)


# ---------------------------------------------------------------- 三进程编排

class Pipeline:
    """capture → extract → increment 三个子进程的编排。"""

    def __init__(self, task_id, task_dir, jvm_opts=None):
        self.task_id = task_id
        self.task_dir = task_dir
        self.jvm = jvm_opts or ["-Xmx256m"]
        self.procs = {}
        self.logs = {}
        self._stop = False

    def start(self, name):
        proc = subprocess.Popen(
            ["java"] + self.jvm + ["-Dtask.id=" + self.task_id, "-cp", JARS[name], MAINS[name],
                                   "--config", os.path.join(self.task_dir, "config.properties")],
            cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.procs[name] = proc
        self.logs.setdefault(name, [])

        # 必须持续把子进程 stdout 读走：管道缓冲只有 64KB，写满之后子进程会阻塞在写日志上，
        # 表现为"进程还在、CPU 也不高、就是不干活"，极难归因
        def drain():
            for line in proc.stdout:
                self.logs[name].append(line)
                if len(self.logs[name]) > 6000:
                    del self.logs[name][:3000]

        threading.Thread(target=drain, daemon=True).start()
        return proc

    def start_all(self):
        for name in ("capture", "extract", "increment"):
            self.start(name)
            time.sleep(1)

    def stop(self, name):
        proc = self.procs.pop(name, None)
        if not proc:
            return
        proc.terminate()
        for _ in range(30):
            if proc.poll() is not None:
                break
            time.sleep(0.2)
        if proc.poll() is None:
            proc.kill()

    def stop_all(self):
        self._stop = True
        for name in list(self.procs):
            self.stop(name)
        return {name: "".join(lines) for name, lines in self.logs.items()}

    def log_of(self, name):
        return "".join(self.logs.get(name, []))

    def alive(self):
        return {name: proc.poll() is None for name, proc in self.procs.items()}

    def wait_until(self, fn, timeout, desc):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if fn():
                    return True
            except Exception:
                pass
            for name, proc in self.procs.items():
                if proc.poll() is not None:
                    print(f"    !! {name} 进程已退出 rc={proc.returncode}")
                    return False
            time.sleep(1)
        print(f"    !! 等待超时: {desc}")
        return False


def write_config(task_dir, task_id, src_db, tgt_db, table, cap_dir, thl_dir, extra=""):
    os.makedirs(task_dir, exist_ok=True)
    src_url = (f"jdbc:mysql://localhost:{SRC_PORT}/?useSSL=false&serverTimezone=UTC"
               "&allowPublicKeyRetrieval=true")
    tgt_url = (f"jdbc:mysql://localhost:{TGT_PORT}/{tgt_db}?useSSL=false&serverTimezone=UTC"
               "&allowPublicKeyRetrieval=true")
    cfg = f"""task.id={task_id}
source.db.type=mysql
source.db.host=localhost
source.db.port={SRC_PORT}
source.db.username=root
source.db.password={PWD}
source.db.database={src_db}
source.db.jdbc.url={src_url}
target.db.type=mysql
target.db.host=localhost
target.db.port={TGT_PORT}
target.db.username=root
target.db.password={PWD}
target.db.database={tgt_db}
target.db.jdbc.url={tgt_url}
capture.output.dir={cap_dir}
capture.server.id=99451
capture.gtid.enabled=false
capture.max.events.per.file=100000
capture.position.health.enabled=false
extract.input.dir={cap_dir}
extract.output.dir={thl_dir}
extract.scan.interval=1000
increment.thl.dir={thl_dir}
increment.scan.interval=1000
apply.transaction.mode=TRANSACTION
migration.included.databases={src_db}
migration.included.tables={src_db}.{table}
migration.sync.objects={{"{src_db}":{{"tables":["{table}"],"targetDb":"{tgt_db}"}}}}
schema.mapping.db.{src_db}={tgt_db}
{extra}"""
    with open(os.path.join(task_dir, "config.properties"), "w") as f:
        f.write(cfg)


def require_jars():
    missing = [name for name, path in JARS.items() if not os.path.isfile(path)]
    if missing:
        mods = " ".join("migration-" + m for m in missing)
        raise SystemExit(f"缺少 jar: {missing}\n先打包: mvn -pl {mods.replace(' ', ',')} -am package -DskipTests")


def error_status(task_dir):
    """increment/extract/capture 上报的错误状态文件内容（没有则返回空串）。"""
    path = os.path.join(task_dir, "binlog_output", "error_status")
    if not os.path.isfile(path):
        return ""
    with open(path) as f:
        return f.read().strip()
