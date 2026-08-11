#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PG 逻辑复制值保真判据的公共脚手架（pg → pg 增量）。

拓扑：`postgres_db(5432)` 上两个库 —— `toast_src` 作源、`toast_tgt` 作目标。
逻辑复制槽与 publication 都是**库级**对象，源库单独一个库就够了，不必开第二个实例。

关于 PG 侧的三个必须知道的点（写脚本时很容易踩）：

  * **光把值造大不会触发 `u`，必须真的落到行外存储**（实测撞出来的，第一版判据就栽在这）。
    PG 推到 TOAST 之前会**先压缩**：`repeat('X',40000)` 压完只剩几十字节，直接压缩存在行内，
    于是 pgoutput 照常发完整值、`u` 一次都不出现，判据全绿但什么也没测到。
    列标志 `u` 的判定条件是 `VARATT_IS_EXTERNAL_ONDISK`（值是行外指针），
    所以造数据必须 `ALTER COLUMN … SET STORAGE EXTERNAL`（关掉压缩）或改用不可压缩内容，
    并且**断言 TOAST 附属表真的有字节**，否则判据是睁眼瞎。
  * 建槽/建 publication 是 capture 进程自己做的，但**槽必须先于数据变更存在**，否则那段 WAL
    根本没被保留。判据里的写入一律等 capture 起来之后再发。
  * 判据用完必须 `pg_drop_replication_slot`：槽会一直卡住 WAL 回收，攒几个就把磁盘吃满。
"""
import os
import subprocess
import threading
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

CT = "postgres_db"
PORT = 5432
USER = "app_user"
PWD = "userpassword"
SRC_DB = "toast_src"
TGT_DB = "toast_tgt"

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

TABLE_DDL = """CREATE TABLE {t} (
  id    INT PRIMARY KEY,
  tag   VARCHAR(64),
  note  TEXT,
  blob_txt TEXT
)"""

# 关掉该列的压缩，40KB 的值才会真的被推到行外存储（见文件头注释）
STORAGE_DDL = "ALTER TABLE {t} ALTER COLUMN blob_txt SET STORAGE EXTERNAL"


def toast_bytes(db, table):
    """该表 TOAST 附属表的实际字节数；为 0 说明值还在行内，`u` 不会出现。"""
    return int(scalar(
        f"SELECT COALESCE(pg_relation_size(c.reltoastrelid),0) "
        f"FROM pg_class c WHERE c.relname='{table}'", db=db) or 0)


# ---------------------------------------------------------------- psql 客户端

def psql(sql, db="postgres", want=False, timeout=180):
    args = ["docker", "exec", "-i", "-e", "PGPASSWORD=" + PWD, CT,
            "psql", "-U", USER, "-d", db, "-v", "ON_ERROR_STOP=1"]
    args += ["-tAc", sql] if want else ["-c", sql]
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"psql 失败 ({db}): {(p.stderr or '').strip()}\nSQL: {sql[:200]}")
    return (p.stdout or "").strip()


def scalar(sql, db):
    return psql(sql, db=db, want=True).strip()


def reset_db(db):
    psql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{db}'")
    psql(f"DROP DATABASE IF EXISTS {db}")
    psql(f"CREATE DATABASE {db}")


def drop_slot(slot):
    """判据收尾：槽不删会一直卡住 WAL 回收。删不掉（正被占用）不算失败，打一行提示。"""
    try:
        psql(f"SELECT pg_drop_replication_slot('{slot}') "
             f"FROM pg_replication_slots WHERE slot_name='{slot}'")
    except Exception as e:
        print(f"    (提示) 清理复制槽 {slot} 失败: {e}")


def drop_publication(db, pub):
    try:
        psql(f"DROP PUBLICATION IF EXISTS \"{pub}\"", db=db)
    except Exception as e:
        print(f"    (提示) 清理 publication {pub} 失败: {e}")


def row_of(db, table, pk):
    """一行的四个列，按 |&| 分隔返回；不存在返回 None。大字段只回长度与首尾，避免刷屏。"""
    out = psql(
        f"SELECT id || '|&|' || COALESCE(tag,'<NULL>') || '|&|' "
        f"|| COALESCE(note,'<NULL>') || '|&|' "
        f"|| CASE WHEN blob_txt IS NULL THEN '<NULL>' ELSE "
        f"     length(blob_txt) || ':' || left(blob_txt,1) || right(blob_txt,1) END "
        f"FROM {table} WHERE id = {pk}", db=db, want=True)
    if not out:
        return None
    parts = out.split("|&|")
    return {"id": parts[0], "tag": parts[1], "note": parts[2], "blob": parts[3]}


# ---------------------------------------------------------------- 三进程编排

class Pipeline:
    """capture → extract → increment 三个子进程的编排（与 test_scripts/xa 同形）。"""

    def __init__(self, task_id, task_dir, jvm_opts=None):
        self.task_id = task_id
        self.task_dir = task_dir
        self.jvm = jvm_opts or ["-Xmx256m"]
        self.procs = {}
        self.logs = {}

    def start(self, name):
        proc = subprocess.Popen(
            ["java"] + self.jvm + ["-Dtask.id=" + self.task_id, "-cp", JARS[name], MAINS[name],
                                   "--config", os.path.join(self.task_dir, "config.properties")],
            cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.procs[name] = proc
        self.logs.setdefault(name, [])

        # 必须持续把子进程 stdout 读走：管道缓冲只有 64KB，写满之后子进程阻塞在写日志上，
        # 表现为"进程还在、CPU 也不高、就是不干活"，极难归因
        def drain():
            for line in proc.stdout:
                self.logs[name].append(line)
                if len(self.logs[name]) > 8000:
                    del self.logs[name][:4000]

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
        for name in list(self.procs):
            self.stop(name)
        return {name: "".join(lines) for name, lines in self.logs.items()}

    def log_of(self, name):
        return "".join(self.logs.get(name, []))

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


def write_config(task_dir, task_id, table, cap_dir, thl_dir, slot, pub, extra=""):
    os.makedirs(task_dir, exist_ok=True)

    def url(db):
        return f"jdbc:postgresql://localhost:{PORT}/{db}?currentSchema=public&stringtype=unspecified"

    cfg = f"""task.id={task_id}
capture.type=wal
source.db.type=postgresql
source.db.host=localhost
source.db.port={PORT}
source.db.username={USER}
source.db.password={PWD}
source.db.database={SRC_DB}
source.db.jdbc.driver=org.postgresql.Driver
source.db.jdbc.url={url(SRC_DB)}
target.db.type=postgresql
target.db.host=localhost
target.db.port={PORT}
target.db.username={USER}
target.db.password={PWD}
target.db.database={TGT_DB}
target.db.jdbc.driver=org.postgresql.Driver
target.db.jdbc.url={url(TGT_DB)}
target.db.quote.char="
capture.output.dir={cap_dir}
capture.wal.slot.name={slot}
capture.wal.publication.name={pub}
capture.max.events.per.file=100000
capture.position.health.enabled=false
extract.input.dir={cap_dir}
extract.output.dir={thl_dir}
extract.scan.interval=1000
increment.thl.dir={thl_dir}
increment.scan.interval=1000
apply.transaction.mode=TRANSACTION
migration.included.databases=public
migration.included.tables=public.{table}
migration.sync.objects={{"public":{{"tables":["{table}"],"targetDb":"public"}}}}
{extra}"""
    with open(os.path.join(task_dir, "config.properties"), "w") as f:
        f.write(cfg)


def require_jars():
    missing = [name for name, path in JARS.items() if not os.path.isfile(path)]
    if missing:
        mods = ",".join("migration-" + m for m in missing)
        raise SystemExit(f"缺少 jar: {missing}\n先打包: mvn -pl {mods} -am package -DskipTests")


def error_status(cap_dir):
    """capture/extract/increment 上报的错误状态文件内容（没有则返回空串）。"""
    path = os.path.join(PROJECT_ROOT, cap_dir, "error_status")
    if not os.path.isfile(path):
        return ""
    with open(path) as f:
        return f.read().strip()
