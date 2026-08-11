#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大字段（LONGBLOB）同步测试的公共脚手架。

被 lob_baseline_oom.py / lob_full_e2e.py / lob_incr_e2e.py 复用。

拓扑：dr-mysql-a(33320) 作源、dr-mysql-b(33321) 作目标——两个独立实例，
比同实例跨库更贴近真实场景，也不占用 synctask-mysql。

几个刻意的选择：
  * 1GB 的值不能用 INSERT 直接灌（客户端拼不出这么大的语句，服务端也顶 max_allowed_packet）。
    这里用"先塞 1MB 种子再服务端 CONCAT 自倍增"，全程没有一个字节经过客户端。
  * 校验一律用服务端 MD5(col)：只回传 32 字节，脚本自己不会因为读校验数据而 OOM。
  * 峰值 RSS 用 ps 轮询采样。约束是"进程运行时内存 ≤ 256MB"，说的是 RSS 不是堆，
    所以不能只看 -Xmx，必须实测。
"""
import os
import re
import subprocess
import sys
import threading
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SRC_CT, SRC_PORT = "dr-mysql-a", 33320
TGT_CT, TGT_PORT = "dr-mysql-b", 33321
PWD = "rootpassword"

# 10 个字段，其中 c_blob 是 LONGBLOB（题设场景）
TABLE_DDL = """CREATE TABLE {t} (
  id        INT PRIMARY KEY,
  c_int     INT,
  c_bigint  BIGINT,
  c_dec     DECIMAL(20,4),
  c_vc      VARCHAR(255),
  c_dt      DATETIME,
  c_ts      TIMESTAMP NULL,
  c_json    JSON,
  c_txt     TEXT,
  c_blob    LONGBLOB
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""

COLUMNS = ["id", "c_int", "c_bigint", "c_dec", "c_vc", "c_dt", "c_ts", "c_json", "c_txt", "c_blob"]


# ---------------------------------------------------------------- mysql 客户端

def mysql(container, sql, db=None, want=False, timeout=1800):
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


# ---------------------------------------------------------------- 服务端调优

def tune_server(container, packet_bytes=1073741824):
    """把实例调到能承载 1GB 单值的状态。返回实际生效值，供预检类断言使用。

    max_allowed_packet 的 MySQL 上限就是 1GB——这正是"1GB 字段"卡在边界上的原因：
    一条携带 1GB 值的语句连同其它列一定超过这个上限，所以写入侧必然要走分块追加。
    """
    mysql(container, f"""
SET GLOBAL max_allowed_packet = {packet_bytes};
SET GLOBAL net_read_timeout = 600;
SET GLOBAL net_write_timeout = 600;
SET GLOBAL innodb_redo_log_capacity = 2147483648;
""")
    return {
        "max_allowed_packet": int(scalar(container, "SELECT @@max_allowed_packet")),
        "redo_capacity": int(scalar(container, "SELECT @@innodb_redo_log_capacity")),
    }


# ---------------------------------------------------------------- 建表 / 造数

def reset_db(container, db):
    mysql(container, f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db} CHARACTER SET utf8mb4;")


def create_table(container, db, table):
    mysql(container, TABLE_DDL.format(t=table), db=db)


def seed_rows(container, db, table, rows, blob_bytes, seed_bytes=1048576, progress=True):
    """造 rows 行，每行 c_blob 为 blob_bytes 字节。

    做法：先插入 seed_bytes 的随机种子，再服务端 CONCAT 自倍增到目标大小。
    倍增是 O(n) 重写、总写入约 2×blob_bytes/行，1GB/行大约几十秒——比让客户端
    搬 1GB 过去快得多，而且客户端内存占用恒定为 0。
    """
    if blob_bytes < seed_bytes or (blob_bytes % seed_bytes) != 0:
        raise ValueError(f"blob_bytes({blob_bytes}) 必须是 seed_bytes({seed_bytes}) 的整数倍")

    for i in range(1, rows + 1):
        # 每行种子不同（RANDOM_BYTES 上限 1024，用 REPEAT 铺到 seed_bytes），
        # 保证逐行 MD5 校验真的能区分出行错位。
        mysql(container, f"""
INSERT INTO {table} (id, c_int, c_bigint, c_dec, c_vc, c_dt, c_ts, c_json, c_txt, c_blob)
VALUES ({i}, {i * 7}, {i * 100000000}, {i}.1234, 'row-{i}-中文',
        '2026-08-10 12:00:{i:02d}', '2026-08-10 12:00:{i:02d}',
        JSON_OBJECT('k', {i}), REPEAT('t{i}', 100),
        REPEAT(RANDOM_BYTES(1024), {seed_bytes // 1024}));
""", db=db)
        doublings = 0
        size = seed_bytes
        while size < blob_bytes:
            mysql(container, f"UPDATE {table} SET c_blob = CONCAT(c_blob, c_blob) WHERE id = {i};", db=db)
            size *= 2
            doublings += 1
        got = int(scalar(container, f"SELECT OCTET_LENGTH(c_blob) FROM {table} WHERE id={i}", db=db))
        if got != blob_bytes:
            raise RuntimeError(f"第 {i} 行 blob 实际 {got} != 期望 {blob_bytes}（倍增 {doublings} 次）")
        if progress:
            print(f"    row {i}/{rows}: {got} bytes", flush=True)


# ---------------------------------------------------------------- 校验

def row_digests(container, db, table):
    """逐行取全部 10 列的服务端摘要。大字段只回传 MD5，脚本自身内存恒定。"""
    sql = (f"SELECT id, c_int, c_bigint, c_dec, c_vc, c_dt, c_ts, "
           f"CAST(c_json AS CHAR), MD5(c_txt), OCTET_LENGTH(c_blob), MD5(c_blob) "
           f"FROM {table} ORDER BY id")
    out = mysql(container, sql, db=db, want=True)
    digests = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        digests[parts[0]] = tuple(parts[1:])
    return digests


def compare(src_digests, tgt_digests):
    """返回 (是否一致, 差异描述列表)。"""
    diffs = []
    missing = sorted(set(src_digests) - set(tgt_digests), key=int)
    extra = sorted(set(tgt_digests) - set(src_digests), key=int)
    if missing:
        diffs.append(f"目标缺行: {missing}")
    if extra:
        diffs.append(f"目标多行: {extra}")
    for pk in sorted(set(src_digests) & set(tgt_digests), key=int):
        if src_digests[pk] != tgt_digests[pk]:
            diffs.append(f"id={pk} 不一致:\n      源={src_digests[pk]}\n      目标={tgt_digests[pk]}")
    return (not diffs), diffs


# ---------------------------------------------------------------- 带 RSS 采样的子进程

class RunResult:
    def __init__(self, returncode, output, peak_rss_mb, seconds):
        self.returncode = returncode
        self.output = output
        self.peak_rss_mb = peak_rss_mb
        self.seconds = seconds

    def oom(self):
        return ("OutOfMemoryError" in self.output
                or "Requested array size exceeds VM limit" in self.output
                or "GC overhead limit exceeded" in self.output)


def run_java(argv, jvm_opts=None, timeout=1800, cwd=None, env=None):
    """跑一个 java 子进程并采样峰值 RSS。

    jvm_opts 插在 java 之后、-jar/-cp 之前——顺序错了参数就成了 main 的入参、静默失效。
    """
    cmd = ["java"] + list(jvm_opts or []) + list(argv)
    t0 = time.time()
    proc = subprocess.Popen(cmd, cwd=cwd or PROJECT_ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, env=env)
    peak = [0]

    def sample():
        while proc.poll() is None:
            try:
                out = subprocess.run(["ps", "-o", "rss=", "-p", str(proc.pid)],
                                     capture_output=True, text=True, timeout=5).stdout.strip()
                if out:
                    peak[0] = max(peak[0], int(out))
            except Exception:
                pass
            time.sleep(0.2)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        output = proc.communicate(timeout=timeout)[0] or ""
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        output = (proc.communicate()[0] or "") + "\n[TIMEOUT] 子进程超时被杀"
        rc = -9
    sampler.join(timeout=2)
    return RunResult(rc, output, peak[0] / 1024.0, time.time() - t0)


# ---------------------------------------------------------------- 任务配置

def write_full_config(task_id, src_db, tgt_db, table, extra_lines=""):
    d = os.path.join(PROJECT_ROOT, "files", task_id)
    os.makedirs(d, exist_ok=True)
    src_url = (f"jdbc:mysql://localhost:{SRC_PORT}/?useSSL=false&serverTimezone=UTC"
               f"&characterEncoding=utf8&allowPublicKeyRetrieval=true")
    tgt_url = (f"jdbc:mysql://localhost:{TGT_PORT}/?useSSL=false&serverTimezone=UTC"
               f"&characterEncoding=utf8&allowPublicKeyRetrieval=true")
    cfg = f"""source.db.type=mysql
source.db.host=localhost
source.db.port={SRC_PORT}
source.db.username=root
source.db.password={PWD}
source.db.database=
source.db.jdbc.driver=com.mysql.cj.jdbc.Driver
source.db.jdbc.url={src_url}
target.db.type=mysql
target.db.host=localhost
target.db.port={TGT_PORT}
target.db.username=root
target.db.password={PWD}
target.db.database=
target.db.jdbc.driver=com.mysql.cj.jdbc.Driver
target.db.jdbc.url={tgt_url}
target.db.quote.char=`
migration.included.databases={src_db}
migration.included.tables={src_db}.{table}
migration.sync.objects={{"{src_db}":{{"tables":["{table}"],"targetDb":"{tgt_db}"}}}}
schema.mapping.db.{src_db}={tgt_db}
migration.full.parallelism=1
migration.enable.resume=true
{extra_lines}"""
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write(cfg)
    return d


def require_jar(path, hint):
    if not os.path.exists(path):
        print(f"缺少 fat jar：{path}\n先执行：{hint}")
        sys.exit(2)


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024.0
