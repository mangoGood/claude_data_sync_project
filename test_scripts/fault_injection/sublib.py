#!/usr/bin/env python3
"""
订阅任务（SUBSCRIBE）故障注入 / 断点续传测试的公共库。

与同步任务的差别（决定了这套脚本不能复用 dblib/drlib）：
  - 目标端不是数据库而是 **Kafka**，没法用 SQL 比对，只能把订阅出来的 CDC 事件流回放成
    最终状态再与源库比；
  - 订阅任务只有增量、没有全量：任务启动之后源库的每一次 DML 才应该出现在 Kafka 里。
    因此测试先播种「大表」存量（不进 CDC），再启动任务，再持续增删改 3 分钟；
  - 管线是 capture → extract → subscribe 三段（没有 increment）。

一致性判定用两把尺子，缺一不可：
  1) **不丢**：写入端记录每一次 INSERT/UPDATE 的 (id, val, n) 真值，必须都能在 Kafka 事件里找到；
  2) **可收敛**：按 Kafka 投递顺序（单分区即 offset 顺序）回放 c/u/d，last-write-wins 得到的
     最终状态必须与源表逐行相等。只满足 1) 不满足 2) 说明重投顺序错乱，下游照样是脏数据。

下游 Kafka 用独立的一套（docker-compose-synctask-kafka-sub.yml，localhost:39092），
与控制面 Kafka(29092) 隔离，避免几十万条 CDC 消息挤占任务下发/状态上报。
"""
import json
import os
import random
import subprocess
import sys
import threading
import time

import requests

BASE_URL = os.environ.get("FI_BASE_URL", "http://localhost:38080")
USER = os.environ.get("FI_USER", "admin")
PASSWORD = os.environ.get("FI_PASS", "admin123")

# 下游订阅 Kafka（专用，非控制面）
SUB_KAFKA = os.environ.get("FI_SUB_KAFKA", "localhost:39092")
SUB_KAFKA_CONTAINER = "synctask-kafka-sub"

TABLE = "fi_sub"


# ------------------------------------------------------------------ backend api
def login():
    r = requests.post(f"{BASE_URL}/api/auth/login",
                      json={"username": USER, "password": PASSWORD}, timeout=30)
    d = r.json()
    if "token" not in d:
        print(f"登录失败: {d}")
        sys.exit(1)
    return d["token"]


def api(method, path, token, **kw):
    headers = kw.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    return requests.request(method, f"{BASE_URL}{path}", headers=headers, timeout=60, **kw).json()


def create_subscribe_task(token, name, source_type, src_conn, sync_objects,
                          source_db, topic_prefix, fmt="DEBEZIUM_JSON"):
    r = api("POST", "/api/workflows", token,
            json={"name": name, "sourceType": source_type, "targetType": "kafka",
                  "taskType": "SUBSCRIBE"})
    if not r.get("success"):
        print(f"创建订阅任务失败: {r}")
        sys.exit(1)
    task_id = r["data"]["id"]
    cfg = {
        "sourceConnection": src_conn,
        "targetConnection": f"kafka://{SUB_KAFKA}",
        "migrationMode": "subscribe",
        "syncObjects": sync_objects,
        "sourceDbName": source_db,
        "targetDbName": source_db,
        "sourceType": source_type,
        "targetType": "kafka",
        "kafkaBootstrapServers": SUB_KAFKA,
        "kafkaTopicPrefix": topic_prefix,
        "kafkaTopicStrategy": "TABLE",
        "subscribeFormat": fmt,
    }
    api("PUT", f"/api/workflows/{task_id}/config", token, json=cfg)
    api("POST", f"/api/workflows/{task_id}/launch", token)
    return task_id


def get_task(token, task_id):
    d = api("GET", f"/api/workflows/{task_id}", token)
    return d.get("data") or {}


def get_status(token, task_id):
    return get_task(token, task_id).get("status")


def wait_status(token, task_id, wanted, timeout=360, quiet=False):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        st = get_status(token, task_id)
        if st != last:
            if not quiet:
                print(f"    状态: {st}")
            last = st
        if st in wanted or st == "FAILED":
            return st
        time.sleep(3)
    return last


def stop_task(token, task_id):
    try:
        api("POST", f"/api/workflows/{task_id}/stop", token)
    except Exception:
        pass


def delete_task(token, task_id):
    try:
        api("DELETE", f"/api/workflows/{task_id}", token)
    except Exception:
        pass


# ------------------------------------------------------------------ 子进程故障注入
def child_pids(task_id, engine):
    """某任务某类子进程 pid（engine ∈ capture/extract/subscribe）。"""
    jar = f"migration-{engine}"
    out = subprocess.run(["pgrep", "-f", f"task.id={task_id}"],
                         capture_output=True, text=True).stdout
    pids = []
    for pid in out.split():
        cmd = subprocess.run(["ps", "-o", "command=", "-p", pid],
                             capture_output=True, text=True).stdout
        if jar in cmd:
            pids.append(int(pid))
    return pids


def signal_child(task_id, engine, sig):
    pids = child_pids(task_id, engine)
    for pid in pids:
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
    return pids


# ------------------------------------------------------------------ Kafka 下游
def kafka_topics(prefix=None):
    from kafka import KafkaAdminClient
    a = KafkaAdminClient(bootstrap_servers=SUB_KAFKA, request_timeout_ms=20000)
    try:
        ts = a.list_topics()
    finally:
        a.close()
    return [t for t in ts if not prefix or t.startswith(prefix)]


def delete_topics(prefix):
    """清掉上一轮的订阅 topic，避免历史消息污染本轮判定。"""
    from kafka import KafkaAdminClient
    ts = kafka_topics(prefix)
    if not ts:
        return []
    a = KafkaAdminClient(bootstrap_servers=SUB_KAFKA, request_timeout_ms=20000)
    try:
        a.delete_topics(ts)
    except Exception:
        pass
    finally:
        a.close()
    # 等待真正删干净（Kafka 删 topic 是异步的）
    for _ in range(30):
        time.sleep(1)
        if not kafka_topics(prefix):
            break
    return ts


def consume_all(prefix, idle_timeout=15):
    """把 prefix 下所有 topic 的全部消息按分区 offset 顺序读出来。

    返回 [(topic, partition, offset, value_dict), ...]，同一分区内按 offset 升序，
    即下游消费者实际看到的投递顺序。
    """
    from kafka import KafkaConsumer, TopicPartition
    topics = kafka_topics(prefix)
    if not topics:
        return []
    c = KafkaConsumer(bootstrap_servers=SUB_KAFKA,
                      auto_offset_reset="earliest",
                      enable_auto_commit=False,
                      consumer_timeout_ms=idle_timeout * 1000,
                      max_partition_fetch_bytes=8 * 1024 * 1024,
                      value_deserializer=lambda b: b.decode("utf-8", "replace"))
    tps = []
    for t in topics:
        parts = c.partitions_for_topic(t) or set()
        tps.extend(TopicPartition(t, p) for p in parts)
    if not tps:
        c.close()
        return []
    c.assign(tps)
    for tp in tps:
        c.seek_to_beginning(tp)
    ends = c.end_offsets(tps)
    total = sum(ends.values())

    out = []
    if total == 0:
        c.close()
        return out
    try:
        for msg in c:
            try:
                out.append((msg.topic, msg.partition, msg.offset, json.loads(msg.value)))
            except Exception:
                out.append((msg.topic, msg.partition, msg.offset, {"_raw": msg.value}))
            if len(out) >= total:
                break
    finally:
        c.close()
    out.sort(key=lambda r: (r[0], r[1], r[2]))
    return out


def parse_events(records, fmt="DEBEZIUM_JSON"):
    """把 Kafka 消息解析成 (op, seqno, after_map, before_map) 序列，保持投递顺序。"""
    evs = []
    for _topic, _p, _off, v in records:
        if fmt == "DEBEZIUM_JSON":
            p = v.get("payload") or {}
            op = p.get("op")
            after = p.get("after")
            before = p.get("before")
            seqno = (p.get("source") or {}).get("seqno")
        else:
            op = v.get("op")
            after = v.get("after")
            before = v.get("before")
            seqno = v.get("seqno")
        if op is None:
            continue
        evs.append((op, seqno, after, before))
    return evs


def _norm_id(m, idcol):
    if not m:
        return None
    for k in (idcol, idcol.upper(), idcol.lower()):
        if k in m and m[k] is not None:
            try:
                return int(float(m[k]))
            except (TypeError, ValueError):
                return None
    return None


def _norm_field(m, col):
    if not m:
        return None
    for k in (col, col.upper(), col.lower()):
        if k in m:
            return m[k]
    return None


def replay(events, idcol="id"):
    """按投递顺序回放成最终状态 {id: (val, n)}；c/u 覆盖，d 删除。"""
    state = {}
    unparsed = 0
    for op, _seq, after, before in events:
        if op in ("c", "u", "r"):
            rid = _norm_id(after, idcol)
            if rid is None:
                unparsed += 1
                continue
            state[rid] = (_norm_field(after, "val"), _to_int(_norm_field(after, "n")))
        elif op == "d":
            rid = _norm_id(after, idcol)
            if rid is None:
                rid = _norm_id(before, idcol)
            if rid is None:
                unparsed += 1
                continue
            state.pop(rid, None)
    return state, unparsed


def _to_int(v):
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def written_set(events, idcol="id"):
    """Kafka 中出现过的所有写入后像 {(id, val, n)}，用于「不丢」判定。"""
    s = set()
    for op, _seq, after, _before in events:
        if op not in ("c", "u", "r"):
            continue
        rid = _norm_id(after, idcol)
        if rid is None:
            continue
        s.add((rid, _norm_field(after, "val"), _to_int(_norm_field(after, "n"))))
    return s


# ------------------------------------------------------------------ 源端引擎
class MysqlSource:
    kind = "mysql"
    source_type = "mysql"
    idcol = "id"

    def __init__(self, host="127.0.0.1", port=33306, user="root",
                 password="rootpassword", db="fi_sub_my"):
        self.host, self.port, self.user, self.password, self.db = host, port, user, password, db

    def conn(self, db=None):
        import mysql.connector
        c = mysql.connector.connect(host=self.host, port=self.port, user=self.user,
                                    password=self.password, database=db, use_pure=True,
                                    autocommit=True)
        return c

    def src_conn_str(self):
        return f"mysql://{self.user}:{self.password}@{self.host}:{self.port}"

    def sync_objects(self):
        return json.dumps({self.db: {"tables": [TABLE]}})

    def reset(self):
        c = self.conn("mysql")
        cur = c.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{self.db}`")
        cur.execute(f"CREATE DATABASE `{self.db}` DEFAULT CHARACTER SET utf8mb4")
        cur.close(); c.close()
        c = self.conn(self.db)
        cur = c.cursor()
        cur.execute(f"""CREATE TABLE `{TABLE}` (
              `id` BIGINT NOT NULL,
              `grp` INT NOT NULL,
              `val` VARCHAR(128),
              `payload` VARCHAR(512),
              `n` BIGINT,
              PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        cur.close(); c.close()

    def seed(self, rows):
        c = self.conn(self.db)
        cur = c.cursor()
        batch, sql = [], f"INSERT INTO `{TABLE}` (id,grp,val,payload,n) VALUES (%s,%s,%s,%s,%s)"
        for i in range(1, rows + 1):
            batch.append((i, i % 100, f"seed-{i}", "x" * 200, i))
            if len(batch) >= 2000:
                cur.executemany(sql, batch); batch = []
        if batch:
            cur.executemany(sql, batch)
        cur.close(); c.close()

    def state(self):
        c = self.conn(self.db)
        cur = c.cursor()
        cur.execute(f"SELECT id,val,n FROM `{TABLE}`")
        st = {int(r[0]): (r[1], None if r[2] is None else int(r[2])) for r in cur.fetchall()}
        cur.close(); c.close()
        return st

    def make_writer(self, start_id, rate):
        return SqlWriter(self, start_id, rate)


class PgSource(MysqlSource):
    kind = "pg"
    source_type = "postgresql"

    def __init__(self, host="127.0.0.1", port=5432, user="app_user",
                 password="userpassword", db="fi_sub_pg"):
        super().__init__(host, port, user, password, db)

    def conn(self, db=None):
        import psycopg2
        c = psycopg2.connect(host=self.host, port=self.port, user=self.user,
                             password=self.password, dbname=db or "postgres")
        c.autocommit = True
        return c

    def src_conn_str(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    def sync_objects(self):
        return json.dumps({self.db: [f"public.{TABLE}"]})

    def reset(self):
        c = self.conn("postgres")
        cur = c.cursor()
        cur.execute("SELECT slot_name FROM pg_replication_slots WHERE database=%s", (self.db,))
        for (sn,) in cur.fetchall():
            try:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (sn,))
            except Exception:
                pass
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname=%s AND pid<>pg_backend_pid()", (self.db,))
        cur.execute(f'DROP DATABASE IF EXISTS "{self.db}"')
        cur.execute(f'CREATE DATABASE "{self.db}"')
        cur.close(); c.close()
        c = self.conn(self.db)
        cur = c.cursor()
        cur.execute(f"""CREATE TABLE {TABLE} (
              id BIGINT PRIMARY KEY,
              grp INT NOT NULL,
              val VARCHAR(128),
              payload VARCHAR(512),
              n BIGINT)""")
        # 逻辑解码要拿到 UPDATE/DELETE 的完整前像，否则 before 只有主键
        cur.execute(f"ALTER TABLE {TABLE} REPLICA IDENTITY FULL")
        cur.close(); c.close()

    def seed(self, rows):
        from psycopg2.extras import execute_values
        c = self.conn(self.db)
        cur = c.cursor()
        sql = f"INSERT INTO {TABLE} (id,grp,val,payload,n) VALUES %s"
        batch = []
        for i in range(1, rows + 1):
            batch.append((i, i % 100, f"seed-{i}", "x" * 200, i))
            if len(batch) >= 5000:
                execute_values(cur, sql, batch, page_size=5000); batch = []
        if batch:
            execute_values(cur, sql, batch, page_size=5000)
        cur.close(); c.close()

    def state(self):
        c = self.conn(self.db)
        cur = c.cursor()
        cur.execute(f"SELECT id,val,n FROM {TABLE}")
        st = {int(r[0]): (r[1], None if r[2] is None else int(r[2])) for r in cur.fetchall()}
        cur.close(); c.close()
        return st


class OracleSource:
    kind = "oracle"
    source_type = "oracle"
    idcol = "ID"

    def __init__(self, host="127.0.0.1", port=1521, service="FREEPDB1",
                 user="app_user", password="userpassword"):
        self.host, self.port, self.service = host, port, service
        self.user, self.password = user, password
        self.db = service
        self.schema = user.upper()
        self.table = TABLE.upper()

    def conn(self, db=None):
        import oracledb
        return oracledb.connect(user=self.user, password=self.password,
                                dsn=f"{self.host}:{self.port}/{self.service}")

    def src_conn_str(self):
        return f"oracle://{self.user}:{self.password}@{self.host}:{self.port}/{self.service}"

    def sync_objects(self):
        return json.dumps({self.schema: {"tables": [self.table]}})

    def reset(self):
        c = self.conn()
        cur = c.cursor()
        cur.execute(f"""BEGIN EXECUTE IMMEDIATE 'DROP TABLE {self.table} PURGE';
                        EXCEPTION WHEN OTHERS THEN NULL; END;""")
        cur.execute(f"""CREATE TABLE {self.table} (
              ID NUMBER(19) PRIMARY KEY,
              GRP NUMBER(10) NOT NULL,
              VAL VARCHAR2(128),
              PAYLOAD VARCHAR2(512),
              N NUMBER(19))""")
        c.commit()
        cur.close(); c.close()

    def seed(self, rows):
        c = self.conn()
        cur = c.cursor()
        cur.execute(f"""BEGIN
              FOR i IN 1..{rows} LOOP
                INSERT INTO {self.table} (ID,GRP,VAL,PAYLOAD,N)
                VALUES (i, MOD(i,100), 'seed-'||i, RPAD('x',200,'x'), i);
              END LOOP; COMMIT; END;""")
        cur.close(); c.close()

    def state(self):
        c = self.conn()
        cur = c.cursor()
        cur.execute(f"SELECT ID,VAL,N FROM {self.table}")
        st = {int(r[0]): (r[1], None if r[2] is None else int(r[2])) for r in cur.fetchall()}
        cur.close(); c.close()
        return st

    def make_writer(self, start_id, rate):
        return OracleWriter(self, start_id, rate)


class _WriterBase(threading.Thread):
    """持续写入并记录真值。

    每次 INSERT/UPDATE 都带一个全局唯一的 opseq（写进 val 和 n），因此
    `writes` 里的每个三元组都能在 Kafka 里被唯一定位——丢一条就能查出来。
    """

    def __init__(self, ep, start_id, rate):
        super().__init__(daemon=True)
        self.ep = ep
        self.next_id = start_id
        self.rate = rate                  # 目标 DML 条数/秒
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.writes = set()               # {(id, val, n)} —— 真值：这些必须都出现在 Kafka
        self.deleted = set()
        self.live = []
        self.inserts = self.updates = self.deletes = 0
        self.opseq = 0
        self.error = None

    def _rec(self, rid, val, n):
        self.writes.add((rid, val, n))


class SqlWriter(_WriterBase):
    def run(self):
        try:
            c = self.ep.conn(self.ep.db)
            cur = c.cursor()
            t = f"`{TABLE}`" if self.ep.kind == "mysql" else TABLE
            ph = "%s"
            batch_ops = max(1, int(self.rate / 10))   # 每 100ms 一批
            while not self.stop.is_set():
                t0 = time.time()
                for _ in range(batch_ops):
                    self.opseq += 1
                    seq = self.opseq
                    rid = self.next_id
                    self.next_id += 1
                    val, n = f"ins-{seq}", 1000000 + seq
                    cur.execute(
                        f"INSERT INTO {t} (id,grp,val,payload,n) VALUES ({ph},{ph},{ph},{ph},{ph})",
                        (rid, seq % 100, val, "y" * 200, n))
                    self.inserts += 1
                    self.live.append(rid)
                    self._rec(rid, val, n)

                    if seq % 4 == 0 and self.live:
                        self.opseq += 1
                        s2 = self.opseq
                        tgt = random.choice(self.live)
                        val2, n2 = f"upd-{s2}", 1000000 + s2
                        cur.execute(f"UPDATE {t} SET val={ph}, n={ph} WHERE id={ph}",
                                    (val2, n2, tgt))
                        self.updates += 1
                        self._rec(tgt, val2, n2)

                    if seq % 11 == 0 and len(self.live) > 50:
                        victim = self.live.pop(0)
                        cur.execute(f"DELETE FROM {t} WHERE id={ph}", (victim,))
                        self.deletes += 1
                        self.deleted.add(victim)
                if self.ep.kind == "pg":
                    c.commit()
                dt = time.time() - t0
                if dt < 0.1:
                    time.sleep(0.1 - dt)
            cur.close(); c.close()
        except Exception as e:  # noqa: BLE001
            self.error = e


class OracleWriter(_WriterBase):
    """Oracle 走 LogMiner，写入速率给低一些；每轮一个 PL/SQL 块批量提交以降低往返开销。"""

    def run(self):
        try:
            c = self.ep.conn()
            cur = c.cursor()
            t = self.ep.table
            batch_ops = max(1, int(self.rate / 2))    # 每 500ms 一批
            while not self.stop.is_set():
                t0 = time.time()
                stmts = []
                for _ in range(batch_ops):
                    self.opseq += 1
                    seq = self.opseq
                    rid = self.next_id
                    self.next_id += 1
                    val, n = f"ins-{seq}", 1000000 + seq
                    stmts.append(f"INSERT INTO {t} (ID,GRP,VAL,PAYLOAD,N) VALUES "
                                 f"({rid},{seq % 100},'{val}',RPAD('y',200,'y'),{n});")
                    self.inserts += 1
                    self.live.append(rid)
                    self._rec(rid, val, n)

                    if seq % 4 == 0 and self.live:
                        self.opseq += 1
                        s2 = self.opseq
                        tgt = random.choice(self.live)
                        val2, n2 = f"upd-{s2}", 1000000 + s2
                        stmts.append(f"UPDATE {t} SET VAL='{val2}', N={n2} WHERE ID={tgt};")
                        self.updates += 1
                        self._rec(tgt, val2, n2)

                    if seq % 11 == 0 and len(self.live) > 50:
                        victim = self.live.pop(0)
                        stmts.append(f"DELETE FROM {t} WHERE ID={victim};")
                        self.deletes += 1
                        self.deleted.add(victim)
                cur.execute("BEGIN " + "".join(stmts) + " COMMIT; END;")
                dt = time.time() - t0
                if dt < 0.5:
                    time.sleep(0.5 - dt)
            cur.close(); c.close()
        except Exception as e:  # noqa: BLE001
            self.error = e


SOURCES = {
    "mysql": MysqlSource,
    "pg": PgSource,
    "oracle": OracleSource,
}


def print_result(passed, failed):
    print("\n" + "=" * 68)
    print(f"  通过 {len(passed)} / 失败 {len(failed)}")
    for p in passed:
        print(f"   ✓ {p}")
    for f in failed:
        print(f"   ✗ {f}")
    print("=" * 68)
    return 0 if not failed else 1
