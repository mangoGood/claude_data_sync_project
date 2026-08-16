#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据面端点适配层：mysql / postgresql / mongodb / redis / elasticsearch / oracle。

统一接口，让上层用例对所有链路写同一套断言：
    reset_source()  建库+建表（干净重建）
    reset_target()  只保证目标库存在且为空（目标表由同步引擎按源结构自动创建）
    seed(n)         播种存量
    write(n, base)  写增量：INSERT 为主，穿插 UPDATE/DELETE，主键显式指定
    fingerprint()   (行数, XOR-CRC) —— 顺序无关、对每列敏感、跨引擎可比
    count()

指纹只取 (id, grp, val, payload, n) 五列，且类型在各引擎间规范成 int/str，
规避跨库时区与浮点精度差异把"链路没问题"误判成"数据不一致"。

主键一律由写入方显式指定：自增/identity 在灾备倒换后不随数据推进，双向灾备两端
同时写还会生成相同 id 造成天然写写冲突——那是用例设计缺陷，不是产品缺陷。
"""
import hashlib
import json
import subprocess
import urllib.error
import urllib.request

from . import config as C

TABLE = C.TABLE


# ----------------------------------------------------------------- 跨引擎指纹
def canon_key(row):
    rid, grp, val, payload, n = row
    return "|".join([
        str(int(rid)),
        "\x00" if grp is None else str(int(grp)),
        "\x00" if val is None else str(val),
        "\x00" if payload is None else str(payload),
        "\x00" if n is None else str(int(n)),
    ])


def fp_from_rows(rows):
    """(行数, 摘要)：顺序无关、对每列敏感、对增删改敏感。

    **不要用 XOR 聚合**（早期版本踩过）：CRC32 在 GF(2) 上是线性的，等长行之间只差一个
    常量 Δ 时，逐行 crc 的差也是常量；偶数条这样的行同时存在，XOR 就整体抵消 ——
    两份完全不同的数据会算出同一个指纹，用例把"数据丢了"判成"已追平"。
    而按 id 分段造数的测试数据恰好天然满足"等长 + 常量差"。
    改成：规范化行排序后整体做一次 SHA-256，取前 8 字节。
    """
    h = hashlib.sha256()
    for k in sorted(canon_key(r) for r in rows):
        h.update(k.encode("utf-8"))
        h.update(b"\n")
    return (len(rows), int.from_bytes(h.digest()[:8], "big"))


def fmt_fp(fp):
    return "行数=%s 指纹=%016x" % (fp[0], fp[1] & 0xFFFFFFFFFFFFFFFF)


# ----------------------------------------------------------------- SQL（mysql / pg）
class SqlEndpoint:
    """MySQL / TiDB（kind=mysql）与 PostgreSQL（kind=pg）共用。"""

    def __init__(self, spec, db, schema=None):
        self.kind = spec["kind"]
        self.host = spec["host"]
        self.port = spec["port"]
        self.user = spec["user"]
        self.password = spec["password"]
        self.container = spec.get("container")
        self.db = db
        # mysql→pg 同步把表落在「源库名」schema 下（非 public）；pg→pg 落 public。
        self.schema = schema or "public"

    # -- 连接 --
    def conn(self, db=None, autocommit=True):
        if self.kind == "mysql":
            import mysql.connector
            c = mysql.connector.connect(host=self.host, port=self.port, user=self.user,
                                        password=self.password, database=db, use_pure=True,
                                        autocommit=autocommit, connection_timeout=20)
            cur = c.cursor()
            cur.execute("SET time_zone='+00:00'")
            cur.close()
            return c
        import psycopg2
        c = psycopg2.connect(host=self.host, port=self.port, user=self.user,
                             password=self.password, dbname=db or "postgres",
                             connect_timeout=20)
        c.autocommit = autocommit
        cur = c.cursor()
        cur.execute('SET search_path TO "%s", public' % self.schema)
        cur.close()
        return c

    def _maint_db(self):
        return "mysql" if self.kind == "mysql" else "postgres"

    def _t(self):
        return "`%s`" % TABLE if self.kind == "mysql" else TABLE

    def exec(self, sql, args=None, db=None):
        c = self.conn(db or self.db)
        cur = c.cursor()
        cur.execute(sql, args or ())
        try:
            return cur.fetchall()
        except Exception:
            return []
        finally:
            cur.close()
            c.close()

    # -- 生命周期 --
    def reset_source(self):
        self.drop_create_db()
        self.create_table()

    def reset_target(self):
        self.drop_create_db()

    def drop_create_db(self):
        c = self.conn(self._maint_db())
        cur = c.cursor()
        if self.kind == "mysql":
            cur.execute("DROP DATABASE IF EXISTS `%s`" % self.db)
            cur.execute("CREATE DATABASE `%s` DEFAULT CHARACTER SET utf8mb4" % self.db)
        else:
            # 先删该库遗留的逻辑复制槽（否则 DROP DATABASE 被占用），再踢掉其它连接
            cur.execute("SELECT slot_name FROM pg_replication_slots WHERE database=%s", (self.db,))
            for (sn,) in cur.fetchall():
                try:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (sn,))
                except Exception:
                    pass
            cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname=%s AND pid<>pg_backend_pid()", (self.db,))
            cur.execute('DROP DATABASE IF EXISTS "%s"' % self.db)
            cur.execute('CREATE DATABASE "%s"' % self.db)
        cur.close()
        c.close()

    def drop_db(self):
        c = self.conn(self._maint_db())
        cur = c.cursor()
        try:
            if self.kind == "mysql":
                cur.execute("DROP DATABASE IF EXISTS `%s`" % self.db)
            else:
                cur.execute("SELECT slot_name FROM pg_replication_slots WHERE database=%s", (self.db,))
                for (sn,) in cur.fetchall():
                    try:
                        cur.execute("SELECT pg_drop_replication_slot(%s)", (sn,))
                    except Exception:
                        pass
                cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE datname=%s AND pid<>pg_backend_pid()", (self.db,))
                cur.execute('DROP DATABASE IF EXISTS "%s"' % self.db)
        finally:
            cur.close()
            c.close()

    def create_table(self):
        c = self.conn(self.db)
        cur = c.cursor()
        if self.kind == "mysql":
            cur.execute("""
                CREATE TABLE `%s` (
                  `id` BIGINT NOT NULL,
                  `grp` INT NOT NULL,
                  `val` VARCHAR(128),
                  `payload` VARCHAR(512),
                  `n` BIGINT,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""" % TABLE)
        else:
            cur.execute("""
                CREATE TABLE %s (
                  id BIGINT PRIMARY KEY,
                  grp INT NOT NULL,
                  val VARCHAR(128),
                  payload VARCHAR(512),
                  n BIGINT
                )""" % TABLE)
        cur.close()
        c.close()

    # -- 数据 --
    def seed(self, rows, base=1):
        c = self.conn(self.db)
        cur = c.cursor()
        data = [(base + i, i % 100, "seed-%d" % i, "x" * 120, i) for i in range(rows)]
        if self.kind == "pg":
            from psycopg2.extras import execute_values
            execute_values(cur, "INSERT INTO %s (id,grp,val,payload,n) VALUES %%s" % self._t(),
                           data, page_size=2000)
            c.commit()
        else:
            cur.executemany("INSERT INTO %s (id,grp,val,payload,n) VALUES (%%s,%%s,%%s,%%s,%%s)"
                            % self._t(), data)
        cur.close()
        c.close()
        return [d[0] for d in data]

    def write(self, rows, base, tag="incr"):
        """写增量：rows 条 INSERT，每 5 条一次 UPDATE、每 11 条一次 DELETE（都作用于本次插入的 id）。

        返回 {"inserted": [...存活 id...], "updates": n, "deletes": n}
        """
        c = self.conn(self.db)
        cur = c.cursor()
        t = self._t()
        alive, updates, deletes = [], 0, 0
        for i in range(rows):
            rid = base + i
            cur.execute("INSERT INTO %s (id,grp,val,payload,n) VALUES (%%s,%%s,%%s,%%s,%%s)" % t,
                        (rid, i % 100, "%s-%d" % (tag, i), "y" * 120, 1000 + i))
            alive.append(rid)
            if i % 5 == 4:
                cur.execute("UPDATE %s SET val=%%s, n=n+1 WHERE id=%%s" % t,
                            ("%s-upd-%d" % (tag, i), alive[-1]))
                updates += 1
            if i % 11 == 10 and len(alive) > 3:
                did = alive.pop(0)
                cur.execute("DELETE FROM %s WHERE id=%%s" % t, (did,))
                deletes += 1
            if self.kind == "pg":
                c.commit()
        if self.kind == "pg":
            c.commit()
        cur.close()
        c.close()
        return {"inserted": alive, "updates": updates, "deletes": deletes}

    def delete_ids(self, ids):
        if not ids:
            return 0
        c = self.conn(self.db)
        cur = c.cursor()
        ph = ",".join(["%s"] * len(ids))
        cur.execute("DELETE FROM %s WHERE id IN (%s)" % (self._t(), ph), tuple(ids))
        n = cur.rowcount
        if self.kind == "pg":
            c.commit()
        cur.close()
        c.close()
        return n

    def corrupt_row(self, rid, new_val="CORRUPTED-BY-AUTOTEST"):
        """在目标端制造一处内容差异（用于验证内容对比真的能抓到差异）。"""
        c = self.conn(self.db)
        cur = c.cursor()
        cur.execute("UPDATE %s SET val=%%s WHERE id=%%s" % self._t(), (new_val, rid))
        n = cur.rowcount
        if self.kind == "pg":
            c.commit()
        cur.close()
        c.close()
        return n

    def resolve_schema(self, table=TABLE):
        """目标表由同步引擎自动创建，落在哪个 schema 因链路而异（pg→pg 落 public，
        mysql→pg 落"源库名"schema，oracle→pg 落源 schema 名）。这里查一次实际落点，
        免得把"落点假设写错"当成"数据没同步"。查不到就保持原配置，让断言如实失败。"""
        if self.kind != "pg":
            return self.schema
        try:
            c = self.conn(self.db)
            cur = c.cursor()
            cur.execute("SELECT table_schema FROM information_schema.tables "
                        "WHERE lower(table_name)=lower(%s) "
                        "AND table_schema NOT IN ('pg_catalog','information_schema') "
                        "ORDER BY (table_schema='public') DESC LIMIT 1", (table,))
            row = cur.fetchone()
            cur.close()
            c.close()
            if row:
                self.schema = row[0]
        except Exception:
            pass
        return self.schema

    # -- 读 --
    def fetch_rows(self):
        c = self.conn(self.db)
        cur = c.cursor()
        cur.execute("SELECT id,grp,val,payload,n FROM %s" % self._t())
        rows = [(r[0], r[1], r[2], r[3], r[4]) for r in cur.fetchall()]
        cur.close()
        c.close()
        return rows

    def fingerprint(self):
        return fp_from_rows(self.fetch_rows())

    def count(self):
        try:
            c = self.conn(self.db)
            cur = c.cursor()
            cur.execute("SELECT COUNT(*) FROM %s" % self._t())
            n = cur.fetchone()[0]
            cur.close()
            c.close()
            return n
        except Exception:
            return -1

    def alive(self):
        try:
            c = self.conn(self._maint_db())
            c.close()
            return True
        except Exception:
            return False

    # -- 供 API 用 --
    def conn_str(self, with_db=None):
        if self.kind == "mysql":
            # MySQL 连接串按产品口径不带库名（库名走 sourceDbName/targetDbName）
            return "mysql://%s:%s@%s:%d" % (self.user, self.password, self.host, self.port)
        return "postgresql://%s:%s@%s:%d/%s" % (
            self.user, self.password, self.host, self.port, with_db or self.db)

    def sync_objects(self):
        # MySQL 的 key 是**库名**；PG 的 key 是 **schema 名**（库名已经在连接串里了）。
        # PG 传库名当 key 会被启动前预检判成 "schema xxx 不存在" 而直接拒绝启动。
        if self.kind == "mysql":
            return json.dumps({self.db: {"tables": [TABLE]}})
        return json.dumps({"public": [TABLE]})


# ----------------------------------------------------------------- MongoDB
class MongoEndpoint:
    def __init__(self, spec, db):
        self.kind = "mongo"
        self.host = spec["host"]
        self.port = spec["port"]
        self.user = spec["user"]
        self.password = spec["password"]
        self.container = spec.get("container")
        self.db = db

    def client(self):
        import pymongo
        return pymongo.MongoClient(
            "mongodb://%s:%s@%s:%d/?authSource=admin&directConnection=true"
            % (self.user, self.password, self.host, self.port),
            serverSelectionTimeoutMS=15000)

    def reset_source(self):
        cl = self.client()
        cl.drop_database(self.db)
        cl[self.db].create_collection(TABLE)
        cl.close()

    def reset_target(self):
        cl = self.client()
        cl.drop_database(self.db)
        cl.close()

    def drop_db(self):
        cl = self.client()
        cl.drop_database(self.db)
        cl.close()

    def seed(self, rows, base=1):
        cl = self.client()
        coll = cl[self.db][TABLE]
        docs = [{"_id": base + i, "grp": i % 100, "val": "seed-%d" % i,
                 "payload": "x" * 120, "n": i} for i in range(rows)]
        if docs:
            coll.insert_many(docs)
        cl.close()
        return [d["_id"] for d in docs]

    def write(self, rows, base, tag="incr"):
        cl = self.client()
        coll = cl[self.db][TABLE]
        alive, updates, deletes = [], 0, 0
        for i in range(rows):
            rid = base + i
            coll.insert_one({"_id": rid, "grp": i % 100, "val": "%s-%d" % (tag, i),
                             "payload": "y" * 120, "n": 1000 + i})
            alive.append(rid)
            if i % 5 == 4:
                coll.update_one({"_id": alive[-1]},
                                {"$set": {"val": "%s-upd-%d" % (tag, i)}, "$inc": {"n": 1}})
                updates += 1
            if i % 11 == 10 and len(alive) > 3:
                coll.delete_one({"_id": alive.pop(0)})
                deletes += 1
        cl.close()
        return {"inserted": alive, "updates": updates, "deletes": deletes}

    def delete_ids(self, ids):
        if not ids:
            return 0
        cl = self.client()
        n = cl[self.db][TABLE].delete_many({"_id": {"$in": list(ids)}}).deleted_count
        cl.close()
        return n

    def corrupt_row(self, rid, new_val="CORRUPTED-BY-AUTOTEST"):
        cl = self.client()
        n = cl[self.db][TABLE].update_one({"_id": rid}, {"$set": {"val": new_val}}).modified_count
        cl.close()
        return n

    def fetch_rows(self):
        cl = self.client()
        rows = []
        for d in cl[self.db][TABLE].find({}, {"_id": 1, "grp": 1, "val": 1, "payload": 1, "n": 1}):
            rows.append((d["_id"], d.get("grp"), d.get("val"), d.get("payload"), d.get("n")))
        cl.close()
        return rows

    def fingerprint(self):
        return fp_from_rows(self.fetch_rows())

    def count(self):
        try:
            cl = self.client()
            n = cl[self.db][TABLE].count_documents({})
            cl.close()
            return n
        except Exception:
            return -1

    def alive(self):
        try:
            cl = self.client()
            cl.admin.command("ping")
            cl.close()
            return True
        except Exception:
            return False

    def conn_str(self, with_db=None):
        return "mongodb://%s:%s@%s:%d" % (self.user, self.password, self.host, self.port)

    def sync_objects(self):
        return json.dumps({self.db: [TABLE]})


# ----------------------------------------------------------------- Redis
class RedisEndpoint:
    """走 docker exec redis-cli，免掉 redis-py 依赖。"""

    def __init__(self, spec):
        self.kind = "redis"
        self.host = spec["host"]
        self.port = spec["port"]
        self.password = spec["password"]
        self.container = spec["container"]
        self.db = "0"

    def cli(self, *args):
        p = subprocess.run(["docker", "exec", self.container, "redis-cli",
                            "-a", self.password, "--no-auth-warning", *args],
                           capture_output=True, text=True, timeout=60)
        return (p.stdout or "").strip()

    def reset_source(self):
        self.cli("FLUSHALL")

    def reset_target(self):
        self.cli("FLUSHALL")

    def drop_db(self):
        self.cli("FLUSHALL")

    def seed(self, rows, base=1):
        # 用 pipeline 形式批量写：字符串 + hash + list，覆盖多种类型
        script = []
        for i in range(rows):
            k = "%s:str:%d" % (C.PREFIX, base + i)
            script.append("SET %s seed-%d" % (k, i))
        script.append("HSET %s:hash f1 v1 f2 v2" % C.PREFIX)
        script.append("RPUSH %s:list a b c" % C.PREFIX)
        self._pipe(script)
        return list(range(base, base + rows))

    def _pipe(self, cmds):
        payload = "\n".join(cmds) + "\n"
        p = subprocess.run(["docker", "exec", "-i", self.container, "redis-cli",
                            "-a", self.password, "--no-auth-warning", "--pipe"],
                           input=payload, capture_output=True, text=True, timeout=120)
        return (p.stdout or "") + (p.stderr or "")

    def write(self, rows, base, tag="incr"):
        cmds = []
        alive = []
        for i in range(rows):
            k = "%s:str:%d" % (C.PREFIX, base + i)
            cmds.append("SET %s %s-%d" % (k, tag, i))
            alive.append(base + i)
        cmds.append("HSET %s:hash f3 v3" % C.PREFIX)
        # 删几个
        deletes = 0
        for rid in alive[:3]:
            cmds.append("DEL %s:str:%d" % (C.PREFIX, rid))
            deletes += 1
        alive = alive[3:]
        self._pipe(cmds)
        return {"inserted": alive, "updates": 0, "deletes": deletes}

    def fingerprint(self):
        """Redis 用 keyspace 快照做指纹：所有 at_* key 的 (key, type, value) 摘要。"""
        keys = [k for k in self.cli("--scan", "--pattern", "%s:*" % C.PREFIX).splitlines() if k]
        items = []
        for k in sorted(keys):
            t = self.cli("TYPE", k)
            if t == "string":
                v = self.cli("GET", k)
            elif t == "hash":
                v = "|".join(sorted(self.cli("HGETALL", k).splitlines()))
            elif t == "list":
                v = "|".join(self.cli("LRANGE", k, "0", "-1").splitlines())
            elif t == "set":
                v = "|".join(sorted(self.cli("SMEMBERS", k).splitlines()))
            elif t == "zset":
                v = "|".join(self.cli("ZRANGE", k, "0", "-1", "WITHSCORES").splitlines())
            else:
                v = ""
            items.append("%s\x01%s\x01%s" % (k, t, v))
        # 同样不能用 XOR 聚合，理由见 fp_from_rows
        h = hashlib.sha256()
        for it in sorted(items):
            h.update(it.encode("utf-8"))
            h.update(b"\n")
        return (len(keys), int.from_bytes(h.digest()[:8], "big"))

    def count(self):
        return len([k for k in self.cli("--scan", "--pattern", "%s:*" % C.PREFIX).splitlines() if k])

    def alive(self):
        return self.cli("PING") == "PONG"

    def conn_str(self, with_db=None):
        return "redis://default:%s@%s:%d" % (self.password, self.host, self.port)

    def sync_objects(self):
        return json.dumps({"0": ["*"]})


# ----------------------------------------------------------------- Elasticsearch
class EsEndpoint:
    def __init__(self, spec, index):
        self.kind = "es"
        self.host = spec["host"]
        self.port = spec["port"]
        self.user = spec["user"]
        self.password = spec["password"]
        self.container = spec.get("container")
        self.index = index
        self.db = index

    def req(self, path, method="GET", body=None):
        import base64
        url = "http://%s:%d%s" % (self.host, self.port, path)
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(url, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        tok = base64.b64encode(("%s:%s" % (self.user, self.password)).encode()).decode()
        r.add_header("Authorization", "Basic " + tok)
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            try:
                return json.loads(e.read().decode())
            except Exception:
                return {}
        except Exception:
            return {}

    def indices(self):
        """列出 index 前缀下真实存在的索引名。"""
        r = self.req("/_cat/indices/%s*?format=json" % self.index)
        return [i.get("index") for i in r if isinstance(i, dict) and i.get("index")] \
            if isinstance(r, list) else []

    def reset_target(self):
        self.drop_db()

    def drop_db(self):
        # 不能用通配删除：ES 默认 action.destructive_requires_name=true，
        # `DELETE /idx*` 会以 400 "Wildcard expressions ... are not allowed" 被拒。
        # 而 req() 把错误响应吞了，于是"以为删干净了"，上一轮的文档留在索引里，
        # 下一轮全量比对就多出几十条不属于本轮的数据。必须按实名逐个删。
        for name in self.indices():
            self.req("/" + name, "DELETE")

    def refresh(self):
        self.req("/%s/_refresh" % self.index, "POST")

    def count(self):
        self.refresh()
        r = self.req("/%s/_count" % self.index)
        return int(r.get("count", -1)) if "count" in r else -1

    def fetch_rows(self):
        self.refresh()
        rows = []
        r = self.req("/%s/_search?size=10000" % self.index, "POST",
                     {"query": {"match_all": {}}})
        for h in ((r.get("hits") or {}).get("hits") or []):
            s = h.get("_source") or {}
            try:
                rid = int(s.get("id", h.get("_id")))
            except (TypeError, ValueError):
                continue
            rows.append((rid, s.get("grp"), s.get("val"), s.get("payload"), s.get("n")))
        return rows

    def fingerprint(self):
        return fp_from_rows(self.fetch_rows())

    def alive(self):
        return bool(self.req("/"))

    def conn_str(self, with_db=None):
        return "elastic://%s:%s@%s:%d" % (self.user, self.password, self.host, self.port)


# ----------------------------------------------------------------- Oracle
class OracleEndpoint:
    """Oracle 源端。Oracle 的"库"是 schema/用户，不能像 MySQL 那样 DROP DATABASE，
    因此 reset 只重建表（表名大写，Oracle 默认标识符大写）。"""

    def __init__(self, spec):
        self.kind = "oracle"
        self.host = spec["host"]
        self.port = spec["port"]
        self.user = spec["user"]
        self.password = spec["password"]
        self.service = spec["service"]
        self.container = spec.get("container")
        self.db = spec["user"].upper()      # schema 名即库名
        self.table = TABLE.upper()

    def conn(self):
        import oracledb
        return oracledb.connect(user=self.user, password=self.password,
                                dsn="%s:%d/%s" % (self.host, self.port, self.service))

    def exec(self, sql, args=None, ignore=False):
        c = self.conn()
        cur = c.cursor()
        try:
            cur.execute(sql, args or [])
            c.commit()
        except Exception:
            if not ignore:
                raise
        finally:
            cur.close()
            c.close()

    def reset_source(self):
        self.exec("DROP TABLE %s" % self.table, ignore=True)
        self.exec("""CREATE TABLE %s (
                        id NUMBER(19) PRIMARY KEY,
                        grp NUMBER(10) NOT NULL,
                        val VARCHAR2(128),
                        payload VARCHAR2(512),
                        n NUMBER(19))""" % self.table)
        # 增量捕获需要全列前像
        self.exec("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS" % self.table, ignore=True)

    def drop_db(self):
        self.exec("DROP TABLE %s" % self.table, ignore=True)

    def seed(self, rows, base=1):
        c = self.conn()
        cur = c.cursor()
        cur.executemany("INSERT INTO %s (id,grp,val,payload,n) VALUES (:1,:2,:3,:4,:5)" % self.table,
                        [(base + i, i % 100, "seed-%d" % i, "x" * 120, i) for i in range(rows)])
        c.commit()
        cur.close()
        c.close()
        return list(range(base, base + rows))

    def write(self, rows, base, tag="incr"):
        c = self.conn()
        cur = c.cursor()
        alive, updates, deletes = [], 0, 0
        for i in range(rows):
            rid = base + i
            cur.execute("INSERT INTO %s (id,grp,val,payload,n) VALUES (:1,:2,:3,:4,:5)" % self.table,
                        (rid, i % 100, "%s-%d" % (tag, i), "y" * 120, 1000 + i))
            alive.append(rid)
            if i % 5 == 4:
                cur.execute("UPDATE %s SET val=:1, n=n+1 WHERE id=:2" % self.table,
                            ("%s-upd-%d" % (tag, i), alive[-1]))
                updates += 1
            if i % 11 == 10 and len(alive) > 3:
                cur.execute("DELETE FROM %s WHERE id=:1" % self.table, (alive.pop(0),))
                deletes += 1
            c.commit()
        cur.close()
        c.close()
        return {"inserted": alive, "updates": updates, "deletes": deletes}

    def fetch_rows(self):
        c = self.conn()
        cur = c.cursor()
        cur.execute("SELECT id,grp,val,payload,n FROM %s" % self.table)
        rows = [(int(r[0]), int(r[1]), r[2], r[3], None if r[4] is None else int(r[4]))
                for r in cur.fetchall()]
        cur.close()
        c.close()
        return rows

    def fingerprint(self):
        return fp_from_rows(self.fetch_rows())

    def count(self):
        try:
            return len(self.fetch_rows())
        except Exception:
            return -1

    def alive(self):
        try:
            self.conn().close()
            return True
        except Exception:
            return False

    def conn_str(self, with_db=None):
        return "oracle://%s:%s@%s:%d/%s" % (self.user, self.password, self.host,
                                            self.port, self.service)

    def sync_objects(self):
        return json.dumps({self.db: {"tables": [self.table]}})


# ----------------------------------------------------------------- 只读围栏（灾备倒换用）
def set_read_only(ep, on):
    """把实例置为只读 / 解除只读。

    计划内切换成功后**旧主会保持只读**（产品刻意为之：它现在是备库，继续接受写入会双写分叉）。
    用例跑完必须解除，否则同一实例上的后续 suite 连建库都会被拒。
    """
    if ep.kind == "mysql":
        c = ep.conn("mysql")
        cur = c.cursor()
        try:
            cur.execute("SET GLOBAL super_read_only = %s" % ("ON" if on else "OFF"))
        except Exception:
            pass
        cur.execute("SET GLOBAL read_only = %s" % ("ON" if on else "OFF"))
        cur.close()
        c.close()
    elif ep.kind == "pg":
        c = ep.conn("postgres")
        cur = c.cursor()
        cur.execute("ALTER SYSTEM SET default_transaction_read_only = %s" % ("on" if on else "off"))
        cur.execute("SELECT pg_reload_conf()")
        cur.close()
        c.close()


def is_read_only(ep):
    try:
        if ep.kind == "mysql":
            c = ep.conn("mysql")
            cur = c.cursor()
            cur.execute("SELECT @@global.read_only")
            v = cur.fetchone()[0]
            cur.close()
            c.close()
            return str(v) in ("1", "ON", "True")
        if ep.kind == "pg":
            c = ep.conn("postgres")
            cur = c.cursor()
            cur.execute("SHOW default_transaction_read_only")
            v = cur.fetchone()[0]
            cur.close()
            c.close()
            return str(v).lower() in ("on", "true")
    except Exception:
        return False
    return False
