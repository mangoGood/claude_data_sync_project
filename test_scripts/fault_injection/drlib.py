#!/usr/bin/env python3
"""
灾备（DR）故障注入测试的公共层：链路定义 + 灾备任务 API + 双端一致性判定。

与普通同步任务（SYNC）的差别：
  - taskType=DR，drMode=UNIDIRECTIONAL/BIDIRECTIONAL；migrationMode 由后端强制 fullAndIncre。
  - 双向灾备会额外创建隐藏影子任务（DR_SHADOW，B→A 仅增量），在主方向进入
    INCREMENT_RUNNING 后由后端自动启动，两个方向各有一套 capture/extract/increment 子进程。
  - 灾备源/目标必须是**不同实例**（预校验强制），故用 docker-compose-synctask-dr.yml 起的
    dr-mysql-a/b(33320/33321)、dr-pg-a/b(55432/55433)，两侧都开了 binlog / wal_level=logical
    （倒换后原目标要变成新源）。

复用 dblib 的 SqlEndpoint（建表/播种/持续写/指纹）与 faultlib 的进程信号注入。
"""
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dblib as D  # noqa: E402
import faultlib as F  # noqa: E402

MY_A = dict(kind="mysql", host="127.0.0.1", port=33320, user="root", password="rootpassword")
MY_B = dict(kind="mysql", host="127.0.0.1", port=33321, user="root", password="rootpassword")
PG_A = dict(kind="pg", host="127.0.0.1", port=55432, user="postgres", password="rootpassword")
PG_B = dict(kind="pg", host="127.0.0.1", port=55433, user="postgres", password="rootpassword")

DB = "drtest"

DR_LINKS = {
    # 灾备两端库名一致（倒换后 sourceDbName/targetDbName 直接对调，库名不一致会换不过去）
    "mysql2mysql": dict(
        source_type="mysql", target_type="mysql",
        a={**MY_A, "db": DB}, b={**MY_B, "db": DB},
        a_conn=f"mysql://root:rootpassword@127.0.0.1:33320",
        b_conn=f"mysql://root:rootpassword@127.0.0.1:33321",
        sync_objects='{"%s": {"tables": ["%s"]}}' % (DB, D.TABLE),
        engines=["capture", "extract", "increment"],
    ),
    "pg2pg": dict(
        source_type="postgresql", target_type="postgresql",
        a={**PG_A, "db": DB}, b={**PG_B, "db": DB},
        a_conn=f"postgresql://postgres:rootpassword@127.0.0.1:55432/{DB}",
        b_conn=f"postgresql://postgres:rootpassword@127.0.0.1:55433/{DB}",
        sync_objects='{"%s": ["public.%s"]}' % (DB, D.TABLE),
        engines=["capture", "extract", "increment"],
    ),
}


def endpoints(link):
    """返回 (A 端, B 端) 的 dblib 端点对象。"""
    L = DR_LINKS[link]
    return D.make_endpoint(L["a"]), D.make_endpoint(L["b"])


# ------------------------------------------------------------------ 写入线程（显式主键）

# 灾备场景不能沿用 dblib 的自增主键写入线程，原因有二：
#   1. 双向灾备两端同时写入时，各自的自增序列会生成**相同的 id**，产生天然的写写冲突
#      （active-active 本就无法消解），两端永远收敛不到同一指纹——那是测试设计缺陷不是产品缺陷。
#   2. 主备倒换后新主库（原目标）的自增/identity 序列未必随全量数据推进，靠 DB 生成 id 会撞已有行。
# 因此按端划分 id 段、由写入方显式指定主键：A 段 10,000,000+，B 段 20,000,000+。
A_ID_BASE = 10_000_000
B_ID_BASE = 20_000_000


class DrWriter(threading.Thread):
    """源端持续写入（显式主键）：INSERT 为主，穿插对自己 id 段的 UPDATE/DELETE。"""

    def __init__(self, ep, interval, id_base):
        super().__init__(daemon=True)
        self.ep = ep
        self.interval = interval
        self.id_base = id_base
        self.stop = threading.Event()
        self.inserts = self.updates = self.deletes = 0
        self.error = None
        self.ids = []

    def run(self):
        try:
            c = self.ep._conn(self.ep.db)
            cur = c.cursor()
            t = self.ep._t()
            cur.execute(f"SELECT COALESCE(MAX(id),0) FROM {t} WHERE id>={self.id_base}")
            nxt = max(cur.fetchone()[0], self.id_base) + 1
            seq = 0
            while not self.stop.is_set():
                seq += 1
                rid = nxt
                nxt += 1
                cur.execute(f"INSERT INTO {t} (id,grp,val,payload,n) VALUES (%s,%s,%s,%s,%s)",
                            (rid, seq % 100, f"live-{self.id_base}-{seq}", "y" * 200, 1000000 + seq))
                if self.ep.kind == "pg":
                    c.commit()
                self.ids.append(rid)
                self.inserts += 1
                if seq % 5 == 0 and self.ids:
                    uid = random.choice(self.ids)
                    cur.execute(f"UPDATE {t} SET val=%s, n=n+1 WHERE id=%s", (f"upd-{seq}", uid))
                    if self.ep.kind == "pg":
                        c.commit()
                    self.updates += 1
                if seq % 23 == 0 and len(self.ids) > 10:
                    did = self.ids.pop(0)
                    cur.execute(f"DELETE FROM {t} WHERE id=%s", (did,))
                    if self.ep.kind == "pg":
                        c.commit()
                    self.deletes += 1
                time.sleep(self.interval)
            cur.close(); c.close()
        except Exception as e:  # noqa: BLE001
            self.error = e


def make_writer(ep, interval, side="a"):
    return DrWriter(ep, interval, A_ID_BASE if side == "a" else B_ID_BASE)


def fmt(fp):
    return f"(count={fp[0]}, xor={fp[1]:#010x})"


# ------------------------------------------------------------------ 指纹（库内计算）

# 灾备的源/目标恒为同引擎（mysql↔mysql / pg↔pg），无需 dblib 那种"跨引擎可比"的 Python 指纹——
# 直接在库内算聚合指纹：内存 O(1)、耗时与行数近似线性但快一个数量级，才跑得动百万级全量数据量。
# 语义与 dblib.fp_from_rows 一致：顺序无关(XOR)、对每列敏感、按主键天然去重。
_FP_SQL = {
    "mysql": ("SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', id, grp, "
              "IFNULL(val,''), IFNULL(payload,''), IFNULL(n,'')))), 0) FROM `%s`" % D.TABLE),
    # PG 14+ 才有 bit_xor 聚合；hashtext 返回 int4，掩码成无符号便于打印比对
    "pg": ("SELECT COUNT(*), COALESCE(bit_xor(hashtext(concat_ws('|', id, grp, "
           "coalesce(val,''), coalesce(payload,''), coalesce(n::text,'')))), 0) FROM %s" % D.TABLE),
}


def fingerprint(ep):
    """库内聚合指纹 (count, xor)。表不存在/查询失败返回 (-1,-1)（永不与真实指纹相等）。"""
    try:
        c = ep._conn(ep.db)
        cur = c.cursor()
        try:
            cur.execute(_FP_SQL[ep.kind])
            cnt, x = cur.fetchone()
            return (int(cnt), int(x) & 0xFFFFFFFF)
        finally:
            cur.close(); c.close()
    except Exception:
        return (-1, -1)


# ------------------------------------------------------------------ 灾备任务 API

def create_dr_task(token, name, link, dr_mode="UNIDIRECTIONAL", swap=False):
    """创建并启动一个灾备任务。swap=True 时以 B 为源、A 为目标（倒换后重建场景用）。"""
    L = DR_LINKS[link]
    src_conn, tgt_conn = (L["b_conn"], L["a_conn"]) if swap else (L["a_conn"], L["b_conn"])
    r = F.api("POST", "/api/workflows", token,
              json={"name": name, "sourceType": L["source_type"], "targetType": L["target_type"],
                    "taskType": "DR", "drMode": dr_mode})
    if not r.get("success"):
        print(f"创建灾备任务失败: {r}")
        sys.exit(1)
    task_id = r["data"]["id"]
    cfg = {
        "sourceConnection": src_conn,
        "targetConnection": tgt_conn,
        # 后端对 DR 强制 fullAndIncre，这里显式传保持一致
        "migrationMode": "fullAndIncre",
        "syncObjects": L["sync_objects"],
        "sourceDbName": DB,
        "targetDbName": DB,
        "sourceType": L["source_type"],
        "targetType": L["target_type"],
    }
    F.api("PUT", f"/api/workflows/{task_id}/config", token, json=cfg)
    r = F.api("POST", f"/api/workflows/{task_id}/launch", token)
    if not r.get("success"):
        print(f"启动灾备任务失败: {r}")
        sys.exit(1)
    return task_id


def get_task(token, task_id):
    return F.api("GET", f"/api/workflows/{task_id}", token).get("data") or {}


def shadow_id(token, task_id):
    """双向灾备的反向影子任务 id（未创建返回 None）。"""
    return get_task(token, task_id).get("dr_peer_workflow_id")


def failover(token, task_id):
    return F.api("POST", f"/api/workflows/{task_id}/failover", token)


def retry(token, task_id):
    return F.api("POST", f"/api/workflows/{task_id}/retry", token)


def cleanup_task(token, task_id):
    F.stop_task(token, task_id)
    time.sleep(3)
    F.delete_task(token, task_id)


# ------------------------------------------------------------------ 环境准备/清理

def reset_both(link, seed_rows=0, seed_side="a"):
    """两端库全部重建；可选在一端播种存量。返回 (a_ep, b_ep)。"""
    a, b = endpoints(link)
    a.reset_source()
    b.reset_source()   # B 端也建表：倒换/双向时 B 要能当源，且目标建表会被同步引擎覆盖
    if seed_rows:
        (a if seed_side == "a" else b).seed(seed_rows)
    return a, b


def reset_target_only(link, side="b"):
    a, b = endpoints(link)
    (b if side == "b" else a).reset_target()


def drop_pg_slots(link):
    """清理 PG 端遗留的 inactive 逻辑复制槽，避免 max_replication_slots 打满。"""
    L = DR_LINKS[link]
    if L["source_type"] != "postgresql":
        return
    import psycopg2
    for spec in (L["a"], L["b"]):
        try:
            c = psycopg2.connect(host=spec["host"], port=spec["port"], user=spec["user"],
                                 password=spec["password"], dbname="postgres")
            c.autocommit = True
            cur = c.cursor()
            cur.execute("SELECT slot_name FROM pg_replication_slots WHERE active=false")
            for (sn,) in cur.fetchall():
                try:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (sn,))
                except Exception:
                    pass
            cur.close(); c.close()
        except Exception:
            pass


def wait_converge(src, tgt, token=None, task_id=None, timeout=600, quiet=False):
    """等待目标端追平源端（指纹相等）。返回 (ok, sfp, tfp)。"""
    deadline = time.time() + timeout
    sfp = tfp = (0, 0)
    while time.time() < deadline:
        time.sleep(3)
        sfp, tfp = fingerprint(src), fingerprint(tgt)
        if sfp == tfp:
            return True, sfp, tfp
        if token and task_id and F.get_status(token, task_id) == "FAILED":
            if not quiet:
                print("    任务已 FAILED，停止等待追平")
            return False, sfp, tfp
    return False, sfp, tfp
