#!/usr/bin/env python3
"""
TiDB → MySQL 端到端测试（走真实 backend + agent + Kafka 全链路）。

覆盖：
  - 元数据接口：test-connection / databases / tables / validate（tidb 源）
  - 场景 A：全量同步（migration.mode=full）→ FULL_COMPLETED，逐行逐列比对源/目标
  - 场景 B：全量+增量（fullAndIncre）→ INCREMENT_RUNNING，写 INSERT/UPDATE/DELETE
            （含主键变更、边界值、NULL、二进制、多字节文本）后再比对

数据集是「全类型 + 边界值」表：每种 MySQL/TiDB 类型都取最小值、最大值、NULL、
特殊字符/字节等极端形态，确保 TiCDC canal-json 的取值还原没有静默失真。

前置：docker compose -f docker-compose-synctask-tidb.yml up -d
     （synctask-tidb:14000 源；目标为 synctask-mysql:33306）
用法：python3 test_scripts/tidb/tidb_e2e.py
"""
import json
import sys
import time

import mysql.connector
import requests

BASE_URL = "http://localhost:38080"
USER, PASSWORD = "admin", "admin123"

TIDB = dict(host="127.0.0.1", port=14000, user="root", password="tidbpassword")
MYSQL = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")

SRC_CONN = "mysql://root:tidbpassword@127.0.0.1:14000"
TGT_CONN = "mysql://root:rootpassword@127.0.0.1:33306"

DB_FULL = "tidb_full_db"       # 场景 A 用库
DB_INCR = "tidb_incr_db"       # 场景 B 用库
TABLE = "t_all_types"

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'✓' if cond else '✗'}] {name}{(' — ' + detail) if detail else ''}")
    return cond


# ---------------------------------------------------------------- db helpers

def conn(cfg, db=None):
    c = mysql.connector.connect(database=db, use_pure=True, autocommit=True, **cfg)
    cur = c.cursor()
    # 两端会话时区统一 UTC：TIMESTAMP 是按会话时区渲染的，时区不一致会让比对出现整点偏移
    cur.execute("SET time_zone = '+00:00'")
    cur.close()
    return c


def run(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args or ())
    try:
        rows = cur.fetchall()
    except mysql.connector.Error:
        rows = []
    cur.close()
    return rows


def fetch_all(cfg, db, table):
    c = conn(cfg, db)
    cur = c.cursor()
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND DATA_TYPE = 'json'", (db, table))
    json_cols = {r[0] for r in cur.fetchall()}
    cur.execute(f"SELECT * FROM `{table}` ORDER BY id")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    c.close()
    return cols, rows, json_cols


DDL = f"""
CREATE TABLE `{TABLE}` (
  `id` INT NOT NULL,
  `c_bit1` BIT(1),
  `c_bit8` BIT(8),
  `c_bit64` BIT(64),
  `c_bool` TINYINT(1),
  `c_tinyint` TINYINT,
  `c_tinyint_u` TINYINT UNSIGNED,
  `c_smallint` SMALLINT,
  `c_smallint_u` SMALLINT UNSIGNED,
  `c_mediumint` MEDIUMINT,
  `c_mediumint_u` MEDIUMINT UNSIGNED,
  `c_int` INT,
  `c_int_u` INT UNSIGNED,
  `c_bigint` BIGINT,
  `c_bigint_u` BIGINT UNSIGNED,
  `c_decimal` DECIMAL(20,6),
  `c_decimal_max` DECIMAL(65,30),
  `c_float` FLOAT,
  `c_double` DOUBLE,
  `c_date` DATE,
  `c_datetime` DATETIME(6),
  `c_timestamp` TIMESTAMP(6) NULL,
  `c_time` TIME(6),
  `c_year` YEAR,
  `c_char` CHAR(32),
  `c_varchar` VARCHAR(500),
  `c_tinytext` TINYTEXT,
  `c_text` TEXT,
  `c_mediumtext` MEDIUMTEXT,
  `c_longtext` LONGTEXT,
  `c_binary` BINARY(16),
  `c_varbinary` VARBINARY(255),
  `c_tinyblob` TINYBLOB,
  `c_blob` BLOB,
  `c_mediumblob` MEDIUMBLOB,
  `c_longblob` LONGBLOB,
  `c_enum` ENUM('a','b','c','中文'),
  `c_set` SET('r','w','x'),
  `c_json` JSON,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

COLS = [
    "id", "c_bit1", "c_bit8", "c_bit64", "c_bool", "c_tinyint", "c_tinyint_u",
    "c_smallint", "c_smallint_u", "c_mediumint", "c_mediumint_u", "c_int", "c_int_u",
    "c_bigint", "c_bigint_u", "c_decimal", "c_decimal_max", "c_float", "c_double",
    "c_date", "c_datetime", "c_timestamp", "c_time", "c_year", "c_char", "c_varchar",
    "c_tinytext", "c_text", "c_mediumtext", "c_longtext", "c_binary", "c_varbinary",
    "c_tinyblob", "c_blob", "c_mediumblob", "c_longblob", "c_enum", "c_set", "c_json",
]

# 每种类型的最小/最大/NULL/特殊值。字节串用 bytes，驱动会按二进制参数绑定。
ROW_MIN = (
    1, 0, 0, 0, 0, -128, 0, -32768, 0, -8388608, 0, -2147483648, 0,
    -9223372036854775808, 0, "-99999999999999.999999",
    "-99999999999999999999999999999999999.999999999999999999999999999999",
    -3.4e38, -1.7e308,
    "1000-01-01", "1000-01-01 00:00:00.000000", "1970-01-01 00:00:01.000000",
    "-838:59:59.000000", 1901,
    "", "", "", "", "", "",
    b"\x00" * 16, b"", b"", b"", b"", b"",
    "a", "", '{"k": null}',
)

ROW_MAX = (
    2, 1, 255, 18446744073709551615, 1, 127, 255, 32767, 65535, 8388607, 16777215,
    2147483647, 4294967295, 9223372036854775807, 18446744073709551615,
    "99999999999999.999999",
    "99999999999999999999999999999999999.999999999999999999999999999999",
    3.4e38, 1.7e308,
    "9999-12-31", "9999-12-31 23:59:59.999999", "2038-01-19 03:14:07.999999",
    "838:59:59.000000", 2155,
    "x" * 32, "y" * 500, "t" * 255, "T" * 1000, "m" * 2000, "l" * 4000,
    b"\xff" * 16, b"\xff" * 255, b"\xfe" * 255, b"\xfd" * 1000, b"\xfc" * 2000, b"\xfb" * 4000,
    "中文", "r,w,x", '{"a": [1, 2, 3], "b": {"c": "\\u4e2d\\u6587"}}',
)

ROW_NULL = (3,) + (None,) * (len(COLS) - 1)

# 特殊字符/字节：引号、反斜杠、换行、NUL、emoji、多字节
ROW_SPECIAL = (
    4, 1, 170, 12345678901234567890, 1, -1, 1, -1, 1, -1, 1, -1, 1, -1, 1,
    "0.000001", "0.000000000000000000000000000001", 0.0, 0.0,
    "2024-02-29", "2024-02-29 12:34:56.789012", "2024-02-29 12:34:56.789012",
    "00:00:00.000001", 2000,
    "a'b\"c\\d", "line1\nline2\ttab 'q' \"dq\" \\bs 中文 😀",
    "emoji 😀🎉", "反斜杠\\\\ 引号'' 双引号\"\"", "混合 mixed 中文 abc 123",
    "\r\n\t special",
    # VARBINARY(255) 放不下 0..255 全 256 个字节，取前 255 个（0x00~0xFE 已覆盖全部字节形态）
    bytes(range(16)), bytes(range(255)), b"\x00\x01\x02\xff", b"\x00" * 5 + b"\xff" * 5,
    b"\x7f\x80\x81", b"\xc3\x28",   # 后者是非法 UTF-8 字节序列，必须按裸字节保真
    "b", "w,x", '{"emoji": "😀", "quote": "he said \\"hi\\"", "nested": {"n": 1.5}}',
)

ROW_ZERO = (
    5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0.000000", "0.000000000000000000000000000000",
    0.0, 0.0, "2000-01-01", "2000-01-01 00:00:00.000000", "2000-01-01 00:00:00.000000",
    "00:00:00.000000", 2000, "0", "0", "0", "0", "0", "0",
    b"0", b"0", b"0", b"0", b"0", b"0", "c", "r", "[]",
)

BASE_ROWS = [ROW_MIN, ROW_MAX, ROW_NULL, ROW_SPECIAL, ROW_ZERO]


def insert_rows(c, rows):
    placeholders = ", ".join(["%s"] * len(COLS))
    collist = ", ".join(f"`{x}`" for x in COLS)
    cur = c.cursor()
    cur.executemany(f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders})", rows)
    cur.close()


def seed(db, rows):
    c = conn(TIDB)
    run(c, f"DROP DATABASE IF EXISTS `{db}`")
    run(c, f"CREATE DATABASE `{db}` DEFAULT CHARACTER SET utf8mb4")
    c.close()
    c = conn(TIDB, db)
    run(c, DDL)
    insert_rows(c, rows)
    c.close()


def drop_target(db):
    c = conn(MYSQL)
    run(c, f"DROP DATABASE IF EXISTS `{db}`")
    c.close()


def json_equal(a, b):
    """JSON 列语义相等：解析后比对，解析不了则退回原样比对。"""
    def parse(v):
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8")
        return json.loads(v) if isinstance(v, str) else v
    try:
        return parse(a) == parse(b)
    except (ValueError, TypeError):
        return a == b


def compare(db, label):
    """逐行逐列比对源库与目标库，返回 (是否一致, 差异描述列表)。"""
    try:
        scols, srows, json_cols = fetch_all(TIDB, db, TABLE)
    except mysql.connector.Error as e:
        return False, [f"读取源表失败: {e}"]
    try:
        tcols, trows, _ = fetch_all(MYSQL, db, TABLE)
    except mysql.connector.Error as e:
        return False, [f"读取目标表失败: {e}"]

    diffs = []
    if scols != tcols:
        diffs.append(f"列不一致: 源={scols} 目标={tcols}")
        return False, diffs
    if len(srows) != len(trows):
        diffs.append(f"行数不一致: 源={len(srows)} 目标={len(trows)} "
                     f"(源id={[r[0] for r in srows]}, 目标id={[r[0] for r in trows]})")
    for s, t in zip(srows, trows):
        for i, name in enumerate(scols):
            sv, tv = s[i], t[i]
            if name in json_cols and sv is not None and tv is not None:
                # JSON 列按文档语义比对，不比字符串：两端对 JSON 对象键的规范化顺序不同
                # （MySQL 先按键长度再按字节序，TiDB 纯字典序），同一份文档序列化结果必然不同，
                # 这是引擎差异而非同步失真。
                same = json_equal(sv, tv)
            elif isinstance(sv, float) and isinstance(tv, float):
                same = sv == tv or abs(sv - tv) <= abs(sv) * 1e-12
            else:
                same = sv == tv
            if not same:
                diffs.append(f"id={s[0]} 列 {name}: 源={sv!r} 目标={tv!r}")
    print(f"    [{label}] 源 {len(srows)} 行 / 目标 {len(trows)} 行，差异 {len(diffs)} 处")
    return not diffs, diffs


# ---------------------------------------------------------------- backend api

def api(method, path, token=None, **kw):
    headers = kw.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.request(method, f"{BASE_URL}{path}", headers=headers, timeout=60, **kw).json()


def login():
    d = api("POST", "/api/auth/login", json={"username": USER, "password": PASSWORD})
    if "token" not in d:
        print(f"登录失败: {d}")
        sys.exit(1)
    return d["token"]


def create_task(token, name, mode, db):
    r = api("POST", "/api/workflows", token,
            json={"name": name, "sourceType": "tidb", "targetType": "mysql", "taskType": "SYNC"})
    if not r.get("success"):
        print(f"创建任务失败: {r}")
        sys.exit(1)
    task_id = r["data"]["id"]
    cfg = {
        "sourceConnection": SRC_CONN,
        "targetConnection": TGT_CONN,
        "migrationMode": mode,
        "syncObjects": json.dumps({db: {"tables": [TABLE]}}),
        "targetDbName": db,
        "sourceType": "tidb",
        "targetType": "mysql",
    }
    api("PUT", f"/api/workflows/{task_id}/config", token, json=cfg)
    api("POST", f"/api/workflows/{task_id}/launch", token)
    return task_id


def wait_status(token, task_id, wanted, timeout=300):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        d = api("GET", f"/api/workflows/{task_id}", token)
        st = (d.get("data") or {}).get("status")
        if st != last:
            print(f"    状态: {st}")
            last = st
        if st in wanted:
            return st
        if st == "FAILED":
            return st
        time.sleep(3)
    return last


# ---------------------------------------------------------------- scenarios

def scenario_metadata(token):
    print("\n[元数据接口]")
    r = api("POST", "/api/metadata/test-connection", token,
            json={"sourceConnection": SRC_CONN, "dbType": "tidb"})
    check("test-connection(tidb 源)", bool(r.get("success")) and (r.get("data") or {}).get("connected"),
          str(r.get("data") or r.get("message"))[:160])

    r = api("POST", "/api/metadata/databases", token, json={"sourceConnection": SRC_CONN})
    dbs = ((r.get("data") or {}).get("databases")) or []
    check("databases 列出 TiDB 用户库", DB_FULL in dbs, f"{dbs[:8]}")

    r = api("POST", "/api/metadata/tables", token,
            json={"sourceConnection": SRC_CONN, "database": DB_FULL})
    tbls = [t.get("name") for t in ((r.get("data") or {}).get("tables") or [])]
    check("tables 列出全类型表", TABLE in tbls, f"{tbls}")

    r = api("POST", "/api/metadata/validate", token,
            json={"sourceConnection": SRC_CONN, "targetConnection": TGT_CONN,
                  "migrationMode": "fullAndIncre", "sourceType": "tidb", "targetType": "mysql"})
    data = r.get("data") or {}
    items = data.get("checkItems") or []
    names = {i["name"]: i for i in items}
    check("validate 不再跑 binlog 检查（TiDB 无 binlog）",
          not any("Binlog" in n for n in names), f"检查项={list(names)}")
    check("validate 含 TiCDC 服务检查", "TiCDC 服务" in names,
          names.get("TiCDC 服务", {}).get("message", ""))
    check("validate 整体通过", bool(data.get("allPassed")),
          "; ".join(f"{i['name']}={i['message']}" for i in items if not i["passed"])[:300])


def scenario_full(token):
    print("\n[场景 A：全量同步]")
    seed(DB_FULL, BASE_ROWS)
    drop_target(DB_FULL)
    task_id = create_task(token, "E2E-TiDB全量-" + str(int(time.time())), "full", DB_FULL)
    print(f"    任务: {task_id}")
    st = wait_status(token, task_id, {"FULL_COMPLETED", "COMPLETED"}, timeout=300)
    if not check("全量任务到达 FULL_COMPLETED", st in ("FULL_COMPLETED", "COMPLETED"), f"status={st}"):
        return task_id
    ok, diffs = compare(DB_FULL, "全量")
    check("全量数据逐列一致（全类型 + 边界值）", ok, "\n      ".join(diffs[:15]))
    return task_id


def scenario_increment(token):
    print("\n[场景 B：全量 + 增量同步]")
    seed(DB_INCR, BASE_ROWS)
    drop_target(DB_INCR)
    task_id = create_task(token, "E2E-TiDB增量-" + str(int(time.time())), "fullAndIncre", DB_INCR)
    print(f"    任务: {task_id}")
    st = wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=360)
    if not check("增量任务到达 INCREMENT_RUNNING", st == "INCREMENT_RUNNING", f"status={st}"):
        return task_id

    ok, diffs = compare(DB_INCR, "增量任务的全量阶段")
    check("增量任务全量阶段数据一致", ok, "\n      ".join(diffs[:15]))

    print("    写入增量变更（INSERT / UPDATE / DELETE / 主键变更）...")
    c = conn(TIDB, DB_INCR)
    # INSERT：再来一轮边界值（换 id）
    new_min = (11,) + ROW_MIN[1:]
    new_max = (12,) + ROW_MAX[1:]
    new_special = (13,) + ROW_SPECIAL[1:]
    new_null = (14,) + ROW_NULL[1:]
    insert_rows(c, [new_min, new_max, new_special, new_null])
    # UPDATE：把最小值行改成最大值形态（覆盖所有列的前后镜像）
    setclause = ", ".join(f"`{x}` = %s" for x in COLS[1:])
    run(c, f"UPDATE `{TABLE}` SET {setclause} WHERE id = 1", ROW_MAX[1:])
    # UPDATE：只改单列（canal-json 的 old 只带变化列，验证前镜像重建）
    run(c, f"UPDATE `{TABLE}` SET `c_varchar` = %s WHERE id = 4", ("单列改动 😀",))
    # UPDATE：NULL ←→ 非 NULL 互转
    run(c, f"UPDATE `{TABLE}` SET `c_text` = NULL, `c_blob` = NULL WHERE id = 2")
    run(c, f"UPDATE `{TABLE}` SET `c_text` = %s, `c_blob` = %s WHERE id = 3", ("从NULL变来", b"\x00\xff"))
    # UPDATE：主键变更（TiDB 聚簇索引下 TiCDC 会拆成 DELETE + INSERT）
    run(c, f"UPDATE `{TABLE}` SET `id` = 20 WHERE id = 12")
    # DELETE
    run(c, f"DELETE FROM `{TABLE}` WHERE id = 5")
    run(c, f"DELETE FROM `{TABLE}` WHERE id = 14")
    c.close()

    print("    等待增量追平...")
    ok = False
    diffs = ["增量未追平"]
    for _ in range(40):
        time.sleep(3)
        ok, diffs = compare(DB_INCR, "增量")
        if ok:
            break
    check("增量数据逐列一致（INSERT/UPDATE/DELETE/主键变更/NULL 互转）", ok,
          "\n      ".join(diffs[:15]))
    return task_id


def main():
    token = login()
    print("✓ 登录成功")

    # 元数据检查需要 DB_FULL 已存在，先播种
    seed(DB_FULL, BASE_ROWS)
    scenario_metadata(token)
    full_task = scenario_full(token)
    incr_task = scenario_increment(token)

    print("\n============================================================")
    print(f"  通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        for f in FAIL:
            print(f"   ✗ {f}")
    print(f"  任务: full={full_task} incr={incr_task}")
    print("============================================================")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
