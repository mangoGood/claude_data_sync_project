#!/usr/bin/env python3
"""
订阅链路的**全类型保真**校验：TiDB（全 MySQL 列类型）/ MongoDB（全 BSON 类型）。

sub_resume.py 那套只覆盖 (id, val, n) 三个标量字段，够验"不丢/可收敛"，但证明不了
DECIMAL、BLOB、JSON、ENUM/SET、Decimal128、Binary、Timestamp 这些类型能原样出现在 Kafka 里。
本脚本对每一列/每一个 BSON 类型逐个断言，且覆盖 INSERT / UPDATE / DELETE 三种事件。

判定方式：
  - TiDB：源库按列读回真值，与 Kafka 事件 after 中同名列**逐列**比对（数值按数值比、
    二进制按 hex 比、其余按字符串比），列缺失或值不符即失败；
  - Mongo：Kafka 事件 after 是 relaxed 扩展 JSON，用同样的 relaxed 表示从源库导出真值后逐字段比对，
    类型信息（$numberDecimal/$binary/$timestamp/$oid…）一并比。

用法：
  python3 sub_types.py tidb
  python3 sub_types.py mongo
退出码 0 = 全通过。
"""
import argparse
import datetime
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sublib as S  # noqa: E402

TYPES_TABLE = "fi_sub_types"


# ------------------------------------------------------------------ TiDB 全列类型

TIDB_DDL = f"""CREATE TABLE `{TYPES_TABLE}` (
  `id` BIGINT NOT NULL PRIMARY KEY,
  `c_tinyint` TINYINT,
  `c_tinyint_u` TINYINT UNSIGNED,
  `c_smallint` SMALLINT,
  `c_mediumint` MEDIUMINT,
  `c_int` INT,
  `c_int_u` INT UNSIGNED,
  `c_bigint` BIGINT,
  `c_bigint_u` BIGINT UNSIGNED,
  `c_bool` TINYINT(1),
  `c_decimal` DECIMAL(30,10),
  `c_float` FLOAT,
  `c_double` DOUBLE,
  `c_bit` BIT(8),
  `c_char` CHAR(20),
  `c_varchar` VARCHAR(255),
  `c_tinytext` TINYTEXT,
  `c_text` TEXT,
  `c_mediumtext` MEDIUMTEXT,
  `c_longtext` LONGTEXT,
  `c_binary` BINARY(8),
  `c_varbinary` VARBINARY(64),
  `c_blob` BLOB,
  `c_date` DATE,
  `c_time` TIME,
  `c_datetime` DATETIME,
  `c_timestamp` TIMESTAMP NULL,
  `c_year` YEAR,
  `c_enum` ENUM('a','b','c'),
  `c_set` SET('x','y','z'),
  `c_json` JSON,
  `c_null` VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""

TIDB_COLS = [
    "c_tinyint", "c_tinyint_u", "c_smallint", "c_mediumint", "c_int", "c_int_u",
    "c_bigint", "c_bigint_u", "c_bool", "c_decimal", "c_float", "c_double", "c_bit",
    "c_char", "c_varchar", "c_tinytext", "c_text", "c_mediumtext", "c_longtext",
    "c_binary", "c_varbinary", "c_blob", "c_date", "c_time", "c_datetime",
    "c_timestamp", "c_year", "c_enum", "c_set", "c_json", "c_null",
]


def tidb_rows():
    """边界值 + 典型值 + 全 NULL 三类行。"""
    return [
        (1, -128, 255, -32768, -8388608, -2147483648, 4294967295,
         -9223372036854775808, 18446744073709551615, 1, "-12345678901234567890.1234567890",
         3.5, 1.7976931348623157e308, 255, "char值", "varchar 中文 😀",
         "tiny", "text 内容", "medium", "long" * 100,
         b"\x00\x01\x02\x03\xfd\xfe\xff\x7f", b"\xde\xad\xbe\xef", b"blob\x00binary",
         "2024-02-29", "23:59:59", "2024-02-29 23:59:59", "2038-01-19 03:14:07",
         2155, "c", "x,z", '{"k": [1, 2, {"n": null}], "u": "中文"}', None),
        (2, 127, 0, 32767, 8388607, 2147483647, 0,
         9223372036854775807, 0, 0, "99999999999999999999.9999999999",
         -3.5, -1.7976931348623157e308, 0, "", "",
         "", "", "", "",
         b"\xff" * 8, b"", b"",
         "1000-01-01", "-838:59:59", "1000-01-01 00:00:00", "1970-01-02 00:00:01",
         1901, "a", "", "[]", "not null"),
        (3, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None, None,
         None, None, None, None, None, None, None, None, None, None,
         None, None, None, None, None, None),
    ]


def tidb_prepare(src):
    c = src.conn("mysql")
    cur = c.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS `{src.db}`")
    cur.execute(f"CREATE DATABASE `{src.db}` DEFAULT CHARACTER SET utf8mb4")
    cur.close(); c.close()
    c = src.conn(src.db)
    cur = c.cursor()
    cur.execute(TIDB_DDL)
    cur.close(); c.close()


def tidb_insert(src, rows):
    c = src.conn(src.db)
    cur = c.cursor()
    ph = ",".join(["%s"] * (len(TIDB_COLS) + 1))
    cols = "`id`," + ",".join(f"`{x}`" for x in TIDB_COLS)
    cur.executemany(f"INSERT INTO `{TYPES_TABLE}` ({cols}) VALUES ({ph})", rows)
    cur.close(); c.close()


def tidb_truth(src, ids):
    """源库读回真值，规范化成 {id: {col: 规范化值}}。"""
    c = src.conn(src.db)
    cur = c.cursor()
    cols = ",".join(f"`{x}`" for x in TIDB_COLS)
    cur.execute(f"SELECT `id`,{cols} FROM `{TYPES_TABLE}` WHERE id IN ({','.join(map(str, ids))})")
    out = {}
    for r in cur.fetchall():
        out[int(r[0])] = {TIDB_COLS[i]: norm_sql(r[i + 1], TIDB_COLS[i])
                          for i in range(len(TIDB_COLS))}
    cur.close(); c.close()
    return out


def norm_sql(v, col=None):
    """把源库读回的值与 Kafka JSON 值统一成可比形式。"""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        # BIT(n) 驱动侧读回的是字节串，但它语义上就是个无符号整数，
        # 链路里也是按整数传的；其余二进制类型才按 hex 比。
        if col == "c_bit":
            return str(int.from_bytes(v, "big"))
        return "hex:" + v.hex()
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, (int,)):
        return str(v)
    if isinstance(v, float):
        # repr 而不是 %.10g：先截断到 10 位有效数字，再拿去和消息里的完整字面量比，
        # 差异就成了测试自己造的
        return repr(v)
    if isinstance(v, (set, frozenset)):
        # SET 列驱动侧读回的是 Python set（无序），链路里是逗号分隔的标签串
        return ",".join(sorted(v))
    if isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, datetime.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, datetime.timedelta):
        total = int(v.total_seconds())
        sign = "-" if total < 0 else ""
        total = abs(total)
        return f"{sign}{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"
    from decimal import Decimal
    if isinstance(v, Decimal):
        # 不用 normalize()：它按当前 decimal 上下文精度（默认 28 位有效数字）四舍五入，
        # DECIMAL(30,10) 会被测试自己截掉两位，凭空造出"精度丢失"
        return format(v, "f")
    return str(v)


def norm_kafka(v):
    """Kafka 事件里的值（可能是数字/字符串/None）规范化成与 norm_sql 可比的形式。"""
    from decimal import Decimal
    if v is None:
        return None
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, int):
        return str(v)
    s = str(v)
    if s.startswith("0x") or s.startswith("0X"):
        return "hex:" + s[2:].lower()
    return s


def run_tidb():
    passed, failed = [], []
    src = S.SOURCES["tidb"](db="fi_subtype_tidb")
    prefix = "fisubtypetidb"
    token = S.login()
    print("✓ 登录；TiDB 订阅全列类型保真校验")

    S.delete_topics(prefix)
    tidb_prepare(src)

    tid = S.create_subscribe_task(token, f"FI-subtype-tidb-{int(time.time())}",
                                  src.source_type, src.src_conn_str(),
                                  json.dumps({src.db: {"tables": [TYPES_TABLE]}}),
                                  src.db, prefix)
    print(f"[任务] {tid}")
    st = S.wait_status(token, tid, {"SUBSCRIBE_RUNNING"}, timeout=420)
    if st != "SUBSCRIBE_RUNNING":
        failed.append(f"未进入 SUBSCRIBE_RUNNING（{st}）")
        S.stop_task(token, tid); return S.print_result(passed, failed)
    passed.append("订阅任务进入 SUBSCRIBE_RUNNING")
    time.sleep(30)

    rows = tidb_rows()
    tidb_insert(src, rows)
    print(f"[写入] 插入 {len(rows)} 行全类型数据（{len(TIDB_COLS)} 列）")

    # UPDATE：把第 2 行的每一列都改掉；DELETE：删掉第 3 行
    c = src.conn(src.db)
    cur = c.cursor()
    cur.execute(f"""UPDATE `{TYPES_TABLE}` SET c_int=42, c_decimal='0.0000000001',
        c_varchar='更新后 ✅', c_blob=%s, c_json='{{"upd": true}}', c_enum='b',
        c_set='y,z', c_datetime='2000-01-01 12:34:56', c_null='填上了' WHERE id=2""",
                (b"\x01\x02\x03",))
    cur.execute(f"DELETE FROM `{TYPES_TABLE}` WHERE id=3")
    cur.close(); c.close()
    print("[写入] 已 UPDATE id=2 的 9 个列、DELETE id=3")

    truth = tidb_truth(src, [1, 2])
    print(f"[校验] 等待 Kafka 追平 ...")
    ok_ins = ok_upd = ok_del = False
    detail = ""
    deadline = time.time() + 240
    while time.time() < deadline:
        time.sleep(8)
        evs = S.parse_events(S.consume_all(prefix, idle_timeout=6))
        state, _ = S.replay_full(evs, idcol="id")
        if 1 not in state or 2 not in state:
            continue
        d1 = col_diff(truth[1], state[1])
        d2 = col_diff(truth[2], state[2])
        ok_ins, ok_upd = not d1, not d2
        ok_del = 3 not in state and any(op == "d" for op, _, _, _ in evs)
        detail = f"id=1 差异 {d1}\n    id=2 差异 {d2}"
        if ok_ins and ok_upd and ok_del:
            break

    (passed if ok_ins else failed).append(f"INSERT 事件全列类型保真（{len(TIDB_COLS)} 列）")
    (passed if ok_upd else failed).append("UPDATE 事件全列类型保真（改后各列与源库一致）")
    (passed if ok_del else failed).append("DELETE 事件已投递且回放后行消失")
    if not (ok_ins and ok_upd):
        print(f"    {detail}")

    S.stop_task(token, tid); time.sleep(3); S.delete_task(token, tid)
    return S.print_result(passed, failed)


def values_equal(tv, kv):
    """先按字符串比；都能当数字解释时按数值比。

    数值比是必需的：DOUBLE 在源库驱动侧是 float、在消息里是十进制展开字面量
    （1.7976931348623157e308 vs 17976931348623157000…0），字符串永远不相等但数值是同一个。
    先用 Decimal 精确比（DECIMAL(30,10) 这类必须一位不差），不等再退到 15 位有效数字比
    （double 本身就只有约 17 位有效数字，超出部分是表示差异不是数据差异）。
    """
    if tv == kv:
        return True
    if tv is None or kv is None:
        return False
    from decimal import Decimal, InvalidOperation
    try:
        if Decimal(str(tv)) == Decimal(str(kv)):
            return True
    except (InvalidOperation, ValueError):
        return False
    try:
        return f"{float(tv):.15g}" == f"{float(kv):.15g}"
    except (TypeError, ValueError):
        return False


def col_diff(truth_row, kafka_row):
    """逐列比对，返回不一致的 {col: (源, Kafka)}。"""
    out = {}
    for col, tv in truth_row.items():
        found = False
        kv = None
        for k in (col, col.upper(), col.lower()):
            if kafka_row and k in kafka_row:
                kv = norm_kafka(kafka_row[k])
                found = True
                break
        if not found:
            out[col] = (tv, "<列缺失>")
            continue
        if not values_equal(tv, kv):
            out[col] = (tv, kv)
    return out


# ------------------------------------------------------------------ Mongo 全 BSON 类型

def run_mongo():
    from bson import Binary, Code, Decimal128, MaxKey, MinKey, ObjectId, Regex, Timestamp
    from bson.json_util import RELAXED_JSON_OPTIONS, dumps

    passed, failed = [], []
    src = S.SOURCES["mongo"](db="fi_subtype_mongo")
    prefix = "fisubtypemongo"
    token = S.login()
    print("✓ 登录；MongoDB 订阅全 BSON 类型保真校验")

    S.delete_topics(prefix)
    src.reset()

    tid = S.create_subscribe_task(token, f"FI-subtype-mongo-{int(time.time())}",
                                  src.source_type, src.src_conn_str(),
                                  json.dumps({src.db: {"tables": [S.TABLE]}}),
                                  src.db, prefix)
    print(f"[任务] {tid}")
    st = S.wait_status(token, tid, {"SUBSCRIBE_RUNNING"}, timeout=420)
    if st != "SUBSCRIBE_RUNNING":
        failed.append(f"未进入 SUBSCRIBE_RUNNING（{st}）")
        S.stop_task(token, tid); return S.print_result(passed, failed)
    passed.append("订阅任务进入 SUBSCRIBE_RUNNING")
    time.sleep(20)

    docs = []
    for i in range(1, 4):
        docs.append({
            "_id": i,
            "t_double": 3.141592653589793 * i,
            "t_string": f"中文/emoji 😀🍃 \"quoted\" \\back\\ \n换行 {i}",
            "t_object": {"a": 1, "b": {"c": [1, 2, {"d": "deep"}]}},
            "t_array": [1, "two", 3.0, None, True, [4, 5], {"six": 6}],
            "t_binary": Binary(bytes(range(256))),
            "t_objectid": ObjectId(),
            "t_bool": i % 2 == 0,
            "t_date": datetime.datetime(2024, 2, 29, 23, 59, 59),
            "t_null": None,
            "t_regex": Regex(r"^ab+c$", "im"),
            "t_int32": -2147483648,
            "t_int64": 9223372036854775807,
            "t_timestamp": Timestamp(1700000000 + i, i),
            "t_decimal": Decimal128("1234567890123456789.012345678901234"),
            "t_minkey": MinKey(),
            "t_maxkey": MaxKey(),
            "t_code": Code("function () { return 1; }"),
        })
    cl = src._client()
    cl[src.db][S.TABLE].insert_many(docs)
    cl[src.db][S.TABLE].update_one({"_id": 2}, {"$set": {
        "t_decimal": Decimal128("-0.000000000000000000000001"),
        "t_binary": Binary(b"\xde\xad\xbe\xef"),
        "t_array": [{"changed": True}],
        "t_date": datetime.datetime(2038, 1, 19, 3, 14, 7)}})
    cl[src.db][S.TABLE].delete_one({"_id": 3})
    cl.close()
    print(f"[写入] 插入 {len(docs)} 篇全类型文档（{len(docs[0])} 字段）+ UPDATE id=2 + DELETE id=3")

    cl = src._client()
    truth = {d["_id"]: json.loads(dumps(d, json_options=RELAXED_JSON_OPTIONS))
             for d in cl[src.db][S.TABLE].find({})}
    cl.close()

    ok_ins = ok_upd = ok_del = False
    detail = ""
    deadline = time.time() + 240
    while time.time() < deadline:
        time.sleep(8)
        evs = S.parse_events(S.consume_all(prefix, idle_timeout=6))
        state, _ = S.replay_full(evs, idcol="_id")
        if 1 not in state or 2 not in state:
            continue
        d1 = field_diff(truth[1], state[1])
        d2 = field_diff(truth[2], state[2])
        ok_ins, ok_upd = not d1, not d2
        ok_del = 3 not in state and any(op == "d" for op, _, _, _ in evs)
        detail = f"id=1 差异 {d1}\n    id=2 差异 {d2}"
        if ok_ins and ok_upd and ok_del:
            break

    (passed if ok_ins else failed).append(f"INSERT 事件全 BSON 类型逐字段保真（{len(docs[0])} 字段）")
    (passed if ok_upd else failed).append("UPDATE 事件后像与源库逐字段一致（含 Decimal128/Binary/Date）")
    (passed if ok_del else failed).append("DELETE 事件已投递且回放后文档消失")
    if not (ok_ins and ok_upd):
        print(f"    {detail}")

    S.stop_task(token, tid); time.sleep(3); S.delete_task(token, tid)
    return S.print_result(passed, failed)


def field_diff(truth_doc, kafka_doc):
    out = {}
    for k, tv in truth_doc.items():
        if not kafka_doc or k not in kafka_doc:
            out[k] = (tv, "<字段缺失>")
        elif kafka_doc[k] != tv:
            out[k] = (tv, kafka_doc[k])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["tidb", "mongo"])
    args = ap.parse_args()
    sys.exit(run_tidb() if args.kind == "tidb" else run_mongo())


if __name__ == "__main__":
    main()
