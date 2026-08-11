#!/usr/bin/env python3
"""
订阅 Avro + Schema Registry 的端到端判据（mysql → Kafka）。

订阅此前只有 JSON。大数据下游（Flink/Spark）普遍要 Avro + Schema Registry 做 schema 演进，
而且 JSON 里所有值都是文本形态——下游要自己猜"这个字段到底是数字还是字符串"。

产出的字节是 **Confluent wire format**（`0x00` + 4 字节大端 schema id + Avro binary），
判据用的是 **Apicurio**（另一家实现的 Confluent 兼容 registry）+ 标准 Avro 解码器：
能被别人家的实现读出来，才叫"支持 Avro"，而不是"我们自己能读自己写的东西"。

六把尺子：

1. **wire format** —— 每条消息第一个字节是 0x00，接着 4 字节是 registry 里真实存在的 schema id。
2. **subject 已注册** —— registry 里有 `<topic>-value`。
3. **能用 registry 的 schema 解出来** —— 拿 registry 返回的 schema 直接 Avro 解码，
   字段对得上（op / db / table / after）。
4. **值保真** —— 整数解出来是 int 不是 float、字符串是 str、NULL 是 None。
   订阅链路专门修过"整数变浮点""NULL 被丢"，换个格式不能又丢一遍。
5. **UPDATE 带前后镜像** —— before/after 都解得出来且值不同。
6. **加列 = schema 新版本** —— 源端 ADD COLUMN 之后 registry 里该 subject 出现第 2 个版本
   （每个列字段都带 default:null，所以这是一次向后兼容的演进）。

前置：
    docker compose -f docker-compose-synctask-kafka-sub.yml up -d
    （含 synctask-schema-registry，宿主 38081）

用法：
    python3 test_scripts/fault_injection/subscribe_avro.py
"""
import io as _io
import json
import os
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sublib as S  # noqa: E402
import faultlib as F  # noqa: E402

import fastavro  # noqa: E402
import requests  # noqa: E402
from kafka import KafkaConsumer  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
SRC_DB = "avro_src"
TABLE = "avro_t"
# Apicurio 的 Confluent 兼容 API 挂在 /apis/ccompat/v7 下。用它而不是 Confluent 自家实现
# 是有意的：换一家实现照样跑通，才说明我们产出的 wire format 是通用的而不是自说自话。
REGISTRY = os.environ.get("FI_SCHEMA_REGISTRY", "http://localhost:38081/apis/ccompat/v7")
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def rebuild():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}"])
    F.sql_exec(CFG, [f"""CREATE TABLE `{TABLE}` (
        id BIGINT PRIMARY KEY, qty INT, price DOUBLE, name VARCHAR(64), note VARCHAR(64)
    ) ENGINE=InnoDB"""], db=SRC_DB)


def restart_agent_with_registry():
    """agent 要带上 SUBSCRIBE_SCHEMA_REGISTRY_URL 才会把它下发进 config.properties。"""
    env = dict(os.environ)
    env["SUBSCRIBE_SCHEMA_REGISTRY_URL"] = REGISTRY
    r = subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR, env=env,
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(f"重启 agent 失败: {r.stdout}\n{r.stderr}")
    print(f"    agent 已重启（SUBSCRIBE_SCHEMA_REGISTRY_URL={REGISTRY}）")


def registry_get(path):
    r = requests.get(REGISTRY + path, timeout=10)
    r.raise_for_status()
    return r.json()


def schema_by_id(schema_id):
    return json.loads(registry_get(f"/schemas/ids/{schema_id}")["schema"])


def consume(topic, expect, timeout=120):
    c = KafkaConsumer(topic, bootstrap_servers=S.SUB_KAFKA, auto_offset_reset="earliest",
                      consumer_timeout_ms=timeout * 1000, value_deserializer=None,
                      group_id=f"avro-check-{int(time.time())}")
    out = []
    for msg in c:
        out.append(msg.value)
        if len(out) >= expect:
            break
    c.close()
    return out


def decode(raw):
    """Confluent wire format → (schema_id, 解码后的记录)。"""
    assert raw[0] == 0, f"第一个字节应为 magic 0x00，实际 {raw[0]}"
    schema_id = struct.unpack(">I", raw[1:5])[0]
    schema = schema_by_id(schema_id)
    return schema_id, fastavro.schemaless_reader(_io.BytesIO(raw[5:]), schema)


def main():
    token = S.login()
    rebuild()
    restart_agent_with_registry()

    prefix = f"avro{int(time.time())}"
    task_id = S.create_subscribe_task(
        token, f"sub-avro-{int(time.time())}", "mysql", CONN,
        json.dumps({SRC_DB: {"tables": [TABLE]}}), SRC_DB, prefix, fmt="AVRO")
    topic = f"{prefix}.{task_id}.{SRC_DB}.{TABLE}"
    print(f"    taskId={task_id}  topic={topic}")

    passed, failed = [], []
    try:
        if S.wait_status(token, task_id, {"SUBSCRIBE_RUNNING"}, timeout=360) != "SUBSCRIBE_RUNNING":
            print("任务未进入订阅，放弃")
            sys.exit(2)
        time.sleep(5)

        F.sql_exec(CFG, [
            f"INSERT INTO `{TABLE}` VALUES (1, 7, 3.5, 'alice', NULL)",
            f"UPDATE `{TABLE}` SET name='bob', qty=8 WHERE id=1",
        ], db=SRC_DB)

        raws = consume(topic, 2)
        ok = len(raws) >= 2
        (passed if ok else failed).append(f"收到 {len(raws)} 条 Avro 消息（期望 ≥2）")
        if not ok:
            raise SystemExit(1)

        # ---- 尺子1/3：wire format + 用 registry 的 schema 解出来 ----
        sid, ins = decode(raws[0])
        (passed if raws[0][0] == 0 else failed).append(
            f"wire format：magic=0x{raws[0][0]:02x}，schema id={sid}（registry 里能查到）")

        ok = ins.get("op") == "c" and ins.get("db") == SRC_DB and ins.get("table") == TABLE
        (passed if ok else failed).append(
            f"信封字段正确：op={ins.get('op')} db={ins.get('db')} table={ins.get('table')}")

        # ---- 尺子2：subject 已注册 ----
        subjects = registry_get("/subjects")
        subject = topic + "-value"
        ok = subject in subjects
        (passed if ok else failed).append(
            f"Schema Registry 里已有 subject {subject}"
            + ("" if ok else f"  ← 现有 subjects: {subjects}"))

        # ---- 尺子4：值保真 ----
        after = ins.get("after") or {}
        checks = [
            ("qty", after.get("qty"), 7, int),
            ("price", after.get("price"), 3.5, float),
            ("name", after.get("name"), "alice", str),
        ]
        bad = [f"{n}={v!r}({type(v).__name__})" for n, v, exp, ty in checks
               if v != exp or not isinstance(v, ty)]
        null_ok = after.get("note", "MISSING") is None
        ok = not bad and null_ok
        (passed if ok else failed).append(
            f"值保真：qty={after.get('qty')!r} price={after.get('price')!r} "
            f"name={after.get('name')!r} note={after.get('note')!r}"
            + ("" if ok else f"  ← 异常: {bad}{'' if null_ok else ' / NULL 没保住'}"))

        # ---- 尺子5：UPDATE 前后镜像 ----
        _, upd = decode(raws[1])
        b, a = upd.get("before") or {}, upd.get("after") or {}
        ok = upd.get("op") == "u" and b.get("name") == "alice" and a.get("name") == "bob"
        (passed if ok else failed).append(
            f"UPDATE 前后镜像：op={upd.get('op')} before.name={b.get('name')!r} after.name={a.get('name')!r}")

        # ---- 尺子6：加列 → schema 新版本且兼容 ----
        before_versions = registry_get(f"/subjects/{subject}/versions")
        F.sql_exec(CFG, [f"ALTER TABLE `{TABLE}` ADD COLUMN extra VARCHAR(16) NULL"], db=SRC_DB)
        time.sleep(3)
        F.sql_exec(CFG, [f"INSERT INTO `{TABLE}` VALUES (2, 1, 1.0, 'carol', NULL, 'x')"], db=SRC_DB)
        deadline = time.time() + 90
        versions = before_versions
        while time.time() < deadline:
            versions = registry_get(f"/subjects/{subject}/versions")
            if len(versions) > len(before_versions):
                break
            time.sleep(3)
        ok = len(versions) > len(before_versions)
        (passed if ok else failed).append(
            f"加列后 schema 出现新版本：{before_versions} → {versions}"
            + ("" if ok else "  ← 没注册新版本，说明列集变化没被感知"))
    finally:
        S.stop_task(token, task_id)
        S.delete_task(token, task_id)
        subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR, capture_output=True, timeout=300)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
