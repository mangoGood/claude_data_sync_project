#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""订阅链路用的 Kafka 工具（kafka-python）。"""
import json
import time

from . import config as C


def list_topics(prefix=None):
    from kafka import KafkaAdminClient
    a = KafkaAdminClient(bootstrap_servers=C.SUB_KAFKA, request_timeout_ms=20000)
    try:
        ts = a.list_topics()
    finally:
        a.close()
    return [t for t in ts if not prefix or t.startswith(prefix)]


def delete_topics(prefix):
    """删掉上一轮的 topic，避免历史消息污染本轮判定（Kafka 删 topic 是异步的，要等）。"""
    from kafka import KafkaAdminClient
    ts = list_topics(prefix)
    if not ts:
        return []
    a = KafkaAdminClient(bootstrap_servers=C.SUB_KAFKA, request_timeout_ms=20000)
    try:
        a.delete_topics(ts)
    except Exception:
        pass
    finally:
        a.close()
    for _ in range(30):
        time.sleep(1)
        if not list_topics(prefix):
            break
    return ts


def consume_all(prefix, idle_timeout=15):
    """把 prefix 下所有 topic 的消息按 (topic, partition, offset) 顺序读出来。

    顺序就是下游消费者实际看到的投递顺序——回放判定收敛性靠的就是它。
    parse_float=Decimal：按 float 读会让测试自己把精度弄丢，再拿去和源库比就成了误判。
    """
    from decimal import Decimal
    from kafka import KafkaConsumer, TopicPartition
    topics = list_topics(prefix)
    if not topics:
        return []
    c = KafkaConsumer(bootstrap_servers=C.SUB_KAFKA, auto_offset_reset="earliest",
                      enable_auto_commit=False, consumer_timeout_ms=idle_timeout * 1000,
                      max_partition_fetch_bytes=8 * 1024 * 1024,
                      value_deserializer=lambda b: b.decode("utf-8", "replace"))
    tps = []
    for t in topics:
        for p in (c.partitions_for_topic(t) or set()):
            tps.append(TopicPartition(t, p))
    if not tps:
        c.close()
        return []
    c.assign(tps)
    for tp in tps:
        c.seek_to_beginning(tp)
    total = sum(c.end_offsets(tps).values())
    out = []
    if total == 0:
        c.close()
        return out
    try:
        for msg in c:
            try:
                out.append((msg.topic, msg.partition, msg.offset,
                            json.loads(msg.value, parse_float=Decimal)))
            except Exception:
                out.append((msg.topic, msg.partition, msg.offset, {"_raw": msg.value}))
            if len(out) >= total:
                break
    finally:
        c.close()
    out.sort(key=lambda r: (r[0], r[1], r[2]))
    return out


def _num(v):
    """把消息里的字面量归一成 int（跨引擎 NUMBER/BIGINT/Decimal 表示不一）。"""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _lower(d):
    """列名按小写归一：Oracle 的 CDC 事件里列名是大写（ID/VAL/N），
    MySQL/PG 是小写。不归一就会把"字段取不到"当成"这一行没同步"。"""
    return {str(k).lower(): v for k, v in d.items()} if isinstance(d, dict) else {}


def replay(records, idcol="id"):
    """把 CDC 事件按投递顺序回放成最终状态 {id: (id,grp,val,payload,n)}。

    支持 DEBEZIUM_JSON（payload.op c/u/d + before/after）与 Mongo 的 documentKey 形态。
    last-write-wins：只满足"不丢"不满足"可收敛"说明重投顺序错乱，下游照样是脏数据。
    """
    state = {}
    for _, _, _, msg in records:
        p = msg.get("payload") if isinstance(msg.get("payload"), dict) else msg
        if not isinstance(p, dict):
            continue
        op = p.get("op") or p.get("operation")
        after = _lower(p.get("after")) if isinstance(p.get("after"), dict) else None
        before = _lower(p.get("before")) if isinstance(p.get("before"), dict) else None
        dk = _lower(p.get("documentKey")) if isinstance(p.get("documentKey"), dict) else None

        if op in ("d", "DELETE", "delete"):
            src = before or dk or after or {}
            rid = _num(src.get(idcol) if idcol in src else src.get("_id"))
            if rid is not None:
                state.pop(rid, None)
            continue

        src = after or {}
        if not src and dk:
            src = dk
        rid = _num(src.get(idcol) if idcol in src else src.get("_id"))
        if rid is None:
            continue
        # Mongo UPDATE 只给 updatedFields，需要在已有状态上打补丁
        upd = (_lower((p.get("updateDescription") or {}).get("updatedFields") or {})
               if isinstance(p.get("updateDescription"), dict) else None)
        cur = dict(zip(("id", "grp", "val", "payload", "n"), state.get(rid, (rid, None, None, None, None))))
        base = dict(cur)
        for col in ("grp", "val", "payload", "n"):
            if col in src:
                base[col] = src[col]
        if upd:
            for col in ("grp", "val", "payload", "n"):
                if col in upd:
                    base[col] = upd[col]
        state[rid] = (rid, _num(base.get("grp")), None if base.get("val") is None else str(base["val"]),
                      None if base.get("payload") is None else str(base["payload"]),
                      _num(base.get("n")))
    return state
