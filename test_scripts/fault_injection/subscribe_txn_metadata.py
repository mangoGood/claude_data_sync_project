#!/usr/bin/env python3
"""
订阅消息是否携带源事务信息的判定（mysql → Kafka）。

背景：CdcEvent 此前只有 seqno/table/before/after，消息里<b>完全没有事务标识</b>——
下游拿到两条消息根本无从判断它们是不是同一笔业务操作，也就无法重组源事务
（Debezium 有 source.txId + transaction 块 + 事务元数据 topic，这里一个都没有）。

三条判据：
  1. 同一源事务产生的多条消息带<b>相同</b>的 transaction.id；
  2. 不同源事务的 transaction.id <b>不同</b>；
  3. transaction.total_order 在事务内从 1 递增；
  4. （开了 subscribe.transaction.topic.enabled 时）事务标记 topic 有 BEGIN/END，
     且 END 的 event_count 等于该事务的消息数。

消息用 snappy 压缩，本机 python 缺解码库，故走 Kafka 容器里的 kafka-console-consumer 读。
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sublib as S  # noqa: E402
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
SRC_DB = "subtx_src"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
KAFKA_CONTAINER = "synctask-kafka-sub"
KAFKA_INTERNAL = "localhost:9092"

DDL = """
CREATE TABLE `acct` (
  `id` INT NOT NULL,
  `bal` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def consume(topic, max_messages, timeout_ms=15000):
    """用 Kafka 容器自带的 console consumer 读整个 topic（它能解 snappy）。"""
    cmd = ["docker", "exec", KAFKA_CONTAINER, "kafka-console-consumer",
           "--bootstrap-server", KAFKA_INTERNAL, "--topic", topic,
           "--from-beginning", "--max-messages", str(max_messages),
           "--timeout-ms", str(timeout_ms)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    out = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out


def restart_agent(txn_topic):
    env = dict(os.environ)
    env["SUBSCRIBE_TRANSACTION_TOPIC_ENABLED"] = "true" if txn_topic else "false"
    r = subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR, env=env,
                       capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"重启 agent 失败: {r.stdout}\n{r.stderr}")
    print(f"[agent] 已按 SUBSCRIBE_TRANSACTION_TOPIC_ENABLED={txn_topic} 重启")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--txns", type=int, default=20)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    restart_agent(True)
    token = F.login()
    print("✓ 登录成功")

    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}"])
    F.sql_exec(CFG, [DDL, "INSERT INTO acct (id,bal) VALUES (1,1000),(2,1000)"], db=SRC_DB)

    prefix = f"subtx{int(time.time())}"
    task_id = S.create_subscribe_task(
        token, f"sub-txn-meta-{int(time.time())}", "mysql", CONN,
        f'{{"{SRC_DB}": {{"tables": ["acct"]}}}}', SRC_DB, prefix)
    print(f"[任务] {task_id}  topic 前缀 {prefix}")

    passed, failed = [], []
    try:
        st = F.wait_status(token, task_id, {"SUBSCRIBE_RUNNING"}, timeout=420)
        if st != "SUBSCRIBE_RUNNING":
            failed.append(f"未进入 SUBSCRIBE_RUNNING（{st}）")
            return F.print_result(passed, failed)
        time.sleep(5)

        # 每个事务 2 条 UPDATE：同一事务的两条消息必须带同一个 transaction.id
        c = F.sql_conn(CFG, SRC_DB)
        c.autocommit = False
        cur = c.cursor()
        for i in range(args.txns):
            d = 1 if i % 2 == 0 else -1
            cur.execute(f"UPDATE acct SET bal = bal - {d} WHERE id = 1")
            cur.execute(f"UPDATE acct SET bal = bal + {d} WHERE id = 2")
            c.commit()
            time.sleep(0.05)
        cur.close(); c.close()
        print(f"[写入] {args.txns} 个事务 × 2 条 UPDATE = {args.txns * 2} 条变更")
        time.sleep(20)

        data_topic = f"{prefix}.{task_id}.{SRC_DB}.acct"
        msgs = consume(data_topic, args.txns * 2)
        print(f"[消费] 数据 topic {data_topic} 收到 {len(msgs)} 条")
        if not msgs:
            failed.append("数据 topic 没有消息")
            return F.print_result(passed, failed)

        # 判据1/2/3：事务分组
        groups = []          # [(txid, [total_order,...])]
        missing_tx = 0
        for m in msgs:
            payload = m.get("payload", m)
            tx = payload.get("transaction")
            if not tx or not tx.get("id"):
                missing_tx += 1
                continue
            if not groups or groups[-1][0] != tx["id"]:
                groups.append((tx["id"], []))
            groups[-1][1].append(tx.get("total_order"))

        print(f"[判据1] 带 transaction 块的消息: {len(msgs) - missing_tx}/{len(msgs)}")
        if missing_tx:
            failed.append(f"{missing_tx} 条消息没有 transaction 块")
        else:
            passed.append("每条消息都带 transaction.id")

        sizes = [len(o) for _, o in groups]
        two_per_tx = sum(1 for s in sizes if s == 2)
        print(f"[判据2] 识别出 {len(groups)} 个事务，其中 {two_per_tx} 个正好 2 条消息")
        if len(groups) < args.txns * 0.8:
            failed.append(f"只识别出 {len(groups)} 个事务，远少于源侧 {args.txns} 个")
        elif two_per_tx < len(groups) * 0.9:
            failed.append(f"只有 {two_per_tx}/{len(groups)} 个事务恰好含 2 条消息")
        else:
            passed.append(f"同一源事务的消息共享 transaction.id（{len(groups)} 个事务）")

        ids = [g[0] for g in groups]
        if len(set(ids)) != len(ids):
            failed.append("不同事务出现了重复的 transaction.id")
        else:
            passed.append("不同源事务的 transaction.id 互不相同")

        bad_order = [o for _, o in groups if o != list(range(1, len(o) + 1))]
        print(f"[判据3] total_order 序列异常的事务: {len(bad_order)}")
        if bad_order:
            failed.append(f"{len(bad_order)} 个事务的 total_order 不是从 1 递增：{bad_order[:3]}")
        else:
            passed.append("total_order 在事务内从 1 递增")

        # 判据4：事务标记 topic
        marker_topic = f"{prefix}.{task_id}.transaction"
        markers = consume(marker_topic, args.txns * 2)
        begins = [m for m in markers if m.get("status") == "BEGIN"]
        ends = [m for m in markers if m.get("status") == "END"]
        print(f"[判据4] 事务标记 topic {marker_topic}: BEGIN {len(begins)} / END {len(ends)}")
        if not begins or not ends:
            failed.append("事务标记 topic 没有 BEGIN/END 消息")
        else:
            bad_count = [m for m in ends if m.get("event_count") != 2]
            if bad_count:
                failed.append(f"{len(bad_count)} 条 END 的 event_count != 2")
            else:
                passed.append(f"事务标记 topic BEGIN/END 齐全，END 的 event_count 均为 2")

    finally:
        if not args.keep:
            F.stop_task(token, task_id)
            time.sleep(3)
            F.delete_task(token, task_id)
            try:
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}"])
            except Exception:
                pass
            try:
                restart_agent(False)
            except Exception as e:
                print(f"[警告] 还原 agent 失败: {e}")

    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
