#!/usr/bin/env python3
"""
增量应用吞吐基准（mysql→mysql）。

第 5 批把**全量**从 291 行/秒提到 38,365 行/秒（语句重写批量通道），但**增量应用**
从来没被量过。代码事实是：`ContinuousIncrementMain#executeTypedOn` 对每一行
`prepareStatement → executeUpdate → close`，既不攒批（addBatch/executeBatch）、
不缓存语句（`cachePrepStmts`），目标连接串上也没有 `rewriteBatchedStatements`——
即"一行一次往返"。本脚本把这条天花板量成一个数。

方法：任务进增量并静置后，往源库灌 N 行（分批提交，模拟真实事务流），
记 T0=源端最后一次提交完成时刻，T1=目标端行数追平时刻，吞吐 = N / (T1 - T0)。
这测的是**应用端**能力：源端写入本身走的是批量 INSERT，几秒就写完了，
剩下的时间全花在增量链路上。

两档负载：
  * INSERT 档：纯新增（目标端是 upsert，1 行 1 语句）
  * UPDATE 档：改已存在的行（UPDATE 无法被 upsert 合并，是更典型的 CDC 形态）

判据：吞吐低于 `--min-rps`（默认 2000 行/秒）即判失败。这个阈值不是拍脑袋——
它比全量通道低一个数量级还多，只要求"别停留在几百行/秒"。

用法：
    python3 test_scripts/fault_injection/apply_throughput.py [--rows 20000] [--min-rps 2000]
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
SRC_DB = "tput_src"
TGT_DB = "tput_tgt"
TABLE = "tput_load"

DDL = f"""
CREATE TABLE `{TABLE}` (
  `id` BIGINT NOT NULL,
  `grp` INT NOT NULL,
  `payload` VARCHAR(120),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def rebuild():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}", f"CREATE DATABASE {TGT_DB}"])
    F.sql_exec(CFG, [DDL], db=SRC_DB)


def count(db):
    return F.sql_fetch(CFG, db, f"SELECT COUNT(*) FROM `{TABLE}`")[0][0]


def changed(db):
    return F.sql_fetch(CFG, db, f"SELECT COUNT(*) FROM `{TABLE}` WHERE payload LIKE 'v2-%'")[0][0]


def bulk_insert(start, n, batch=500):
    """分批提交地灌 n 行，返回最后一次提交完成的时刻。"""
    c = F.sql_conn(CFG, SRC_DB)
    cur = c.cursor()
    for base in range(start, start + n, batch):
        vals = ",".join(f"({i},{i % 97},'p-{i}')"
                        for i in range(base, min(base + batch, start + n)))
        cur.execute(f"INSERT INTO `{TABLE}` (id, grp, payload) VALUES {vals}")
    cur.close()
    c.close()
    return time.time()


def bulk_update(start, n, batch=500):
    c = F.sql_conn(CFG, SRC_DB)
    cur = c.cursor()
    for base in range(start, start + n, batch):
        hi = min(base + batch, start + n)
        cur.execute(f"UPDATE `{TABLE}` SET payload=CONCAT('v2-', id) "
                    f"WHERE id >= {base} AND id < {hi}")
    cur.close()
    c.close()
    return time.time()


def create_task_with_consistency(token, name, sync_objects, consistency):
    """F.create_task 不带一致性语义，这里自己建——事务一致档位会把
    increment.apply.parallelism 钉成 1（ConfigService#applyConsistencyMode），
    两档的吞吐差就是"事务一致的代价"。"""
    r = F.api("POST", "/api/workflows", token,
              json={"name": name, "sourceType": "mysql", "targetType": "mysql",
                    "taskType": "SYNC", "consistencyMode": consistency})
    task_id = r["data"]["id"]
    F.api("PUT", f"/api/workflows/{task_id}/config", token, json={
        "sourceConnection": CONN, "targetConnection": CONN,
        "migrationMode": "fullAndIncre", "syncObjects": json.dumps(sync_objects),
        "sourceDbName": SRC_DB, "targetDbName": TGT_DB,
        "sourceType": "mysql", "targetType": "mysql",
        "consistencyMode": consistency,
    })
    F.api("POST", f"/api/workflows/{task_id}/launch", token)
    return task_id


def wait_reach(fn, target, timeout=900):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn() >= target:
            return time.time()
        time.sleep(0.2)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--min-rps", type=float, default=2000.0)
    ap.add_argument("--consistency", choices=["EVENTUAL", "TRANSACTIONAL"], default="EVENTUAL")
    args = ap.parse_args()

    token = F.login()
    # 增量限速配额会被下发成 increment.rate.limit.rows.per.sec，量的就不是引擎能力了。
    # 先抬到不限速，finally 里还原。（本机残留过 50 行/秒的测试值，不清掉测出来的是配额。）
    prev_quota = F.get_increment_quota()
    F.set_increment_quota(0)
    print(f"    增量限速配额: {prev_quota} → 0（不限速），跑完还原")

    rebuild()
    sync_objects = {SRC_DB: {"tables": [TABLE], "targetDb": TGT_DB}}
    task_id = create_task_with_consistency(
        token, f"apply-tput-{int(time.time())}", sync_objects, args.consistency)
    print(f"    taskId={task_id}, rows={args.rows}, 一致性语义={args.consistency}")
    passed, failed = [], []
    try:
        if F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=300) != "INCREMENT_RUNNING":
            print("任务未进入增量，放弃")
            sys.exit(2)
        time.sleep(5)

        # ---- INSERT 档 ----
        t0 = bulk_insert(1, args.rows)
        src_n = count(SRC_DB)
        print(f"    源端已写入 {src_n} 行（写入耗时 {t0 - t0:.0f}s，开始计时追平）")
        t1 = wait_reach(lambda: count(TGT_DB), src_n)
        if t1 is None:
            failed.append(f"INSERT 档：{args.rows} 行在 900s 内未追平（目标 {count(TGT_DB)} 行）")
            ins_rps = 0.0
        else:
            ins_rps = args.rows / max(t1 - t0, 1e-6)
            ok = ins_rps >= args.min_rps
            (passed if ok else failed).append(
                f"INSERT 档吞吐 {ins_rps:,.0f} 行/秒（追平耗时 {t1 - t0:.1f}s，阈值 {args.min_rps:,.0f}）")

        time.sleep(3)
        # ---- UPDATE 档 ----
        t0 = bulk_update(1, args.rows)
        src_c = changed(SRC_DB)
        t1 = wait_reach(lambda: changed(TGT_DB), src_c)
        if t1 is None:
            failed.append(f"UPDATE 档：{args.rows} 行在 900s 内未追平（目标已改 {changed(TGT_DB)} 行）")
        else:
            upd_rps = args.rows / max(t1 - t0, 1e-6)
            ok = upd_rps >= args.min_rps
            (passed if ok else failed).append(
                f"UPDATE 档吞吐 {upd_rps:,.0f} 行/秒（追平耗时 {t1 - t0:.1f}s，阈值 {args.min_rps:,.0f}）")

        # 顺带留一条指纹，确认提速判据不是拿"少同步了"换来的
        s = F.sql_fetch(CFG, SRC_DB, f"SELECT COUNT(*), SUM(CRC32(CONCAT_WS('|',id,grp,IFNULL(payload,'')))) FROM `{TABLE}`")
        t = F.sql_fetch(CFG, TGT_DB, f"SELECT COUNT(*), SUM(CRC32(CONCAT_WS('|',id,grp,IFNULL(payload,'')))) FROM `{TABLE}`")
        ok = (s == t)
        (passed if ok else failed).append(f"两端指纹一致 src={s[0]} tgt={t[0]}")
    finally:
        F.stop_task(token, task_id)
        F.delete_task(token, task_id)
        if prev_quota is not None:
            F.set_increment_quota(prev_quota)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
