#!/usr/bin/env python3
"""
增量应用是否保持「源事务原子性」的判定（mysql→mysql）。

背景：ContinuousIncrementMain 以**单个 THL 事件**为事务边界逐个 commit，源 binlog 的 XID
（事务提交点）只被翻译成一条 "COMMIT;" 且在执行时被显式跳过。若确实如此，一个包含 N 个行事件
的源事务会在目标库被拆成 N 个独立事务落地，目标端在中间时刻可以被读到"半个事务"。

两把互相独立的尺子（都不依赖抓拍时机的运气）：

1. **目标库 binlog 计数（确定性）**——目标是 MySQL，把源侧执行的 K 个「每个含 2 条 UPDATE」的
   事务同步过去后，数目标库 binlog 中属于目标库的 XID（提交点）个数。
   保持原子性 → 约 K 个提交点；每事件一提交 → 约 2K 个。

2. **不变量抓拍**——源侧转账事务保持 `sum(bal)` 恒定，后台高频读目标两行求和。
   读到 sum != 常量即抓到"半个事务"可见（torn read）。抓不到不代表没问题（尺子 1 才是判据），
   抓到则是直接证据。

用法：
    python3 test_scripts/fault_injection/txn_atomicity.py [--txns 200] [--mode TRANSACTION]

--mode 决定用哪种投递语义跑（会带着对应的 APPLY_TRANSACTION_MODE 重启 agent，跑完还原）：
    EVENT        逐事件提交（历史行为），预期尺子1 判为"未保持"——用来复现缺陷；
    TRANSACTION  源事务 → 目标事务 1:1，预期提交点数≈源事务数、抓拍破缺 0 次。
"""
import argparse
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
SRC_DB = "tx_src"
TGT_DB = "tx_tgt"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

DDL = """
CREATE TABLE `acct` (
  `id` INT NOT NULL,
  `bal` BIGINT NOT NULL,
  `ver` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

TOTAL = 2000  # 两个账户余额之和恒为 TOTAL


def setup_source():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}"])
    F.sql_exec(CFG, [DDL,
                     f"INSERT INTO acct (id,bal,ver) VALUES (1,{TOTAL//2},0),(2,{TOTAL//2},0)"], db=SRC_DB)


def drop_target():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {TGT_DB}"])


def target_gtid_or_pos():
    """目标库当前 binlog 位点（file, pos），作为计数起点。"""
    rows = F.sql_fetch(CFG, None, "SHOW MASTER STATUS")
    return rows[0][0], rows[0][1]


def count_target_commits(start_file, start_pos, end_file, end_pos):
    """数 [start,end) 区间内、涉及目标库 acct 表的事务提交点(XID)与行事件数。

    做法：SHOW BINLOG EVENTS 顺序扫描，遇到 Table_map 指向 TGT_DB.acct 就把当前事务标记为
    "本事务写了目标表"，遇到 Xid 时若已标记则计一个提交点。
    """
    c = F.sql_conn(CFG, None)
    cur = c.cursor()
    commits = 0
    row_events = 0
    f = start_file
    pos = start_pos
    marked = False
    while True:
        cur.execute(f"SHOW BINLOG EVENTS IN '{f}' FROM {pos} LIMIT 20000")
        rows = cur.fetchall()
        if not rows:
            break
        last_pos = pos
        for (logname, p, etype, sid, endpos, info) in rows:
            last_pos = endpos
            if logname == end_file and p >= end_pos:
                cur.close(); c.close()
                return commits, row_events
            # info 形如 "table_id: 123 (tx_tgt.acct)"（无反引号）
            if etype == "Table_map" and f"({TGT_DB}.acct)" in info:
                marked = True
            elif etype in ("Update_rows", "Write_rows", "Delete_rows") and marked:
                row_events += 1
            elif etype == "Xid":
                if marked:
                    commits += 1
                marked = False
            elif etype == "Rotate":
                pass
        if last_pos <= pos:
            break
        pos = last_pos
        if f == end_file and pos >= end_pos:
            break
    cur.close(); c.close()
    return commits, row_events


class InvariantWatcher(threading.Thread):
    """高频读目标两行求和，记录读到的破缺不变量次数。"""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop_flag = False
        self.torn = 0
        self.reads = 0
        self.samples = []

    def run(self):
        try:
            c = F.sql_conn(CFG, TGT_DB)
        except Exception:
            return
        cur = c.cursor()
        # READ COMMITTED：每条 SELECT 看到最新已提交状态，才能观测到"半个事务已提交"
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        while not self.stop_flag:
            try:
                cur.execute("SELECT SUM(bal), COUNT(*) FROM acct")
                s, n = cur.fetchone()
                self.reads += 1
                if n == 2 and s is not None and int(s) != TOTAL:
                    self.torn += 1
                    if len(self.samples) < 5:
                        self.samples.append(int(s))
            except Exception:
                time.sleep(0.05)
        cur.close(); c.close()


def restart_agent(apply_mode):
    """带指定投递语义重启 agent（APPLY_TRANSACTION_MODE 由 ConfigService 落进任务 config）。"""
    env = dict(os.environ)
    if apply_mode:
        env["APPLY_TRANSACTION_MODE"] = apply_mode
    else:
        env.pop("APPLY_TRANSACTION_MODE", None)
    r = subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR, env=env,
                       capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"重启 agent 失败: {r.stdout}\n{r.stderr}")
    print(f"[agent] 已按 APPLY_TRANSACTION_MODE={apply_mode or '(未设置=EVENT)'} 重启")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--txns", type=int, default=200)
    ap.add_argument("--mode", choices=["EVENT", "TRANSACTION"], default="TRANSACTION",
                    help="投递语义；TRANSACTION 为本判据的期望态")
    ap.add_argument("--converge", type=int, default=600,
                    help="停写后等待目标收敛的最长秒数")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    restart_agent(args.mode)

    token = F.login()
    print("✓ 登录成功")

    setup_source()
    drop_target()
    print(f"[准备] 源库 {SRC_DB}.acct 两行，余额和恒为 {TOTAL}")

    task_id = F.create_task(
        token, f"txn-atomicity-{int(time.time())}", "mysql", "mysql", CONN, CONN,
        "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["acct"]}}}}', TGT_DB, source_db=SRC_DB)
    print(f"[任务] {task_id}")

    failures = []
    watcher = None
    try:
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            print(f"✗ 任务未进入增量阶段: {st}")
            return 1

        # 等目标表可读
        for _ in range(60):
            try:
                r = F.sql_fetch(CFG, TGT_DB, "SELECT COUNT(*) FROM acct")
                if r and r[0][0] == 2:
                    break
            except Exception:
                pass
            time.sleep(2)

        watcher = InvariantWatcher()
        watcher.start()

        start_f, start_p = target_gtid_or_pos()
        print(f"[基线] 目标 binlog 起点 {start_f}:{start_p}")

        # 源侧执行 K 个转账事务，每个事务 2 条 UPDATE（两行），保持 sum 恒定
        c = F.sql_conn(CFG, SRC_DB)
        c.autocommit = False
        cur = c.cursor()
        for i in range(args.txns):
            d = 1 if i % 2 == 0 else -1
            # ver 单调递增：余额是 ±1 交替的，只看余额无法判断目标是否真的追平
            # （偶数个事务后余额回到初始值，"目标==源"在一开始就成立）
            cur.execute(f"UPDATE acct SET bal = bal - {d}, ver = ver + 1 WHERE id = 1")
            cur.execute(f"UPDATE acct SET bal = bal + {d}, ver = ver + 1 WHERE id = 2")
            c.commit()
            time.sleep(0.01)
        cur.close(); c.close()
        print(f"[写入] 源侧完成 {args.txns} 个事务，每个含 2 条 UPDATE（共 {args.txns*2} 个行事件）")

        # 等目标追平
        deadline = time.time() + args.converge
        src_rows = F.sql_fetch(CFG, SRC_DB, "SELECT id,bal,ver FROM acct ORDER BY id")
        while time.time() < deadline:
            try:
                tgt_rows = F.sql_fetch(CFG, TGT_DB, "SELECT id,bal,ver FROM acct ORDER BY id")
                if tgt_rows == src_rows:
                    break
            except Exception:
                pass
            time.sleep(2)
        tgt_rows = F.sql_fetch(CFG, TGT_DB, "SELECT id,bal,ver FROM acct ORDER BY id")
        print(f"[追平] 源={src_rows} 目标={tgt_rows}")
        if tgt_rows != src_rows:
            failures.append("最终数据未收敛一致")

        time.sleep(3)
        watcher.stop_flag = True
        watcher.join(timeout=10)

        end_f, end_p = target_gtid_or_pos()
        commits, row_events = count_target_commits(start_f, start_p, end_f, end_p)

        expect_atomic = args.mode == "TRANSACTION"

        print("\n" + "=" * 68)
        print(f"投递语义: apply.transaction.mode={args.mode}"
              f"（期望{'保持' if expect_atomic else '不保持'}原子性）")
        print("尺子1｜目标库 binlog 提交点计数（确定性）")
        print(f"  源事务数            : {args.txns}")
        print(f"  源行事件数          : {args.txns * 2}")
        print(f"  目标库写目标表的行事件: {row_events}")
        print(f"  目标库提交点(Xid)数  : {commits}")
        if commits >= args.txns * 1.8:
            print(f"  → 判定: 源事务被拆成了 ~{commits/args.txns:.1f} 个目标事务，事务原子性【未保持】")
            if expect_atomic:
                failures.append(f"源事务原子性未保持：{args.txns} 个源事务 → {commits} 个目标事务")
        elif commits <= args.txns * 1.2:
            print("  → 判定: 提交点数≈源事务数，事务原子性【保持】")
            if not expect_atomic:
                failures.append(f"EVENT 模式下本应逐事件提交，实测只有 {commits} 个提交点（判据失效？）")
        else:
            print(f"  → 判定: 介于两者之间（{commits}），需人工确认")
            failures.append(f"提交点数 {commits} 介于 {args.txns} 与 {args.txns*2} 之间，判定不明确")

        print("\n尺子2｜不变量抓拍（目标端能否读到半个事务）")
        print(f"  目标读取次数        : {watcher.reads}")
        print(f"  读到 sum != {TOTAL} 的次数: {watcher.torn}")
        if watcher.samples:
            print(f"  破缺样本            : {watcher.samples}")
        if watcher.torn > 0:
            print("  → 判定: 目标端确实可读到【半个事务已提交】的中间态")
            if expect_atomic:
                failures.append(f"目标端观测到 {watcher.torn} 次事务中间态（torn read）")
        else:
            print("  → 判定: 本轮未抓到中间态（抓拍是概率性的，以尺子1为准）")
        print("=" * 68)

    finally:
        if watcher:
            watcher.stop_flag = True
        if not args.keep:
            F.stop_task(token, task_id)
            time.sleep(3)
            F.delete_task(token, task_id)
            try:
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}",
                                 f"DROP DATABASE IF EXISTS {TGT_DB}"])
            except Exception:
                pass
            # 还原 agent 的默认投递语义，避免污染后续用例
            try:
                restart_agent(None)
            except Exception as e:
                print(f"[警告] 还原 agent 失败: {e}")

    return F.print_result([] if failures else ["txn_atomicity"], failures)


if __name__ == "__main__":
    sys.exit(main())
