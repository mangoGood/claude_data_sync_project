#!/usr/bin/env python3
"""
SQL 链路（mysql→mysql / tidb→mysql）断点续传 + 故障注入一致性测试。

流程：
  1. 源建表 + 播种一批存量数据（全量阶段要搬的量）。
  2. 创建 full+incre 任务，等到 INCREMENT_RUNNING。
  3. 启动后台写入线程，持续 INSERT/UPDATE/DELETE（默认 ~5 分钟）。
  4. 故障注入循环：每隔一段时间 SIGKILL 一个受 ProcessGuard 守护的子进程
     （capture/extract/increment 轮流），验证自动重启 + checkpoint 续传。
  5. 停止写入，等待增量追平，逐指纹比对源/目标一致。

用法：
  python3 sql_resume.py mysql   [--minutes 5]
  python3 sql_resume.py tidb    [--minutes 5]
  python3 sql_resume.py mysql --mode full   # 仅全量：全量搬运中途杀 migration-full，验证恢复
"""
import argparse
import os
import signal
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

LINKS = {
    "mysql": dict(
        source_type="mysql", target_type="mysql",
        src_cfg=dict(host="127.0.0.1", port=33306, user="root", password="rootpassword"),
        tgt_cfg=dict(host="127.0.0.1", port=33306, user="root", password="rootpassword"),
        src_conn="mysql://root:rootpassword@127.0.0.1:33306",
        tgt_conn="mysql://root:rootpassword@127.0.0.1:33306",
        src_db="fi_src_mysql", tgt_db="fi_tgt_mysql",
    ),
    "tidb": dict(
        source_type="tidb", target_type="mysql",
        src_cfg=dict(host="127.0.0.1", port=14000, user="root", password="tidbpassword"),
        tgt_cfg=dict(host="127.0.0.1", port=33306, user="root", password="rootpassword"),
        src_conn="mysql://root:tidbpassword@127.0.0.1:14000",
        tgt_conn="mysql://root:rootpassword@127.0.0.1:33306",
        src_db="fi_src_tidb", tgt_db="fi_tgt_tidb",
    ),
}

TABLE = "fi_load"
DDL = f"""
CREATE TABLE `{TABLE}` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `grp` INT NOT NULL,
  `val` VARCHAR(128),
  `payload` VARCHAR(512),
  `n` BIGINT,
  `ts` TIMESTAMP(3) NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# 指纹：顺序无关(BIT_XOR)、对增删改敏感(含每列)、天然去重(id 唯一)。ts 不入指纹（时区/精度风险）。
FINGERPRINT = (f"SELECT COUNT(*) c, "
               f"COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', id, grp, IFNULL(val,'∅'), "
               f"IFNULL(payload,'∅'), IFNULL(n,-1)))), 0) x FROM `{TABLE}`")


def seed(cfg, db, rows):
    F.sql_exec(cfg, [f"DROP DATABASE IF EXISTS `{db}`",
                     f"CREATE DATABASE `{db}` DEFAULT CHARACTER SET utf8mb4"])
    c = F.sql_conn(cfg, db)
    cur = c.cursor()
    cur.execute(DDL)
    batch = []
    for i in range(rows):
        batch.append((i % 100, f"seed-{i}", "x" * 200, i))
        if len(batch) >= 1000:
            cur.executemany(
                f"INSERT INTO `{TABLE}` (grp,val,payload,n) VALUES (%s,%s,%s,%s)", batch)
            batch = []
    if batch:
        cur.executemany(f"INSERT INTO `{TABLE}` (grp,val,payload,n) VALUES (%s,%s,%s,%s)", batch)
    cur.close()
    c.close()


def drop_db(cfg, db):
    F.sql_exec(cfg, [f"DROP DATABASE IF EXISTS `{db}`"])


class Writer(threading.Thread):
    """后台持续写入：INSERT 为主，穿插 UPDATE / DELETE。记录已提交的写入次数。"""
    def __init__(self, cfg, db, interval=0.01):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.db = db
        self.interval = interval
        self.stop = threading.Event()
        self.inserts = 0
        self.updates = 0
        self.deletes = 0
        self.error = None

    def run(self):
        try:
            c = F.sql_conn(self.cfg, self.db)
            cur = c.cursor()
            seq = 0
            while not self.stop.is_set():
                seq += 1
                cur.execute(
                    f"INSERT INTO `{TABLE}` (grp,val,payload,n) VALUES (%s,%s,%s,%s)",
                    (seq % 100, f"live-{seq}", "y" * 200, 1000000 + seq))
                self.inserts += 1
                if seq % 5 == 0:
                    cur.execute(
                        f"UPDATE `{TABLE}` SET val=%s, n=n+1 WHERE id=(SELECT * FROM "
                        f"(SELECT MAX(id) FROM `{TABLE}` WHERE grp=%s) t)",
                        (f"upd-{seq}", seq % 100))
                    self.updates += cur.rowcount if cur.rowcount > 0 else 0
                if seq % 23 == 0:
                    cur.execute(
                        f"DELETE FROM `{TABLE}` WHERE id=(SELECT * FROM "
                        f"(SELECT MIN(id) FROM `{TABLE}` WHERE grp=%s AND val LIKE 'live-%%') t)",
                        (seq % 100,))
                    self.deletes += cur.rowcount if cur.rowcount > 0 else 0
                time.sleep(self.interval)
            cur.close()
            c.close()
        except Exception as e:  # noqa: BLE001
            self.error = e


def fingerprint(cfg, db):
    rows = F.sql_fetch(cfg, db, FINGERPRINT)
    return (rows[0][0], rows[0][1])


def run_incremental(link, minutes):
    L = LINKS[link]
    token = F.login()
    print(f"✓ 登录成功；链路 {link}→{L['target_type']}，故障注入时长 {minutes} 分钟")
    passed, failed = [], []

    # 抬高增量限速配额，避免默认 50 行/秒把大数据量测试卡成“永远追不平”（跑完在 finally 还原）。
    old_quota = F.get_increment_quota()
    F.set_increment_quota(100000)
    print(f"[配额] 增量限速临时 {old_quota} → 100000 行/秒")
    try:
        return _run_incremental_inner(link, L, token, minutes, passed, failed)
    finally:
        if old_quota is not None:
            F.set_increment_quota(old_quota)
            print(f"[配额] 已还原增量限速 → {old_quota} 行/秒")


def _run_incremental_inner(link, L, token, minutes, passed, failed):

    seed_rows = 20000
    print(f"[准备] 源播种 {seed_rows} 行存量数据 ...")
    seed(L["src_cfg"], L["src_db"], seed_rows)
    drop_db(L["tgt_cfg"], L["tgt_db"])

    sync_objects = f'{{"{L["src_db"]}": {{"tables": ["{TABLE}"]}}}}'
    task_id = F.create_task(token, f"FI-{link}-incre-{int(time.time())}",
                            L["source_type"], L["target_type"], L["src_conn"], L["tgt_conn"],
                            "fullAndIncre", sync_objects, L["tgt_db"], source_db=L["src_db"])
    print(f"[任务] {task_id}")

    st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
    if not (st == "INCREMENT_RUNNING"):
        failed.append(f"未进入 INCREMENT_RUNNING（当前 {st}）")
        F.stop_task(token, task_id)
        return F.print_result(passed, failed), task_id
    passed.append("全量完成并进入 INCREMENT_RUNNING")

    # 全量阶段一致性（写入尚未开始，此刻源=存量）
    src_fp = fingerprint(L["src_cfg"], L["src_db"])
    tgt_fp = fingerprint(L["tgt_cfg"], L["tgt_db"])
    ok = src_fp == tgt_fp
    (passed if ok else failed).append(
        f"全量阶段一致性 src={src_fp} tgt={tgt_fp}")

    write_interval = float(os.environ.get("FI_WRITE_INTERVAL", "0.04"))   # ~25 写/秒
    kill_every = float(os.environ.get("FI_KILL_EVERY", "90"))             # 每 90s 杀一个子进程
    print(f"[写入] 启动后台写入线程，持续 {minutes} 分钟 + 故障注入"
          f"（写入间隔 {write_interval}s，每 {kill_every}s 崩溃一个子进程）...")
    writer = Writer(L["src_cfg"], L["src_db"], interval=write_interval)
    writer.start()

    engines = ["capture", "extract", "increment"]
    kills = []
    end = time.time() + minutes * 60
    idx = 0
    interval = kill_every
    next_kill = time.time() + interval
    while time.time() < end:
        time.sleep(2)
        if time.time() >= next_kill:
            eng = engines[idx % len(engines)]
            idx += 1
            pids = F.signal_child(task_id, eng, signal.SIGKILL)
            stamp = time.strftime("%H:%M:%S")
            if pids:
                kills.append((eng, pids))
                print(f"    [{stamp}] SIGKILL {eng} pid={pids}  "
                      f"(inserts={writer.inserts} updates={writer.updates} deletes={writer.deletes})")
            else:
                print(f"    [{stamp}] {eng} 无进程（可能正在重启）")
            next_kill = time.time() + interval
            # 任务若被判 FAILED 说明自愈失败，提前结束
            st = F.get_status(token, task_id)
            if st == "FAILED":
                failed.append(f"故障注入期间任务被判 FAILED（{eng} 杀后未自愈）")
                break

    writer.stop.set()
    writer.join(timeout=30)
    if writer.error:
        failed.append(f"写入线程异常: {writer.error}")
    print(f"[写入] 结束：inserts={writer.inserts} updates={writer.updates} "
          f"deletes={writer.deletes}；共注入 {len(kills)} 次进程崩溃")

    st = F.get_status(token, task_id)
    if st != "INCREMENT_RUNNING":
        failed.append(f"故障注入后任务未维持 INCREMENT_RUNNING（当前 {st}）")

    print("[校验] 等待增量追平并逐指纹比对（最多 8 分钟）...")
    src_fp = fingerprint(L["src_cfg"], L["src_db"])
    ok = False
    for _ in range(160):  # 最多等 8 分钟追平（崩溃后重放积压较慢）
        time.sleep(3)
        tgt_fp = fingerprint(L["tgt_cfg"], L["tgt_db"])
        src_fp = fingerprint(L["src_cfg"], L["src_db"])
        if src_fp == tgt_fp:
            ok = True
            break
        if F.get_status(token, task_id) == "FAILED":
            failed.append("追平期间任务被判 FAILED（崩溃自愈失败或看门狗误报）")
            break
    print(f"    最终 src={src_fp} tgt={tgt_fp}")
    (passed if ok else failed).append(
        f"故障注入后断点续传数据一致（{len(kills)} 次崩溃自愈）")

    F.stop_task(token, task_id)
    time.sleep(3)
    F.delete_task(token, task_id)
    return F.print_result(passed, failed), task_id


def run_full(link):
    """仅全量：全量搬运途中杀 migration-full，验证 agent 恢复后跳过已完成表续传，最终一致。"""
    L = LINKS[link]
    token = F.login()
    print(f"✓ 登录成功；链路 {link}→{L['target_type']} 仅全量断点续传")
    passed, failed = [], []

    seed_rows = 200000  # 够大，确保能在搬运途中杀进程
    print(f"[准备] 源播种 {seed_rows} 行 ...")
    seed(L["src_cfg"], L["src_db"], seed_rows)
    drop_db(L["tgt_cfg"], L["tgt_db"])

    sync_objects = f'{{"{L["src_db"]}": {{"tables": ["{TABLE}"]}}}}'
    task_id = F.create_task(token, f"FI-{link}-full-{int(time.time())}",
                            L["source_type"], L["target_type"], L["src_conn"], L["tgt_conn"],
                            "full", sync_objects, L["tgt_db"], source_db=L["src_db"])
    print(f"[任务] {task_id}")

    # 等到全量真正在搬（目标表出现部分数据）再杀 migration-full
    killed = False
    end = time.time() + 180
    while time.time() < end:
        time.sleep(2)
        st = F.get_status(token, task_id)
        if st in ("FULL_COMPLETED", "COMPLETED"):
            break
        if st == "FAILED":
            break
        try:
            cnt = F.sql_fetch(L["tgt_cfg"], L["tgt_db"],
                              f"SELECT COUNT(*) FROM `{TABLE}`")[0][0]
        except Exception:
            cnt = 0
        if not killed and cnt > 5000:
            pids = F.signal_child(task_id, "full", signal.SIGKILL)
            print(f"    全量搬运中（目标已 {cnt} 行），SIGKILL migration-full pid={pids}")
            killed = True
    passed.append("全量搬运途中已注入 migration-full 崩溃" if killed else "未能在搬运途中注入（数据太小）")

    st = F.get_status(token, task_id)
    print(f"    杀后状态: {st}")
    # migration-full 不受 ProcessGuard 守护，崩溃后任务转 FAILED；用 retry 触发恢复续传
    if st == "FAILED":
        print("    任务 FAILED（预期，全量进程无守护）→ 调用 retry 恢复续传")
        F.api("POST", f"/api/workflows/{task_id}/retry", token)

    # 分片续传纠偏后是「清空目标 + 全量重搬」，20万行重搬耗时较长，放宽等待到 12 分钟
    st = F.wait_status(token, task_id, {"FULL_COMPLETED", "COMPLETED"}, timeout=720)
    ok = st in ("FULL_COMPLETED", "COMPLETED")
    (passed if ok else failed).append(f"全量断点续传后完成（终态 {st}）")

    if ok:
        src_fp = fingerprint(L["src_cfg"], L["src_db"])
        tgt_fp = fingerprint(L["tgt_cfg"], L["tgt_db"])
        c_ok = src_fp == tgt_fp
        (passed if c_ok else failed).append(f"全量续传数据一致 src={src_fp} tgt={tgt_fp}")

    F.delete_task(token, task_id)
    return F.print_result(passed, failed), task_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link", choices=list(LINKS.keys()))
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--mode", choices=["incre", "full"], default="incre")
    args = ap.parse_args()

    if args.mode == "full":
        rc, _ = run_full(args.link)
    else:
        rc, _ = run_incremental(args.link, args.minutes)
    sys.exit(rc)


if __name__ == "__main__":
    main()
