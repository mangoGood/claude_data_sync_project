#!/usr/bin/env python3
"""
熔断自愈与 crash-loop 可见性的判定（mysql→mysql）。

背景（两个方向相反的洞，都出在 ProcessGuard/CircuitBreaker 上）：

1. **熔断打开即永久打死**——CircuitBreaker 只有 CLOSED/OPEN 两态，reset() 全仓无人调用。
   连续 5 次启动失败后 allowRequest() 恒 false → attemptRecovery 返回 false → 守护线程退出 →
   该进程<b>再也不会被拉起</b>，任务判 FAILED 必须人工 retry。
   重试退避 5s→10s→20s→40s→80s 合计约 2.5 分钟，也就是说：目标库/依赖一次超过 2.5 分钟的
   计划内维护窗口，就足以把任务永久打死。

2. **永久 crash-loop 完全不可见**——只要进程每次能活过 5s，就判定"启动成功"，
   上报"进程已自动重启恢复 + INCREMENT_RUNNING"。反复崩溃的任务在看板上跟健康任务一模一样。

判据：
  1. 依赖不可用（这里用"临时移走 increment jar"模拟）时，任务进入 **RECONNECTING**，不是 FAILED；
  2. 依赖恢复后，任务<b>自己</b>回到 INCREMENT_RUNNING（全程无人工 retry）；
  3. 自愈后数据继续追平，两端指纹一致；
  4. 窗口内反复重启达到阈值时，任务错误码变成 **E3007**（反复崩溃），而不是一路"已自动重启恢复"。

用法：
    python3 test_scripts/fault_injection/selfheal_reconnect.py [--rows 400]
"""
import argparse
import os
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INCREMENT_JAR = os.path.join(PROJECT_DIR, "migration-increment/target/migration-increment-1.0.0.jar")
JAR_BACKUP = INCREMENT_JAR + ".selfheal-bak"

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
SRC_DB = "heal_src"
TGT_DB = "heal_tgt"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

DDL = """
CREATE TABLE `acct` (
  `id` INT NOT NULL,
  `val` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# 把 agent 的重试/熔断/crash-loop 参数压到秒级，否则一轮用例要跑一小时
FAST_ENV = {
    "MIGRATION_AGENT_RETRY_MAX_ATTEMPTS": "2",
    "MIGRATION_AGENT_RETRY_INITIAL_DELAY_MS": "2000",
    "MIGRATION_AGENT_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "2",
    "MIGRATION_AGENT_CIRCUIT_BREAKER_OPEN_TIMEOUT_MS": "15000",
    "MIGRATION_AGENT_CIRCUIT_BREAKER_OPEN_TIMEOUT_MAX_MS": "30000",
    "MIGRATION_AGENT_RECONNECT_INTERVAL_MS": "15000",
    "MIGRATION_AGENT_RECONNECT_MAX_ATTEMPTS": "30",
    "MIGRATION_AGENT_CRASHLOOP_THRESHOLD": "3",
    "MIGRATION_AGENT_CRASHLOOP_WINDOW_MS": "900000",
}


def restart_agent(extra_env=None):
    env = dict(os.environ)
    env.update(extra_env or {})
    r = subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR, env=env,
                       capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"重启 agent 失败: {r.stdout}\n{r.stderr}")
    print(f"[agent] 已重启（{'快速熔断参数' if extra_env else '默认参数'}）")


def task_detail(token, task_id):
    return (F.api("GET", f"/api/workflows/{task_id}", token).get("data") or {})


def wait_for(token, task_id, predicate, timeout, label):
    """轮询任务详情直到 predicate(detail) 为真；返回 (成功?, 最后一次 detail)。"""
    deadline = time.time() + timeout
    last = {}
    seen = None
    while time.time() < deadline:
        last = task_detail(token, task_id)
        cur = (last.get("status"), last.get("error_code"))
        if cur != seen:
            print(f"    [{label}] status={cur[0]} errorCode={cur[1]}")
            seen = cur
        if predicate(last):
            return True, last
        time.sleep(2)
    return False, last


def write_rows(start, count):
    c = F.sql_conn(CFG, SRC_DB)
    cur = c.cursor()
    for i in range(start, start + count):
        cur.execute(f"INSERT INTO acct (id,val) VALUES ({i},{i * 7}) "
                    f"ON DUPLICATE KEY UPDATE val=VALUES(val)")
    cur.close()
    c.close()


def fingerprint(db):
    return F.sql_fetch(CFG, db, "SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS(',',id,val))) FROM acct")[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=400)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    restart_agent(FAST_ENV)
    token = F.login()
    print("✓ 登录成功")

    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}"])
    F.sql_exec(CFG, [DDL, "INSERT INTO acct (id,val) VALUES (0,0)"], db=SRC_DB)

    task_id = F.create_task(
        token, f"selfheal-{int(time.time())}", "mysql", "mysql", CONN, CONN,
        "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["acct"]}}}}', TGT_DB, source_db=SRC_DB)
    print(f"[任务] {task_id}")

    passed, failed = [], []
    jar_moved = False
    try:
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            failed.append(f"任务未进入增量阶段（{st}）")
            return F.print_result(passed, failed)
        time.sleep(5)
        write_rows(1, args.rows // 2)
        time.sleep(8)

        # ---------- 判据4：crash-loop 可见性 ----------
        rounds = int(FAST_ENV["MIGRATION_AGENT_CRASHLOOP_THRESHOLD"])
        print(f"\n[阶段1] 连杀 increment {rounds} 次（crash-loop 阈值 {rounds}），看是否报 E3007")
        for i in range(rounds):
            old = set(F.signal_child(task_id, "increment", signal.SIGKILL))
            print(f"    第 {i + 1} 次 SIGKILL increment: {sorted(old)}")
            # 必须等到"新 pid"——刚发完 SIGKILL 时旧进程还在进程表里，
            # 只看"有没有 increment 进程"会立刻返回，下一刀砍在同一条命上，重启次数就攒不够
            deadline = time.time() + 150
            newborn = None
            while time.time() < deadline:
                cur = set(F.child_pids(task_id, "increment"))
                fresh = cur - old
                if fresh:
                    newborn = sorted(fresh)
                    break
                time.sleep(2)
            if not newborn:
                failed.append(f"第 {i + 1} 次杀掉后 increment 没被重新拉起")
                break
            print(f"        已重启为 {newborn}")
            time.sleep(3)

        ok, detail = wait_for(token, task_id, lambda d: d.get("error_code") == "E3007", 90, "crash-loop")
        if ok:
            passed.append("窗口内反复重启被识别为 crash-loop 并上报 E3007")
        else:
            failed.append(f"反复重启后没有 E3007（error_code={detail.get('error_code')}）——"
                          f"crash-loop 在监控上仍不可见")
        if detail.get("status") == "FAILED":
            failed.append("crash-loop 不该直接把任务判失败（进程每次都拉起来了）")

        # ---------- 判据1/2：熔断打开 → RECONNECTING → 自愈 ----------
        print("\n[阶段2] 移走 increment jar 模拟依赖长时间不可用")
        shutil.move(INCREMENT_JAR, JAR_BACKUP)
        jar_moved = True
        F.signal_child(task_id, "increment", signal.SIGKILL)

        ok, detail = wait_for(token, task_id,
                              lambda d: d.get("status") in ("RECONNECTING", "FAILED"), 180, "熔断")
        if detail.get("status") == "RECONNECTING":
            passed.append("短期重试耗尽后进入 RECONNECTING（可自愈），未被判死")
        elif detail.get("status") == "FAILED":
            failed.append("依赖不可用直接判 FAILED —— 熔断打开即永久打死的老行为还在")
        else:
            failed.append(f"180s 内没进入 RECONNECTING（当前 {detail.get('status')}）")

        print("\n[阶段3] 放回 jar，看任务能否自己回到 INCREMENT_RUNNING")
        shutil.move(JAR_BACKUP, INCREMENT_JAR)
        jar_moved = False
        ok, detail = wait_for(token, task_id,
                              lambda d: d.get("status") == "INCREMENT_RUNNING", 240, "自愈")
        if ok:
            passed.append("依赖恢复后任务自行回到 INCREMENT_RUNNING（无人工干预）")
        else:
            failed.append(f"240s 内未自愈（当前 {detail.get('status')}）")

        # ---------- 判据3：自愈后继续追平 ----------
        print("\n[阶段4] 再写一批数据，校验自愈后仍能追平")
        write_rows(args.rows // 2 + 1, args.rows // 2)
        deadline = time.time() + 300
        src = tgt = None
        while time.time() < deadline:
            src = fingerprint(SRC_DB)
            try:
                tgt = fingerprint(TGT_DB)
            except Exception:
                tgt = None
            if tgt == src:
                break
            time.sleep(3)
        print(f"    源 = {src}\n    目标 = {tgt}")
        if src == tgt:
            passed.append(f"自愈后两端指纹一致（{src[0]} 行）")
        else:
            failed.append(f"自愈后数据不一致: 源={src} 目标={tgt}")

    finally:
        if jar_moved and os.path.exists(JAR_BACKUP):
            shutil.move(JAR_BACKUP, INCREMENT_JAR)
            print("[清理] increment jar 已还原")
        if not args.keep:
            F.stop_task(token, task_id)
            time.sleep(3)
            F.delete_task(token, task_id)
            try:
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}",
                                 f"DROP DATABASE IF EXISTS {TGT_DB}"])
            except Exception:
                pass
            try:
                restart_agent()
            except Exception as e:
                print(f"[警告] 还原 agent 默认参数失败: {e}")

    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
