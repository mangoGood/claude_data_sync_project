#!/usr/bin/env python3
"""
位点持续持久化（中心化）的判据脚本。

`position_resume.py` 守的是**同一台机器上重启**别整段重放；这里守的是位点**离开这台机器之后**
还在不在——也就是 V8 集群化留下的那个静默数据丢失：

  V8 的注释写着「接管方走既有的崩溃恢复路径（从各自 checkpoint 续传）」，
  而那个 checkpoint 在 `files/<taskId>/` 里。换一台 agent 接管时那个目录是空的，
  `AbstractTaskExecutor.initMysqlCheckpoint` 的 `loadCheckpoint()==null` 分支就去取
  **源库此刻的位点**——崩溃到接管之间的全部变更被直接跳过，不报错、不告警、进度条 100%。
  故障转移越成功，丢得越干净。

因此本脚本必须真的起**两个 agent 实例**：agent-B 有自己的工作目录（`files/` 是空的，
这正是"另一台机器"的本质），只共享元数据库与 Kafka。用同一个 agent 重启是测不出这条的。

  尺子1｜跨机接管不丢数据 —— A 跑到增量、持续写入时 SIGKILL；B 接管后两端指纹必须相等。
  尺子2｜位点确实来自中心库 —— B 的 capture 起始位点 == A 崩溃前中心库里的位点（而非源库当前位点）。
  尺子3｜对照组必须复现丢数据 —— 关掉 `checkpoint.central.enabled` 跑同一场景，**必须**丢行；
                                 复现不出来就说明用例根本没测到这条路径，判为失败。
  尺子4｜上卷活性 —— 任务跑着的时候中心库里的位点在推进（不是写一次就不动了）。
  尺子5｜位点重置（PITR）—— 暂停任务、把位点重置到更早的采样点、重启，capture 必须从那个点续。
  尺子6｜保留期在线巡检 —— 运行中产出 retention_metric，且状态可读。

用法：
    python3 test_scripts/fault_injection/checkpoint_durability.py                 # 全部
    python3 test_scripts/fault_injection/checkpoint_durability.py --skip-control  # 跳过对照组（省一轮）
    python3 test_scripts/fault_injection/checkpoint_durability.py --only reset    # 只跑重置用例

前置：./start.sh 已起后端与 agent-A；docker 里 synctask-mysql 在跑。
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
SRC_DB = "ckpt_src"
TGT_DB = "ckpt_tgt"
TABLE = "ckpt_load"

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AGENT_B_DIR = os.path.join(PROJECT_DIR, "files", ".agent_b_workdir")
AGENT_B_PORT = 8084
AGENT_B_ID = "agent-b-ckpt-test"

# 子进程 jar 是按 agent 的**工作目录**相对解析的（agent.properties 的 jar.*.path），
# 所以 B 的工作目录要把这些模块目录链过去；files/ 则必须是空的——那才是"另一台机器"。
LINKED_MODULES = ["migration-capture", "migration-extract", "migration-increment",
                  "migration-full", "migration-mongo", "migration-elastic",
                  "migration-redis", "migration-subscribe"]

DDL = f"""
CREATE TABLE `{TABLE}` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `grp` INT NOT NULL,
  `payload` VARCHAR(256),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

FINGERPRINT = f"SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS('|', id, grp, IFNULL(payload,'')))) FROM `{TABLE}`"


# ------------------------------------------------------------------ 中心位点表

def central_rows(task_id):
    return F.sql_fetch(F.META_DB, "sync_task_db",
                       "SELECT stage, payload, monotonic_key, agent_id, lease_epoch "
                       f"FROM task_checkpoints WHERE task_id='{task_id}' ORDER BY stage")


def central_capture_payload(task_id):
    for stage, payload, _key, _agent, _epoch in central_rows(task_id):
        if stage == "CAPTURE":
            return payload
    return None


def central_capture_key(task_id):
    for stage, _payload, key, _agent, _epoch in central_rows(task_id):
        if stage == "CAPTURE":
            return key
    return None


def parse_payload(text):
    out = {}
    for line in (text or "").splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def history_rows(task_id, reason=None):
    where = f"task_id='{task_id}'" + (f" AND reason='{reason}'" if reason else "")
    return F.sql_fetch(F.META_DB, "sync_task_db",
                       f"SELECT id, stage, payload, reason FROM task_checkpoint_history "
                       f"WHERE {where} ORDER BY id")


# ------------------------------------------------------------------ agent-B

def secret(name):
    path = os.path.join(PROJECT_DIR, name)
    with open(path) as f:
        return f.read().strip()


def prepare_agent_b(central_enabled=True):
    """搭一个"另一台机器"：独立工作目录 + 空的 files/ + 链过去的模块 jar。"""
    if os.path.isdir(AGENT_B_DIR):
        shutil.rmtree(AGENT_B_DIR)
    os.makedirs(os.path.join(AGENT_B_DIR, "files"))
    for mod in LINKED_MODULES:
        src = os.path.join(PROJECT_DIR, mod)
        if os.path.isdir(src):
            os.symlink(src, os.path.join(AGENT_B_DIR, mod))

    # 外部 agent.properties 一旦存在就完全取代 classpath 里那份（AgentConfig.loadFromFile 会提前 return），
    # 但 loadDefaults 已经先跑过，所以这里只写需要覆盖的键即可。
    with open(os.path.join(AGENT_B_DIR, "agent.properties"), "w") as f:
        f.write(f"http.server.port={AGENT_B_PORT}\n")
        f.write(f"checkpoint.central.enabled={'true' if central_enabled else 'false'}\n")
        # 接管场景就是要它快，别让测试等 5 分钟
        f.write("checkpoint.central.upload.interval.ms=2000\n")
        f.write("checkpoint.history.sample.interval.s=10\n")


def start_agent_b():
    java_home = subprocess.run(["/usr/libexec/java_home", "-v", "21"],
                               capture_output=True, text=True).stdout.strip()
    java = os.path.join(java_home, "bin", "java") if java_home else "java"
    env = os.environ.copy()
    env.update({
        "SYNCTASK_MASTER_KEY": secret(".synctask_master_key"),
        "JWT_SECRET": secret(".synctask_jwt_secret"),
        "AGENT_API_TOKEN": secret(".synctask_agent_token"),
        "MIGRATION_AGENT_ID": AGENT_B_ID,
    })
    log = open(os.path.join(AGENT_B_DIR, "agent-b.out"), "w")
    proc = subprocess.Popen(
        [java, "-Dh2.bindAddress=127.0.0.1", "-jar",
         os.path.join(PROJECT_DIR, "migration-agent/target/migration-agent-1.0.0.jar")],
        cwd=AGENT_B_DIR, stdout=log, stderr=subprocess.STDOUT, env=env,
        preexec_fn=os.setsid)
    print(f"    agent-B 已启动 (pid={proc.pid}, port={AGENT_B_PORT}, cwd={AGENT_B_DIR})")
    return proc


def stop_agent_b(proc):
    if proc is None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        pass
    # B 拉起来的子进程按 task.id 匹配，随进程组一起收掉；漏网的再补一刀
    subprocess.run(["pkill", "-f", AGENT_B_DIR], capture_output=True)


def agent_b_alive_in_registry():
    rows = F.sql_fetch(F.META_DB, "sync_task_db",
                       f"SELECT status FROM agents WHERE agent_id='{AGENT_B_ID}'")
    return bool(rows) and rows[0][0] == "ONLINE"


def wait_agent_b_registered(timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if agent_b_alive_in_registry():
            return True
        time.sleep(2)
    return False


def kill_agent_a():
    """SIGKILL 掉 A（含它拉起的子进程），模拟整机失联。"""
    out = subprocess.run(["pgrep", "-f", "migration-agent/target/migration-agent-1.0.0.jar"],
                         capture_output=True, text=True).stdout.split()
    killed = []
    for pid in out:
        cmd = subprocess.run(["ps", "-o", "command=", "-p", pid],
                             capture_output=True, text=True).stdout
        if AGENT_B_DIR in cmd:
            continue  # 别误杀 B
        try:
            os.kill(int(pid), signal.SIGKILL)
            killed.append(int(pid))
        except ProcessLookupError:
            pass
    return killed


def task_owner(task_id):
    rows = F.sql_fetch(F.META_DB, "sync_task_db",
                       f"SELECT agent_id, lease_epoch FROM workflows WHERE id='{task_id}'")
    return rows[0] if rows else (None, None)


def wait_takeover(task_id, timeout=240):
    """等后端把任务改派给 B（租约 90s 过期 + 巡检周期）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        owner, epoch = task_owner(task_id)
        if owner == AGENT_B_ID:
            print(f"    任务已改派给 {owner}（lease_epoch={epoch}）")
            return True
        time.sleep(5)
    return False


# ------------------------------------------------------------------ 负载

def seed(rows):
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}", f"CREATE DATABASE {TGT_DB}"])
    F.sql_exec(CFG, [DDL], db=SRC_DB)
    batch = []
    for i in range(rows):
        batch.append(f"INSERT INTO `{TABLE}`(grp,payload) VALUES({i % 7},'seed-{i}')")
        if len(batch) >= 200:
            F.sql_exec(CFG, batch, db=SRC_DB)
            batch = []
    if batch:
        F.sql_exec(CFG, batch, db=SRC_DB)


def write_rows(n, tag):
    """持续写入，返回写入条数。跨机接管窗口里的写入正是要验证的那部分。"""
    batch = [f"INSERT INTO `{TABLE}`(grp,payload) VALUES({i % 7},'{tag}-{i}')" for i in range(n)]
    F.sql_exec(CFG, batch, db=SRC_DB)
    return n


def fingerprints():
    src = F.sql_fetch(CFG, SRC_DB, FINGERPRINT)[0]
    tgt = F.sql_fetch(CFG, TGT_DB, FINGERPRINT)[0]
    return src, tgt


def wait_converge(timeout=180):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        src, tgt = fingerprints()
        if src == tgt:
            return True, src, tgt
        last = (src, tgt)
        time.sleep(5)
    src, tgt = fingerprints()
    return False, src, tgt


# ------------------------------------------------------------------ 用例

def run_takeover(token, passed, failed, central_enabled):
    """跨机接管主用例。central_enabled=False 时是对照组：**必须**丢数据。"""
    label = "跨机接管" if central_enabled else "对照组（关掉中心位点）"
    print(f"\n=== {label} ===")
    agent_b = None
    task_id = None
    try:
        seed(600)
        task_id = F.create_task(token, f"ckpt-durability-{int(time.time())}", "mysql", "mysql",
                                CONN, CONN, "fullAndIncre",
                                json.dumps({SRC_DB: [TABLE]}), TGT_DB, source_db=SRC_DB)
        print(f"    taskId={task_id}")
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=360)
        if st != "INCREMENT_RUNNING":
            failed.append(f"{label}: 任务未进入增量（{st}）")
            return
        write_rows(200, "before")
        time.sleep(8)   # 让 capture 推进并上卷

        central_before = central_capture_payload(task_id)
        key_before = central_capture_key(task_id)
        if central_enabled:
            if not central_before:
                failed.append("尺子4 上卷活性: 任务跑着但中心库里没有 CAPTURE 位点")
                return
            passed.append("尺子4 上卷活性: 中心库已有 CAPTURE 位点")
            time.sleep(6)
            if central_capture_key(task_id) < key_before:
                failed.append("尺子4 上卷活性: 中心位点回退了")

        # ---- 起 B，杀 A ----
        prepare_agent_b(central_enabled=central_enabled)
        agent_b = start_agent_b()
        if not wait_agent_b_registered():
            failed.append(f"{label}: agent-B 未能注册到元数据库")
            return

        killed = kill_agent_a()
        print(f"    已 SIGKILL agent-A 及其子进程: {killed}")
        # 崩溃窗口内持续写入：这批数据正是"丢没丢"的判据
        gap_rows = write_rows(400, "gap")
        print(f"    崩溃窗口内写入 {gap_rows} 行")

        if not wait_takeover(task_id):
            failed.append(f"{label}: 任务未被改派给 agent-B（租约未过期或巡检未生效）")
            return

        write_rows(100, "after")
        ok, src, tgt = wait_converge(timeout=240)

        if central_enabled:
            if ok:
                passed.append(f"尺子1 跨机接管不丢数据: 两端指纹一致 {src}")
            else:
                failed.append(f"尺子1 跨机接管不丢数据: 源 {src} != 目标 {tgt}")

            # 尺子2：B 的起始位点必须来自中心库，而不是"源库当前位点"
            hydrated = agent_b_hydrated_from_central()
            if hydrated:
                passed.append("尺子2 位点来自中心库: agent-B 日志出现回灌记录")
            else:
                failed.append("尺子2 位点来自中心库: agent-B 未出现回灌日志（可能又取了源库当前位点）")
        else:
            # 对照组：关掉中心位点后，B 只能从"源库当前位点"开始，崩溃窗口那 400 行必然丢。
            if ok:
                failed.append("尺子3 对照组: 关掉中心位点竟然也没丢数据——用例没测到这条路径，判据失效")
            else:
                passed.append(f"尺子3 对照组: 如期复现丢数据（源 {src} != 目标 {tgt}）")
    finally:
        stop_agent_b(agent_b)
        if task_id:
            F.stop_task(token, task_id)
            F.delete_task(token, task_id)


def agent_b_hydrated_from_central():
    log = os.path.join(AGENT_B_DIR, "agent-b.out")
    if not os.path.isfile(log):
        return False
    with open(log, errors="ignore") as f:
        text = f.read()
    return "开始回灌" in text or "已回灌位点" in text


def run_reset(token, passed, failed):
    """位点重置（PITR）：唯一允许位点倒退的入口。不需要第二个 agent。"""
    print("\n=== 位点重置（PITR） ===")
    task_id = None
    try:
        seed(400)
        task_id = F.create_task(token, f"ckpt-reset-{int(time.time())}", "mysql", "mysql",
                                CONN, CONN, "fullAndIncre",
                                json.dumps({SRC_DB: [TABLE]}), TGT_DB, source_db=SRC_DB)
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=360)
        if st != "INCREMENT_RUNNING":
            failed.append(f"重置: 任务未进入增量（{st}）")
            return

        # 攒出采样点（agent-A 默认 300s 采样一次，所以这里可能只有首次上卷那一条；
        # 要更密的采样把 agent.properties 的 checkpoint.history.sample.interval.s 调小）
        write_rows(100, "r1")
        time.sleep(8)
        write_rows(100, "r2")
        time.sleep(8)

        rows = history_rows(task_id)
        if not rows:
            failed.append("尺子5 位点重置: 中心库里没有任何位点历史采样（采样间隔太长？）")
            return
        passed.append(f"尺子5 位点重置: 位点历史已产出 {len(rows)} 条")

        F.api("POST", f"/api/workflows/{task_id}/stop", token)
        F.wait_status(token, task_id, {"PAUSED", "STOPPED", "FAILED"}, timeout=120)

        target_id = rows[0][0]
        r = F.api("POST", f"/api/workflows/{task_id}/checkpoint/reset", token,
                  json={"stage": "CAPTURE", "target": {"type": "HISTORY_ID", "value": target_id}})
        if r.get("success") is False:
            failed.append(f"尺子5 位点重置: 接口返回失败 {r}")
            return

        audit = history_rows(task_id, reason="RESET")
        if audit:
            passed.append("尺子5 位点重置: 已落 RESET 审计记录")
        else:
            failed.append("尺子5 位点重置: 没有 RESET 审计记录")

        reset_at = F.sql_fetch(F.META_DB, "sync_task_db",
                               f"SELECT reset_at FROM task_checkpoints "
                               f"WHERE task_id='{task_id}' AND stage='CAPTURE'")
        if reset_at and reset_at[0][0] is not None:
            passed.append("尺子5 位点重置: reset_at 已打标（agent 下次启动会强制覆盖本地位点）")
        else:
            failed.append("尺子5 位点重置: reset_at 未打标，agent 会走同机重启分支、重置不生效")
    finally:
        if task_id:
            F.stop_task(token, task_id)
            F.delete_task(token, task_id)


def run_retention(token, passed, failed):
    """保留期在线巡检：运行中就该产出 retention_metric。"""
    print("\n=== 保留期在线巡检 ===")
    task_id = None
    try:
        seed(200)
        task_id = F.create_task(token, f"ckpt-retention-{int(time.time())}", "mysql", "mysql",
                                CONN, CONN, "fullAndIncre",
                                json.dumps({SRC_DB: [TABLE]}), TGT_DB, source_db=SRC_DB)
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=360)
        if st != "INCREMENT_RUNNING":
            failed.append(f"保留期巡检: 任务未进入增量（{st}）")
            return
        write_rows(50, "ret")

        path = os.path.join(PROJECT_DIR, "files", task_id, "binlog_output", "retention_metric")
        deadline = time.time() + 120
        content = None
        while time.time() < deadline:
            if os.path.isfile(path):
                with open(path) as f:
                    content = f.read().strip()
                if content:
                    break
            time.sleep(5)

        if not content:
            failed.append("尺子6 保留期巡检: 运行中未产出 retention_metric")
            return
        parts = content.split("|")
        if len(parts) >= 3 and parts[1] in ("OK", "WARN", "LOST", "UNKNOWN"):
            passed.append(f"尺子6 保留期巡检: 状态={parts[1]} 余量={parts[2]}")
        else:
            failed.append(f"尺子6 保留期巡检: 结果格式异常 {content}")
    finally:
        if task_id:
            F.stop_task(token, task_id)
            F.delete_task(token, task_id)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-control", action="store_true", help="跳过对照组（省一轮完整场景）")
    ap.add_argument("--only", choices=["takeover", "control", "reset", "retention"],
                    help="只跑其中一个用例")
    args = ap.parse_args()

    token = F.login()
    passed, failed = [], []

    try:
        if args.only in (None, "takeover"):
            run_takeover(token, passed, failed, central_enabled=True)
        if args.only == "control" or (args.only is None and not args.skip_control):
            run_takeover(token, passed, failed, central_enabled=False)
        if args.only in (None, "reset"):
            run_reset(token, passed, failed)
        if args.only in (None, "retention"):
            run_retention(token, passed, failed)
    finally:
        if os.path.isdir(AGENT_B_DIR):
            shutil.rmtree(AGENT_B_DIR, ignore_errors=True)

    print("\n提示：本脚本会 SIGKILL agent-A，跑完请重新 ./start.sh 或 ./restart_agent.sh")
    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
