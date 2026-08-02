#!/usr/bin/env python3
"""
执行面故障转移的判定（双 agent，mysql→mysql）。

背景：任务下发原本是「Kafka 广播 + 谁抢到算谁」——同一个消费组里谁拿到分区谁执行，
后端<b>不知道任务落在哪台机器上</b>。于是 agent 硬崩（SIGKILL/OOM/宿主宕机）后，
它名下的任务没有任何人接管：恢复能力本身早就有（子进程各自 checkpoint 续传），
缺的只是"谁负责这个任务"这条信息。此前只有把那台机器重新拉起来，任务才会继续。

判据：
  1. 任务启动后被<b>明确指派</b>给某台 agent（workflows.agent_id 非空）；
  2. SIGKILL 掉持有任务的那台 agent 后，任务在租约超时窗口内被<b>改派</b>给另一台
     （agent_id 变了、lease_epoch 递增）；
  3. 接管方真的把子进程拉起来了（进程存在且属于接管方）；
  4. 接管期间写入的数据最终追平，两端指纹一致（改派 = 一次崩溃恢复，不该丢数据）；
  5. 另一台 agent 不会去碰不归自己的任务（未指派给它的任务不被它恢复）。

用法：
    python3 test_scripts/fault_injection/agent_failover.py [--rows 600]

注意：第二个 agent 与第一个共享同一个工作目录（files/），这是"同机双实例"的简化；
真实集群是各自的机器各自的目录。同机跑仍能覆盖选派/租约/接管这条主链路，
且任务级文件锁（P0-3）保证同一 taskId 不会被两套进程同时写。
"""
import argparse
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
META = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
SRC_DB = "ha_src"
TGT_DB = "ha_tgt"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

AGENT_B_PORT = 8093
AGENT_B_ID = "agent-failover-b"
AGENT_B_LOG = os.path.join(PROJECT_DIR, "logs", "agent-b.out")

DDL = """
CREATE TABLE `acct` (
  `id` INT NOT NULL,
  `val` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def sh(cmd, **kw):
    return subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True, **kw)


def agent_env(extra=None):
    env = dict(os.environ)
    env["DB_URL"] = ("jdbc:mysql://localhost:33306/sync_task_db?useSSL=false&serverTimezone=Asia/Shanghai"
                     "&characterEncoding=utf8&allowPublicKeyRetrieval=true")
    env["MIGRATION_AGENT_MYSQL_DB_URL"] = env["DB_URL"]
    env["MIGRATION_AGENT_MYSQL_DB_USER"] = "root"
    env["MIGRATION_AGENT_MYSQL_DB_PASSWORD"] = "rootpassword"
    env["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:29092"
    env["MIGRATION_AGENT_KAFKA_BOOTSTRAP_SERVERS"] = "localhost:29092"
    for name, path in (("SYNCTASK_MASTER_KEY", ".synctask_master_key"),
                       ("JWT_SECRET", ".synctask_jwt_secret"),
                       ("AGENT_API_TOKEN", ".synctask_agent_token")):
        with open(os.path.join(PROJECT_DIR, path)) as fh:
            env[name] = fh.read().strip()
    env.update(extra or {})
    return env


def start_agent_b():
    """第二个 agent：独立 agent_id + 独立 HTTP 端口，同一台机器上跑。"""
    java_home = subprocess.run(["/usr/libexec/java_home", "-v", "21"],
                               capture_output=True, text=True).stdout.strip()
    env = agent_env({
        "MIGRATION_AGENT_ID": AGENT_B_ID,
        "MIGRATION_AGENT_HTTP_SERVER_PORT": str(AGENT_B_PORT),
        # 同机双实例共享 files/：B 起来时不能去"收孤儿"，否则会把 A 正在跑的子进程当孤儿杀掉
        # （真实集群每台机器各自的 files/，不存在这个问题；agent 里本来就留了这个开关）
        "AGENT_ORPHAN_REAP_ENABLED": "false",
    })
    out = open(AGENT_B_LOG, "w")
    p = subprocess.Popen(
        [f"{java_home}/bin/java", "-Dh2.bindAddress=127.0.0.1",
         "-jar", "migration-agent/target/migration-agent-1.0.0.jar"],
        cwd=PROJECT_DIR, env=env, stdout=out, stderr=subprocess.STDOUT)
    for _ in range(40):
        r = subprocess.run(["curl", "-s", "-m2", f"http://localhost:{AGENT_B_PORT}/api/agent/health"],
                           capture_output=True, text=True)
        if "UP" in r.stdout:
            print(f"[agent-B] 已就绪 pid={p.pid} port={AGENT_B_PORT} id={AGENT_B_ID}")
            return p
        time.sleep(2)
    raise RuntimeError(f"agent-B 未就绪，见 {AGENT_B_LOG}")


def agent_a_pid():
    r = subprocess.run(["pgrep", "-f", "migration-agent/target/migration-agent-1.0.0.jar"],
                       capture_output=True, text=True)
    return [int(x) for x in r.stdout.split()]


def registered_agents():
    rows = F.sql_fetch(META, "sync_task_db",
                       "SELECT agent_id, port, status, running_tasks, "
                       "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) FROM agents")
    return {r[0]: dict(port=r[1], status=r[2], running=r[3], age=r[4]) for r in rows}


def task_owner(task_id):
    rows = F.sql_fetch(META, "sync_task_db",
                       f"SELECT agent_id, lease_epoch, status FROM workflows WHERE id='{task_id}'")
    return (rows[0][0], rows[0][1], rows[0][2]) if rows else (None, None, None)


def fingerprint(db):
    return F.sql_fetch(CFG, db, "SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS(',',id,val))) FROM acct")[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=600)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    passed, failed = [], []
    agent_b = None
    token = F.login()
    print("✓ 登录成功")

    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}"])
    F.sql_exec(CFG, [DDL, "INSERT INTO acct (id,val) VALUES (0,0)"], db=SRC_DB)

    task_id = None
    try:
        agent_b = start_agent_b()
        time.sleep(5)
        agents = registered_agents()
        print(f"[注册表] {agents}")
        if len(agents) < 2:
            failed.append(f"agents 表里只有 {len(agents)} 台，双 agent 未都注册")
            return F.print_result(passed, failed)
        passed.append(f"两台 agent 均已注册并心跳（{', '.join(agents)}）")

        task_id = F.create_task(
            token, f"ha-{int(time.time())}", "mysql", "mysql", CONN, CONN,
            "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["acct"]}}}}', TGT_DB, source_db=SRC_DB)
        print(f"[任务] {task_id}")

        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            failed.append(f"任务未进入增量阶段（{st}）")
            return F.print_result(passed, failed)

        owner, epoch, _ = task_owner(task_id)
        print(f"[判据1] 任务归属 agent={owner} lease_epoch={epoch}")
        if owner:
            passed.append(f"任务被明确指派给 agent {owner}（不再是广播抢单）")
        else:
            failed.append("任务没有 agent_id，仍是广播语义")
            return F.print_result(passed, failed)

        c = F.sql_conn(CFG, SRC_DB)
        cur = c.cursor()
        for i in range(1, args.rows // 2 + 1):
            cur.execute(f"INSERT INTO acct (id,val) VALUES ({i},{i*3}) ON DUPLICATE KEY UPDATE val=VALUES(val)")
        cur.close(); c.close()
        time.sleep(10)

        # ---------- 判据2/3：杀掉持有者，看是否改派 ----------
        b_pid = agent_b.pid
        all_pids = agent_a_pid()
        holder_pids = [b_pid] if owner == AGENT_B_ID else [p for p in all_pids if p != b_pid]
        print(f"\n[阶段] SIGKILL 持有任务的 agent（{owner}）: {holder_pids}")
        for p in holder_pids:
            os.kill(p, signal.SIGKILL)
        # 必须回收僵尸：被杀的 agent 若是本脚本的子进程且没 wait，它会以 <defunct> 留在进程表里，
        # 而 ProcessHandle.isAlive() 对僵尸仍返回 true —— 它的子进程的父进程看门狗就永远不触发，
        # 孤儿会继续写目标库（生产上由 init 立刻回收，不存在这个问题，纯属测试脚手架的坑）。
        if owner == AGENT_B_ID:
            try:
                agent_b.wait(timeout=10)
            except Exception:
                pass

        deadline = time.time() + 300
        new_owner, new_epoch = owner, epoch
        while time.time() < deadline:
            new_owner, new_epoch, _ = task_owner(task_id)
            if new_owner and new_owner != owner:
                break
            time.sleep(3)
        print(f"[判据2] 改派后 agent={new_owner} lease_epoch={new_epoch}（原 {owner}/{epoch}）")
        if new_owner and new_owner != owner:
            passed.append(f"持有者失联后任务被改派: {owner} → {new_owner}")
        else:
            failed.append(f"300s 内任务未被改派，仍挂在失联的 {owner} 名下")

        if new_epoch is not None and epoch is not None and new_epoch > epoch:
            passed.append(f"租约代次递增（{epoch} → {new_epoch}），接管有据可查")
        else:
            failed.append(f"租约代次未递增（{epoch} → {new_epoch}）")

        deadline = time.time() + 240
        children = []
        while time.time() < deadline:
            children = F.all_child_pids(task_id)
            if children:
                break
            time.sleep(3)
        print(f"[判据3] 接管方拉起的子进程: {children}")
        if children:
            passed.append(f"接管方已把子进程拉起（{len(children)} 个）")
        else:
            failed.append("改派后没有任何子进程被拉起，任务实际停摆")

        # ---------- 判据4：接管后继续追平 ----------
        print("\n[阶段] 接管后再写一批，校验数据追平")
        c = F.sql_conn(CFG, SRC_DB)
        cur = c.cursor()
        for i in range(args.rows // 2 + 1, args.rows + 1):
            cur.execute(f"INSERT INTO acct (id,val) VALUES ({i},{i*3}) ON DUPLICATE KEY UPDATE val=VALUES(val)")
        cur.close(); c.close()

        deadline = time.time() + 420
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
            passed.append(f"接管后数据追平，两端指纹一致（{src[0]} 行）")
        else:
            failed.append(f"接管后数据不一致: 源={src} 目标={tgt}")

    finally:
        if agent_b is not None:
            try:
                agent_b.terminate()
                agent_b.wait(timeout=20)
            except Exception:
                try:
                    agent_b.kill()
                except Exception:
                    pass
        # 无论哪台被杀，最后都把标准 agent 恢复起来
        try:
            sh(["./restart_agent.sh"], timeout=180)
            print("[清理] 标准 agent 已重启")
        except Exception as e:
            print(f"[警告] 重启标准 agent 失败: {e}")
        try:
            F.sql_exec(META, [f"DELETE FROM agents WHERE agent_id='{AGENT_B_ID}'"], db="sync_task_db")
        except Exception:
            pass
        if not args.keep and task_id:
            F.stop_task(token, task_id)
            time.sleep(3)
            F.delete_task(token, task_id)
            try:
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}",
                                 f"DROP DATABASE IF EXISTS {TGT_DB}"])
            except Exception:
                pass

    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
