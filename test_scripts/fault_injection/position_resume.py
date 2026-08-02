#!/usr/bin/env python3
"""
第 1 批（P0-2 位点持久化 + P0-3 单实例互斥）的判据脚本。

现有的 sql_resume / sub_resume / dr_resume 只判「不丢 + 可收敛」，因此下面两类缺陷在 CI 里
完全隐形——数据最终确实一致，只是代价高得离谱：

  * **P0-2 重放放大**：capture 每次崩溃重启都从「任务启动时写死在 config.properties 里的位点」
    重新拉，重放量与任务已运行时长成正比。实测一个跑了 10 分钟的任务，一次重启重放出 18 万条
    THL / 83MB；订阅链路下游收到的消息量是真实写入的 1.67 倍。
  * **P0-3 孤儿双写**：agent 硬崩后子进程不会随之退出，新 agent 恢复任务时再起一套，
    同一批变更被应用两遍（非幂等语句直接算错）。

本脚本给这两条各加一把确定性的尺子：

  尺子1｜重放量  —— 崩溃重启后**停写静置**，看新 capture 吐出多少行变更事件。位点正确续传时
                    只该有心跳（≈0 条）；从任务起始位点重放则会把历史行变更原样再吐一遍。
  尺子2｜续传位点 —— capture 重启后的起始位点必须等于崩溃前落盘的位点，
                    而不是 config.properties 里的任务起始位点。
  尺子3｜孤儿自杀 —— SIGKILL 掉 agent，子进程应在看门狗周期内自行退出（不留孤儿）。
  尺子4｜互斥兜底 —— 手工再起一个同 taskId 的 capture，必须被任务实例锁挡住并退出。
  尺子5｜最终一致 —— 上述折腾之后源/目标仍然逐指纹相等（不能为了少重放而丢数据）。

用法：
    python3 test_scripts/fault_injection/position_resume.py [--rows 4000] [--converge 600]
    python3 test_scripts/fault_injection/position_resume.py --skip-agent-kill   # 不动 agent
"""
import argparse
import glob
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
SRC_DB = "pos_src"
TGT_DB = "pos_tgt"
TABLE = "pos_load"

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DDL = f"""
CREATE TABLE `{TABLE}` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `grp` INT NOT NULL,
  `payload` VARCHAR(256),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

FINGERPRINT = f"SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS('|', id, grp, IFNULL(payload,'')))) FROM `{TABLE}`"


# ------------------------------------------------------------------ 观测

DATA_EVENT_PREFIXES = (b"WRITE_ROWS", b"EXT_WRITE_ROWS", b"UPDATE_ROWS", b"EXT_UPDATE_ROWS",
                       b"DELETE_ROWS", b"EXT_DELETE_ROWS")


def cap_files(task_id):
    return sorted(glob.glob(f"{PROJECT_DIR}/files/{task_id}/binlog_output/*.cap"))


def cap_event_count(task_id, only=None):
    """.cap 文件里的事件总条数（一行一个事件，见 MySQLBinlogCapture 的 RECORD_SEP='\\n'）。"""
    total = 0
    for path in (only if only is not None else cap_files(task_id)):
        try:
            with open(path, "rb") as f:
                total += f.read().count(b"\n")
        except OSError:
            pass
    return total


def cap_data_event_count(paths):
    """行变更事件条数（排除心跳/事务边界/TABLE_MAP 等）。字段分隔符是 \\001，事件类型在第一列。"""
    n = 0
    for path in paths:
        try:
            with open(path, "rb") as f:
                for line in f:
                    if line.split(b"\001", 1)[0] in DATA_EVENT_PREFIXES:
                        n += 1
        except OSError:
            pass
    return n


def persisted_position(task_id):
    """读 capture 落盘的位点，返回 (file, pos, gtid)。"""
    path = f"{PROJECT_DIR}/files/{task_id}/binlog_output/capture_position.properties"
    vals = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip()
    except OSError:
        return None
    if "binlog.file" not in vals:
        return None
    return (vals["binlog.file"], int(vals.get("binlog.position", "0")), vals.get("gtid.set", ""))


def config_start_position(task_id):
    path = f"{PROJECT_DIR}/files/{task_id}/config.properties"
    vals = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip()
    except OSError:
        return None
    if "capture.binlog.file" not in vals:
        return None
    return (vals["capture.binlog.file"], int(vals.get("capture.binlog.position", "0")),
            vals.get("capture.gtid.set", ""))


def capture_log_lines(task_id):
    out = []
    for path in glob.glob(f"{PROJECT_DIR}/files/{task_id}/logs/*capture*.log") \
            + glob.glob(f"{PROJECT_DIR}/logs/agent.out"):
        try:
            with open(path, errors="replace") as f:
                out.extend(f.readlines())
        except OSError:
            pass
    return out


FILEPOS_MARK = "从binlog位点开始捕获:"
GTID_MARK = "按 GTID 集开始捕获（自动定位）:"


def capture_starts(task_id):
    """capture 每次启动打印的起始位点，按时间顺序。元素为 ('filepos', file, pos) 或 ('gtid', set)。"""
    out = []
    for line in capture_log_lines(task_id):
        if FILEPOS_MARK in line:
            frag = line.split(FILEPOS_MARK)[1].strip().split()[0]
            if ":" in frag:
                f_, p_ = frag.rsplit(":", 1)
                try:
                    out.append(("filepos", f_, int(p_)))
                except ValueError:
                    pass
        elif GTID_MARK in line:
            out.append(("gtid", line.split(GTID_MARK)[1].strip().split()[0]))
    return out


def count_capture_starts(task_id):
    return len(capture_starts(task_id))


# ------------------------------------------------------------------ 负载

def write_rows(n, batch=200):
    c = F.sql_conn(CFG, SRC_DB)
    cur = c.cursor()
    done = 0
    while done < n:
        k = min(batch, n - done)
        cur.executemany(
            f"INSERT INTO `{TABLE}` (grp, payload) VALUES (%s, %s)",
            [(i % 8, f"p-{done + i}-{'x' * 32}") for i in range(k)])
        done += k
        time.sleep(0.02)
    cur.close()
    c.close()
    return n


def wait_target_rows(expect, timeout):
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        try:
            last = F.sql_fetch(CFG, TGT_DB, f"SELECT COUNT(*) FROM `{TABLE}`")[0][0]
            if last >= expect:
                return last
        except Exception:
            pass
        time.sleep(2)
    return last


def agent_pid():
    out = subprocess.run(
        ["pgrep", "-f", "migration-agent/target/migration-agent-1.0.0.jar"],
        capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ------------------------------------------------------------------ 主流程

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=4000, help="崩溃前后各写入的行数")
    ap.add_argument("--converge", type=int, default=600)
    ap.add_argument("--skip-agent-kill", action="store_true", help="跳过 SIGKILL agent 的用例")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    token = F.login()
    print("✓ 登录成功")

    # 默认的增量行速率配额只有 50 行/秒，本用例要在有限时间内搬完上万行，必须先抬高
    # （与 dr_resume/dr_hang 同一套做法），结束后恢复原值。
    old_quota = F.get_increment_quota()
    if old_quota is not None:
        F.set_increment_quota(100000)
        print(f"[准备] 增量速率配额 {old_quota} → 100000 行/秒（结束后恢复）")

    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}"])
    F.sql_exec(CFG, [DDL], db=SRC_DB)
    write_rows(500)
    print(f"[准备] 源库 {SRC_DB}.{TABLE} 播种 500 行存量")

    task_id = F.create_task(
        token, f"pos-resume-{int(time.time())}", "mysql", "mysql", CONN, CONN,
        "fullAndIncre", f'{{"{SRC_DB}": {{"tables": ["{TABLE}"]}}}}', TGT_DB, source_db=SRC_DB)
    print(f"[任务] {task_id}")

    passed, failed = [], []
    try:
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
        if st != "INCREMENT_RUNNING":
            print(f"✗ 任务未进入增量阶段: {st}")
            return 1
        wait_target_rows(500, 180)

        # ---------- 第一段写入：制造"历史"，让重放放大有可观测的差别 ----------
        write_rows(args.rows)
        total_written = 500 + args.rows
        wait_target_rows(total_written, args.converge)
        # 等 capture 把位点落盘（每 1000 事件 / 每若干秒一次）
        time.sleep(8)

        pos_before = persisted_position(task_id)
        cfg_start = config_start_position(task_id)
        old_caps = cap_files(task_id)
        cap_before = cap_event_count(task_id, old_caps)
        data_before = cap_data_event_count(old_caps)
        starts_before = count_capture_starts(task_id)
        print(f"[基线] 已落盘位点={pos_before} config起始位点={cfg_start}")
        print(f"[基线] .cap 事件数={cap_before}（其中行变更事件 {data_before} 条，"
              f"对应 {500 + args.rows} 行写入）")
        if pos_before is None:
            failed.append("capture 未落盘位点文件，后续判据无法进行")
            raise SystemExit(1)

        # ---------- 崩溃 capture ----------
        killed = F.signal_child(task_id, "capture", signal.SIGKILL)
        print(f"[注入] SIGKILL capture pid={killed}")
        # 等 ProcessGuard 拉起新 capture（waitForStartup 最长 30s，留足余量）
        deadline = time.time() + 150
        while time.time() < deadline and count_capture_starts(task_id) <= starts_before:
            time.sleep(3)

        starts = capture_starts(task_id)
        restart_pos = starts[-1] if len(starts) > starts_before else None
        print(f"[观测] capture 重启后的起始位点={restart_pos}")

        # 尺子2：续传位点来自"已落盘位点"，而不是 config 里的任务起始位点。
        # 注意不能断言与采样值严格相等——capture 每隔几秒就会重写位点文件，
        # 从"读文件"到"SIGKILL"之间它完全可能又前进了一点。判据是"≥ 采样值且 ≠ 任务起始位点"。
        if restart_pos is None:
            failed.append("未在日志中找到 capture 重启后的起始位点，无法判定续传")
        elif restart_pos[0] == "gtid":
            # GTID 自动定位：起始集必须非空且不等于 config 里的任务起始集
            if restart_pos[1] and restart_pos[1] != (cfg_start[2] if cfg_start else None):
                passed.append("capture 从已落盘 GTID 集续传")
            else:
                failed.append(f"capture 的起始 GTID 集 {restart_pos[1]} 仍是任务起始集，P0-2 未生效")
        elif cfg_start and restart_pos[1] == cfg_start[0] and restart_pos[2] == cfg_start[1]:
            failed.append(f"capture 仍从任务起始位点重放（{restart_pos[1]}:{restart_pos[2]}），P0-2 未生效")
        elif restart_pos[1] == pos_before[0] and restart_pos[2] >= pos_before[1]:
            passed.append(f"capture 从已落盘位点续传（{restart_pos[1]}:{restart_pos[2]} "
                          f"≥ 采样时的落盘位点 {pos_before[1]}，任务起始位点是 {cfg_start[1] if cfg_start else '?'}）")
        else:
            failed.append(f"capture 起始位点异常：{restart_pos}，采样落盘位点 {pos_before[:2]}，"
                          f"配置起始位点 {cfg_start}")

        # ---------- 尺子1：静默窗口里的重放量 ----------
        # 重启后**一个字都不往源库写**，静置观察新 capture 到底吐出多少行变更事件。
        # 位点正确续传 → 只有心跳，行变更事件 ≈ 0（最多重放落盘位点后的那一两条）。
        # 仍从任务起始位点重放 → 会把前面 data_before 条行变更事件原样再吐一遍。
        print("[静默] 停写 30s，观察重启后的 capture 重放了多少行变更 ...")
        time.sleep(30)
        new_caps = [p for p in cap_files(task_id) if p not in old_caps]
        replayed = cap_data_event_count(new_caps)
        print(f"[观测] 重启后新 .cap 文件 {len(new_caps)} 个，静默窗口内行变更事件={replayed} 条"
              f"（若从任务起始位点重放，应≈{data_before} 条）")
        tolerance = max(2, int(data_before * 0.1))
        if replayed <= tolerance:
            passed.append(f"崩溃重启只重放了 {replayed} 条行变更事件（历史共 {data_before} 条），"
                          f"未从任务起始位点整段重放")
        else:
            failed.append(f"崩溃重启重放了 {replayed} 条行变更事件（历史共 {data_before} 条，"
                          f"容忍 {tolerance}），疑似仍从任务起始位点整段重放")

        # ---------- 第二段写入 ----------
        write_rows(args.rows)
        total_written += args.rows
        got = wait_target_rows(total_written, args.converge)
        time.sleep(8)
        new_cap = cap_event_count(task_id) - cap_before
        print(f"[观测] 崩溃后新增 .cap 事件共 {new_cap} 条（含第二段 {args.rows} 行写入与心跳）")
        # 先确认追平再去动 agent：否则"崩溃时还有积压"和"重启后跳过了事件"两种情况混在一起，
        # 最终不一致到底该算谁的账就说不清了。
        if got < total_written:
            failed.append(f"第二段写入后 {args.converge}s 未追平（目标 {got} / 源 {total_written}），"
                          f"后续 agent 崩溃用例的判据会失真")

        # ---------- 尺子4：单实例互斥 ----------
        # 必须带上 agent 启动时用的环境变量（主密钥），否则子进程会先在凭证解密上失败退出，
        # 根本走不到取锁那一步，判据就成了假阴性。
        env = dict(os.environ)
        for var, path in (("SYNCTASK_MASTER_KEY", ".synctask_master_key"),
                          ("JWT_SECRET", ".synctask_jwt_secret"),
                          ("AGENT_API_TOKEN", ".synctask_agent_token")):
            try:
                with open(os.path.join(PROJECT_DIR, path)) as f:
                    env[var] = f.read().strip()
            except OSError:
                pass
        dup = subprocess.run(
            ["java", f"-Dtask.id={task_id}", "-Dh2.bindAddress=127.0.0.1",
             "-jar", "migration-capture/target/migration-capture-1.0.0.jar"],
            cwd=PROJECT_DIR, env=env, capture_output=True, text=True, timeout=120)
        if dup.returncode == 9:
            passed.append("重复启动的 capture 被任务实例锁挡住并退出（exit=9）")
        else:
            failed.append(f"重复启动的 capture 未被挡住（exit={dup.returncode}），存在双写风险。"
                          f"末尾输出: {(dup.stdout or dup.stderr or '')[-300:]}")

        # ---------- 尺子3：孤儿自杀 ----------
        if args.skip_agent_kill:
            print("[跳过] --skip-agent-kill：不注入 agent 崩溃")
        else:
            apid = agent_pid()
            children = F.all_child_pids(task_id)
            tgt_at_kill = F.sql_fetch(CFG, TGT_DB, f"SELECT COUNT(*) FROM `{TABLE}`")[0][0]
            # 记下崩溃瞬间的目标行数：agent 崩溃时增量若还在追赶积压，重启后是否"跳过未应用的事件"
            # 只能靠这个基线判断（源已写 total_written 行，崩溃时目标才 tgt_at_kill 行）
            print(f"[注入] SIGKILL agent pid={apid}，当前子进程 {children}，"
                  f"此刻目标 {tgt_at_kill} 行 / 源已写 {total_written} 行")
            if apid and children:
                os.kill(apid, signal.SIGKILL)
                deadline = time.time() + 60   # 看门狗 5s 一轮 + 优雅退出余量
                while time.time() < deadline and any(pid_alive(p) for p in children):
                    time.sleep(2)
                survivors = [p for p in children if pid_alive(p)]
                if survivors:
                    failed.append(f"agent 崩溃 60s 后仍有孤儿子进程存活: {survivors}")
                else:
                    passed.append("agent 崩溃后子进程在看门狗周期内全部自行退出，无孤儿")
            else:
                failed.append(f"无法注入 agent 崩溃（agent pid={apid} 子进程={children}）")

            print("[恢复] 重启 agent ...")
            r = subprocess.run(["./restart_agent.sh"], cwd=PROJECT_DIR,
                               capture_output=True, text=True, timeout=180)
            if r.returncode != 0:
                failed.append(f"agent 重启失败: {r.stdout[-400:]} {r.stderr[-400:]}")
            else:
                # 恢复后再写一段，确认任务确实被接管且没有双写
                time.sleep(20)
                write_rows(500)
                total_written += 500
                wait_target_rows(total_written, args.converge)

        # ---------- 尺子5：最终一致 ----------
        src = F.sql_fetch(CFG, SRC_DB, FINGERPRINT)[0]
        tgt = F.sql_fetch(CFG, TGT_DB, FINGERPRINT)[0]
        print(f"[校验] src=(count={src[0]}, xor={src[1]}) tgt=(count={tgt[0]}, xor={tgt[1]})")
        if src == tgt:
            passed.append(f"最终源/目标逐指纹一致（{src[0]} 行）")
        else:
            failed.append(f"最终不一致 src={src} tgt={tgt}")

    except SystemExit:
        pass
    finally:
        if old_quota is not None:
            F.set_increment_quota(old_quota)
        if not args.keep:
            try:
                F.stop_task(token, task_id)
                time.sleep(3)
                F.delete_task(token, task_id)
                F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}",
                                 f"DROP DATABASE IF EXISTS {TGT_DB}"])
            except Exception as e:
                print(f"[清理] 忽略清理异常: {e}")

    return F.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
