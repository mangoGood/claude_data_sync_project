#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PG → PG 增量延迟指标是否可信。

三个场景，前两个在改造前都会给出**假的低延迟**：

  1. **capture 被暂停期间产生的变更**：延迟的起点必须是"源库提交那一刻"。
     改造前 PG 给事件打的时间戳是"capture 读到这条消息的本机时刻"，
     于是「源库提交 → walsender 投递 → capture 读到」整段不计入 —— capture 积压再多，
     面板上的延迟也只反映下游。
  2. **capture 进程被杀掉之后**：extract 的兜底心跳用**本机时钟**，increment 拿它算
     `now − sourceTstamp` 恒得 ≈0 并持续刷新 rto_metric，面板显示"延迟极低"而一条数据都没动。
     改造后合成心跳带标记、不刷 rto_metric，指标随即变陈旧（agent 侧按无数据处理）。
  3. **源库空闲期**：capture 打的是带**源端时钟**的心跳，延迟有值且量的是真实链路耗时。

用法：python3 test_scripts/pg_toast/pg_latency_e2e.py
前置：postgres_db 容器在跑；三个 fat jar 已 package。
"""
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pgtoastlib as L

TASK = "pg-latency-e2e"
TABLE = "lat"
SLOT = "pg_latency_slot"
PUB = "pg_latency_pub"
CAP_DIR = f"files/{TASK}/binlog_output"
THL_DIR = f"files/{TASK}/thl_output"
PAUSE_SECONDS = 12

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def rto_metric():
    """rto_metric 的三个字段：写入时刻 | 延迟ms | 已应用事件的源端时间戳。不存在返回 None。"""
    path = os.path.join(L.PROJECT_ROOT, CAP_DIR, "rto_metric")
    if not os.path.isfile(path):
        return None
    try:
        parts = open(path).read().strip().split("|")
        return {"written": int(parts[0]), "rto": int(parts[1]), "src": int(parts[2])}
    except Exception:
        return None


def prepare():
    L.reset_db(L.SRC_DB)
    L.reset_db(L.TGT_DB)
    L.drop_slot(SLOT)
    ddl = f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, v TEXT)"
    for db in (L.SRC_DB, L.TGT_DB):
        L.psql(ddl, db=db)
    task_dir = os.path.join(L.PROJECT_ROOT, "files", TASK)
    shutil.rmtree(task_dir, ignore_errors=True)
    L.write_config(task_dir, TASK, TABLE, CAP_DIR, THL_DIR, SLOT, PUB,
                   extra="capture.idle.heartbeat.ms=2000\n")
    return task_dir


def signal_backpressure(state):
    """直接写背压信号文件，让 capture 停止/恢复读 WAL（extract 平时也是这么通知它的）。"""
    path = os.path.join(L.PROJECT_ROOT, "files", TASK, "backpressure.signal")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(state + "\n")


def main():
    L.require_jars()
    task_dir = prepare()
    pipe = L.Pipeline(TASK, task_dir)
    print("=== PG 增量延迟指标判据 ===")
    try:
        pipe.start_all()
        if not pipe.wait_until(
                lambda: L.scalar(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{SLOT}'",
                                 L.SRC_DB) == "1", 90, "复制槽建立"):
            record("复制槽建立", False, "capture 没能建槽")
            return 1
        time.sleep(3)

        L.psql(f"INSERT INTO {TABLE} VALUES (1,'v1')", db=L.SRC_DB)
        ok = pipe.wait_until(lambda: L.row_json(L.TGT_DB, TABLE, 1) is not None, 90, "首行同步")
        record("链路打通", ok)
        ok = pipe.wait_until(lambda: rto_metric() is not None, 60, "rto_metric 出现")
        record("延迟指标有值", ok, str(rto_metric()))

        # --- 场景 1：capture 暂停期间提交的变更 ---------------------------------
        signal_backpressure("PAUSE")
        time.sleep(2)
        L.psql(f"INSERT INTO {TABLE} VALUES (2,'v2')", db=L.SRC_DB)
        print(f"    （已暂停 capture，等 {PAUSE_SECONDS}s 让这条变更在源端"
              f"「已提交但没被读到」的状态下积压）")
        time.sleep(PAUSE_SECONDS)
        signal_backpressure("RESUME")

        ok = pipe.wait_until(lambda: L.row_json(L.TGT_DB, TABLE, 2) is not None, 120, "第二行同步")
        record("暂停解除后数据补上来了", ok, str(L.row_json(L.TGT_DB, TABLE, 2)))
        m = rto_metric()
        # 起点是源库提交时刻 ⇒ 延迟必须覆盖整个暂停时长
        floor_ms = (PAUSE_SECONDS - 3) * 1000
        record("延迟起点是源库提交时刻（覆盖 capture 积压的那一段）",
               m is not None and m["rto"] >= floor_ms,
               f"rto={m['rto'] if m else None}ms，期望 ≥{floor_ms}ms"
               f"（改造前只量 capture→apply，会远小于这个数）")

        # --- 场景 2：capture 死掉之后指标不能继续报 ≈0 -----------------------------
        pipe.stop("capture")
        time.sleep(3)
        before = rto_metric()
        L.psql(f"INSERT INTO {TABLE} VALUES (3,'v3')", db=L.SRC_DB)
        print("    （capture 已杀掉，等 20s 看指标会不会被 extract 的本机时钟心跳刷成 ≈0）")
        time.sleep(20)
        after = rto_metric()
        record("capture 死后数据确实没同步（判据前提）",
               L.row_json(L.TGT_DB, TABLE, 3) is None, str(L.row_json(L.TGT_DB, TABLE, 3)))
        record("capture 死后延迟指标不再被刷新",
               before is not None and after is not None and after["written"] == before["written"],
               f"写入时刻 {before['written'] if before else None} -> "
               f"{after['written'] if after else None}（改造前会每几秒刷一次、值 ≈0）")
        record("capture 死后指标已陈旧（agent 侧按无数据处理，不会显示假的低延迟）",
               after is not None and time.time() * 1000 - after["written"] > 15000,
               f"陈旧 {int(time.time() * 1000 - after['written']) if after else 0}ms")

        # --- 场景 3：空闲期的源端时钟心跳 ------------------------------------------
        pipe.start("capture")
        ok = pipe.wait_until(lambda: L.row_json(L.TGT_DB, TABLE, 3) is not None, 120, "capture 重启后补数据")
        record("capture 重启后积压数据补齐", ok, str(L.row_json(L.TGT_DB, TABLE, 3)))

        marker = rto_metric()
        print("    （源库保持空闲，等 25s 看空闲期心跳有没有在量真实链路耗时）")
        time.sleep(25)
        idle = rto_metric()
        record("空闲期指标仍在刷新（capture 打了源端时钟心跳）",
               idle is not None and marker is not None and idle["written"] > marker["written"],
               f"写入时刻 {marker['written'] if marker else None} -> {idle['written'] if idle else None}")
        record("空闲期延迟是个合理的小数值（不是 0 也不是天文数字）",
               idle is not None and 0 <= idle["rto"] < 60000,
               f"rto={idle['rto'] if idle else None}ms")

        err = L.error_status(CAP_DIR)
        record("全程无 error_status", err == "", err)
    finally:
        logs = pipe.stop_all()
        L.drop_slot(SLOT)
        L.drop_publication(L.SRC_DB, PUB)
        log_dir = os.path.join(task_dir, "judge_logs")
        os.makedirs(log_dir, exist_ok=True)
        for name, text in logs.items():
            with open(os.path.join(log_dir, name + ".log"), "w") as f:
                f.write(text)

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n=== {passed}/{len(results)} 通过 ===")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL {name}: {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
