#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基线：**关掉 XA 缓冲**（`sync.xa.enabled=false`）时，源库 XA 事务会怎么错。

这是 `xa_e2e.py` 的对照组，用来证明那些判据不是自证的——同一条链路、同一批数据，
只把缓冲开关关掉，就会复现改造前的两个数据面故障：

  1. **prepare 就落库**：源库还没提交（XA PREPARE 之后、XA COMMIT 之前），
     目标库已经能查到这批行了；
  2. **回滚留幻影行**：源库 `XA ROLLBACK` 掉的分支，目标库永远留着那几行，
     没有任何机制会去撤回，也不会有任何告警。

第三个故障（`XA START` 被原样打到目标库、把应用连接卡进 XA ACTIVE 态，
`COMMIT`/`ROLLBACK` 双双报 1399 XAER_RMFAIL 导致任务永久停摆）在这里复现不出来：
`SqlClassifier` 对 XA 控制语句的拦截是无条件的第二道闸，不受开关控制。
那条可以直接在库上验：

    SET autocommit=0;
    XA START X'6161',X'6262',1;   -- 成功，会话进入 XA ACTIVE
    COMMIT;                        -- ERROR 1399 (XAE07) XAER_RMFAIL

用法：
    python3 test_scripts/xa/xa_baseline.py
"""
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xalib as X

TASK = "xa-baseline"
SRC_DB, TGT_DB, TABLE = "xa_base_src", "xa_base_tgt", "t_xa"
TASK_DIR = os.path.join(X.PROJECT_ROOT, "files", TASK)
CAP_DIR = os.path.join(TASK_DIR, "binlog_output")
THL_DIR = os.path.join(TASK_DIR, "thl_output")

results = []
marker_seq = [2000]


def record(name, reproduced, detail=""):
    results.append((name, reproduced, detail))
    flag = "复现" if reproduced else "未复现"
    print(f"  [{flag}] {name}" + (f" — {detail}" if detail else ""))


def marker(pipe, what):
    marker_seq[0] += 1
    mid = marker_seq[0]
    X.mysql(X.SRC_CT, f"INSERT INTO {TABLE} VALUES ({mid}, 'marker-{what}', 0)", db=SRC_DB)
    return pipe.wait_until(lambda: mid in X.rows_of(X.TGT_CT, TGT_DB, TABLE), 90, f"标记 {mid}")


def main():
    X.require_jars()

    print("=" * 72)
    print("基线对照：关掉 XA 缓冲（sync.xa.enabled=false）后的行为")
    print("=" * 72)

    X.reset_db(X.SRC_CT, SRC_DB)
    X.reset_db(X.TGT_CT, TGT_DB)
    X.create_table(X.SRC_CT, SRC_DB, TABLE)
    X.create_table(X.TGT_CT, TGT_DB, TABLE)
    for gtrid in X.xa_recover(X.SRC_CT):
        try:
            X.xa_rollback(X.SRC_CT, gtrid)
        except Exception:
            pass

    shutil.rmtree(TASK_DIR, ignore_errors=True)
    os.makedirs(CAP_DIR, exist_ok=True)
    os.makedirs(THL_DIR, exist_ok=True)
    X.write_config(TASK_DIR, TASK, SRC_DB, TGT_DB, TABLE, CAP_DIR, THL_DIR,
                   extra="sync.xa.enabled=false\n")

    pipe = X.Pipeline(TASK, TASK_DIR)
    try:
        pipe.start_all()
        time.sleep(6)
        if not marker(pipe, "boot"):
            print("  !! 链路没打通，基线无从对照")
            return 1

        X.xa_prepare_branch(X.SRC_CT, SRC_DB, "base-rollback", [
            f"INSERT INTO {TABLE} VALUES (70, 'baseline', 1)",
            f"INSERT INTO {TABLE} VALUES (71, 'baseline', 2)",
        ])
        marker(pipe, "after-prepare")

        early = {70, 71} & set(X.rows_of(X.TGT_CT, TGT_DB, TABLE).keys())
        record("故障①：源库尚未提交，目标库已落数据", bool(early),
               f"prepare 阶段目标库已出现 {sorted(early)}" if early else "目标库仍是干净的")

        X.xa_rollback(X.SRC_CT, "base-rollback")
        marker(pipe, "after-rollback")
        phantom = {70, 71} & set(X.rows_of(X.TGT_CT, TGT_DB, TABLE).keys())
        record("故障②：源库回滚后目标库留下幻影行", bool(phantom),
               f"源库已 XA ROLLBACK，目标库仍有 {sorted(phantom)}（且无任何告警）"
               if phantom else "目标库没有残留")

        src_rows = set(X.rows_of(X.SRC_CT, SRC_DB, TABLE).keys())
        tgt_rows = set(X.rows_of(X.TGT_CT, TGT_DB, TABLE).keys())
        drift = tgt_rows - src_rows
        record("两端数据不一致", bool(drift),
               f"目标库比源库多了 {sorted(drift)}" if drift else "两端一致")
    finally:
        pipe.stop_all()
        for gtrid in X.xa_recover(X.SRC_CT):
            try:
                X.xa_rollback(X.SRC_CT, gtrid)
            except Exception:
                pass

    print("\n" + "=" * 72)
    reproduced = sum(1 for _, ok, _ in results if ok)
    print(f"改造前故障复现: {reproduced}/{len(results)}")
    print("对照组 xa_e2e.py（缓冲开启，默认）应当全部判 PASS。")
    print("=" * 72)
    return 0 if reproduced == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
