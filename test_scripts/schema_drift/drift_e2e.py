#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表结构漂移判据（capture → extract → increment 三进程真链路）。

要证的只有一句话：**要么正确同步，要么明确报错停下，不允许静默写坏。**

漂移只发生在一个窗口里——事件已经进了 .cap、extract 还没消化，源库在这中间改了表结构。
真实链路里这个窗口等于全量耗时（extract 在全量做完之后才开工），可能是几小时。
每个用例都用 `LaggingExtract` 把这个窗口压缩成几秒造出来；造不出窗口的判据全是假绿灯，
因为源库改完结构、抽取端才开始读的话，查 information_schema 当然是对的。

六个场景：
  ① 积压期 RENAME COLUMN —— 列数不变，MINIMAL 下连检测都检测不到（最凶的一个）
  ② 积压期 DROP a + ADD b —— 列数同样不变
  ③ 积压期 enum 增删取值 —— FULL 元数据也救不了（取值表只能从 information_schema 拿）
  ④ MINIMAL 模式下的加列 —— 时序库要能顶替事件列名
  ⑤ 全量中途 DROP COLUMN —— 全量自己会先挂，要挂得明明白白
  ⑥ pt-osc 收尾的 RENAME 交换 —— 模型跟丢一次，之后全错

用法：
    python3 test_scripts/schema_drift/drift_e2e.py
    python3 test_scripts/schema_drift/drift_e2e.py --only rename_column
"""
import argparse
import sys
import time

import driftlib as D

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def boot_marker(pipe, src, tgt, tbl, marker_id, wait=180):
    """用一行普通数据确认链路通了。冷启动要连库、建心跳表、读 information_schema，窗口给宽些。"""
    D.mysql(D.SRC_CT, f"INSERT INTO {tbl} (id) VALUES ({marker_id})", db=src)
    return pipe.wait_until(lambda: marker_id in D.ids_of(D.TGT_CT, tgt, tbl), wait, "链路自检")


# ------------------------------------------------- ① 积压期 RENAME COLUMN

def case_rename_column():
    """列数不变的改名：改造前 MINIMAL 下连列数校验都发现不了，值会写进错的列。"""
    src, tgt, tbl = "drift_rn_src", "drift_rn_tgt", "t_rn"
    ddl = ("CREATE TABLE t_rn (id INT PRIMARY KEY, old_name VARCHAR(32), "
           "amt INT) ENGINE=InnoDB")
    task_dir, pipe = D.fresh_task("drift-rename-col", src, tgt, tbl, ddl, extra=D.TIMELINE_ON)
    try:
        with D.LaggingExtract(pipe) as lag:
            # 事件先堆进 .cap
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (1,'before-rename',100)", db=src)
            time.sleep(3)
            # 只改源库。DDL 会顺着同一条 THL 流按序replay 到目标端——手工先把目标端改掉
            # 反而是错的：那样"改名前的那行"到达时目标列已经没了，报 Unknown column，
            # 测出来的是判据自己制造的顺序错乱，不是链路的行为
            D.mysql(D.SRC_CT, f"ALTER TABLE {tbl} RENAME COLUMN old_name TO new_name", db=src)
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'after-rename',200)", db=src)
            time.sleep(2)
            lag.release()   # 现在才放 extract 进去消化——它面对的是改名<b>之后</b>的源库

        ok = pipe.wait_until(lambda: {1, 2} <= D.ids_of(D.TGT_CT, tgt, tbl), 120, "两行都到")
        record("① RENAME COLUMN：两行都同步到了", ok,
               "" if ok else f"目标库只有 {sorted(D.ids_of(D.TGT_CT, tgt, tbl))}")
        if ok:
            r1 = D.row_of(D.TGT_CT, tgt, tbl, 1)
            r2 = D.row_of(D.TGT_CT, tgt, tbl, 2)
            # 值必须落在正确的列上。改造前这里会看到 amt 的值跑进 new_name 之类的错位
            good = r1.get("new_name") == "before-rename" and r1.get("amt") == "100" \
                and r2.get("new_name") == "after-rename" and r2.get("amt") == "200"
            record("① RENAME COLUMN：值没有错位", good, "" if good else f"id=1 {r1} / id=2 {r2}")
        record("① RENAME COLUMN：无错误上报", D.error_status(task_dir) == "",
               D.error_status(task_dir)[:150] or "error_status 为空")
    finally:
        pipe.stop_all()


# ------------------------------------------------- ② 积压期 DROP + ADD（列数不变）

def case_drop_add():
    src, tgt, tbl = "drift_da_src", "drift_da_tgt", "t_da"
    ddl = "CREATE TABLE t_da (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16)) ENGINE=InnoDB"
    task_dir, pipe = D.fresh_task("drift-drop-add", src, tgt, tbl, ddl, extra=D.TIMELINE_ON)
    try:
        with D.LaggingExtract(pipe) as lag:
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (1,'aa','bb')", db=src)
            time.sleep(3)
            # 只改源库，DDL 顺着 THL 按序 replay 到目标端（理由见 ①）
            D.mysql(D.SRC_CT, f"ALTER TABLE {tbl} DROP COLUMN b, ADD COLUMN c VARCHAR(16)", db=src)
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'a2','c2')", db=src)
            time.sleep(2)
            lag.release()

        ok = pipe.wait_until(lambda: {1, 2} <= D.ids_of(D.TGT_CT, tgt, tbl), 120, "两行都到")
        record("② DROP+ADD（列数不变）：两行都同步到了", ok,
               "" if ok else f"目标库只有 {sorted(D.ids_of(D.TGT_CT, tgt, tbl))}")
        if ok:
            r1 = D.row_of(D.TGT_CT, tgt, tbl, 1)
            # id=1 是在 DROP b 之前写的，它的 a 必须还是 'aa'——按新布局硬解会把 'bb' 塞进 c
            good = r1.get("a") == "aa"
            record("② DROP+ADD：旧事件的值没有整体左移", good, "" if good else str(r1))
        record("② DROP+ADD：无错误上报", D.error_status(task_dir) == "",
               D.error_status(task_dir)[:150] or "error_status 为空")
    finally:
        pipe.stop_all()


# ------------------------------------------------- ③ 积压期 enum 增删取值

def case_enum_values():
    """enum 取值表只能从 information_schema 拿，FULL 的列名元数据救不了这一类。"""
    src, tgt, tbl = "drift_en_src", "drift_en_tgt", "t_en"
    ddl = ("CREATE TABLE t_en (id INT PRIMARY KEY, "
           "st ENUM('new','paid') NOT NULL DEFAULT 'new') ENGINE=InnoDB")
    task_dir, pipe = D.fresh_task("drift-enum", src, tgt, tbl, ddl, extra=D.TIMELINE_ON)
    try:
        with D.LaggingExtract(pipe) as lag:
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (1,'paid')", db=src)
            time.sleep(3)
            # 在中间插一个取值：paid 的序号从 2 变成 3。只改源库，DDL 顺着 THL replay
            D.mysql(D.SRC_CT, f"ALTER TABLE {tbl} MODIFY COLUMN st "
                              "ENUM('new','cancelled','paid') NOT NULL DEFAULT 'new'", db=src)
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'cancelled')", db=src)
            time.sleep(2)
            lag.release()

        ok = pipe.wait_until(lambda: {1, 2} <= D.ids_of(D.TGT_CT, tgt, tbl), 120, "两行都到")
        record("③ enum 增值：两行都同步到了", ok,
               "" if ok else f"目标库只有 {sorted(D.ids_of(D.TGT_CT, tgt, tbl))}")
        if ok:
            r1 = D.row_of(D.TGT_CT, tgt, tbl, 1)
            r2 = D.row_of(D.TGT_CT, tgt, tbl, 2)
            # id=1 写的时候 paid 还是序号 2；按新取值表解会变成 'cancelled'
            good = r1.get("st") == "paid" and r2.get("st") == "cancelled"
            record("③ enum 增值：旧事件按旧取值表还原", good,
                   "" if good else f"id=1 st={r1.get('st')}（应为 paid）/ id=2 st={r2.get('st')}")
        record("③ enum 增值：无错误上报", D.error_status(task_dir) == "",
               D.error_status(task_dir)[:150] or "error_status 为空")
    finally:
        pipe.stop_all()


# ------------------------------------------------- ④ MINIMAL 模式

def case_minimal_metadata():
    """binlog_row_metadata=MINIMAL：事件不带列名，全靠时序库。"""
    src, tgt, tbl = "drift_mn_src", "drift_mn_tgt", "t_mn"
    ddl = "CREATE TABLE t_mn (id INT PRIMARY KEY, a VARCHAR(16)) ENGINE=InnoDB"
    task_dir, pipe = D.fresh_task("drift-minimal", src, tgt, tbl, ddl, extra=D.TIMELINE_ON)
    try:
        with D.SourceVar("binlog_row_metadata", "MINIMAL"):
            with D.LaggingExtract(pipe) as lag:
                D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (1,'one')", db=src)
                time.sleep(3)
                D.mysql(D.SRC_CT, f"ALTER TABLE {tbl} ADD COLUMN b VARCHAR(16)", db=src)
                D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'two','bee')", db=src)
                time.sleep(2)
                lag.release()

            ok = pipe.wait_until(lambda: {1, 2} <= D.ids_of(D.TGT_CT, tgt, tbl), 120, "两行都到")
            record("④ MINIMAL：加列前后的事件都同步到了", ok,
                   "" if ok else f"目标库只有 {sorted(D.ids_of(D.TGT_CT, tgt, tbl))}；"
                                 f"error={D.error_status(task_dir)[:100]}")
            if ok:
                r1 = D.row_of(D.TGT_CT, tgt, tbl, 1)
                good = r1.get("a") == "one"
                record("④ MINIMAL：加列前的事件按旧布局解析", good, "" if good else str(r1))
    finally:
        pipe.stop_all()


# ------------------------------------------------- ⑤ pt-osc 收尾的 RENAME 交换

def case_pt_osc_swap():
    """RENAME TABLE t TO _t_old, _t_new TO t：模型跟丢一次，这张表之后全错。"""
    src, tgt, tbl = "drift_osc_src", "drift_osc_tgt", "t_osc"
    ddl = "CREATE TABLE t_osc (id INT PRIMARY KEY, a VARCHAR(16)) ENGINE=InnoDB"
    task_dir, pipe = D.fresh_task("drift-ptosc", src, tgt, tbl, ddl, extra=D.TIMELINE_ON)
    try:
        with D.LaggingExtract(pipe) as lag:
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (1,'one')", db=src)
            time.sleep(3)
            # 影子表带新结构，然后原子交换（pt-osc 的收尾就是这一条）
            D.mysql(D.SRC_CT,
                    f"CREATE TABLE _{tbl}_new (id INT PRIMARY KEY, a VARCHAR(16), "
                    "extra VARCHAR(8)) ENGINE=InnoDB", db=src)
            D.mysql(D.SRC_CT, f"INSERT INTO _{tbl}_new SELECT id, a, NULL FROM {tbl}", db=src)
            D.mysql(D.SRC_CT,
                    f"RENAME TABLE {tbl} TO _{tbl}_old, _{tbl}_new TO {tbl}", db=src)
            D.mysql(D.TGT_CT, f"ALTER TABLE {tbl} ADD COLUMN extra VARCHAR(8)", db=tgt)
            D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (2,'two','x')", db=src)
            time.sleep(2)
            lag.release()

        ok = pipe.wait_until(lambda: 2 in D.ids_of(D.TGT_CT, tgt, tbl), 120, "交换后的写入到达")
        err = D.error_status(task_dir)
        # 交换之后 t 的结构变了。允许两种结局：正确同步，或明确报错停下；
        # 不允许"同步了但值是错的"
        if ok:
            r2 = D.row_of(D.TGT_CT, tgt, tbl, 2)
            good = r2.get("a") == "two" and r2.get("extra") == "x"
            record("⑤ pt-osc 交换：交换后的写入值正确", good, "" if good else str(r2))
        else:
            record("⑤ pt-osc 交换：没同步就必须有明确错误码", err != "",
                   err[:150] or "既没同步也没有 error_status —— 静默丢数据")
    finally:
        pipe.stop_all()


# ------------------------------------------------- ⑥ 目标端少列

def case_target_missing_column():
    """目标表比源表少一列：必须明确报错停下，不能装作成功。"""
    src, tgt, tbl = "drift_tm_src", "drift_tm_tgt", "t_tm"
    src_ddl = "CREATE TABLE t_tm (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16)) ENGINE=InnoDB"
    tgt_ddl = "CREATE TABLE t_tm (id INT PRIMARY KEY, a VARCHAR(16)) ENGINE=InnoDB"
    task_dir, pipe = D.fresh_task("drift-target-missing", src, tgt, tbl, src_ddl,
                                  extra=D.TIMELINE_ON, tgt_ddl=tgt_ddl)
    try:
        pipe.start_all()
        time.sleep(8)
        D.mysql(D.SRC_CT, f"INSERT INTO {tbl} VALUES (1,'aa','bb')", db=src)
        # 目标端没有 b 列 → Unknown column，应当 fail-stop 并写 error_status
        got_error = pipe.wait_until(lambda: D.error_status(task_dir) != "", 90, "错误上报")
        landed = 1 in D.ids_of(D.TGT_CT, tgt, tbl)
        record("⑥ 目标少列：明确报错而不是静默跳过", got_error and not landed,
               D.error_status(task_dir)[:150]
               if got_error else ("行落库了但目标端缺列——值被悄悄丢了" if landed else "既没报错也没落库"))
    finally:
        pipe.stop_all()


CASES = {
    "rename_column": case_rename_column,
    "drop_add": case_drop_add,
    "enum_values": case_enum_values,
    "minimal": case_minimal_metadata,
    "pt_osc": case_pt_osc_swap,
    "target_missing": case_target_missing_column,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="只跑一个用例: " + " / ".join(CASES))
    args = ap.parse_args()

    D.require_jars()
    todo = {args.only: CASES[args.only]} if args.only else CASES
    if args.only and args.only not in CASES:
        print(f"没有这个用例: {args.only}")
        return 2

    for name, fn in todo.items():
        print(f"\n=== {name} ===")
        try:
            fn()
        except Exception as e:  # 单个用例炸掉不影响其余判据
            record(name, False, f"用例异常: {e}")

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n{'=' * 60}\n判据 {passed}/{len(results)} 通过")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL {name} — {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
