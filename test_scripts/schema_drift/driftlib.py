#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表结构时序库判据的脚手架。

拓扑与三进程编排复用 `test_scripts/xa/xalib.py`（与 edge 判据同源），
这里加的是这类判据特有的三样东西：

  * 打开时序库的配置（mode / fallback）；
  * **让抽取端滞后**——本方案要救的场景全都发生在"事件已经进了 .cap、extract 还没消化，
    源库在这中间做了 DDL"这个窗口里。窗口造不出来，判据就全是假绿灯；
  * 读时序库历史文件与抽取端的错误码。
"""
import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "xa"))
import xalib as X  # noqa: E402

PROJECT_ROOT = X.PROJECT_ROOT
SRC_CT, SRC_PORT = X.SRC_CT, X.SRC_PORT
TGT_CT, TGT_PORT = X.TGT_CT, X.TGT_PORT
PWD = X.PWD
mysql = X.mysql
scalar = X.scalar
reset_db = X.reset_db
Pipeline = X.Pipeline
write_config = X.write_config
require_jars = X.require_jars
error_status = X.error_status
SourceVar = None  # 见下面从 edgelib 借的实现


def _load_source_var():
    """借 edgelib 的 SourceVar（源端全局参数改了必复原）。"""
    global SourceVar
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "edge"))
    import edgelib as E  # noqa: E402
    SourceVar = E.SourceVar


_load_source_var()


def ids_of(container, db, table):
    """表里现有的主键集合。

    不用 xalib.rows_of——它把列名写死成 XA 判据的表结构，在别的表上会 SQL 报错，
    而 wait_until 把异常当"还没到"一路等到超时，判据就成了永远失败且看不出原因。
    """
    out = mysql(container, f"SELECT id FROM {table} ORDER BY id", db=db, want=True)
    return {int(line.strip()) for line in out.splitlines() if line.strip()}


def row_of(container, db, table, pk):
    """一行的全部列，返回 {列名: 值}。列名从 information_schema 现查，不写死。"""
    cols = mysql(container,
                 "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                 f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}' ORDER BY ORDINAL_POSITION",
                 want=True).split()
    if not cols:
        return {}
    sel = ", ".join(f"`{c}`" for c in cols)
    out = mysql(container, f"SELECT {sel} FROM `{table}` WHERE id={pk}", db=db, want=True)
    line = out.strip().splitlines()
    if not line:
        return {}
    return dict(zip(cols, line[0].split("\t")))


def history_path(task_id):
    return os.path.join(PROJECT_ROOT, "files", task_id, "schema_history.jsonl")


def history(task_id):
    """时序库历史文件里的全部版本，按写入顺序。坏行跳过（与抽取端的容忍口径一致）。"""
    path = history_path(task_id)
    if not os.path.isfile(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                break
    return out


def versions_of(task_id, db, table):
    """某张表的全部版本，按位点升序。"""
    rows = [r for r in history(task_id)
            if (r.get("db") or "").lower() == db.lower()
            and (r.get("tbl") or "").lower() == table.lower()]
    rows.sort(key=lambda r: ((int(''.join(ch for ch in (r.get("f") or "0") if ch.isdigit()) or 0)),
                             r.get("p", 0)))
    return rows


def columns_at(task_id, db, table, index=-1):
    """某个版本的列名清单（默认最后一版）。"""
    vs = versions_of(task_id, db, table)
    if not vs:
        return []
    schema = vs[index].get("schema") or {}
    return [c.get("name") for c in schema.get("columns", [])]


def _mode(default):
    """允许用 DRIFT_MODE=OFF/SHADOW/ON 整体切档。

    对拍用：同一个场景在 OFF 下跑一遍，就能分清"这条判据挂了"是时序库的问题，
    还是改造之前就存在的老问题。
    """
    return os.environ.get("DRIFT_MODE", default).strip().upper()


TIMELINE_ON = (f"extract.schema.timeline.mode={_mode('ON')}\n"
               "extract.schema.timeline.fallback=RESNAPSHOT\n")

TIMELINE_SHADOW = ("extract.schema.timeline.mode=SHADOW\n"
                   "extract.schema.timeline.fallback=RESNAPSHOT\n")

TIMELINE_OFF = "extract.schema.timeline.mode=OFF\n"


def fresh_task(task_id, src_db, tgt_db, table, ddl, extra="", tgt_ddl=None):
    """建库建表 + 清任务目录 + 写配置，返回 (task_dir, pipeline)。

    `tgt_ddl` 让目标表可以与源表不同（"目标少列"那条判据要用）。
    """
    task_dir = os.path.join(PROJECT_ROOT, "files", task_id)
    cap = os.path.join(task_dir, "binlog_output")
    thl = os.path.join(task_dir, "thl_output")
    reset_db(SRC_CT, src_db)
    reset_db(TGT_CT, tgt_db)
    mysql(SRC_CT, ddl, db=src_db)
    mysql(TGT_CT, tgt_ddl or ddl, db=tgt_db)
    shutil.rmtree(task_dir, ignore_errors=True)
    os.makedirs(cap, exist_ok=True)
    os.makedirs(thl, exist_ok=True)
    write_config(task_dir, task_id, src_db, tgt_db, table, cap, thl, extra=extra)
    return task_dir, Pipeline(task_id, task_dir)


class LaggingExtract:
    """制造"事件已进 .cap、extract 还没消化"的窗口。

    这是本方案要救的场景的**前提**：漂移只发生在这个窗口里。造不出窗口的判据全是假绿灯——
    源库改完结构、抽取端才开始读，查 information_schema 当然是对的。

    做法是只起 capture，让事件先在 .cap 里堆着；等源端把 DDL 做完，再放 extract/increment
    进去消化。这与真实链路里"全量跑了几小时、extract 才开工"是同一个形状，只是把
    几小时压缩成几秒。
    """

    def __init__(self, pipe):
        self.pipe = pipe

    def __enter__(self):
        self.pipe.start("capture")
        time.sleep(6)
        return self

    def release(self):
        """放行抽取与应用。"""
        self.pipe.start("extract")
        time.sleep(1)
        self.pipe.start("increment")

    def __exit__(self, *exc):
        return False
