#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
binlog 边界形态判据的脚手架。

拓扑与三进程编排跟 XA 判据完全一样，直接复用 `test_scripts/xa/xalib.py`，
这里只加两样东西：任意建表 DDL、源端全局参数的"改了必复原"。

覆盖的四类源端形态（都是实测复现过的静默故障）：
  * 压缩 binlog（binlog_transaction_compression=ON）
  * JSON 差量（binlog_row_value_options=PARTIAL_JSON）
  * 生成列（STORED / VIRTUAL）
  * 事务中间的 SAVEPOINT
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "xa"))
import xalib as X  # noqa: E402

PROJECT_ROOT = X.PROJECT_ROOT
SRC_CT, SRC_PORT = X.SRC_CT, X.SRC_PORT
TGT_CT, TGT_PORT = X.TGT_CT, X.TGT_PORT
PWD = X.PWD
mysql = X.mysql
scalar = X.scalar
reset_db = X.reset_db
rows_of = X.rows_of
Pipeline = X.Pipeline
write_config = X.write_config
require_jars = X.require_jars
error_status = X.error_status


def create(container, db, ddl):
    mysql(container, ddl, db=db)


def ids_of(container, db, table):
    """表里现有的主键集合。

    不用 xalib.rows_of——那个把列名写死成 XA 判据的表结构（id/tag/amt），
    在别的表上会直接 SQL 报错，而 wait_until 会把异常当成"还没到"一路等到超时，
    判据就成了永远失败且看不出原因。
    """
    out = mysql(container, f"SELECT id FROM {table} ORDER BY id", db=db, want=True)
    return {int(line.strip()) for line in out.splitlines() if line.strip()}


class SourceVar:
    """源端全局参数的临时改动：with 块退出时一定复原，判据挂了也不会污染下一个用例。"""

    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.old = None

    def __enter__(self):
        self.old = scalar(SRC_CT, f"SELECT @@global.{self.name}")
        mysql(SRC_CT, f"SET GLOBAL {self.name}={self._lit(self.value)}")
        return self

    def __exit__(self, *exc):
        mysql(SRC_CT, f"SET GLOBAL {self.name}={self._lit(self.old)}")
        return False

    @staticmethod
    def _lit(v):
        v = "" if v is None else str(v)
        return v if v.isdigit() else f"'{v}'"


def fresh_task(task_id, src_db, tgt_db, table, ddl, extra=""):
    """建库建表 + 清任务目录 + 写配置，返回 (task_dir, pipeline)。"""
    import shutil
    task_dir = os.path.join(PROJECT_ROOT, "files", task_id)
    cap = os.path.join(task_dir, "binlog_output")
    thl = os.path.join(task_dir, "thl_output")
    reset_db(SRC_CT, src_db)
    reset_db(TGT_CT, tgt_db)
    create(SRC_CT, src_db, ddl)
    create(TGT_CT, tgt_db, ddl)
    shutil.rmtree(task_dir, ignore_errors=True)
    os.makedirs(cap, exist_ok=True)
    os.makedirs(thl, exist_ok=True)
    write_config(task_dir, task_id, src_db, tgt_db, table, cap, thl, extra=extra)
    return task_dir, Pipeline(task_id, task_dir)
