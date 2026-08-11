#!/usr/bin/env python3
"""
预检拦截率判据：预检"查出来"不等于"拦得住"。

现状（代码事实）：
  * 对象级预检 `DiagnosticService#schemaPrecheck` 只由**前端** `launchTask()` 在弹窗里调用
    （`admin-dashboard.js#schemaPrecheckGate`），点"确定"即可强制启动；
  * 后端 `WorkflowService#launchWorkflow` 里**没有任何预检调用**，
    而调度（`TaskScheduleService`）、依赖任务（`TaskDependencyService`）、
    批量启动（`BatchOperationService`）、故障转移改派全部直接走 `launchWorkflow`。

于是"预检拦截率"在所有非人工点击的路径上是 **0**：预检把问题算出来了，然后没人用它。

四把尺子：

1. **预检确实能查出问题** —— 构造一个引用不存在源表的任务，`/api/advanced/schema-precheck`
   必须返回 overall=FAIL（这一条通过说明检查项本身是好的，问题只在门禁）。
2. **API 直启是否被拦** —— 同一个任务直接 `POST /launch`：期望被拒（当前会成功放行）。
3. **强制启动是否留痕** —— 若允许强制启动，`audit_logs` 里应有一条"忽略预检强启"的记录
   （当前完全没有，事后无法追责）。
4. **非 MySQL 源的覆盖面** —— PostgreSQL 源任务跑预检，期望产出真实检查项；
   当前返回的是一条"仅支持 MySQL 源库，已跳过"的 WARNING（= 该链路预检覆盖率 0）。

用法：
    python3 test_scripts/fault_injection/precheck_gate.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import faultlib as F  # noqa: E402

CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword")
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
PG_CONN = "postgresql://app_user:userpassword@127.0.0.1:5432/myapp_db"
SRC_DB = "pchk_src"
TGT_DB = "pchk_tgt"


def rebuild():
    F.sql_exec(CFG, [f"DROP DATABASE IF EXISTS {SRC_DB}", f"CREATE DATABASE {SRC_DB}",
                     f"DROP DATABASE IF EXISTS {TGT_DB}", f"CREATE DATABASE {TGT_DB}"])
    F.sql_exec(CFG, ["CREATE TABLE ok_tbl (id INT PRIMARY KEY, v VARCHAR(16))"], db=SRC_DB)


def create_unlaunched(token, name, src_type, src_conn, sync_objects, src_db, tgt_db):
    """建任务并下发配置，但**不**启动（create_task 会顺手 launch，这里要拆开）。"""
    r = F.api("POST", "/api/workflows", token,
              json={"name": name, "sourceType": src_type, "targetType": "mysql", "taskType": "SYNC"})
    task_id = r["data"]["id"]
    F.api("PUT", f"/api/workflows/{task_id}/config", token, json={
        "sourceConnection": src_conn, "targetConnection": CONN,
        "migrationMode": "fullAndIncre", "syncObjects": json.dumps(sync_objects),
        "sourceDbName": src_db, "targetDbName": tgt_db,
        "sourceType": src_type, "targetType": "mysql",
    })
    return task_id


def precheck(token, task_id):
    return F.api("POST", f"/api/advanced/schema-precheck/{task_id}", token).get("data") or {}


def precheck_records(task_id):
    """预检留档行 (overall, forced)。事后要能回答"这个任务当初是在什么前提下启动的"。"""
    return F.sql_fetch(F.META_DB, "sync_task_db",
                       "SELECT overall, forced FROM task_precheck_results "
                       "WHERE workflow_id='%s' ORDER BY id" % task_id)


def main():
    token = F.login()
    rebuild()
    passed, failed = [], []
    created = []

    try:
        # ---- 尺子1/2/3/4：MySQL 源，同步对象引用一张不存在的表 ----
        bad = create_unlaunched(token, f"pchk-bad-{int(time.time())}", "mysql", CONN,
                                {SRC_DB: {"tables": ["ok_tbl", "table_that_does_not_exist"],
                                          "targetDb": TGT_DB}}, SRC_DB, TGT_DB)
        created.append(bad)
        d = precheck(token, bad)
        overall = d.get("overall")
        ok = (overall == "FAIL")
        (passed if ok else failed).append(
            f"预检能查出问题：overall={overall}（failed={d.get('failed')}, warnings={d.get('warnings')}）")

        r = F.api("POST", f"/api/workflows/{bad}/launch", token)
        launched = bool(r.get("success"))
        ok = not launched
        (passed if ok else failed).append(
            "预检 FAIL 的任务经 API 直启被拒"
            + ("" if ok else f"  ← 实际启动成功（success={launched}），"
                             "后端 launchWorkflow 未接预检，调度/依赖/批量启动全部绕过"))

        time.sleep(2)
        recs = precheck_records(bad)
        ok = any(x[0] == "FAIL" for x in recs)
        (passed if ok else failed).append(
            f"被拦的这次启动有留档（{len(recs)} 条）"
            + ("" if ok else "  ← task_precheck_results 无记录，事后查不到当初拦了什么"))

        # ---- force=true 才放行，且留档标记 forced ----
        r2 = F.api("POST", f"/api/workflows/{bad}/launch?force=true", token)
        forced_ok = bool(r2.get("success"))
        recs2 = precheck_records(bad)
        ok = forced_ok and any(x[1] for x in recs2)
        (passed if ok else failed).append(
            f"force=true 可强启且留下 forced 标记（success={forced_ok}）"
            + ("" if ok else "  ← 强启没留痕，等于门禁可以无声绕过"))

        # ---- 尺子5：非 MySQL 源的预检覆盖面 ----
        pg = create_unlaunched(token, f"pchk-pg-{int(time.time())}", "postgresql", PG_CONN,
                               {"public": {"tables": ["pg_table_that_does_not_exist"], "targetDb": TGT_DB}},
                               "public", TGT_DB)
        created.append(pg)
        dpg = precheck(token, pg)
        checks = dpg.get("checks") or []
        skipped = any("暂不支持该源类型" in str(c.get("message", ""))
                      or "仅支持 MySQL" in str(c.get("message", "")) for c in checks)
        ok = not skipped and len(checks) > 0
        (passed if ok else failed).append(
            f"PostgreSQL 源有实质预检项（共 {len(checks)} 项：{[c.get('checkName') for c in checks]}）"
            + ("" if ok else "  ← 仍是一句「已跳过」，该链路对象级预检覆盖率为 0"))
    finally:
        for t in created:
            F.stop_task(token, t)
            F.delete_task(token, t)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
