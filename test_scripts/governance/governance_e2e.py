#!/usr/bin/env python3
"""数据治理端到端判据：字段级血缘 + 数据分级 + Schema 演进审批。

三块是<b>互相依赖的一套</b>，判据也按这个顺序验：
  1. 血缘 —— 保存任务配置即自动生成；算子（脱敏/改名/原样）判定正确；
             影响面与上游追溯双向可查
  2. 分级 —— 内置规则自动打标；人工打标优先于规则（重跑不覆盖）；
             与血缘联动的策略校验能发现"RESTRICTED 列未脱敏"
  3. 审批 —— 引擎写的待审批记录能同步成单并按 (task,seqno) 去重；
             单子带血缘与分级来的风险标注；批准即应用到目标库且不可重复审批

用法: .venv_fi/bin/python test_scripts/governance/governance_e2e.py
前置: ./start.sh 起好后端(38080)与 agent；MySQL 在 33306

判据会自建 gov_src/gov_tgt 两个库与一个任务，跑完请自行清理
（任务数有 50 上限，反复跑会撞到）。
"""
import json, sys, time
import mysql.connector, requests

API = "http://localhost:38080/api"
CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword", autocommit=True)
SRC, TGT, TBL = "gov_src", "gov_tgt", "members"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"
FAILS = []

def chk(name, ok, detail=""):
    print(("  ✓ " if ok else "  ✗ ") + name + (("  — " + detail) if detail else ""))
    if not ok: FAILS.append(name)

def sql(db, q, args=None, fetch=False):
    kw = dict(CFG)
    if db: kw["database"] = db
    c = mysql.connector.connect(**kw)
    try:
        cur = c.cursor(); cur.execute(q, args or ())
        out = cur.fetchall() if fetch else None
        cur.close(); return out
    finally: c.close()

H = {}
def api(method, path, **kw):
    return requests.request(method, API + path, headers=H, timeout=60, **kw)

def main():
    r = requests.post(f"{API}/auth/login", json={"username":"admin","password":"admin123"}, timeout=15)
    H["Authorization"] = "Bearer " + r.json()["token"]
    H["Content-Type"] = "application/json"

    print("== 准备库表 ==")
    for db in (SRC, TGT):
        sql(None, f"DROP DATABASE IF EXISTS {db}"); sql(None, f"CREATE DATABASE {db}")
    sql(SRC, f"""CREATE TABLE {TBL}(
        id BIGINT PRIMARY KEY, real_name VARCHAR(64), phone VARCHAR(32),
        id_card VARCHAR(32), email VARCHAR(128), amount INT, note VARCHAR(128))""")
    for i in range(1, 6):
        sql(SRC, f"INSERT INTO {TBL} VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (i, f"张{i}", f"1380013{i:04d}", f"11010119900101{i:04d}", f"u{i}@corp.com", i*100, f"n{i}"))

    # 建任务：phone 脱敏、email 改名、note 加附加列场景
    sync_objects = {SRC: {
        "tables": [TBL],
        "columnMapping": {TBL: {"email": "mail_addr"}},
        "columnMask": {TBL: [{"column":"phone","rule":"MASK_PARTIAL","keepPrefix":3,"keepSuffix":4}]}}}

    print("== 建任务并保存配置（血缘应自动生成）==")
    r = api("POST", "/workflows", json={"name": f"gov-e2e-{int(time.time())}",
            "sourceType":"mysql","targetType":"mysql","taskType":"SYNC"})
    wid = (r.json().get("data") or {}).get("id")
    if not wid:
        print("建任务失败:", r.text[:300]); return 1
    print("  taskId =", wid)
    r = api("PUT", f"/workflows/{wid}/config", json={
        "sourceConnection": CONN, "targetConnection": CONN, "migrationMode": "fullAndIncre",
        "sourceType":"mysql","targetType":"mysql","sourceDbName":SRC,"targetDbName":TGT,
        "syncObjects": json.dumps(sync_objects)})
    if not r.json().get("success"):
        print("配置失败:", r.text[:400]); return 1

    print("\n== 1. 字段级血缘 ==")
    g = api("GET", f"/governance/lineage/graph/{wid}").json().get("data") or []
    chk("配置保存后血缘自动生成", len(g) >= 7, f"{len(g)} 条边")
    ops = {e["operator"] for e in g}
    chk("识别出脱敏算子 MASK", "MASK" in ops, str(sorted(ops)))
    chk("识别出改名算子 RENAME", "RENAME" in ops)
    chk("未配规则的列记为 IDENTITY", "IDENTITY" in ops)
    mask_edge = next((e for e in g if e["operator"] == "MASK"), None)
    chk("脱敏边指向 phone", mask_edge and "phone" in mask_edge["source"],
        mask_edge["source"] if mask_edge else "无")
    rn = next((e for e in g if e["operator"] == "RENAME"), None)
    chk("改名边目标是 mail_addr", rn and rn["target"].endswith("mail_addr"),
        rn["target"] if rn else "无")

    imp = api("GET", f"/governance/lineage/impact?db={SRC}&table={TBL}&column=phone&depth=5").json().get("data") or {}
    chk("影响面分析给出下游", len(imp.get("downstream") or []) >= 1, str(imp.get("downstream")))
    chk("脱敏路径被标出（下游不参与内容对比）", imp.get("hasMask") is True)

    up = api("GET", f"/governance/lineage/upstream?db={TGT}&table={TBL}&column=mail_addr&depth=5").json().get("data") or {}
    chk("上游追溯能找回源列 email",
        any("email" in x for x in (up.get("upstream") or [])), str(up.get("upstream")))

    print("\n== 2. 数据分级 ==")
    r = api("POST", "/governance/classification/apply-rules",
            json={"workflowId": wid, "db": SRC, "table": TBL})
    n = (r.json().get("data") or {}).get("classified", 0)
    chk("内置规则自动打标", n >= 4, f"打标 {n} 列")
    cl = api("GET", f"/governance/classification?db={SRC}&table={TBL}").json().get("data") or []
    lv = {c["columnName"]: c["level"] for c in cl}
    chk("id_card 被判为 RESTRICTED", lv.get("id_card") == "RESTRICTED", str(lv))
    chk("phone 被判为 SENSITIVE", lv.get("phone") == "SENSITIVE")
    chk("amount 被判为 INTERNAL", lv.get("amount") == "INTERNAL")

    # 人工打标优先于规则
    api("POST", "/governance/classification",
        json={"db":SRC,"table":TBL,"column":"note","level":"RESTRICTED","tags":"人工"})
    api("POST", "/governance/classification/apply-rules", json={"workflowId": wid, "db": SRC, "table": TBL})
    cl2 = {c["columnName"]: c for c in (api("GET", f"/governance/classification?db={SRC}&table={TBL}").json().get("data") or [])}
    chk("重跑规则不覆盖人工打标",
        cl2.get("note", {}).get("level") == "RESTRICTED" and cl2.get("note", {}).get("source") == "MANUAL",
        str(cl2.get("note")))

    pol = api("GET", f"/governance/classification/policy/{wid}").json().get("data") or {}
    chk("策略校验能发现 RESTRICTED 列未脱敏",
        len(pol.get("unmaskedSensitive") or []) >= 1, str(pol.get("unmaskedSensitive"))[:150])
    chk("高敏流向被列出", len(pol.get("sensitiveFlows") or []) >= 2, f"{len(pol.get('sensitiveFlows') or [])} 条")

    flows = api("GET", "/governance/classification/flows?level=RESTRICTED").json().get("data") or []
    chk("按级别查流向可用", len(flows) >= 1, f"{len(flows)} 条")
    unmasked = [f for f in flows if not f.get("masked")]
    chk("流向里标出了哪些未脱敏", len(unmasked) >= 1)

    print("\n== 3. Schema 审批 ==")
    import os
    d = f"files/{wid}"
    os.makedirs(d, exist_ok=True)
    with open(f"{d}/schema_pending.jsonl", "w") as f:
        f.write(json.dumps({"ts": int(time.time()*1000), "seqno": 1001,
            "eventId":"binlog.000001:100","ddlType":"ALTER_TABLE","db":SRC,"table":TBL,
            "reason":"MANUAL 策略，需人工应用",
            "sql":f"ALTER TABLE {TBL} ADD COLUMN id_card_ext VARCHAR(32)"}, ensure_ascii=False) + "\n")
        # 同一条重复写一遍，验去重
        f.write(json.dumps({"ts": int(time.time()*1000), "seqno": 1001,
            "eventId":"binlog.000001:100","ddlType":"ALTER_TABLE","db":SRC,"table":TBL,
            "reason":"重放","sql":f"ALTER TABLE {TBL} ADD COLUMN id_card_ext VARCHAR(32)"}, ensure_ascii=False) + "\n")

    created = (api("POST", f"/governance/schema-changes/sync/{wid}").json().get("data") or {}).get("created", 0)
    chk("同步待审批 DDL 并按 (task,seqno) 去重", created == 1, f"created={created}（重复行应被去重）")

    pending = api("GET", "/governance/schema-changes/pending").json().get("data") or []
    mine = [p for p in pending if p["workflowId"] == wid]
    chk("待审批单可见", len(mine) == 1)
    if mine:
        req = mine[0]
        chk("单子带敏感级别标注（来自分级）", req.get("maxLevel") is not None, str(req.get("maxLevel")))
        # 目标表要先存在才能 ALTER
        sql(TGT, f"CREATE TABLE {TBL}(id BIGINT PRIMARY KEY, real_name VARCHAR(64))")
        r = api("POST", f"/governance/schema-changes/{req['id']}/approve", json={"comment":"判据批准"})
        st = (r.json().get("data") or {}).get("status")
        chk("批准即应用到目标库", st == "APPLIED", f"status={st} err={(r.json().get('data') or {}).get('errorMessage')}")
        cols = [c[0] for c in sql(TGT, f"SHOW COLUMNS FROM {TBL}", fetch=True)]
        chk("目标库真的多出了那一列", "id_card_ext" in cols, str(cols))
        r2 = api("POST", f"/governance/schema-changes/{req['id']}/approve", json={})
        chk("已处理的单不能重复审批", not r2.json().get("success"), r2.json().get("message",""))

    print("\n== 清理 ==")
    try:
        api("POST", f"/workflows/{wid}/stop")
        time.sleep(2)
        api("DELETE", f"/workflows/{wid}")
        for db in (SRC, TGT):
            sql(None, f"DROP DATABASE IF EXISTS {db}")
        print("  已清理任务与测试库")
    except Exception as e:
        print("  清理失败（不影响判据结论）:", e)

    print("\n" + ("✓ 判据全部通过" if not FAILS else f"✗ {len(FAILS)} 项失败: " + ", ".join(FAILS)))
    return 0 if not FAILS else 1

sys.exit(main())
