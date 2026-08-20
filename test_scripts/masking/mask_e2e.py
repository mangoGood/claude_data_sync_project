#!/usr/bin/env python3
"""脱敏端到端判据（mysql→mysql）。

守四件事：
  1. 目标端确实脱敏了——原值在目标端不存在
  2. 未配脱敏的列原样同步（脱敏不能误伤别的列）
  3. **全量与增量都脱敏**——只脱一头等于没脱
  4. 确定性：同一原值恒得到同一脱敏值

第 3 条不是凑数的：首次实现时全量脱了、增量没脱，根因是
`ColumnProcessingConfig.isEmpty()` 不认识 masks，于是"只配了脱敏"的任务
被判定为无列处理，增量整条脱敏逻辑被跳过。单测碰不到这个洞
（单测直接构造配置对象，不走 isEmpty 那条判断），只有端到端能抓。

用法: .venv_fi/bin/python test_scripts/masking/mask_e2e.py
前置: ./start.sh 起好后端(38080)与 agent；MySQL 在 33306
"""
import json, sys, time, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
import mysql.connector, requests

API = "http://localhost:38080/api"
CFG = dict(host="127.0.0.1", port=33306, user="root", password="rootpassword", autocommit=True)
SRC, TGT, TBL = "mask_src", "mask_tgt", "users"
CONN = "mysql://root:rootpassword@127.0.0.1:33306"

def sql(db, q, args=None, fetch=False):
    kw = dict(CFG)
    if db:
        kw["database"] = db
    c = mysql.connector.connect(**kw)
    try:
        cur = c.cursor()
        cur.execute(q, args or ())
        out = cur.fetchall() if fetch else None
        cur.close()
        return out
    finally:
        c.close()

def login():
    r = requests.post(f"{API}/auth/login", json={"username":"admin","password":"admin123"}, timeout=10)
    r.raise_for_status()
    return {"Authorization": "Bearer " + r.json()["token"]}

def main():
    H = login()
    print("== 准备源/目标库 ==")
    for db in (SRC, TGT):
        sql(None, f"DROP DATABASE IF EXISTS {db}")
        sql(None, f"CREATE DATABASE {db}")
    sql(SRC, f"""CREATE TABLE {TBL}(
        id BIGINT PRIMARY KEY, name VARCHAR(64), phone VARCHAR(32),
        email VARCHAR(128), remark VARCHAR(128), amount INT)""")
    rows = [(i, f"用户{i}", f"1380013{i:04d}", f"u{i}@corp.com", f"备注{i}", i*100) for i in range(1, 21)]
    for r in rows:
        sql(SRC, f"INSERT INTO {TBL} VALUES(%s,%s,%s,%s,%s,%s)", r)

    sync_objects = {SRC: {"tables":[TBL], "columnMask":{TBL:[
        {"column":"phone","rule":"MASK_PARTIAL","keepPrefix":3,"keepSuffix":4},
        {"column":"email","rule":"HASH"},
        {"column":"name","rule":"FAKE","arg":"NAME"},
        {"column":"remark","rule":"NULLIFY"}]}}}

    print("== 建任务 ==")
    r = requests.post(f"{API}/workflows", headers=H, timeout=30,
                      json={"name": f"mask-e2e-{int(time.time())}",
                            "sourceType":"mysql","targetType":"mysql","taskType":"SYNC"})
    wid = (r.json().get("data") or {}).get("id")
    if not wid:
        print("建任务失败:", r.status_code, r.text[:300]); return 1
    print("  taskId =", wid)

    # 正确流程是 建任务 → PUT /config → launch。配置里带 columnMask，
    # 后端的 ColumnMaskValidator 就在 config 这一步生效。
    r = requests.put(f"{API}/workflows/{wid}/config", headers=H, timeout=60, json={
        "sourceConnection": CONN, "targetConnection": CONN,
        "migrationMode": "fullAndIncre",
        "sourceType": "mysql", "targetType": "mysql",
        "sourceDbName": SRC, "targetDbName": TGT,
        "syncObjects": json.dumps(sync_objects)})
    if not r.json().get("success"):
        print("配置失败:", r.text[:400]); return 1
    print("  配置已保存（含 columnMask）")

    r = requests.post(f"{API}/workflows/{wid}/launch", headers=H, timeout=60, json={})
    if not r.json().get("success"):
        print("启动失败:", r.text[:300]); return 1

    print("== 等待进入增量 ==")
    for _ in range(90):
        st = requests.get(f"{API}/workflows/{wid}", headers=H, timeout=10).json()
        s = (st.get("data") or st).get("status")
        if s == "INCREMENT_RUNNING": break
        if s in ("FAILED","STOPPED"): print("  任务异常:", s); return 1
        time.sleep(2)
    print("  状态 =", s)

    print("== 增量再写 5 行 ==")
    for i in range(21, 26):
        sql(SRC, f"INSERT INTO {TBL} VALUES(%s,%s,%s,%s,%s,%s)",
            (i, f"用户{i}", f"1380013{i:04d}", f"u{i}@corp.com", f"备注{i}", i*100))
    for _ in range(40):
        n = sql(TGT, f"SELECT COUNT(*) FROM {TBL}", fetch=True)[0][0]
        if n >= 25: break
        time.sleep(2)

    print("\n== 校验 ==")
    ok = True
    tgt = sql(TGT, f"SELECT id,name,phone,email,remark,amount FROM {TBL} ORDER BY id", fetch=True)
    print(f"  目标端行数 {len(tgt)}（期望 25）")
    ok &= len(tgt) == 25

    full = [t for t in tgt if t[0] <= 20]
    incr = [t for t in tgt if t[0] > 20]
    for label, subset in (("全量", full), ("增量", incr)):
        bad = []
        for row in subset:
            i, name, phone, email, remark, amount = row
            if phone == f"1380013{i:04d}": bad.append(f"id={i} phone 未脱敏")
            if not (phone.startswith("138") and phone.endswith(f"{i:04d}"[-4:])): bad.append(f"id={i} phone 形态错: {phone}")
            if email == f"u{i}@corp.com": bad.append(f"id={i} email 未脱敏")
            if name == f"用户{i}": bad.append(f"id={i} name 未脱敏")
            if remark is not None: bad.append(f"id={i} remark 应为 NULL，实为 {remark}")
            if amount != i*100: bad.append(f"id={i} 未脱敏列 amount 被改动: {amount}")
        print(f"  {label}（{len(subset)} 行）: {'✓ 全部脱敏正确' if not bad else '✗ ' + '; '.join(bad[:4])}")
        ok &= not bad

    # 确定性：同一 email 原值在两行里若相同，脱敏后也应相同
    sql(SRC, f"INSERT INTO {TBL} VALUES(99,'x','13800139999','same@corp.com','r',1)")
    sql(SRC, f"INSERT INTO {TBL} VALUES(98,'y','13800139998','same@corp.com','r',1)")
    for _ in range(30):
        n = sql(TGT, f"SELECT COUNT(*) FROM {TBL} WHERE id IN (98,99)", fetch=True)[0][0]
        if n == 2: break
        time.sleep(2)
    d = sql(TGT, f"SELECT email FROM {TBL} WHERE id IN (98,99)", fetch=True)
    det = len(d) == 2 and d[0][0] == d[1][0]
    print(f"  确定性（同原值→同脱敏值）: {'✓' if det else '✗'}")
    ok &= det

    print("\n== 内容对比 ==")
    r = requests.post(f"{API}/metadata/compare-content/start", headers=H, timeout=60, json={
        "sourceConnection":CONN,"targetConnection":CONN,
        "sourceType":"mysql","targetType":"mysql",
        "syncObjects":{SRC:[TBL]}})
    if r.status_code < 400:
        print("  对比接口可调用（直传路径不带脱敏配置，故仍会比全部列）")
    else:
        print("  对比接口返回", r.status_code)

    print("\n" + ("✓ 判据通过" if ok else "✗ 判据失败"))
    return 0 if ok else 1

sys.exit(main())
