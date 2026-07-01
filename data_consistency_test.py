#!/usr/bin/env python3
import pymysql
import time
import requests
import json
import sys

TASK_ID = "cafd4946-109f-404d-9180-f11059625304"
BASE_URL = "http://localhost:8082"

DB_6 = {"host": "192.168.107.6", "port": 3306, "user": "root", "password": "rootpassword", "database": "myapp_db", "charset": "utf8mb4"}
DB_7 = {"host": "192.168.107.7", "port": 3306, "user": "root", "password": "rootpassword", "database": "myapp_db", "charset": "utf8mb4"}

def get_conn(db_config):
    return pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor)

def get_token():
    resp = requests.post(f"{BASE_URL}/api/auth/login", json={"username": "drtest", "password": "123456"})
    return resp.json().get("token", "")

def get_workflow(token):
    resp = requests.get(f"{BASE_URL}/api/workflows/{TASK_ID}", headers={"Authorization": f"Bearer {token}"})
    return resp.json().get("data", {})

def trigger_failover(token):
    resp = requests.post(f"{BASE_URL}/api/workflows/{TASK_ID}/failover", headers={"Authorization": f"Bearer {token}"})
    return resp.json()

def wait_for_increment_running(token, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        wf = get_workflow(token)
        status = wf.get("status")
        dr_status = wf.get("drStatus")
        if status == "INCREMENT_RUNNING" and (dr_status is None or dr_status == "None"):
            return True, time.time() - start
        time.sleep(3)
    return False, time.time() - start

def insert_test_data(db_config, prefix, count=10):
    conn = get_conn(db_config)
    try:
        with conn.cursor() as cursor:
            for i in range(1, count + 1):
                sql = """INSERT INTO chinese_person_info 
                    (name, gender, birth_date, id_card, phone, email, address, occupation) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, (
                    f"{prefix}_{i:03d}",
                    "男" if i % 2 == 1 else "女",
                    f"1990-0{i:02d}-{i:02d}",
                    f"9999001{i:011d}",
                    f"1390000{i:04d}",
                    f"drtest{i}@test.com",
                    f"测试地址{i}号",
                    f"测试职业{i}"
                ))
            conn.commit()
            cursor.execute(f"SELECT id, name FROM chinese_person_info WHERE name LIKE '{prefix}%' ORDER BY id")
            rows = cursor.fetchall()
            print(f"  Inserted {len(rows)} rows with prefix '{prefix}' into {db_config['host']}")
            for r in rows:
                print(f"    id={r['id']}, name={r['name']}")
            return [r['id'] for r in rows]
    finally:
        conn.close()

def compare_data(db1_config, db2_config, test_ids=None):
    conn1 = get_conn(db1_config)
    conn2 = get_conn(db2_config)
    try:
        with conn1.cursor() as cur1, conn2.cursor() as cur2:
            cur1.execute("SELECT COUNT(*) as cnt FROM chinese_person_info")
            cnt1 = cur1.fetchone()['cnt']
            cur2.execute("SELECT COUNT(*) as cnt FROM chinese_person_info")
            cnt2 = cur2.fetchone()['cnt']
            print(f"  Row count: {db1_config['host']}={cnt1}, {db2_config['host']}={cnt2}")
            
            if test_ids:
                placeholders = ','.join(['%s'] * len(test_ids))
                cur1.execute(f"SELECT * FROM chinese_person_info WHERE id IN ({placeholders}) ORDER BY id", test_ids)
                rows1 = cur1.fetchall()
                cur2.execute(f"SELECT * FROM chinese_person_info WHERE id IN ({placeholders}) ORDER BY id", test_ids)
                rows2 = cur2.fetchall()
                
                if len(rows1) != len(rows2):
                    print(f"  ❌ MISMATCH: {db1_config['host']} has {len(rows1)} test rows, {db2_config['host']} has {len(rows2)}")
                    return False
                
                all_match = True
                for r1, r2 in zip(rows1, rows2):
                    if r1 != r2:
                        print(f"  ❌ MISMATCH on id={r1['id']}:")
                        for k in r1:
                            if r1[k] != r2[k]:
                                print(f"    {k}: {db1_config['host']}={r1[k]}, {db2_config['host']}={r2[k]}")
                        all_match = False
                
                if all_match:
                    print(f"  ✅ All {len(rows1)} test rows match between {db1_config['host']} and {db2_config['host']}")
                return all_match
            else:
                if cnt1 == cnt2:
                    print(f"  ✅ Row counts match: {cnt1}")
                    return True
                else:
                    print(f"  ❌ Row counts differ: {cnt1} vs {cnt2}")
                    return False
    finally:
        conn1.close()
        conn2.close()

def wait_for_sync(seconds=15):
    print(f"  Waiting {seconds}s for sync...")
    time.sleep(seconds)

print("=" * 60)
print("主备倒换数据一致性测试")
print("=" * 60)

token = get_token()
wf = get_workflow(token)
source = wf.get('source_connection', '')
target = wf.get('target_connection', '')
print(f"\n当前状态: source={source}, target={target}, status={wf.get('status')}")

if '107.6' in source:
    current_primary = DB_6
    current_standby = DB_7
    print(f"当前主库: 192.168.107.6, 备库: 192.168.107.7")
else:
    current_primary = DB_7
    current_standby = DB_6
    print(f"当前主库: 192.168.107.7, 备库: 192.168.107.6")

# ===== Step 1: 倒换前向主库写入测试数据 =====
print("\n" + "=" * 60)
print("Step 1: 倒换前 - 向主库写入测试数据")
print("=" * 60)
before_ids = insert_test_data(current_primary, "倒换前测试数据", 10)

# ===== Step 2: 等待同步，验证数据一致性 =====
print("\n" + "=" * 60)
print("Step 2: 等待同步，验证主备数据一致性")
print("=" * 60)
wait_for_sync(20)
result1 = compare_data(current_primary, current_standby, before_ids)

if not result1:
    print("\n❌ 倒换前数据不一致！需要排查问题。")
    sys.exit(1)

# ===== Step 3: 执行主备倒换 =====
print("\n" + "=" * 60)
print("Step 3: 执行主备倒换")
print("=" * 60)
token = get_token()
failover_result = trigger_failover(token)
print(f"  Failover triggered: success={failover_result.get('success')}")

token = get_token()
success, elapsed = wait_for_increment_running(token)
if not success:
    print(f"  ❌ Failover failed after {elapsed:.1f}s")
    sys.exit(1)
print(f"  ✅ Failover completed in {elapsed:.1f}s")

token = get_token()
wf = get_workflow(token)
new_source = wf.get('source_connection', '')
new_target = wf.get('target_connection', '')
print(f"  新状态: source={new_source}, target={new_target}")

if '107.6' in new_source:
    new_primary = DB_6
    new_standby = DB_7
else:
    new_primary = DB_7
    new_standby = DB_6

print(f"  新主库: {new_primary['host']}, 新备库: {new_standby['host']}")

# ===== Step 4: 验证倒换后旧数据仍然一致 =====
print("\n" + "=" * 60)
print("Step 4: 验证倒换后旧测试数据仍然一致")
print("=" * 60)
wait_for_sync(10)
result2 = compare_data(new_primary, new_standby, before_ids)

# ===== Step 5: 倒换后向新主库写入测试数据 =====
print("\n" + "=" * 60)
print("Step 5: 倒换后 - 向新主库写入测试数据")
print("=" * 60)
after_ids = insert_test_data(new_primary, "倒换后测试数据", 10)

# ===== Step 6: 等待同步，验证新主备数据一致性 =====
print("\n" + "=" * 60)
print("Step 6: 等待同步，验证新主备数据一致性")
print("=" * 60)
wait_for_sync(20)
result3 = compare_data(new_primary, new_standby, after_ids)

# ===== 最终汇总 =====
print("\n" + "=" * 60)
print("测试结果汇总")
print("=" * 60)
print(f"  倒换前数据一致性: {'✅ 通过' if result1 else '❌ 失败'}")
print(f"  倒换后旧数据一致性: {'✅ 通过' if result2 else '❌ 失败'}")
print(f"  倒换后新数据一致性: {'✅ 通过' if result3 else '❌ 失败'}")

if result1 and result2 and result3:
    print("\n🎉 所有数据一致性测试通过！")
else:
    print("\n❌ 存在数据不一致问题，需要排查！")
    sys.exit(1)
