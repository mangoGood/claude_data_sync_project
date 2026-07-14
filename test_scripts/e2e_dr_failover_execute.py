#!/usr/bin/env python3
"""执行主备倒换并验证"""
import requests
import json
import time
import subprocess

BASE_URL = "http://localhost:8082"
PRIMARY = "192.168.107.6"
STANDBY = "192.168.107.7"

token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]
task_id = "68add763-1a79-4bf2-bb0c-2950f5bf3e5a"

def get_task():
    resp = requests.get(f"{BASE_URL}/api/workflows/{task_id}", headers={"Authorization": f"Bearer {token}"})
    return resp.json()["data"]

def mysql_exec(host, sql):
    result = subprocess.run(["mysql", f"-h{host}", "-uroot", "-prootpassword", "-e", sql], capture_output=True, text=True)
    return result.stdout + result.stderr

print("=" * 60)
print("执行主备倒换")
print("=" * 60)

# 倒换前状态
task = get_task()
print(f"\n倒换前:")
print(f"  状态: {task['status']}, DR: {task['dr_status']}")
print(f"  源库(主): {task['source_connection']}")
print(f"  目标库(备): {task['target_connection']}")
print(f"  倒换次数: {task['dr_switch_count']}")

# 执行倒换
print("\n--- 执行主备倒换 ---")
resp = requests.post(f"{BASE_URL}/api/workflows/{task_id}/failover", headers={"Authorization": f"Bearer {token}"})
print(f"响应: {json.dumps(resp.json(), ensure_ascii=False)}")

# 等待倒换完成
print("\n--- 等待倒换完成 ---")
for i in range(60):
    time.sleep(3)
    task = get_task()
    status = task['status']
    dr_status = task['dr_status']
    progress = task['progress']
    print(f"  [{i*3}s] 状态: {status}, DR: {dr_status}, 进度: {progress}%")
    if status == "INCREMENT_RUNNING" and dr_status == "DR_RUNNING":
        print("  ✓ 倒换完成，已恢复增量同步")
        break
    if status == "FAILED":
        print(f"  ✗ 倒换失败: {task.get('error_message')}")
        break

# 倒换后状态
task = get_task()
print(f"\n倒换后:")
print(f"  状态: {task['status']}, DR: {task['dr_status']}")
print(f"  源库(新主): {task['source_connection']}")
print(f"  目标库(新备): {task['target_connection']}")
print(f"  倒换次数: {task['dr_switch_count']}")

# 在新主库（原备库 107.7）写入数据
print(f"\n--- 在新主库({STANDBY})写入数据 ---")
mysql_exec(STANDBY, "USE e2e_dr_test; INSERT INTO dr_test_table (name, value) VALUES ('dr-after-switch-001', 500), ('dr-after-switch-002', 600);")
print("✓ 已插入 2 条数据")

time.sleep(12)

# 验证新备库（原主库 107.6）是否收到数据
print(f"\n--- 验证新备库({PRIMARY})数据 ---")
output = mysql_exec(PRIMARY, "USE e2e_dr_test; SELECT * FROM dr_test_table ORDER BY id;")
print(output.strip())

print(f"\n--- 新主库({STANDBY})数据 ---")
output = mysql_exec(STANDBY, "USE e2e_dr_test; SELECT COUNT(*) AS total FROM dr_test_table;")
print(output.strip())

# 保存结果
with open("/tmp/e2e_task_ids.json") as f:
    ids = json.load(f)
ids["dr_failover"] = task_id
with open("/tmp/e2e_task_ids.json", "w") as f:
    json.dump(ids, f)
print(f"\n✓ 灾备任务 ID: {task_id}")
