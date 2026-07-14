#!/usr/bin/env python3
"""全类型同步验证（修复版）"""
import requests
import json
import time
import subprocess

BASE_URL = "http://localhost:8082"
MYSQL_HOST = "192.168.107.6"
PG_CONTAINER = "test_pg_e2e"

token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]
print("✓ 登录成功")

def api(method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    return requests.request(method, f"{BASE_URL}{path}", headers=headers, **kwargs).json()

def pg_exec(sql):
    return subprocess.run(["docker", "exec", PG_CONTAINER, "psql", "-U", "app_user", "-d", "myapp_db", "-c", sql],
                         capture_output=True, text=True).stdout

def mysql_exec(sql):
    return subprocess.run(["mysql", f"-h{MYSQL_HOST}", "-uroot", "-prootpassword", "-e", sql],
                         capture_output=True, text=True).stdout

# 准备 PG schema
pg_exec("CREATE SCHEMA IF NOT EXISTS e2e_fulltype_test;")
pg_exec("DROP SCHEMA IF EXISTS e2e_fulltype_test CASCADE; CREATE SCHEMA e2e_fulltype_test;")
print("✓ PG schema 已准备")

# 创建同步任务
result = api("POST", "/api/workflows", json={
    "name": "E2E-全类型同步-V2",
    "sourceType": "mysql",
    "targetType": "postgresql",
    "taskType": "SYNC"
})
task_id = result["data"]["id"]
print(f"✓ 任务创建: {task_id}")

config = {
    "sourceConnection": f"mysql://root:rootpassword@{MYSQL_HOST}:3306",
    "targetConnection": "postgresql://app_user:userpassword@localhost:15432/myapp_db",
    "migrationMode": "fullAndIncre",
    "syncObjects": '{"e2e_fulltype_test": ["test_all_types"]}',
    "sourceDbName": "e2e_fulltype_test",
    "targetDbName": "e2e_fulltype_test",
    "sourceType": "mysql",
    "targetType": "postgresql"
}
api("PUT", f"/api/workflows/{task_id}/config", json=config)
api("POST", f"/api/workflows/{task_id}/launch")
print("✓ 任务已启动")

# 等待同步完成
print("\n等待同步完成...")
for i in range(120):
    time.sleep(3)
    task = api("GET", f"/api/workflows/{task_id}")
    status = task.get("status", "")
    progress = task.get("progress", 0)
    if i % 5 == 0:
        print(f"  [{i*3}s] 状态: {status}, 进度: {progress}%")
    if status == "INCREMENT_RUNNING":
        print(f"  ✓ 进入增量同步状态")
        break
    if status == "FAILED":
        print(f"  ✗ 失败: {task.get('error_message')}")
        break

# 验证全量同步
print("\n=== 全量同步验证 ===")
output = pg_exec("SELECT COUNT(*) AS total FROM e2e_fulltype_test.test_all_types;")
print(f"PG 数据行数: {output.strip()}")

# 验证关键类型
print("\n=== 关键类型数据验证 ===")
output = pg_exec("SELECT id, col_tinyint, col_bigint_unsigned, col_decimal, col_enum, col_set FROM e2e_fulltype_test.test_all_types ORDER BY id;")
print(output.strip())

print("\n=== JSON 类型验证 ===")
output = pg_exec("SELECT id, col_json FROM e2e_fulltype_test.test_all_types WHERE col_json IS NOT NULL ORDER BY id;")
print(output.strip())

print("\n=== 日期时间类型验证 ===")
output = pg_exec("SELECT id, col_date, col_datetime_6, col_timestamp_6, col_time_6 FROM e2e_fulltype_test.test_all_types ORDER BY id;")
print(output.strip())

# 测试增量
print("\n=== 增量同步测试 ===")
mysql_exec("USE e2e_fulltype_test; INSERT INTO test_all_types (col_tinyint, col_int, col_varchar_255, col_json, col_enum) VALUES (50, 99999, 'incremental-fulltype', '{\"inc\": true}', 'value2');")
print("✓ 已插入增量数据")
time.sleep(15)

output = pg_exec("SELECT COUNT(*) AS total FROM e2e_fulltype_test.test_all_types;")
print(f"增量后 PG 数据行数: {output.strip()}")

output = pg_exec("SELECT id, col_tinyint, col_varchar_255, col_json FROM e2e_fulltype_test.test_all_types WHERE col_varchar_255 = 'incremental-fulltype';")
print(f"增量数据验证:\n{output.strip()}")

# 保存 task_id
with open("/tmp/e2e_task_ids.json") as f:
    ids = json.load(f)
ids["fulltype_v2"] = task_id
with open("/tmp/e2e_task_ids.json", "w") as f:
    json.dump(ids, f)
print(f"\n✓ 全类型同步任务 ID: {task_id}")
