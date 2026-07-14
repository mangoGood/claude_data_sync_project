#!/usr/bin/env python3
"""E2E 测试：场景2 PG → MySQL 同步"""
import requests
import json
import time
import subprocess
import sys

BASE_URL = "http://localhost:8082"

def login():
    resp = requests.post(f"{BASE_URL}/api/auth/login", json={"username": "user1", "password": "123456"})
    return resp.json()["token"]

def api_call(token, method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.request(method, f"{BASE_URL}{path}", headers=headers, **kwargs)
    return resp.json()

def wait_for_status(token, task_id, target_statuses, timeout=120, interval=3):
    start = time.time()
    while time.time() - start < timeout:
        task = api_call(token, "GET", f"/api/workflows/{task_id}")
        status = task.get("status", "")
        progress = task.get("progress", 0)
        print(f"  状态: {status}, 进度: {progress}%")
        if status in target_statuses:
            return task
        if status == "FAILED":
            print(f"  任务失败: {task.get('error_message','')}")
            return task
        time.sleep(interval)
    print(f"  超时（{timeout}s）")
    return api_call(token, "GET", f"/api/workflows/{task_id}")

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr

if __name__ == "__main__":
    print("=" * 60)
    print("场景 2：PG → MySQL 同步基本流程验证（全量+增量）")
    print("=" * 60)

    token = login()
    print("✓ 登录成功")

    # 0. 停止场景1的任务
    print("\n--- 步骤 0：停止场景1任务 ---")
    with open("/tmp/e2e_task_ids.json") as f:
        ids = json.load(f)
    old_task = ids.get("mysql_pg")
    if old_task:
        try:
            api_call(token, "POST", f"/api/workflows/{old_task}/stop")
            print(f"✓ 已停止场景1任务: {old_task}")
        except:
            pass

    # 1. 在 PG 中准备测试数据
    print("\n--- 步骤 1：在 PG 中准备测试数据 ---")
    sql = """
SET search_path TO public;
DROP TABLE IF EXISTS pg_source_test;
CREATE TABLE pg_source_test (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    amount NUMERIC(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO pg_source_test (title, amount, is_active) VALUES
('PG数据-001', 99.99, TRUE),
('PG数据-002', 199.50, FALSE),
('PG数据-003', 299.00, TRUE);
SELECT * FROM pg_source_test;
"""
    output = run_cmd(["docker", "exec", "test_pg_e2e", "psql", "-U", "app_user", "-d", "myapp_db", "-c", sql])
    print(output.strip())

    # 2. 在 MySQL 中创建目标数据库
    print("\n--- 步骤 2：在 MySQL 中创建目标数据库 ---")
    output = run_cmd(["mysql", "-h192.168.107.6", "-uroot", "-prootpassword", "-e",
                       "CREATE DATABASE IF NOT EXISTS e2e_pg_to_mysql;"])
    print("✓ MySQL 目标数据库 e2e_pg_to_mysql 已创建")

    # 3. 创建同步任务
    print("\n--- 步骤 3：创建 PG → MySQL 同步任务 ---")
    result = api_call(token, "POST", "/api/workflows", json={
        "name": "E2E-PG-MySQL-全量增量",
        "sourceType": "postgresql",
        "targetType": "mysql",
        "taskType": "SYNC"
    })
    task_id = result["data"]["id"]
    print(f"✓ 任务创建成功: {task_id}")

    # 4. 配置任务
    print("\n--- 步骤 4：配置任务 ---")
    config = {
        "sourceConnection": "postgresql://app_user:userpassword@localhost:15432/myapp_db",
        "targetConnection": "mysql://root:rootpassword@192.168.107.6:3306",
        "migrationMode": "fullAndIncre",
        "syncObjects": '{"myapp_db": ["public.pg_source_test"]}',
        "sourceDbName": "myapp_db",
        "targetDbName": "e2e_pg_to_mysql",
        "sourceType": "postgresql",
        "targetType": "mysql"
    }
    api_call(token, "PUT", f"/api/workflows/{task_id}/config", json=config)
    print("✓ 任务配置成功")

    # 5. 启动任务
    print("\n--- 步骤 5：启动任务 ---")
    api_call(token, "POST", f"/api/workflows/{task_id}/launch")
    print("✓ 任务已启动")

    # 6. 等待全量完成
    print("\n--- 步骤 6：等待全量同步完成 ---")
    task = wait_for_status(token, task_id, ["INCREMENT_RUNNING", "FULL_COMPLETED"], timeout=120)

    # 7. 验证 MySQL 中的数据
    print("\n--- 步骤 7：验证 MySQL 中的数据 ---")
    output = run_cmd(["mysql", "-h192.168.107.6", "-uroot", "-prootpassword", "-e",
                       "USE e2e_pg_to_mysql; SHOW TABLES; SELECT * FROM pg_source_test;"])
    print(output.strip())

    # 8. 测试增量同步
    print("\n--- 步骤 8：测试增量同步（PG 插入新数据）---")
    output = run_cmd(["docker", "exec", "test_pg_e2e", "psql", "-U", "app_user", "-d", "myapp_db", "-c",
                       "INSERT INTO pg_source_test (title, amount, is_active) VALUES ('PG增量-004', 399.99, TRUE);"])
    print("✓ 已在 PG 插入 1 条增量数据")

    time.sleep(10)

    print("\n--- 步骤 9：验证增量同步结果 ---")
    output = run_cmd(["mysql", "-h192.168.107.6", "-uroot", "-prootpassword", "-e",
                       "USE e2e_pg_to_mysql; SELECT COUNT(*) AS total FROM pg_source_test;"])
    print(output.strip())

    # 保存 task_id
    ids["pg_mysql"] = task_id
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump(ids, f)
    print(f"\n✓ Task ID 已保存: {task_id}")
