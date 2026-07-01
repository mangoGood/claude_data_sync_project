#!/usr/bin/env python3
"""E2E 测试：MySQL → PG 同步基本流程验证"""
import requests
import json
import time
import sys

BASE_URL = "http://localhost:8082"

def login():
    resp = requests.post(f"{BASE_URL}/api/auth/login", json={"username": "user1", "password": "123456"})
    data = resp.json()
    if "token" not in data:
        print(f"登录失败: {data}")
        sys.exit(1)
    return data["token"]

def create_task(token, name, source_type, target_type, task_type="SYNC"):
    resp = requests.post(f"{BASE_URL}/api/workflows",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": name, "sourceType": source_type, "targetType": target_type, "taskType": task_type})
    data = resp.json()
    if not data.get("success"):
        print(f"创建任务失败: {json.dumps(data, ensure_ascii=False)}")
        sys.exit(1)
    return data["data"]["id"]

def config_task(token, task_id, config):
    resp = requests.put(f"{BASE_URL}/api/workflows/{task_id}/config",
        headers={"Authorization": f"Bearer {token}"},
        json=config)
    data = resp.json()
    if not data.get("success"):
        print(f"配置任务失败: {json.dumps(data, ensure_ascii=False)}")
        sys.exit(1)
    return data["data"]

def launch_task(token, task_id):
    resp = requests.post(f"{BASE_URL}/api/workflows/{task_id}/launch",
        headers={"Authorization": f"Bearer {token}"})
    data = resp.json()
    if not data.get("success"):
        print(f"启动任务失败: {json.dumps(data, ensure_ascii=False)}")
        sys.exit(1)
    return data["data"]

def get_task(token, task_id):
    resp = requests.get(f"{BASE_URL}/api/workflows/{task_id}",
        headers={"Authorization": f"Bearer {token}"})
    data = resp.json()
    return data.get("data", {})

def wait_for_status(token, task_id, target_statuses, timeout=120, interval=3):
    """等待任务达到目标状态"""
    start = time.time()
    while time.time() - start < timeout:
        task = get_task(token, task_id)
        status = task.get("status", "")
        progress = task.get("progress", 0)
        error = task.get("error_message", "")
        current_table = task.get("current_table", "")
        print(f"  状态: {status}, 进度: {progress}%, 表: {current_table}, 错误: {error}")

        if status in target_statuses:
            return task
        if status == "FAILED":
            print(f"  任务失败: {error}")
            return task
        time.sleep(interval)
    print(f"  超时（{timeout}s）")
    return get_task(token, task_id)

def stop_task(token, task_id):
    resp = requests.post(f"{BASE_URL}/api/workflows/{task_id}/stop",
        headers={"Authorization": f"Bearer {token}"})
    return resp.json()

def delete_task(token, task_id):
    resp = requests.delete(f"{BASE_URL}/api/workflows/{task_id}",
        headers={"Authorization": f"Bearer {token}"})
    return resp.json()

if __name__ == "__main__":
    print("=" * 60)
    print("场景 1：MySQL → PG 同步基本流程验证（全量+增量）")
    print("=" * 60)

    token = login()
    print(f"✓ 登录成功")

    # 1. 创建任务
    print("\n--- 步骤 1：创建同步任务 ---")
    task_id = create_task(token, "E2E-MySQL-PG-全量增量", "mysql", "postgresql")
    print(f"✓ 任务创建成功: {task_id}")

    # 2. 配置任务（使用 fullAndIncre 模式）
    print("\n--- 步骤 2：配置任务 ---")
    config = {
        "sourceConnection": "mysql://root:rootpassword@192.168.107.6:3306",
        "targetConnection": "postgresql://app_user:userpassword@localhost:15432/myapp_db",
        "migrationMode": "fullAndIncre",
        "syncObjects": '{"e2e_test_db": ["e2e_simple_test"]}',
        "sourceDbName": "e2e_test_db",
        "targetDbName": "myapp_db",
        "sourceType": "mysql",
        "targetType": "postgresql"
    }
    config_task(token, task_id, config)
    print(f"✓ 任务配置成功")

    # 3. 启动任务
    print("\n--- 步骤 3：启动任务 ---")
    launch_task(token, task_id)
    print(f"✓ 任务已启动")

    # 4. 等待全量同步完成并进入增量
    print("\n--- 步骤 4：等待全量完成并进入增量同步 ---")
    task = wait_for_status(token, task_id, ["INCREMENT_RUNNING"], timeout=120)

    # 5. 测试增量同步：插入新数据
    print("\n--- 步骤 5：测试增量同步（插入新数据）---")
    import subprocess
    subprocess.run([
        "mysql", "-h192.168.107.6", "-uroot", "-prootpassword", "-e",
        "USE e2e_test_db; INSERT INTO e2e_simple_test (name, value, description) VALUES "
        "('test_004', 400, '增量测试记录'), ('test_005', 500, '增量测试记录2');"
    ], capture_output=True)
    print("✓ 已在源 MySQL 插入 2 条增量数据")

    # 6. 等待增量同步
    print("\n--- 步骤 6：等待增量同步完成 ---")
    time.sleep(10)

    # 7. 验证 PG 中的数据
    print("\n--- 步骤 7：验证 PG 中的数据 ---")
    result = subprocess.run([
        "docker", "exec", "test_pg_e2e", "psql", "-U", "app_user", "-d", "myapp_db",
        "-c", "SET search_path TO e2e_test_db; SELECT COUNT(*) AS total, MAX(id) AS max_id FROM e2e_simple_test;"
    ], capture_output=True, text=True)
    print(result.stdout)

    # 8. 保存 task_id
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump({"mysql_pg": task_id}, f)
    print(f"\n✓ Task ID 已保存: {task_id}")
    print(f"  最终状态: {task.get('status')}")
