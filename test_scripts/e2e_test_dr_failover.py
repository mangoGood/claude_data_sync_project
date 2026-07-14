#!/usr/bin/env python3
"""E2E 测试：场景3 MySQL 灾备主备倒换验证（107.6 ↔ 107.7）"""
import requests
import json
import time
import subprocess
import sys

BASE_URL = "http://localhost:8082"
PRIMARY = "192.168.107.6"   # 主库
STANDBY = "192.168.107.7"   # 备库

def login():
    resp = requests.post(f"{BASE_URL}/api/auth/login", json={"username": "user1", "password": "123456"})
    return resp.json()["token"]

def api_call(token, method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.request(method, f"{BASE_URL}{path}", headers=headers, **kwargs)
    return resp.json()

def wait_for_status(token, task_id, target_statuses, timeout=180, interval=3):
    start = time.time()
    while time.time() - start < timeout:
        task = api_call(token, "GET", f"/api/workflows/{task_id}")
        status = task.get("status", "")
        progress = task.get("progress", 0)
        dr_status = task.get("dr_status", "")
        print(f"  状态: {status}, 进度: {progress}%, DR: {dr_status}")
        if status in target_statuses:
            return task
        if status == "FAILED":
            print(f"  任务失败: {task.get('error_message','')}")
            return task
        time.sleep(interval)
    print(f"  超时（{timeout}s）")
    return api_call(token, "GET", f"/api/workflows/{task_id}")

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, shell=isinstance(cmd, str))
    return result.stdout + result.stderr

def mysql_exec(host, sql):
    return run_cmd(["mysql", f"-h{host}", "-uroot", "-prootpassword", "-e", sql])

if __name__ == "__main__":
    print("=" * 60)
    print("场景 3：MySQL 灾备主备倒换验证（107.6 ↔ 107.7）")
    print("=" * 60)

    token = login()
    print("✓ 登录成功")

    # 0. 停止之前的任务
    print("\n--- 步骤 0：停止之前的任务 ---")
    with open("/tmp/e2e_task_ids.json") as f:
        ids = json.load(f)
    for key in ["mysql_pg", "pg_mysql"]:
        old_task = ids.get(key)
        if old_task:
            try:
                api_call(token, "POST", f"/api/workflows/{old_task}/stop")
                print(f"  已停止任务: {old_task}")
            except:
                pass

    # 1. 准备测试数据
    print("\n--- 步骤 1：准备灾备测试数据 ---")
    # 在主库创建测试数据库和表
    mysql_exec(PRIMARY, "CREATE DATABASE IF NOT EXISTS e2e_dr_test;")
    mysql_exec(PRIMARY, "USE e2e_dr_test; DROP TABLE IF EXISTS dr_test_table; CREATE TABLE dr_test_table (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), value INT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
    mysql_exec(PRIMARY, "USE e2e_dr_test; INSERT INTO dr_test_table (name, value) VALUES ('dr-init-001', 100), ('dr-init-002', 200), ('dr-init-003', 300);")
    output = mysql_exec(PRIMARY, "USE e2e_dr_test; SELECT * FROM dr_test_table;")
    print(f"主库({PRIMARY})初始数据:")
    print(output.strip())

    # 确保备库有目标数据库
    mysql_exec(STANDBY, "CREATE DATABASE IF NOT EXISTS e2e_dr_test;")
    print(f"✓ 备库({STANDBY})目标数据库已创建")

    # 2. 创建灾备任务
    print("\n--- 步骤 2：创建灾备任务（taskType=DR）---")
    result = api_call(token, "POST", "/api/workflows", json={
        "name": "E2E-MySQL-灾备倒换",
        "sourceType": "mysql",
        "targetType": "mysql",
        "taskType": "DR"
    })
    task_id = result["data"]["id"]
    print(f"✓ 灾备任务创建成功: {task_id}")

    # 3. 配置灾备任务
    print("\n--- 步骤 3：配置灾备任务 ---")
    config = {
        "sourceConnection": f"mysql://root:rootpassword@{PRIMARY}:3306",
        "targetConnection": f"mysql://root:rootpassword@{STANDBY}:3306",
        "migrationMode": "fullAndIncre",
        "syncObjects": '{"e2e_dr_test": ["dr_test_table"]}',
        "sourceDbName": "e2e_dr_test",
        "targetDbName": "e2e_dr_test",
        "sourceType": "mysql",
        "targetType": "mysql"
    }
    api_call(token, "PUT", f"/api/workflows/{task_id}/config", json=config)
    print("✓ 灾备任务配置成功")

    # 4. 启动灾备任务
    print("\n--- 步骤 4：启动灾备任务 ---")
    api_call(token, "POST", f"/api/workflows/{task_id}/launch")
    print("✓ 灾备任务已启动")

    # 5. 等待全量完成并进入增量
    print("\n--- 步骤 5：等待全量同步完成并进入增量同步 ---")
    task = wait_for_status(token, task_id, ["INCREMENT_RUNNING"], timeout=180)

    if task.get("status") != "INCREMENT_RUNNING":
        print(f"✗ 灾备任务未进入增量状态: {task.get('status')}")
        sys.exit(1)

    # 6. 验证全量同步结果
    print("\n--- 步骤 6：验证备库全量同步结果 ---")
    output = mysql_exec(STANDBY, "USE e2e_dr_test; SELECT * FROM dr_test_table;")
    print(f"备库({STANDBY})全量同步后数据:")
    print(output.strip())

    # 7. 测试增量同步（主库写入）
    print("\n--- 步骤 7：测试增量同步（主库写入新数据）---")
    mysql_exec(PRIMARY, "USE e2e_dr_test; INSERT INTO dr_test_table (name, value) VALUES ('dr-increment-before-switch', 400);")
    print(f"✓ 已在主库({PRIMARY})插入增量数据")
    time.sleep(10)
    output = mysql_exec(STANDBY, "USE e2e_dr_test; SELECT COUNT(*) AS total FROM dr_test_table;")
    print(f"备库({STANDBY})增量同步后数据:")
    print(output.strip())

    # 8. 执行主备倒换
    print("\n--- 步骤 8：执行主备倒换 ---")
    print(f"  倒换前: 主库={PRIMARY}, 备库={STANDBY}")
    result = api_call(token, "POST", f"/api/workflows/{task_id}/failover")
    print(f"✓ 主备倒换已触发: {result.get('message','')}")
    time.sleep(5)

    # 查看倒换后任务状态
    task = api_call(token, "GET", f"/api/workflows/{task_id}")
    print(f"  倒换后状态: {task.get('status')}, DR: {task.get('dr_status')}")
    print(f"  倒换后源库: {task.get('source_connection')}")
    print(f"  倒换后目标库: {task.get('target_connection')}")
    print(f"  倒换次数: {task.get('dr_switch_count')}")

    # 9. 等待倒换完成
    print("\n--- 步骤 9：等待倒换完成并恢复增量同步 ---")
    task = wait_for_status(token, task_id, ["INCREMENT_RUNNING"], timeout=120)

    # 10. 在新主库（原备库 107.7）写入数据，验证反向同步
    print("\n--- 步骤 10：测试倒换后增量同步（新主库写入）---")
    print(f"  新主库: {STANDBY}（原备库）")
    print(f"  新备库: {PRIMARY}（原主库）")
    mysql_exec(STANDBY, "USE e2e_dr_test; INSERT INTO dr_test_table (name, value) VALUES ('dr-after-switch-001', 500), ('dr-after-switch-002', 600);")
    print(f"✓ 已在新主库({STANDBY})插入 2 条数据")
    time.sleep(12)

    # 11. 验证新备库（原主库）是否收到数据
    print("\n--- 步骤 11：验证倒换后增量同步结果 ---")
    output = mysql_exec(PRIMARY, "USE e2e_dr_test; SELECT * FROM dr_test_table ORDER BY id;")
    print(f"新备库({PRIMARY})倒换后数据:")
    print(output.strip())

    output = mysql_exec(STANDBY, "USE e2e_dr_test; SELECT COUNT(*) AS total FROM dr_test_table;")
    print(f"新主库({STANDBY})数据总数:")
    print(output.strip())

    # 保存 task_id
    ids["dr_failover"] = task_id
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump(ids, f)
    print(f"\n✓ 灾备任务 ID 已保存: {task_id}")
    print(f"  最终状态: {task.get('status')}, DR: {task.get('dr_status')}")
