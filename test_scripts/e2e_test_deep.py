#!/usr/bin/env python3
"""E2E 深入测试：MySQL→PG 全类型+临界值+大数据量同步验证"""
import requests
import json
import time
import subprocess
import sys

BASE_URL = "http://localhost:8082"
MYSQL_HOST = "192.168.107.6"
PG_CONTAINER = "test_pg_e2e"

def login():
    resp = requests.post(f"{BASE_URL}/api/auth/login", json={"username": "user1", "password": "123456"})
    return resp.json()["token"]

def api_call(token, method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.request(method, f"{BASE_URL}{path}", headers=headers, **kwargs)
    return resp.json()

def wait_for_status(token, task_id, target_statuses, timeout=300, interval=5):
    start = time.time()
    while time.time() - start < timeout:
        resp = api_call(token, "GET", f"/api/workflows/{task_id}")
        task = resp.get("data", resp) if isinstance(resp, dict) else resp
        status = task.get("status", "")
        progress = task.get("progress", 0)
        elapsed = int(time.time() - start)
        print(f"  [{elapsed}s] 状态: {status}, 进度: {progress}%")
        if status in target_statuses:
            return task
        if status == "FAILED":
            print(f"  任务失败: {task.get('error_message','')}")
            return task
        time.sleep(interval)
    print(f"  超时（{timeout}s）")
    resp = api_call(token, "GET", f"/api/workflows/{task_id}")
    return resp.get("data", resp) if isinstance(resp, dict) else resp

def run_cmd(cmd, timeout=120):
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return result.stdout + result.stderr

def mysql_exec(sql, timeout=120):
    return run_cmd(["mysql", f"-h{MYSQL_HOST}", "-uroot", "-prootpassword", "-e", sql], timeout)

def pg_exec(sql, timeout=120):
    return run_cmd(["docker", "exec", PG_CONTAINER, "psql", "-U", "app_user", "-d", "myapp_db", "-c", sql], timeout)

def pg_exec_file(filepath):
    with open(filepath, "r") as f:
        sql = f.read()
    return run_cmd(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "app_user", "-d", "myapp_db"], timeout=60) if False else \
           subprocess.run(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "app_user", "-d", "myapp_db"],
                          input=sql, capture_output=True, text=True, timeout=60)

if __name__ == "__main__":
    print("=" * 70)
    print("阶段 3 深入测试：MySQL→PG 全类型+临界值+大数据量")
    print("=" * 70)

    token = login()
    print("✓ 登录成功")

    # ============ 测试 1：全类型同步 ============
    print("\n" + "=" * 70)
    print("测试 1：MySQL 全类型数据同步到 PostgreSQL")
    print("=" * 70)

    # 准备 MySQL 源数据
    print("\n--- 准备 MySQL 全类型测试数据 ---")
    mysql_exec("CREATE DATABASE IF NOT EXISTS e2e_fulltype_test;")
    # 执行 test_insert.sql
    with open("test_insert.sql", "r") as f:
        sql_content = f.read()
    result = subprocess.run(["mysql", f"-h{MYSQL_HOST}", "-uroot", "-prootpassword", "e2e_fulltype_test"],
                          input=sql_content, capture_output=True, text=True, timeout=60)
    print(f"SQL 执行结果: {result.stdout}{result.stderr}")

    output = mysql_exec("USE e2e_fulltype_test; SELECT COUNT(*) AS total FROM test_all_types;")
    print(f"MySQL 源数据行数:\n{output.strip()}")

    # 准备 PG 目标 schema
    pg_exec("CREATE SCHEMA IF NOT EXISTS e2e_fulltype_test;")

    # 创建同步任务
    print("\n--- 创建全类型同步任务 ---")
    result = api_call(token, "POST", "/api/workflows", json={
        "name": "E2E-全类型同步-MySQL-PG",
        "sourceType": "mysql",
        "targetType": "postgresql",
        "taskType": "SYNC"
    })
    task_id = result["data"]["id"]
    print(f"✓ 任务创建成功: {task_id}")

    # 配置任务
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
    api_call(token, "PUT", f"/api/workflows/{task_id}/config", json=config)
    print("✓ 任务配置成功")

    # 启动任务
    api_call(token, "POST", f"/api/workflows/{task_id}/launch")
    print("✓ 任务已启动")

    # 等待全量完成
    print("\n--- 等待全量同步完成 ---")
    task = wait_for_status(token, task_id, ["INCREMENT_RUNNING"], timeout=300)

    # 验证全量同步结果
    print("\n--- 验证全量同步结果 ---")
    output = pg_exec("SELECT COUNT(*) AS total FROM e2e_fulltype_test.test_all_types;")
    print(f"PG 目标数据行数:\n{output.strip()}")

    # 验证关键类型数据
    print("\n--- 验证关键类型数据 ---")
    output = pg_exec("SELECT id, col_tinyint, col_bigint, col_decimal, col_json, col_enum FROM e2e_fulltype_test.test_all_types ORDER BY id;")
    print(output.strip())

    # 测试增量
    print("\n--- 测试增量同步（插入新数据）---")
    mysql_exec("USE e2e_fulltype_test; INSERT INTO test_all_types (col_tinyint, col_int, col_varchar_255, col_json) VALUES (99, 12345, 'incremental-test', '{\"test\": true}');")
    print("✓ 已插入增量数据")
    time.sleep(15)

    output = pg_exec("SELECT COUNT(*) AS total FROM e2e_fulltype_test.test_all_types;")
    print(f"增量同步后 PG 数据行数:\n{output.strip()}")

    # 保存 task_id
    with open("/tmp/e2e_task_ids.json") as f:
        ids = json.load(f)
    ids["fulltype_mysql_pg"] = task_id
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump(ids, f)
    print(f"\n✓ 全类型同步任务 ID: {task_id}")

    # ============ 测试 2：大数据量同步 ============
    print("\n" + "=" * 70)
    print("测试 2：大数据量同步（10000 行）")
    print("=" * 70)

    # 准备大数据量表
    print("\n--- 准备大数据量测试表 ---")
    mysql_exec("USE e2e_fulltype_test; DROP TABLE IF EXISTS large_data_test; CREATE TABLE large_data_test (id INT PRIMARY KEY AUTO_INCREMENT, batch_id INT, seq_no INT, data_str VARCHAR(200), data_json JSON, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")

    # 批量插入 10000 行
    print("插入 10000 行数据...")
    # 使用存储过程批量插入
    insert_sql = """
    USE e2e_fulltype_test;
    DELIMITER //
    DROP PROCEDURE IF EXISTS bulk_insert //
    CREATE PROCEDURE bulk_insert()
    BEGIN
        DECLARE i INT DEFAULT 1;
        WHILE i <= 10000 DO
            INSERT INTO large_data_test (batch_id, seq_no, data_str, data_json) VALUES (i DIV 1000, i, CONCAT('data-', i), JSON_OBJECT('id', i, 'batch', i DIV 1000, 'value', i * 1.5));
            SET i = i + 1;
        END WHILE;
    END //
    DELIMITER ;
    CALL bulk_insert();
    DROP PROCEDURE bulk_insert;
    """
    result = subprocess.run(["mysql", f"-h{MYSQL_HOST}", "-uroot", "-prootpassword"],
                          input=insert_sql, capture_output=True, text=True, timeout=120)
    print(f"批量插入结果: {result.stderr[-200:] if result.stderr else 'OK'}")

    output = mysql_exec("USE e2e_fulltype_test; SELECT COUNT(*) AS total FROM large_data_test;")
    print(f"MySQL 大数据量行数:\n{output.strip()}")

    # 更新同步任务配置以包含新表
    print("\n--- 更新同步任务配置以包含大数据量表 ---")
    config2 = {
        "sourceConnection": f"mysql://root:rootpassword@{MYSQL_HOST}:3306",
        "targetConnection": "postgresql://app_user:userpassword@localhost:15432/myapp_db",
        "migrationMode": "fullAndIncre",
        "syncObjects": '{"e2e_fulltype_test": ["test_all_types", "large_data_test"]}',
        "sourceDbName": "e2e_fulltype_test",
        "targetDbName": "e2e_fulltype_test",
        "sourceType": "mysql",
        "targetType": "postgresql"
    }

    # 停止旧任务，创建新任务
    api_call(token, "POST", f"/api/workflows/{task_id}/stop")
    print("✓ 已停止旧任务")

    result = api_call(token, "POST", "/api/workflows", json={
        "name": "E2E-大数据量同步",
        "sourceType": "mysql",
        "targetType": "postgresql",
        "taskType": "SYNC"
    })
    task_id2 = result["data"]["id"]
    print(f"✓ 大数据量任务创建成功: {task_id2}")

    api_call(token, "PUT", f"/api/workflows/{task_id2}/config", json=config2)
    api_call(token, "POST", f"/api/workflows/{task_id2}/launch")
    print("✓ 大数据量任务已启动")

    # 等待同步完成
    print("\n--- 等待大数据量同步完成 ---")
    task = wait_for_status(token, task_id2, ["INCREMENT_RUNNING"], timeout=600)

    # 验证大数据量同步结果
    print("\n--- 验证大数据量同步结果 ---")
    output = pg_exec("SELECT COUNT(*) AS total FROM e2e_fulltype_test.large_data_test;")
    print(f"PG 大数据量行数:\n{output.strip()}")

    # 验证数据一致性
    output = pg_exec("SELECT batch_id, COUNT(*) AS cnt, MIN(seq_no) AS min_seq, MAX(seq_no) AS max_seq FROM e2e_fulltype_test.large_data_test GROUP BY batch_id ORDER BY batch_id;")
    print(f"PG 数据分布:\n{output.strip()}")

    # 保存 task_id
    ids["large_data_mysql_pg"] = task_id2
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump(ids, f)
    print(f"\n✓ 大数据量同步任务 ID: {task_id2}")

    print("\n" + "=" * 70)
    print("阶段 3 深入测试完成！")
    print("=" * 70)
