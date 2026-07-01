#!/usr/bin/env python3
"""E2E 测试：场景2 PG → MySQL 全类型同步"""
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

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr

def pg_exec(sql):
    return run_cmd(["docker", "exec", "test_pg_e2e", "psql", "-U", "app_user", "-d", "myapp_db", "-c", sql])

def mysql_exec(sql):
    return run_cmd(["mysql", "-h192.168.107.6", "-uroot", "-prootpassword", "-e", sql])

if __name__ == "__main__":
    print("=" * 60)
    print("场景 2：PG → MySQL 全类型同步")
    print("=" * 60)

    token = login()
    print("✓ 登录成功")

    # 1. 在 PG 中创建全类型测试表
    print("\n--- 步骤 1：在 PG 中创建全类型测试表 ---")
    pg_exec("DROP TABLE IF EXISTS pg_fulltype_test;")
    create_sql = """
CREATE TABLE pg_fulltype_test (
    id SERIAL PRIMARY KEY,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_decimal NUMERIC(10,2),
    col_numeric NUMERIC(15,6),
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_char CHAR(10),
    col_varchar VARCHAR(255),
    col_text TEXT,
    col_boolean BOOLEAN,
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    col_json JSON,
    col_jsonb JSONB,
    col_bytea BYTEA,
    col_uuid UUID,
    col_interval INTERVAL,
    col_inet INET,
    col_cidr CIDR,
    col_macaddr MACADDR
);
"""
    pg_exec(create_sql)
    print("✓ PG 全类型表已创建")

    # 2. 插入测试数据（覆盖边界值和特殊值）
    print("\n--- 步骤 2：插入测试数据 ---")
    insert_sql = """
INSERT INTO pg_fulltype_test (
    col_smallint, col_integer, col_bigint, col_decimal, col_numeric,
    col_real, col_double, col_char, col_varchar, col_text,
    col_boolean, col_date, col_time, col_timestamp, col_timestamptz,
    col_json, col_jsonb, col_bytea, col_uuid, col_interval,
    col_inet, col_cidr, col_macaddr
) VALUES
-- 行1：最大值/正数边界
(32767, 2147483647, 9223372036854775807, 99999999.99, 9999.999999,
 3.4028235E+37, 1.7976931348623157E+308, 'MAX', '最大值测试', '长文本内容',
 TRUE, '9999-12-31', '23:59:59', '9999-12-31 23:59:59', '9999-12-31 23:59:59+08',
 '{"key": "max", "value": 99999}', '{"k": "v"}', E'\\\\x414243', '550e8400-e29b-41d4-a716-446655440000', '1 year 2 mons 3 days',
 '192.168.1.1', '192.168.0.0/16', '08:00:2b:01:02:03'),
-- 行2：最小值/负数边界
(-32768, -2147483648, -9223372036854775808, -99999999.99, -9999.999999,
 -3.4028235E+37, -1.7976931348623157E+308, 'MIN', '最小值测试', '负数文本',
 FALSE, '1000-01-01', '00:00:01', '1000-01-01 00:00:00', '1000-01-01 00:00:00+08',
 '{"key": "min", "value": -99999}', '{"k": null}', E'\\\\x00', '00000000-0000-0000-0000-000000000000', '-1 year',
 '10.0.0.1', '10.0.0.0/8', '00:00:00:00:00:00'),
-- 行3：零值/空值
(0, 0, 0, 0.00, 0.000000,
 0.0, 0.0, 'ZERO', '零值测试', '',
 FALSE, '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00',
 '{"key": "zero"}', '{}', E'\\\\x', '11111111-2222-3333-4444-555555555555', '0',
 '127.0.0.1', '127.0.0.0/8', 'ff:ff:ff:ff:ff:ff'),
-- 行4：特殊值
(1, 100, 1000, 3.14, 3.141593,
 1.5, 2.71828, 'SPECIAL', '特殊字符: <>&"''', 'Unicode: 中文 日本語 한국어',
 TRUE, '2026-06-20', '12:30:45', '2026-06-20 12:30:45', '2026-06-20 12:30:45+08',
 '{"emoji": "😀", "unicode": "中文"}', '{"nested": {"a": 1}}', E'\\\\x41424344', 'a3f5f1d4-2c8b-4e7a-9f6d-1b2c3d4e5f60', '2 hours 30 mins',
 '172.16.0.1', '172.16.0.0/12', '01:23:45:67:89:ab');
"""
    pg_exec(insert_sql)
    print("✓ 已插入 4 行测试数据")

    # 3. 清理 MySQL 目标数据库
    print("\n--- 步骤 3：清理 MySQL 目标数据库 ---")
    mysql_exec("DROP DATABASE IF EXISTS e2e_pg_fulltype; CREATE DATABASE e2e_pg_fulltype;")
    print("✓ MySQL 目标数据库已重建")

    # 4. 创建同步任务
    print("\n--- 步骤 4：创建 PG → MySQL 同步任务 ---")
    result = api_call(token, "POST", "/api/workflows", json={
        "name": "E2E-PG-MySQL-FullType",
        "sourceType": "postgresql",
        "targetType": "mysql",
        "taskType": "SYNC"
    })
    task_id = result["data"]["id"]
    print(f"✓ 任务创建成功: {task_id}")

    # 5. 配置任务
    print("\n--- 步骤 5：配置任务 ---")
    config = {
        "sourceConnection": "postgresql://app_user:userpassword@localhost:15432/myapp_db",
        "targetConnection": "mysql://root:rootpassword@192.168.107.6:3306",
        "migrationMode": "fullAndIncre",
        "syncObjects": '{"myapp_db": ["public.pg_fulltype_test"]}',
        "sourceDbName": "myapp_db",
        "targetDbName": "e2e_pg_fulltype",
        "sourceType": "postgresql",
        "targetType": "mysql"
    }
    api_call(token, "PUT", f"/api/workflows/{task_id}/config", json=config)
    print("✓ 任务配置成功")

    # 6. 启动任务
    print("\n--- 步骤 6：启动任务 ---")
    api_call(token, "POST", f"/api/workflows/{task_id}/launch")
    print("✓ 任务已启动")

    # 7. 等待全量完成
    print("\n--- 步骤 7：等待全量同步完成 ---")
    start = time.time()
    while time.time() - start < 360:
        task = api_call(token, "GET", f"/api/workflows/{task_id}")
        status = task.get("status", "")
        progress = task.get("progress", 0)
        print(f"  [{int(time.time()-start)}s] 状态: {status}, 进度: {progress}%")
        if status in ["INCREMENT_RUNNING", "FULL_COMPLETED", "COMPLETED"]:
            break
        if status == "FAILED":
            print(f"  任务失败: {task.get('error_message','')}")
            break
        time.sleep(15)

    # 8. 验证 MySQL 中的数据
    print("\n--- 步骤 8：验证 MySQL 中的数据 ---")
    output = mysql_exec("USE e2e_pg_fulltype; SHOW TABLES; SELECT COUNT(*) AS total FROM pg_fulltype_test;")
    print(output.strip())

    print("\n--- 关键类型数据验证 ---")
    output = mysql_exec("USE e2e_pg_fulltype; SELECT id, col_smallint, col_integer, col_bigint, col_decimal, col_boolean FROM pg_fulltype_test ORDER BY id;")
    print(output.strip())

    print("\n--- JSON/文本类型验证 ---")
    output = mysql_exec("USE e2e_pg_fulltype; SELECT id, col_varchar, col_text, col_json FROM pg_fulltype_test ORDER BY id;")
    print(output.strip())

    print("\n--- 日期时间类型验证 ---")
    output = mysql_exec("USE e2e_pg_fulltype; SELECT id, col_date, col_time, col_timestamp, col_timestamptz FROM pg_fulltype_test ORDER BY id;")
    print(output.strip())

    # 9. 测试增量同步
    print("\n--- 步骤 9：测试增量同步 ---")
    pg_exec("INSERT INTO pg_fulltype_test (col_smallint, col_integer, col_varchar, col_json) VALUES (999, 9999, 'incremental-pg-mysql', '{\"inc\": true}');")
    print("✓ 已在 PG 插入增量数据")
    time.sleep(15)

    print("\n--- 验证增量同步结果 ---")
    output = mysql_exec("USE e2e_pg_fulltype; SELECT COUNT(*) AS total FROM pg_fulltype_test;")
    print(output.strip())
    output = mysql_exec("USE e2e_pg_fulltype; SELECT id, col_smallint, col_varchar, col_json FROM pg_fulltype_test WHERE col_varchar='incremental-pg-mysql';")
    print(output.strip())

    # 保存 task_id
    try:
        with open("/tmp/e2e_task_ids.json") as f:
            ids = json.load(f)
    except:
        ids = {}
    ids["pg_mysql_fulltype"] = task_id
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump(ids, f)
    print(f"\n✓ PG→MySQL 全类型同步任务 ID: {task_id}")
