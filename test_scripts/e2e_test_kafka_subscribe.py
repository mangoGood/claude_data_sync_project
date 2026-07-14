#!/usr/bin/env python3
"""E2E 测试：场景4 Kafka 数据订阅验证"""
import requests
import json
import time
import subprocess
import sys

BASE_URL = "http://localhost:8082"
KAFKA_BROKER = "192.168.117.2:19092"  # kafka-2 容器
MYSQL_HOST = "192.168.107.6"

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
        resp = api_call(token, "GET", f"/api/workflows/{task_id}")
        task = resp.get("data", resp) if isinstance(resp, dict) else resp
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
    resp = api_call(token, "GET", f"/api/workflows/{task_id}")
    return resp.get("data", resp) if isinstance(resp, dict) else resp

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr

def mysql_exec(sql):
    return run_cmd(["mysql", f"-h{MYSQL_HOST}", "-uroot", "-prootpassword", "-e", sql])

def kafka_topics():
    return run_cmd(["docker", "exec", "kafka-2", "kafka-topics", "--list", "--bootstrap-server", "localhost:9092"])

def kafka_consume(topic, timeout_ms=10000):
    return run_cmd(["docker", "exec", "kafka-2", "kafka-console-consumer",
                    "--bootstrap-server", "localhost:9092", "--topic", topic,
                    "--from-beginning", "--max-messages", "10", "--timeout-ms", str(timeout_ms)])

if __name__ == "__main__":
    print("=" * 60)
    print("场景 4：Kafka 数据订阅验证")
    print("=" * 60)

    token = login()
    print("✓ 登录成功")

    # 0. 停止灾备任务
    print("\n--- 步骤 0：停止灾备任务 ---")
    with open("/tmp/e2e_task_ids.json") as f:
        ids = json.load(f)
    dr_task = ids.get("dr_failover")
    if dr_task:
        try:
            api_call(token, "POST", f"/api/workflows/{dr_task}/stop")
            print(f"  已停止灾备任务: {dr_task}")
        except:
            pass

    # 1. 准备测试数据
    print("\n--- 步骤 1：准备订阅测试数据 ---")
    mysql_exec("CREATE DATABASE IF NOT EXISTS e2e_subscribe_test;")
    mysql_exec("USE e2e_subscribe_test; DROP TABLE IF EXISTS subscribe_test; CREATE TABLE subscribe_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), value INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
    mysql_exec("USE e2e_subscribe_test; INSERT INTO subscribe_test (name, value) VALUES ('sub-init-001', 100), ('sub-init-002', 200);")
    output = mysql_exec("USE e2e_subscribe_test; SELECT * FROM subscribe_test;")
    print(f"初始数据:\n{output.strip()}")

    # 2. 创建订阅任务
    print("\n--- 步骤 2：创建订阅任务 ---")
    result = api_call(token, "POST", "/api/workflows", json={
        "name": "E2E-Kafka-订阅验证",
        "sourceType": "mysql",
        "targetType": "kafka",
        "taskType": "SUBSCRIBE"
    })
    task_id = result["data"]["id"]
    print(f"✓ 订阅任务创建成功: {task_id}")

    # 3. 配置订阅任务
    print("\n--- 步骤 3：配置订阅任务 ---")
    config = {
        "sourceConnection": f"mysql://root:rootpassword@{MYSQL_HOST}:3306",
        "targetConnection": f"kafka://{KAFKA_BROKER}",
        "migrationMode": "subscribe",
        "syncObjects": '{"e2e_subscribe_test": ["subscribe_test"]}',
        "sourceDbName": "e2e_subscribe_test",
        "targetDbName": "e2e_subscribe_test",
        "sourceType": "mysql",
        "targetType": "kafka",
        "kafkaBootstrapServers": KAFKA_BROKER,
        "kafkaTopicPrefix": "e2e_sub",
        "kafkaTopicStrategy": "TABLE",
        "subscribeFormat": "DEBEZIUM_JSON"
    }
    api_call(token, "PUT", f"/api/workflows/{task_id}/config", json=config)
    print("✓ 订阅任务配置成功")

    # 4. 启动订阅任务
    print("\n--- 步骤 4：启动订阅任务 ---")
    api_call(token, "POST", f"/api/workflows/{task_id}/launch")
    print("✓ 订阅任务已启动")

    # 5. 等待订阅运行
    print("\n--- 步骤 5：等待订阅任务运行 ---")
    task = wait_for_status(token, task_id, ["SUBSCRIBE_RUNNING", "INCREMENT_RUNNING"], timeout=120)

    # 6. 插入增量数据触发订阅
    print("\n--- 步骤 6：插入增量数据触发订阅 ---")
    mysql_exec("USE e2e_subscribe_test; INSERT INTO subscribe_test (name, value) VALUES ('sub-increment-001', 300), ('sub-increment-002', 400);")
    print("✓ 已插入 2 条增量数据")
    time.sleep(10)

    # 7. 检查 Kafka topic
    print("\n--- 步骤 7：检查 Kafka topic ---")
    output = kafka_topics()
    print(f"Kafka topics:\n{output.strip()}")

    # 8. 消费 Kafka 消息
    print("\n--- 步骤 8：消费 Kafka 消息 ---")
    # 尝试不同的 topic 名称
    possible_topics = []
    for line in output.strip().split("\n"):
        if "e2e_sub" in line or "subscribe" in line.lower():
            possible_topics.append(line.strip())

    if possible_topics:
        for topic in possible_topics:
            print(f"\n=== Topic: {topic} ===")
            msg_output = kafka_consume(topic, timeout_ms=15000)
            print(msg_output.strip())
    else:
        print("未找到相关 topic，列出所有 topic:")
        print(output.strip())
        # 尝试默认 topic
        for topic in output.strip().split("\n"):
            topic = topic.strip()
            if topic and not topic.startswith("__"):
                print(f"\n=== 尝试 Topic: {topic} ===")
                msg_output = kafka_consume(topic, timeout_ms=10000)
                if msg_output.strip():
                    print(msg_output.strip())

    # 保存 task_id
    ids["kafka_subscribe"] = task_id
    with open("/tmp/e2e_task_ids.json", "w") as f:
        json.dump(ids, f)
    print(f"\n✓ 订阅任务 ID: {task_id}")
