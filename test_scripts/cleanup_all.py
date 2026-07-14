#!/usr/bin/env python3
"""清理所有测试任务、数据和容器"""
import requests
import json
import subprocess
import time

BASE = "http://localhost:8082"

def main():
    print("=" * 60)
    print("阶段 4：清理测试环境")
    print("=" * 60)

    # 1. 登录
    token = requests.post(f"{BASE}/api/auth/login",
                         json={"username": "user1", "password": "123456"}).json()["token"]
    headers = {"Authorization": f"Bearer {token}"}
    print("✓ 登录成功")

    # 2. 停止所有运行中的任务
    print("\n--- 停止所有运行中的任务 ---")
    resp = requests.get(f"{BASE}/api/workflows?page=1&pageSize=100", headers=headers).json()
    data = resp.get("data", {})
    items = data.get("list", [])
    print(f"  共 {len(items)} 个任务")
    for t in items:
        tid = t.get("id")
        status = t.get("status")
        name = t.get("name", "")
        if status not in ["STOPPED", "FAILED", "COMPLETED", None, ""]:
            r = requests.post(f"{BASE}/api/workflows/{tid}/stop", headers=headers)
            print(f"  停止: {name} ({tid}) 状态={status}")
        else:
            print(f"  跳过: {name} ({tid}) 状态={status}")

    # 3. 清理 MySQL 测试数据库
    print("\n--- 清理 MySQL 测试数据库 ---")
    mysql_dbs = [
        "e2e_fulltype_test",
        "e2e_subscribe_test",
        "e2e_dr_test",
        "e2e_pg_fulltype",
        "e2e_test_db",
    ]
    for db in mysql_dbs:
        r = subprocess.run(["mysql", "-h192.168.107.6", "-uroot", "-prootpassword", "-e", f"DROP DATABASE IF EXISTS {db};"],
                         capture_output=True, text=True)
        print(f"  MySQL(107.6) 删除数据库 {db}: {'OK' if r.returncode == 0 else r.stderr.strip()}")

    # 清理备库
    for db in mysql_dbs:
        r = subprocess.run(["mysql", "-h192.168.107.7", "-uroot", "-prootpassword", "-e", f"DROP DATABASE IF EXISTS {db};"],
                         capture_output=True, text=True)
        print(f"  MySQL(107.7) 删除数据库 {db}: {'OK' if r.returncode == 0 else r.stderr.strip()}")

    # 4. 清理 PostgreSQL 测试 schema
    print("\n--- 清理 PostgreSQL 测试 schema ---")
    pg_schemas = [
        "e2e_fulltype_test",
        "e2e_pg_to_mysql",
        "e2e_test_schema",
    ]
    for schema in pg_schemas:
        r = subprocess.run(["docker", "exec", "test_pg_e2e", "psql", "-U", "app_user", "-d", "myapp_db",
                          "-c", f"DROP SCHEMA IF EXISTS {schema} CASCADE;"],
                         capture_output=True, text=True)
        print(f"  PG 删除 schema {schema}: {'OK' if r.returncode == 0 else r.stderr.strip()}")

    # 5. 清理 Kafka topics
    print("\n--- 清理 Kafka 测试 topics ---")
    r = subprocess.run(["docker", "exec", "kafka-2", "kafka-topics", "--list", "--bootstrap-server", "localhost:9092"],
                     capture_output=True, text=True)
    topics = [t.strip() for t in r.stdout.strip().split("\n") if t.strip() and
              ("e2e_sub" in t or "cdc." in t)]
    for topic in topics:
        r = subprocess.run(["docker", "exec", "kafka-2", "kafka-topics", "--delete",
                          "--bootstrap-server", "localhost:9092", "--topic", topic],
                         capture_output=True, text=True)
        print(f"  删除 topic: {topic}")

    # 6. 删除 Docker 容器（PG 和 Kafka）
    print("\n--- 删除 Docker 容器 ---")
    containers = ["test_pg_e2e", "kafka-1", "kafka-2", "kafka-3", "kafka-ui",
                  "zookeeper-1", "zookeeper-2", "zookeeper-3"]
    for c in containers:
        r = subprocess.run(["docker", "rm", "-f", c], capture_output=True, text=True)
        if r.returncode == 0:
            print(f"  删除容器: {c}")
        else:
            print(f"  跳过容器: {c} ({r.stderr.strip()})")

    # 7. 清理临时文件
    print("\n--- 清理临时文件 ---")
    import os
    temp_files = ["/tmp/e2e_task_ids.json", "/tmp/pg_mysql_fulltype_v2_output.txt",
                  "/tmp/dr_failover_output.txt", "/tmp/kafka_subscribe_output.txt",
                  "/tmp/deep_test_output.txt"]
    for f in temp_files:
        if os.path.exists(f):
            os.remove(f)
            print(f"  删除: {f}")

    print("\n" + "=" * 60)
    print("清理完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()
