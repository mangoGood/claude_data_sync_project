#!/usr/bin/env python3
import requests
import json
import subprocess

BASE_URL = "http://localhost:8082"
token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]

task_id = "68add763-1a79-4bf2-bb0c-2950f5bf3e5a"
resp = requests.get(f"{BASE_URL}/api/workflows/{task_id}", headers={"Authorization": f"Bearer {token}"})
print("=== 完整任务详情 ===")
print(json.dumps(resp.json(), indent=2, ensure_ascii=False))

print("\n=== Agent 日志 ===")
result = subprocess.run(["grep", task_id, "logs/agent.out"], capture_output=True, text=True)
lines = result.stdout.strip().split("\n")
for line in lines[-20:]:
    print(line)

print("\n=== 迁移日志 ===")
result = subprocess.run(["cat", f"files/{task_id}/logs/migration.log"], capture_output=True, text=True)
print(result.stdout[-2000:] if result.stdout else "无日志文件")
