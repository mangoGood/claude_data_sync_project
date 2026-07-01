#!/usr/bin/env python3
"""检查任务状态并测试增量"""
import requests
import json
import time
import subprocess

BASE_URL = "http://localhost:8082"
token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]

task_id = "9661ee62-38ba-4817-9885-2782a8c4f598"
resp = requests.get(f"{BASE_URL}/api/workflows/{task_id}", headers={"Authorization": f"Bearer {token}"})
print("=== 完整任务详情 ===")
print(json.dumps(resp.json(), indent=2, ensure_ascii=False))

# 检查 Agent 日志
print("\n=== Agent 日志 ===")
result = subprocess.run(["grep", task_id, "logs/agent.out"], capture_output=True, text=True)
lines = result.stdout.strip().split("\n")
for line in lines[-15:]:
    print(line)
