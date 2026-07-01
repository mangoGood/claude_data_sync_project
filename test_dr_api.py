#!/usr/bin/env python3
import requests
import json

BASE_URL = "http://localhost:8082"
token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]

# 测试创建 DR 任务
resp = requests.post(f"{BASE_URL}/api/workflows",
    headers={"Authorization": f"Bearer {token}"},
    json={"name": "E2E-DR-Test", "sourceType": "mysql", "targetType": "mysql", "taskType": "DR"})
print("Status:", resp.status_code)
print("Response:", json.dumps(resp.json(), indent=2, ensure_ascii=False))
