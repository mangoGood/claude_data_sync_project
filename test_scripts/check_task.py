#!/usr/bin/env python3
"""检查任务状态"""
import requests
import json

BASE_URL = "http://localhost:8082"
token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]

task_id = "cffd5906-05c8-4238-a5ab-6a69c7f6b826"
resp = requests.get(f"{BASE_URL}/api/workflows/{task_id}", headers={"Authorization": f"Bearer {token}"})
data = resp.json().get("data", {})
print("=== 任务详情 ===")
for k in ["id","name","status","sourceType","targetType","migrationMode","sourceConnection","targetConnection","syncObjects","sourceDbName","targetDbName","progress","error_message"]:
    print(f"  {k}: {data.get(k)}")
