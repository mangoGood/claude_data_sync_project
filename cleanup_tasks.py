#!/usr/bin/env python3
"""清理旧任务以释放配额"""
import requests
import json

BASE_URL = "http://localhost:8082"
token = requests.post(f"{BASE_URL}/api/auth/login", json={"username":"user1","password":"123456"}).json()["token"]

# 获取所有任务
resp = requests.get(f"{BASE_URL}/api/workflows?page=1&pageSize=100",
    headers={"Authorization": f"Bearer {token}"})
data = resp.json()
tasks = data.get("data", {}).get("list", [])
print(f"总任务数: {len(tasks)}")

# 保留最近5个，删除其余
# 按创建时间排序，删除最早的
tasks_sorted = sorted(tasks, key=lambda x: x.get("created_at", ""))
to_delete = tasks_sorted[:-5] if len(tasks_sorted) > 5 else []

print(f"将删除 {len(to_delete)} 个旧任务")
deleted = 0
for task in to_delete:
    task_id = task["id"]
    task_name = task.get("name", "")
    try:
        # 先停止再删除
        requests.post(f"{BASE_URL}/api/workflows/{task_id}/stop",
            headers={"Authorization": f"Bearer {token}"}, timeout=5)
        resp = requests.delete(f"{BASE_URL}/api/workflows/{task_id}",
            headers={"Authorization": f"Bearer {token}"}, timeout=5)
        if resp.status_code == 200:
            deleted += 1
            print(f"  已删除: {task_name} ({task_id[:8]}...)")
        else:
            print(f"  删除失败: {task_name} - {resp.status_code}")
    except Exception as e:
        print(f"  删除异常: {task_name} - {e}")

print(f"\n共删除 {deleted} 个任务")

# 验证剩余任务数
resp = requests.get(f"{BASE_URL}/api/workflows?page=1&pageSize=100",
    headers={"Authorization": f"Bearer {token}"})
data = resp.json()
tasks = data.get("data", {}).get("list", [])
print(f"剩余任务数: {len(tasks)}")
