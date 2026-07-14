#!/usr/bin/env python3
import requests
import time
import sys
import subprocess
import json

TASK_ID = "cafd4946-109f-404d-9180-f11059625304"
BACKEND_URL = "http://localhost:8082"
AGENT_URL = "http://localhost:8083"
TOTAL_TESTS = 12
PASS = 0
FAIL = 0

def get_token():
    r = requests.post(f"{BACKEND_URL}/api/auth/login", json={"username": "drtest", "password": "123456"})
    return r.json()["token"]

def get_task_status(token):
    r = requests.get(f"{BACKEND_URL}/api/workflows/{TASK_ID}", headers={"Authorization": f"Bearer {token}"})
    return r.json()["data"]

def trigger_failover_via_backend(token):
    r = requests.post(f"{BACKEND_URL}/api/workflows/{TASK_ID}/failover", headers={"Authorization": f"Bearer {token}"})
    return r.json()

def reset_task_in_db(src, tgt):
    subprocess.run([
        "mysql", "-h", "192.168.107.2", "-u", "root", "-prootpassword", "sync_task_db",
        "-e", f"UPDATE workflows SET status='INCREMENT_RUNNING', dr_status='DR_RUNNING', "
              f"error_message=NULL, error_code=NULL, "
              f"source_connection='mysql://root:rootpassword@{src}:3306', "
              f"target_connection='mysql://root:rootpassword@{tgt}:3306' WHERE id='{TASK_ID}'"
    ], capture_output=True)

def parse_host(conn_str):
    if "107.6" in conn_str:
        return "192.168.107.6"
    elif "107.7" in conn_str:
        return "192.168.107.7"
    return "unknown"

def wait_for_increment_running(token, max_wait=60):
    waited = 0
    while waited < max_wait:
        time.sleep(5)
        waited += 5
        try:
            task = get_task_status(token)
            status = task.get("status")
            src_conn = task.get("source_connection", "")
            tgt_conn = task.get("target_connection", "")
            switch = task.get("dr_switch_count")
            err = task.get("error_message", "")
            if status == "INCREMENT_RUNNING":
                return task
            elif status == "FAILED":
                return task
        except:
            pass
    return None

print("=" * 60)
print(f"  FAILOVER TEST - {TOTAL_TESTS} iterations")
print(f"  Using Backend API for failover (which calls Agent API)")
print("=" * 60)

consecutive_pass = 0

for i in range(1, TOTAL_TESTS + 1):
    print(f"\n{'='*40}")
    print(f"  FAILOVER TEST #{i}")
    print(f"{'='*40}")

    try:
        token = get_token()
    except Exception as e:
        print(f"FAIL: Cannot get token: {e}")
        FAIL += 1
        continue

    try:
        task = get_task_status(token)
    except Exception as e:
        print(f"FAIL: Cannot get task status: {e}")
        FAIL += 1
        continue

    status = task.get("status")
    src_conn = task.get("source_connection", "")
    tgt_conn = task.get("target_connection", "")
    switch = task.get("dr_switch_count")
    src_host = parse_host(src_conn)
    tgt_host = parse_host(tgt_conn)

    print(f"Before: Status={status}, Src={src_host}, Tgt={tgt_host}, Switch={switch}")

    if status == "FAILED":
        err = task.get("error_message", "")
        print(f"  Task FAILED: {err}")
        print(f"  Resetting DB to INCREMENT_RUNNING and waiting for Agent recovery...")
        reset_task_in_db(src_host, tgt_host)
        time.sleep(5)
        try:
            token = get_token()
            task = wait_for_increment_running(token, max_wait=60)
            if task and task.get("status") == "INCREMENT_RUNNING":
                src_host = parse_host(task.get("source_connection", ""))
                tgt_host = parse_host(task.get("target_connection", ""))
                switch = task.get("dr_switch_count")
                print(f"  Recovered: Status=INCREMENT_RUNNING, Src={src_host}, Tgt={tgt_host}")
            else:
                print(f"  Failed to recover task, skipping")
                FAIL += 1
                continue
        except Exception as e:
            print(f"  Recovery error: {e}")
            FAIL += 1
            continue

    if status not in ("INCREMENT_RUNNING",):
        if status in ("SWITCHING",):
            print(f"  Task in SWITCHING, waiting to stabilize...")
            task = wait_for_increment_running(token, max_wait=120)
            if task and task.get("status") == "INCREMENT_RUNNING":
                src_host = parse_host(task.get("source_connection", ""))
                tgt_host = parse_host(task.get("target_connection", ""))
                switch = task.get("dr_switch_count")
                print(f"  Stabilized: Src={src_host}, Tgt={tgt_host}, Switch={switch}")
            else:
                print(f"  Task did not stabilize, skipping")
                FAIL += 1
                continue
        else:
            print(f"SKIP: Task not in INCREMENT_RUNNING (status={status})")
            FAIL += 1
            continue

    new_src = tgt_host
    new_tgt = src_host

    print(f"  Triggering failover via Backend API: {src_host} -> {tgt_host}  ==>  {new_src} -> {new_tgt}")

    try:
        result = trigger_failover_via_backend(token)
        if not result.get("success"):
            print(f"FAIL: Backend failover API error: {result}")
            FAIL += 1
            continue
    except Exception as e:
        print(f"FAIL: Backend failover API exception: {e}")
        FAIL += 1
        continue

    print(f"  Failover initiated, waiting for stabilization (up to 120s)...")

    waited = 0
    max_wait = 120
    final_status = ""
    final_src = src_host
    final_switch = switch

    while waited < max_wait:
        time.sleep(5)
        waited += 5

        try:
            token = get_token()
            task = get_task_status(token)
            status = task.get("status")
            src_conn = task.get("source_connection", "")
            tgt_conn = task.get("target_connection", "")
            switch = task.get("dr_switch_count")
            err = task.get("error_message", "")
            src_host_new = parse_host(src_conn)
        except Exception as e:
            print(f"  Error checking status: {e}")
            continue

        if status == "INCREMENT_RUNNING" and src_host_new == new_src:
            final_status = status
            final_src = src_host_new
            final_switch = switch
            print(f"  SUCCESS after {waited}s: Status={status}, Src={src_host_new}, Tgt={parse_host(tgt_conn)}, Switch={switch}")
            break
        elif status == "FAILED":
            final_status = status
            print(f"  FAIL after {waited}s: Status={status}, Error={err}")
            break
        elif status == "FULL_MIGRATING":
            final_status = status
            print(f"  FAIL after {waited}s: FULL_MIGRATING detected! (should not do full sync)")
            break
        else:
            if waited % 15 == 0:
                print(f"  Waiting... ({waited}s) Status={status}, Src={src_host_new}")

    if final_status == "INCREMENT_RUNNING" and final_src == new_src:
        PASS += 1
        consecutive_pass += 1
        print(f"  TEST #{i}: PASS (consecutive: {consecutive_pass})")
        print(f"  Waiting 30s for processes to fully stabilize before next test...")
        time.sleep(30)
    else:
        FAIL += 1
        consecutive_pass = 0
        print(f"  TEST #{i}: FAIL (final: {final_status})")
        if final_status == "FAILED":
            new_src_for_reset = new_src
            new_tgt_for_reset = new_tgt
            reset_task_in_db(new_src_for_reset, new_tgt_for_reset)
        time.sleep(10)

print()
print("=" * 60)
print(f"  TEST RESULTS: PASS={PASS}, FAIL={FAIL}, TOTAL={TOTAL_TESTS}")
if PASS == TOTAL_TESTS:
    print("  *** ALL TESTS PASSED! ***")
elif PASS >= TOTAL_TESTS - 1:
    print(f"  NEARLY ALL PASSED ({FAIL} failure)")
else:
    print(f"  {FAIL} FAILURES - needs investigation")
print("=" * 60)
