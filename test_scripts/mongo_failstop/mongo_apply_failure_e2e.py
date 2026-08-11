#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mongo 增量：单事件应用失败时位点不得越过它（SILENT_DATA_LOSS_AUDIT 第 7 项）。

改造前 `applyEvent` 捕获异常后只记一行日志，外层紧接着把 `cursor.getResumeToken()` 落盘 ——
位点越过刚失败的那条，重放永远不会再碰到它。注释里写的"下轮 resume 重放可自愈"并不成立：
目标端一次唯一索引冲突、一次 WriteConflict、一次网络抖动 = 永久丢一条文档变更，而任务全绿。

判据构造：目标端建一个源端没有的唯一索引，让第二条文档必然写失败。
  - 改造前：进程继续跑，doc2 永久丢失，重启也拿不回来。
  - 改造后：进程 fail-stop 退出；去掉冲突约束后重启，doc2 必须补上来。

前置：docker-compose-synctask-mongo.yml 起来且副本集已 initiate；migration-mongo fat jar 已 package。
用法：python3 test_scripts/pg_toast/mongo_apply_failure_e2e.py
"""
import json
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JAR = os.path.join(PROJECT_ROOT, "migration-mongo", "target", "migration-mongo-1.0.0.jar")
SRC, DST = "synctask-mongo-a", "synctask-mongo-b"
DB, COLL = "failstopdb", "docs"
TASK = "mongo-applyfail-e2e"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def msh(container, js):
    p = subprocess.run(
        ["docker", "exec", container, "mongosh", "-u", "root", "-p", "rootpassword",
         "--quiet", "--eval", js],
        capture_output=True, text=True, timeout=120)
    lines = [l for l in (p.stdout or "").strip().splitlines() if l.strip()]
    return lines[-1].strip() if lines else ""


def src(js):
    return msh(SRC, js)


def dst(js):
    return msh(DST, js)


def write_config():
    d = os.path.join(PROJECT_ROOT, "files", TASK)
    os.makedirs(d, exist_ok=True)
    cfg = [
        "source.db.host=localhost", "source.db.port=27117",
        "source.db.username=root", "source.db.password=rootpassword",
        "target.db.host=localhost", "target.db.port=27118",
        "target.db.username=root", "target.db.password=rootpassword",
        f"migration.sync.objects={json.dumps({DB: {'tables': [COLL]}})}",
        "migration.mode=fullAndIncre",
    ]
    with open(os.path.join(d, "config.properties"), "w") as f:
        f.write("\n".join(cfg) + "\n")


def start():
    proc = subprocess.Popen(
        ["java", "-cp", JAR, "com.migration.mongo.MongoSyncMain", "--task-id", TASK],
        cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # 子进程 stdout 必须持续排空，否则管道写满后它会阻塞在写日志上（表现为"进程在、就是不干活"）
    out = []

    import threading

    def drain():
        for line in proc.stdout:
            out.append(line)

    threading.Thread(target=drain, daemon=True).start()
    proc.log = out
    return proc


def wait_phase(proc, phase, timeout=90):
    prog = os.path.join(PROJECT_ROOT, "files", TASK, "mongo_progress.json")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with open(prog) as f:
                if json.load(f).get("phase") == phase:
                    return True
        except Exception:
            pass
        if proc.poll() is not None:
            return False
        time.sleep(1)
    return False


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        for _ in range(30):
            if proc.poll() is not None:
                break
            time.sleep(0.2)
        if proc.poll() is None:
            proc.kill()


def tgt_ids():
    out = dst(f"print(JSON.stringify(db.getSiblingDB('{DB}').{COLL}"
              f".find({{}},{{_id:1}}).sort({{_id:1}}).toArray().map(d=>d._id)))")
    try:
        return json.loads(out)
    except Exception:
        return []


def main():
    if not os.path.isfile(JAR):
        raise SystemExit(f"缺少 jar: {JAR}\n先打包: mvn -pl migration-mongo -am package -DskipTests")

    import shutil
    shutil.rmtree(os.path.join(PROJECT_ROOT, "files", TASK), ignore_errors=True)
    src(f"db.getSiblingDB('{DB}').dropDatabase()")
    dst(f"db.getSiblingDB('{DB}').dropDatabase()")
    src(f"db.getSiblingDB('{DB}').createCollection('{COLL}')")
    write_config()

    print("=== Mongo 单事件失败：位点不得越过 ===")
    proc = start()
    try:
        if not wait_phase(proc, "INCREMENT"):
            record("进入增量阶段", False, "".join(proc.log[-25:]))
            return 1
        record("进入增量阶段", True)

        # 目标端建一个源端没有的唯一索引：第二条 tag 相同的文档必然写失败
        dst(f"db.getSiblingDB('{DB}').{COLL}.createIndex({{tag:1}},{{unique:true}})")
        record("目标端已建源端没有的唯一索引（判据前提）",
               "tag_1" in dst(f"print(JSON.stringify("
                              f"db.getSiblingDB('{DB}').{COLL}.getIndexes().map(i=>i.name)))"))

        src(f"db.getSiblingDB('{DB}').{COLL}.insertOne({{_id:1,tag:'same',v:'first'}})")
        ok = False
        for _ in range(60):
            if 1 in tgt_ids():
                ok = True
                break
            time.sleep(1)
        record("第一条文档正常同步", ok, str(tgt_ids()))

        # 这一条在目标端违反唯一索引，必然应用失败
        src(f"db.getSiblingDB('{DB}').{COLL}.insertOne({{_id:2,tag:'same',v:'second'}})")
        exited = False
        for _ in range(60):
            if proc.poll() is not None:
                exited = True
                break
            time.sleep(1)
        record("应用失败后进程 fail-stop 退出（不再记日志继续）",
               exited, f"returncode={proc.returncode}")
        record("失败的那条没有落到目标端（判据前提）", 2 not in tgt_ids(), str(tgt_ids()))

        prog_path = os.path.join(PROJECT_ROOT, "files", TASK, "mongo_progress.json")
        with open(prog_path) as f:
            prog = json.load(f)
        record("进度里带错误信息", bool(prog.get("error")), str(prog.get("error"))[:160])

        # 关键一步：去掉冲突约束后重启，位点若越过了 doc2 就永远补不回来
        dst(f"db.getSiblingDB('{DB}').{COLL}.dropIndex('tag_1')")
        proc2 = start()
        try:
            ok = False
            for _ in range(90):
                if 2 in tgt_ids():
                    ok = True
                    break
                if proc2.poll() is not None:
                    break
                time.sleep(1)
            record("重启后失败的那条补上来了（位点没越过它）",
                   ok, f"目标端 _id={tgt_ids()}（期望含 2）")
            record("重启后数据完整", sorted(tgt_ids()) == [1, 2], str(tgt_ids()))
        finally:
            stop(proc2)
    finally:
        stop(proc)

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n=== {passed}/{len(results)} 通过 ===")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL {name}: {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
