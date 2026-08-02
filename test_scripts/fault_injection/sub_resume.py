#!/usr/bin/env python3
"""
订阅任务（SUBSCRIBE）断点续传 / 数据一致性压测：mysql / pg / oracle 三种源。

流程：
  1. 源库建表 + 播种大表存量（不进 CDC，只为让增量跑在"大表"上）；
  2. 建订阅任务 → 下游是**专用** Kafka(39092)，不是控制面那套(29092)；
  3. 等 SUBSCRIBE_RUNNING 后持续 --minutes 分钟的高频 INSERT/UPDATE/DELETE；
  4. 期间轮流 SIGKILL capture / extract / subscribe 三个子进程，制造崩溃续传；
  5. 停写后等下游追平，把 Kafka 里的 CDC 事件按投递顺序回放，与源表逐行比对。

判定两条（见 sublib 头注释）：不丢（写入真值全部出现在 Kafka）+ 可收敛（回放最终态 == 源表）。

用法：
  python3 sub_resume.py mysql  --minutes 3
  python3 sub_resume.py pg     --minutes 3
  python3 sub_resume.py oracle --minutes 3
  python3 sub_resume.py mysql  --minutes 3 --no-inject   # 不注入故障的基线对照
"""
import argparse
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sublib as S  # noqa: E402

# 各源的默认参数：存量行数（大表）、DML 速率、注入哪些进程
PROFILES = {
    "mysql": dict(seed=200000, rate=200, engines=S.SUB_ENGINES["mysql"]),
    "pg": dict(seed=200000, rate=200, engines=S.SUB_ENGINES["pg"]),
    # Oracle 走 LogMiner，捕获吞吐远低于 binlog/WAL，速率给低一些否则永远追不平
    "oracle": dict(seed=20000, rate=20, engines=S.SUB_ENGINES["oracle"]),
    # TiDB 增量经 TiCDC changefeed → Kafka → capture 消费，比直连 binlog 多一跳，速率适中
    "tidb": dict(seed=50000, rate=100, engines=S.SUB_ENGINES["tidb"]),
    # Mongo 是单进程引擎，唯一可注入的进程就是 mongo 本身
    "mongo": dict(seed=50000, rate=150, engines=S.SUB_ENGINES["mongo"]),
}


def drain_and_check(src, prefix, writer, passed, failed, tag, token=None, tid=None,
                    max_wait=600):
    """等下游追平后做一致性判定。"""
    src_state = {k: v for k, v in src.state().items() if k >= writer_start(writer)}
    truth = set(writer.writes)
    print(f"[校验] 源(增量部分) {len(src_state)} 行；写入真值 {len(truth)} 条；"
          f"等待下游追平（最多 {max_wait}s）...")

    deadline = time.time() + max_wait
    best = None
    while time.time() < deadline:
        time.sleep(10)
        recs = S.consume_all(prefix, idle_timeout=8)
        evs = S.parse_events(recs)
        state, unparsed = S.replay(evs, idcol=src.idcol)
        got = S.written_set(evs, idcol=src.idcol)
        missing = truth - got
        best = (len(recs), state, unparsed, missing)
        print(f"    kafka 消息 {len(recs)}；回放 {len(state)} 行；缺失写入 {len(missing)}")
        if not missing and state == src_state:
            break
        if token and tid and S.get_status(token, tid) == "FAILED":
            print("    任务已 FAILED，停止等待")
            break

    nmsg, state, unparsed, missing = best
    ok_nolost = not missing
    ok_conv = (state == src_state)

    (passed if ok_nolost else failed).append(
        f"{tag}：不丢（写入真值 {len(truth)} 条，Kafka 缺 {len(missing)} 条）")
    (passed if ok_conv else failed).append(
        f"{tag}：可收敛（回放 {len(state)} 行 vs 源 {len(src_state)} 行）")

    if not ok_nolost:
        print(f"    缺失样例: {sorted(missing)[:5]}")
    if not ok_conv:
        miss = set(src_state) - set(state)
        extra = set(state) - set(src_state)
        diff = [k for k in set(state) & set(src_state) if state[k] != src_state[k]]
        print(f"    回放缺 {len(miss)} 行 样例{sorted(miss)[:5]}；"
              f"多 {len(extra)} 行 样例{sorted(extra)[:5]}；"
              f"值不同 {len(diff)} 行 样例{[(k, state[k], src_state[k]) for k in diff[:3]]}")
    if unparsed:
        print(f"    注意：{unparsed} 条事件无法解析出主键")
    return ok_nolost and ok_conv


def writer_start(writer):
    return writer._start_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=list(PROFILES.keys()))
    ap.add_argument("--minutes", type=float, default=3)
    ap.add_argument("--seed-rows", type=int, default=None)
    ap.add_argument("--rate", type=int, default=None)
    ap.add_argument("--no-inject", action="store_true", help="不注入故障（基线对照）")
    ap.add_argument("--keep", action="store_true", help="结束后保留任务不停止不删除")
    args = ap.parse_args()

    prof = PROFILES[args.kind]
    seed_rows = args.seed_rows if args.seed_rows is not None else prof["seed"]
    rate = args.rate if args.rate is not None else prof["rate"]

    src = S.SOURCES[args.kind]()
    prefix = f"fisub{args.kind}"
    passed, failed = [], []

    token = S.login()
    print(f"✓ 登录；订阅断点续传测试 源={args.kind} 存量={seed_rows} 速率≈{rate}/s "
          f"注入={'否' if args.no_inject else '是'} 时长={args.minutes}min")

    print("[准备] 清理上一轮 Kafka topic ...")
    S.delete_topics(prefix)
    print("[准备] 源库重建 + 播种大表存量 ...")
    src.reset()
    t0 = time.time()
    src.seed(seed_rows)
    print(f"    播种 {seed_rows} 行，用时 {time.time() - t0:.0f}s")

    tid = S.create_subscribe_task(token, f"FI-sub-{args.kind}-{int(time.time())}",
                                  src.source_type, src.src_conn_str(),
                                  src.sync_objects(), src.db, prefix)
    print(f"[任务] {tid}")
    st = S.wait_status(token, tid, {"SUBSCRIBE_RUNNING"}, timeout=420)
    if st != "SUBSCRIBE_RUNNING":
        failed.append(f"未进入 SUBSCRIBE_RUNNING（{st}）")
        S.stop_task(token, tid)
        return S.print_result(passed, failed)
    passed.append("订阅任务进入 SUBSCRIBE_RUNNING")

    # capture 需要一点时间真正开始跟位点，早写的数据可能落在起始位点之前
    print("[准备] 等待 30s 让 capture 稳定跟上位点 ...")
    time.sleep(30)

    start_id = 10_000_000
    w = src.make_writer(start_id=start_id, rate=rate)
    w._start_id = start_id
    w.start()
    print(f"[写入] 开始持续增删改 {args.minutes} 分钟 ...")

    kills = []
    end = time.time() + args.minutes * 60
    # 首次注入留 45s 预热，之后每 50s 轮换一个进程
    next_kill = time.time() + 45
    engines = prof["engines"]
    while time.time() < end:
        time.sleep(3)
        if w.error:
            print(f"    写入线程异常: {w.error}")
            break
        if not args.no_inject and time.time() >= next_kill:
            eng = engines[len(kills) % len(engines)]
            pids = S.signal_child(tid, eng, signal.SIGKILL)
            if pids:
                kills.append((eng, pids))
                print(f"    [{time.strftime('%H:%M:%S')}] SIGKILL {eng} pid={pids} "
                      f"(ins={w.inserts} upd={w.updates} del={w.deletes})")
            else:
                print(f"    [{time.strftime('%H:%M:%S')}] {eng} 无进程可杀（可能尚未拉起）")
            next_kill = time.time() + 50

    w.stop.set()
    w.join(timeout=60)
    print(f"[写入] 结束 ins={w.inserts} upd={w.updates} del={w.deletes} "
          f"真值={len(w.writes)} 注入崩溃={len(kills)} 次 err={w.error}")

    if not args.no_inject:
        alive = {e: len(S.child_pids(tid, e)) for e in engines}
        print(f"[自愈] 崩溃后子进程存活情况: {alive}")
        all_up = all(v > 0 for v in alive.values())
        (passed if all_up else failed).append(f"崩溃后三个子进程均自愈重启（{alive}）")

    drain_and_check(src, prefix, w, passed, failed,
                    f"{args.kind}/{'基线' if args.no_inject else f'{len(kills)}次崩溃'}",
                    token=token, tid=tid)

    final_st = S.get_status(token, tid)
    print(f"[任务] 最终状态 {final_st}")
    (passed if final_st == "SUBSCRIBE_RUNNING" else failed).append(
        f"测试结束时任务仍在 SUBSCRIBE_RUNNING（实际 {final_st}）")

    if args.keep:
        print(f"[保留] 任务 {tid} 未停止未删除")
    else:
        S.stop_task(token, tid)
        time.sleep(3)
        S.delete_task(token, tid)
    return S.print_result(passed, failed)


if __name__ == "__main__":
    sys.exit(main())
