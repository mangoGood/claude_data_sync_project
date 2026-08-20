#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""同步平台自动化测试入口。

一般不直接调用，走 autotest/autotest_run（它负责挑解释器、写 PID、落日志）。
"""
import argparse
import atexit
import datetime
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from autotest.framework import cleanup as CL          # noqa: E402
from autotest.framework import config as C            # noqa: E402
from autotest.framework import report as R            # noqa: E402
from autotest.framework import runner as RUN          # noqa: E402
from autotest.framework.api import Api, ApiError      # noqa: E402
from autotest.framework.case import SUITES            # noqa: E402
from autotest.framework.state import State, FAILED, PASSED, SKIPPED  # noqa: E402
import autotest.suites  # noqa: E402,F401  （import 即注册全部 suite）


def build_parser():
    p = argparse.ArgumentParser(
        prog="autotest_run",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="同步平台端到端自动化测试（单线程串行执行）",
        epilog="""\
示例：
  autotest_run                          # 默认跑全部链路（约 13 分钟）
  autotest_run --profile standard       # 只跑主干链路（约 5 分钟）
  autotest_run --profile quick          # 冒烟（约 2 分钟）
  autotest_run --suite sync_mysql2mysql # 只跑 mysql→mysql 同步
  autotest_run --suite dr_mysql         # 只跑 mysql 灾备（单向 + 双向）
  autotest_run --suite subscribe_mysql  # 只跑 mysql 数据订阅
  autotest_run --group dr               # 只跑灾备分组
  autotest_run --resume                 # 接着上次跑（跳过已通过的用例）
  autotest_run --cleanup never          # 跑完不清理，留现场
  autotest_run --list                   # 列出全部用例
""")
    p.add_argument("--profile", default=os.environ.get("AT_PROFILE", "full"),
                   choices=sorted(RUN.PROFILES),
                   help="用例集：full=全部链路（默认，≈13 分钟）/ standard=主干（≈5 分钟）/ quick=冒烟（≈2 分钟）")
    p.add_argument("--suite", action="append", default=[],
                   help="只跑指定用例，支持前缀与通配（可重复；如 dr_mysql、sync_*）")
    p.add_argument("--group", action="append", default=[],
                   choices=["sync", "dr", "subscribe", "traffic", "feature"], help="只跑指定分组（可重复）")
    p.add_argument("--cleanup", default=os.environ.get("AT_CLEANUP", "auto"),
                   choices=["auto", "always", "never"],
                   help="清理策略：auto=通过就删、失败保留（默认）；always=一律删；never=一律留")
    p.add_argument("--resume", action="store_true",
                   help="接着上次的状态跑，跳过已通过的用例（默认从头开始）")
    p.add_argument("--budget-mins", type=int, default=int(os.environ.get("AT_BUDGET_MINS", "30")),
                   help="总时长预算（分钟），塞不下的用例跳过并在报告里注明；0 = 不限制")
    p.add_argument("--seed-rows", type=int, default=None, help="覆盖存量行数")
    p.add_argument("--list", action="store_true", help="列出全部用例后退出")
    p.add_argument("--clean-only", action="store_true",
                   help="不跑用例，只把上次状态文件里登记的残留资源清掉")
    p.add_argument("--verbose", action="store_true", help="打印异常栈")
    return p


def cmd_list():
    print("已注册用例（%d 条）：\n" % len(SUITES))
    groups = {}
    for k, s in sorted(SUITES.items()):
        groups.setdefault(s.group, []).append((k, s))
    for g in ("sync", "dr", "subscribe", "traffic", "feature"):
        if g not in groups:
            continue
        print("  [%s]" % g)
        for k, s in groups[g]:
            print("    %-22s %-46s 预估 %ds" % (k, s.title, s.est_secs))
        print()
    print("用例集：")
    for name, keys in RUN.PROFILES.items():
        print("  %-9s %s" % (name, "全部用例" if keys is None else "、".join(keys)))
    return 0


def _write_pidfile():
    """自己写 PID 文件：autotest_run 里是 `python | tee`，bash 的 $! 拿到的是 tee 的 PID，
    照它去 kill 只会杀掉 tee，用例进程还在跑。"""
    try:
        with open(C.PID_FILE, "w") as f:
            f.write(str(os.getpid()))
    except OSError:
        pass


def _clear_pidfile():
    try:
        os.remove(C.PID_FILE)
    except OSError:
        pass


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.list:
        return cmd_list()

    if args.seed_rows:
        C.SEED_ROWS = args.seed_rows

    state = State().load()
    api = Api()

    try:
        api.login()
    except ApiError as e:
        print("!! 无法连接后端 %s：%s" % (C.BASE_URL, e))
        print("   请先确认 ./start.sh 已把 backend(38080) 与 agent 起起来。")
        return 2

    if args.clean_only:
        print("清理状态文件中登记的残留资源（%s）..." % C.STATE_FILE)
        n = CL.clean_state(api, state)
        print("已处理 %d 条用例的残留。" % n)
        return 0

    suites = RUN.select(args.profile, args.suite, args.group)
    _write_pidfile()
    atexit.register(_clear_pidfile)

    run_id = time.strftime("%Y%m%d-%H%M%S")
    started_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if args.resume and state.data.get("run_id"):
        run_id = state.data["run_id"]
        done = [k for k, v in (state.data.get("suites") or {}).items()
                if v.get("status") == PASSED]
        print(">> 续跑模式：沿用运行 ID %s，已通过 %d 条用例将跳过" % (run_id, len(done)))
    else:
        state.reset(run_id, args.profile, " ".join(sys.argv[1:]))

    class Opts:
        cleanup = args.cleanup
        resume = args.resume
        budget_secs = max(args.budget_mins, 0) * 60
        verbose = args.verbose

    print("=" * 78)
    print("  同步平台端到端自动化测试")
    print("  运行 ID   : %s" % run_id)
    print("  用例集    : %s（%d 条用例，串行执行）" % (args.profile, len(suites)))
    print("  后端      : %s" % C.BASE_URL)
    print("  清理策略  : %s" % {"auto": "通过即清理，失败保留现场",
                                 "always": "一律清理", "never": "一律保留"}[args.cleanup])
    print("  时长预算  : %s" % ("%d 分钟" % args.budget_mins if args.budget_mins else "不限"))
    print("  规模      : 存量 %d 行 / 增量 %d 行" % (C.SEED_ROWS, C.INCR_ROWS))
    print("=" * 78)

    runner = RUN.Runner(api, state, Opts, log=print)
    try:
        runner.run(suites)
    except KeyboardInterrupt:
        print("\n>> 已中断。现场保留，可用 autotest_run --resume 接着跑。")

    # ---------------- 报告 ----------------
    out_dir = os.path.join(C.REPORT_DIR, run_id)
    meta = {"profile": args.profile, "started_str": started_str, "base_url": C.BASE_URL,
            "cleanup": args.cleanup, "budget_mins": args.budget_mins}
    rows = R.write_all(out_dir, state, SUITES, run_id, meta)
    t = R.totals(rows)

    latest = os.path.join(C.REPORT_DIR, "latest")
    try:
        if os.path.islink(latest) or os.path.exists(latest):
            os.remove(latest)
        os.symlink(out_dir, latest)
    except OSError:
        pass

    print("")
    print("=" * 78)
    print("  测试结论：%d 通过 / %d 未通过 / %d 跳过（断言 %d 通过 / %d 失败），总耗时 %d分%02d秒"
          % (t["passed"], t["failed"], t["skipped"], t["checks_passed"], t["checks_failed"],
             int(t["duration"] // 60), int(t["duration"] % 60)))
    print("=" * 78)
    for r in rows:
        icon = {"PASSED": "✓", "FAILED": "✗"}.get(r["status"], "–")
        ids = ("  任务 id: " + ", ".join(r["task_ids"])) if (
            r["status"] == "FAILED" and r["task_ids"]) else ""
        print("  %s %-22s %-46s %s%s"
              % (icon, r["key"], r["title"][:46],
                 "%d/%d" % (r["passed_checks"], len(r["checks"])), ids))
        if r["status"] == "FAILED":
            for c in r["checks"]:
                if not c["ok"]:
                    print("      ✗ %s%s" % (c["name"], (" — " + c["detail"]) if c["detail"] else ""))
            if r.get("error"):
                print("      中止：%s" % r["error"])
        if r["status"] == "SKIPPED" and r.get("error"):
            print("      跳过原因：%s" % r["error"])

    if t["failed"]:
        print("")
        print("  未通过链路的任务与数据库**已保留**，可直接进平台排查；跑完排查后用")
        print("    autotest/autotest_stop --clean   清理残留")
    print("")
    print("  报告：%s" % out_dir)
    print("        report.html / report.md / report.json / junit.xml（Jenkins 用 junit.xml）")
    print("")

    return 1 if t["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
