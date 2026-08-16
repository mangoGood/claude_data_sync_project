#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调度器：选用例 → 串行执行 → 实时进度 → 逐条清理 → 出报告。

刻意的几个取舍：
  - **串行**。一次只跑一个任务：并发跑多条链路会让 agent 同时拉起十几个 JVM 子进程，
    机器扛不住时失败原因全是资源不足，测不出产品问题。
  - **续跑粒度是 suite**。一条跑到一半的实时同步场景从中间接着跑没有意义，必须整条重来。
  - **失败即保留现场**：任务不停不删、库不删，任务 id 进报告。
"""
import fnmatch
import signal
import time

from . import cleanup as CL
from . import config as C
from . import report as R
from .case import SUITES, Ctx, SuiteAbort, SuiteSkip
from .state import FAILED, PASSED, RUNNING, SKIPPED

# ----------------------------------------------------------------- 用例集
PROFILES = {
    # 冒烟：改完代码先跑这个，实测 ≈2 分钟
    "quick": ["sync_mysql2mysql", "dr_mysql_uni", "subscribe_mysql"],
    # 主干：跑得起量又不占太久，实测 ≈5 分钟
    "standard": ["sync_mysql2mysql", "sync_mysql2pg", "sync_pg2pg", "sync_pg2mysql",
                 "sync_mongo2mongo", "dr_mysql_uni", "dr_mysql_bidi", "subscribe_mysql"],
    # 默认：所有已有链路的端到端覆盖，实测 ≈13 分钟（默认预算 30 分钟，留足余量）
    "full": None,   # None = 全部已注册 suite
}


def select(profile, patterns, groups):
    """按 profile / 通配模式 / 分组挑用例，返回有序 suite 列表。

    模式支持精确名、前缀和通配符：`dr_mysql` 命中单向+双向两条，`sync_*` 命中所有同步链路。
    """
    keys = list(SUITES.keys())
    chosen = None

    if patterns or groups:
        # --suite 与 --group 同时给时取**并集**：两者都是"我要跑这些"，
        # 让其中一个静默失效，用户只会以为用例丢了
        chosen = []
        for pat in patterns:
            hit = [k for k in keys
                   if k == pat or k.startswith(pat) or fnmatch.fnmatch(k, pat)
                   or pat in SUITES[k].tags]
            if not hit:
                raise SystemExit("没有匹配 '%s' 的用例。用 --list 看全部用例名。" % pat)
            chosen += [k for k in hit if k not in chosen]
        if groups:
            g = [k for k in keys if SUITES[k].group in groups and k not in chosen]
            if not g and not chosen:
                raise SystemExit("没有属于分组 %s 的用例。" % groups)
            chosen += g
    else:
        want = PROFILES.get(profile)
        if want is None:
            chosen = keys
        else:
            missing = [k for k in want if k not in SUITES]
            if missing:
                raise SystemExit("用例集 %s 引用了不存在的用例: %s" % (profile, missing))
            chosen = list(want)

    order = {"sync": 0, "dr": 1, "subscribe": 2, "feature": 3}
    chosen.sort(key=lambda k: (order.get(SUITES[k].group, 9), k))
    return [SUITES[k] for k in chosen]


# ----------------------------------------------------------------- 执行
class Runner:
    def __init__(self, api, state, opts, log=print):
        self.api = api
        self.state = state
        self.opts = opts
        self.log = log
        self.stopping = False
        self.started = time.time()
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

    def _on_signal(self, signum, frame):
        # 第二次信号才硬退：第一次给正在跑的 suite 一个收尾窗口（落盘状态 + 出报告）
        if self.stopping:
            raise KeyboardInterrupt("收到第二次终止信号，立即退出")
        self.stopping = True
        self.log("\n>> 收到终止信号（%s），当前用例跑完即停。再按一次立即退出。" % signum)

    def elapsed(self):
        return time.time() - self.started

    def budget_left(self):
        if not self.opts.budget_secs:
            return float("inf")
        return self.opts.budget_secs - self.elapsed()

    # -------------------------------------------------- 主循环
    def run(self, suites):
        total = len(suites)
        for i, s in enumerate(suites, 1):
            if self.stopping:
                self._skip(s, "收到终止信号，未执行")
                continue

            if self.opts.resume and self.state.is_passed(s.key):
                self.log("[%d/%d] %-22s 上轮已通过，跳过（--resume）" % (i, total, s.key))
                continue

            # 预算调度：塞不下就明说跳过，而不是跑到一半被 kill 掉留一堆残留
            left = self.budget_left()
            if left < s.est_secs and left != float("inf"):
                self._skip(s, "剩余时长预算 %d 秒 < 本用例预估 %d 秒" % (int(left), s.est_secs))
                continue

            ok, reason = s.preflight()
            if not ok:
                self._skip(s, "环境不满足：%s" % reason)
                continue

            self._run_one(i, total, s)

        return self.state

    def _skip(self, s, reason):
        self.log("[--] %-22s SKIP — %s" % (s.key, reason))
        self.state.mark(s.key, SKIPPED, error=reason, duration=0.0)

    def _header(self, i, total, s):
        el = self.elapsed()
        budget = ("/ 预算 %d 分" % (self.opts.budget_secs // 60)) if self.opts.budget_secs else ""
        self.log("")
        self.log("=" * 78)
        self.log("[%d/%d] %d%%  %s" % (i, total, int(i * 100 / total), s.title))
        self.log("       用例 %s · 分组 %s · 预估 %d 秒 · 已用 %d分%02d秒 %s"
                 % (s.key, s.group, s.est_secs, int(el // 60), int(el % 60), budget))
        self.log("=" * 78)

    def _run_one(self, i, total, s):
        self._header(i, total, s)
        st = self.state.suite(s.key)
        st.update({"status": RUNNING, "checks": [], "error": None,
                   "started_at": time.time()})
        self.state.save()

        ctx = Ctx(self.api, self.state, s.key, self.log)
        t0 = time.time()
        status, err = PASSED, None
        try:
            s.run(ctx)
            if ctx.failed:
                status, err = FAILED, "%d 条断言未通过" % ctx.failed
        except SuiteSkip as e:
            status, err = SKIPPED, str(e)
        except SuiteAbort as e:
            status, err = FAILED, "前置断言失败后中止：%s" % e
        except KeyboardInterrupt:
            status, err = FAILED, "被用户中断"
            self.stopping = True
        except Exception as e:  # noqa: BLE001
            status, err = FAILED, "用例执行异常：%s: %s" % (type(e).__name__, e)
            self.log("    !! %s" % err)
            if self.opts.verbose:
                import traceback
                traceback.print_exc()

        dur = time.time() - t0
        self.state.mark(s.key, status, error=err, duration=dur)

        tids = self.state.suite(s.key).get("task_ids") or []
        self.log("  → %s（%d 通过 / %d 失败，耗时 %d分%02d秒）"
                 % (status, ctx.passed, ctx.failed, int(dur // 60), int(dur % 60)))

        self._cleanup_suite(s, status, tids)

    def _cleanup_suite(self, s, status, tids):
        policy = self.opts.cleanup
        if policy == "never":
            self.log("  保留本用例的任务与数据库（--cleanup never）")
            return
        if status == FAILED and policy != "always":
            self.log("  ** 用例未通过：任务与数据库已保留，供排查。任务 id: %s"
                     % (", ".join(tids) or "(未创建成功)"))
            return
        res = self.state.suite(s.key).get("resources") or []
        if not res:
            return
        self.log("  清理本用例创建的 %d 项资源（任务/数据库/索引/topic）..." % len(res))
        CL.clean_resources(self.api, res, log=self.log)
        self.state.suite(s.key)["resources"] = []
        self.state.save()
