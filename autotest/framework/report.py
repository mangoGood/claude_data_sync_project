#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试报告：JUnit XML（Jenkins 直接吃）+ HTML + JSON + 控制台摘要。

报告必须写明**未通过用例的任务 id** —— 失败时任务和库都刻意保留，
没有 id 就没法回到现场排查，那样保留现场也就白保留了。
"""
import html
import json
import os
import time
from xml.sax.saxutils import escape, quoteattr

STATUS_ICON = {"PASSED": "✓", "FAILED": "✗", "SKIPPED": "–", "RUNNING": "…"}


def _dur(sec):
    sec = int(sec or 0)
    return "%d分%02d秒" % (sec // 60, sec % 60)


def collect(state, suites_meta):
    """把状态文件整理成报告用的结构。"""
    rows = []
    for key, s in (state.data.get("suites") or {}).items():
        meta = suites_meta.get(key)
        checks = s.get("checks") or []
        rows.append({
            "key": key,
            "title": meta.title if meta else key,
            "group": meta.group if meta else "-",
            "status": s.get("status") or "SKIPPED",
            "duration": s.get("duration") or 0.0,
            "task_ids": s.get("task_ids") or [],
            "error": s.get("error"),
            "checks": checks,
            "passed_checks": sum(1 for c in checks if c.get("ok")),
            "failed_checks": sum(1 for c in checks if not c.get("ok")),
            "kept": bool(s.get("resources")),
        })
    order = {"FAILED": 0, "PASSED": 1, "SKIPPED": 2}
    rows.sort(key=lambda r: (order.get(r["status"], 3), r["key"]))
    return rows


def totals(rows):
    return {
        "suites": len(rows),
        "passed": sum(1 for r in rows if r["status"] == "PASSED"),
        "failed": sum(1 for r in rows if r["status"] == "FAILED"),
        "skipped": sum(1 for r in rows if r["status"] == "SKIPPED"),
        "checks": sum(len(r["checks"]) for r in rows),
        "checks_passed": sum(r["passed_checks"] for r in rows),
        "checks_failed": sum(r["failed_checks"] for r in rows),
        "duration": sum(r["duration"] for r in rows),
    }


# ----------------------------------------------------------------- JUnit
def write_junit(path, rows, run_id):
    """每个 check 一个 <testcase>，classname=suite —— Jenkins 上能直接点开看是哪一条断言挂了。"""
    t = totals(rows)
    out = ['<?xml version="1.0" encoding="UTF-8"?>']
    out.append('<testsuites name="synctask-autotest" tests="%d" failures="%d" time="%.1f">'
               % (t["checks"], t["checks_failed"], t["duration"]))
    for r in rows:
        cls = "autotest.%s.%s" % (r["group"], r["key"])
        checks = r["checks"]
        if r["status"] == "SKIPPED" and not checks:
            out.append('  <testsuite name=%s tests="1" failures="0" skipped="1" time="0">'
                       % quoteattr(r["key"]))
            out.append('    <testcase classname=%s name="suite">' % quoteattr(cls))
            out.append('      <skipped message=%s/>' % quoteattr(str(r.get("error") or "已跳过")))
            out.append('    </testcase>')
            out.append('  </testsuite>')
            continue
        out.append('  <testsuite name=%s tests="%d" failures="%d" time="%.1f">'
                   % (quoteattr(r["key"]), max(len(checks), 1), r["failed_checks"], r["duration"]))
        for c in checks:
            out.append('    <testcase classname=%s name=%s time="0">'
                       % (quoteattr(cls), quoteattr(c["name"])))
            if not c.get("ok"):
                msg = c.get("detail") or "断言失败"
                body = "%s\n未通过任务 id: %s" % (msg, ", ".join(r["task_ids"]) or "(无)")
                out.append('      <failure message=%s>%s</failure>'
                           % (quoteattr(msg[:200]), escape(body)))
            out.append('    </testcase>')
        if r["status"] == "FAILED" and r["failed_checks"] == 0:
            # suite 异常退出（没有具体失败断言）也要在 Jenkins 上显形
            out.append('    <testcase classname=%s name="suite-execution" time="0">' % quoteattr(cls))
            out.append('      <failure message=%s>%s</failure>'
                       % (quoteattr(str(r.get("error"))[:200]),
                          escape("%s\n未通过任务 id: %s" % (r.get("error"), ", ".join(r["task_ids"])))))
            out.append('    </testcase>')
        out.append('  </testsuite>')
    out.append('</testsuites>')
    with open(path, "w") as f:
        f.write("\n".join(out))


# ----------------------------------------------------------------- JSON
def write_json(path, rows, run_id, meta):
    payload = {"run_id": run_id, "generated_at": time.time(), "meta": meta,
               "totals": totals(rows), "suites": rows}
    with open(path, "w") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ----------------------------------------------------------------- Markdown
def write_markdown(path, rows, run_id, meta):
    t = totals(rows)
    L = ["# 同步平台自动化测试报告", "",
         "- 运行 ID：`%s`" % run_id,
         "- 开始时间：%s" % meta.get("started_str"),
         "- 用例集：%s" % meta.get("profile"),
         "- 总耗时：%s" % _dur(t["duration"]),
         "- 结论：**%s**" % ("全部通过" if t["failed"] == 0 else "有 %d 条链路未通过" % t["failed"]),
         "",
         "| 结论 | 链路 | 分组 | 断言 | 耗时 | 任务 id |",
         "| --- | --- | --- | --- | --- | --- |"]
    for r in rows:
        ids = ", ".join("`%s`" % i for i in r["task_ids"]) if r["task_ids"] else "-"
        L.append("| %s %s | %s | %s | %d/%d | %s | %s |" % (
            STATUS_ICON.get(r["status"], "?"), r["status"], r["title"], r["group"],
            r["passed_checks"], len(r["checks"]), _dur(r["duration"]), ids))
    failed = [r for r in rows if r["status"] == "FAILED"]
    if failed:
        L += ["", "## 未通过明细（任务与数据库已保留，供排查）", ""]
        for r in failed:
            L.append("### %s（%s）" % (r["title"], r["key"]))
            L.append("")
            L.append("- 任务 id：%s" % (", ".join("`%s`" % i for i in r["task_ids"]) or "(未创建成功)"))
            if r.get("error"):
                L.append("- 中止原因：%s" % r["error"])
            for c in r["checks"]:
                if not c.get("ok"):
                    L.append("- ✗ %s%s" % (c["name"], (" — %s" % c["detail"]) if c["detail"] else ""))
            L.append("")
    with open(path, "w") as f:
        f.write("\n".join(L) + "\n")


# ----------------------------------------------------------------- HTML
_CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif;
     margin:0;padding:32px;background:#f6f7f9;color:#1c1e21}
h1{font-size:22px;margin:0 0 4px}
.sub{color:#666;font-size:13px;margin-bottom:20px}
.cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px}
.card{background:#fff;border:1px solid #e3e5e8;border-radius:8px;padding:14px 18px;min-width:110px}
.card .n{font-size:24px;font-weight:600}
.card .l{font-size:12px;color:#666;margin-top:2px}
.ok{color:#17803d}.bad{color:#c0392b}.skip{color:#8a8f98}
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e3e5e8;border-radius:8px;overflow:hidden}
th,td{padding:9px 12px;text-align:left;font-size:13px;border-bottom:1px solid #eceef0;vertical-align:top}
th{background:#fafbfc;font-weight:600;color:#444}
tr:last-child td{border-bottom:none}
code{background:#f0f1f3;padding:1px 5px;border-radius:4px;font-size:12px}
details{margin:6px 0}
summary{cursor:pointer;font-size:12px;color:#555}
.chk{font-size:12px;margin:3px 0 3px 14px}
.badge{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;font-weight:600}
.b-pass{background:#e4f5ea;color:#17803d}.b-fail{background:#fdeaea;color:#c0392b}
.b-skip{background:#eef0f2;color:#8a8f98}
.note{background:#fff8e6;border:1px solid #f0dca8;border-radius:8px;padding:12px 16px;
      font-size:13px;margin-bottom:20px}
"""


def write_html(path, rows, run_id, meta):
    t = totals(rows)
    E = html.escape
    P = ['<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">',
         '<title>同步平台自动化测试报告 %s</title>' % E(run_id),
         '<style>%s</style></head><body>' % _CSS,
         '<h1>同步平台自动化测试报告</h1>',
         '<div class="sub">运行 ID <code>%s</code> · %s · 用例集 %s · 总耗时 %s</div>'
         % (E(run_id), E(str(meta.get("started_str"))), E(str(meta.get("profile"))), _dur(t["duration"]))]
    P.append('<div class="cards">')
    for n, l, c in ((t["passed"], "通过链路", "ok"), (t["failed"], "未通过链路", "bad"),
                    (t["skipped"], "跳过", "skip"), (t["checks_passed"], "通过断言", "ok"),
                    (t["checks_failed"], "失败断言", "bad")):
        P.append('<div class="card"><div class="n %s">%d</div><div class="l">%s</div></div>' % (c, n, l))
    P.append('</div>')
    if t["failed"]:
        kept = [r for r in rows if r["status"] == "FAILED"]
        ids = ", ".join("<code>%s</code>" % E(i) for r in kept for i in r["task_ids"])
        P.append('<div class="note"><b>未通过链路的任务与数据库已保留</b>，未做清理，可直接进平台排查。'
                 '任务 id：%s</div>' % (ids or "(无，任务未创建成功)"))
    P.append('<table><tr><th>结论</th><th>链路</th><th>分组</th><th>断言</th><th>耗时</th>'
             '<th>任务 id</th><th>明细</th></tr>')
    for r in rows:
        cls = {"PASSED": "b-pass", "FAILED": "b-fail"}.get(r["status"], "b-skip")
        ids = "<br>".join("<code>%s</code>" % E(i) for i in r["task_ids"]) or "-"
        det = []
        if r.get("error"):
            det.append('<div class="chk bad">中止：%s</div>' % E(str(r["error"])))
        for c in r["checks"]:
            k = "ok" if c["ok"] else "bad"
            det.append('<div class="chk %s">%s %s%s</div>'
                       % (k, "✓" if c["ok"] else "✗", E(c["name"]),
                          E(" — " + c["detail"]) if c["detail"] else ""))
        body = ('<details><summary>%d 条断言</summary>%s</details>'
                % (len(r["checks"]), "".join(det))) if det else "-"
        P.append('<tr><td><span class="badge %s">%s</span></td><td>%s</td><td>%s</td>'
                 '<td>%d/%d</td><td>%s</td><td>%s</td><td>%s</td></tr>'
                 % (cls, r["status"], E(r["title"]), E(r["group"]),
                    r["passed_checks"], len(r["checks"]), _dur(r["duration"]), ids, body))
    P.append('</table></body></html>')
    with open(path, "w") as f:
        f.write("\n".join(P))


def write_all(out_dir, state, suites_meta, run_id, meta):
    os.makedirs(out_dir, exist_ok=True)
    rows = collect(state, suites_meta)
    write_junit(os.path.join(out_dir, "junit.xml"), rows, run_id)
    write_json(os.path.join(out_dir, "report.json"), rows, run_id, meta)
    write_markdown(os.path.join(out_dir, "report.md"), rows, run_id, meta)
    write_html(os.path.join(out_dir, "report.html"), rows, run_id, meta)
    return rows
