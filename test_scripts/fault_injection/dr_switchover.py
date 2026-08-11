#!/usr/bin/env python3
"""
计划内主备切换（Switchover）的零丢失判据（mysql2mysql 灾备）。

背景：平台此前只有一种倒换——交换连接串 → 清位点与中间态 → 重新拉起。
它既不停旧主的写、也不等链路追平，而 `FailoverService.cleanFailoverFiles()` 还会把
`thl_output/` 整个清掉：**已捕获但尚未应用的变更就此消失**。
计划外接管这么做是 RPO 的固有代价；计划内切换这么做就是白丢数据。

现在拆成两个端点：
  * `POST /{id}/failover`    计划外接管，有损（行为不变）
  * `POST /{id}/switchover`  计划内切换：停旧主写 → 等追平 → 才切；追不平就什么都不改

四把尺子：

1. **有积压时必须拒绝切换** —— 把增量应用限速压到很低，写一批数据制造未应用的 THL 积压，
   然后用一个很短的追平超时请求计划内切换：必须**失败**，而不是"切过去然后丢掉积压"。
   （不用 SIGSTOP 冻 increment：那会触发僵死看门狗把任务判 FAILED，测的就不是切换语义了。）
2. **拒绝之后旧主要能继续写** —— 停写是为追平服务的，追不平就得把只读解除，
   否则业务被一次失败的切换永久卡死。
3. **积压消化后切换成功，且一行不丢** —— 给足追平时间重新切换，
   两端指纹必须相等（切换前写入的行一条都不能少）。
4. **切换后旧主保持只读** —— 它现在是备库，继续接受业务写入就是双写分叉。

前置：
    docker compose -f docker-compose-synctask-dr.yml up -d dr-mysql-a dr-mysql-b

用法：
    python3 test_scripts/fault_injection/dr_switchover.py
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drlib as DR  # noqa: E402
import faultlib as F  # noqa: E402

LINK = "mysql2mysql"


def switchover(token, task_id, timeout_ms=20000):
    """计划内切换是**同步**接口（要等停写+追平的结论），所以 HTTP 超时必须比 drain 超时长。
    F.api 写死 60s，这里自己发请求。"""
    import requests
    r = requests.post(f"{F.BASE_URL}/api/workflows/{task_id}/switchover",
                      headers={"Authorization": f"Bearer {token}"},
                      json={"drainTimeoutMs": timeout_ms, "fence": True},
                      timeout=timeout_ms / 1000.0 + 180)
    return r.json()


A_CFG = dict(host="127.0.0.1", port=33320, user="root", password="rootpassword")


def read_only():
    return F.sql_fetch(A_CFG, None, "SELECT @@global.read_only")[0][0]


def main():
    token = F.login()
    prev_quota = F.get_increment_quota()
    # 限速必须在**建任务之前**设好：它随 config.properties 一次性下发给增量子进程
    F.set_increment_quota(20)
    a, b = DR.reset_both(LINK, seed_rows=200, seed_side="a")
    task_id = DR.create_dr_task(token, f"dr-switchover-{int(time.time())}", LINK)
    print(f"    taskId={task_id}  A=33320(主) B=33321(备)，增量限速 20 行/秒")
    passed, failed = [], []
    try:
        if F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420) != "INCREMENT_RUNNING":
            print("任务未进入增量，放弃")
            sys.exit(2)
        DR.wait_converge(a, b, timeout=300)

        # ---- 尺子1：制造积压，短超时的切换必须被拒 ----
        a.seed(400)   # 20 行/秒 → 约 20 秒才追得平
        time.sleep(5)
        r = switchover(token, task_id, timeout_ms=3000)
        ok = not r.get("success")
        (passed if ok else failed).append(
            f"有积压时计划内切换被拒（{r.get('message')}）"
            + ("" if ok else "  ← 切过去了，未应用的积压会被 cleanFailoverFiles 清掉"))

        # ---- 尺子2：拒绝之后旧主必须解除只读 ----
        ro = read_only()
        for _ in range(30):
            if str(ro) in ("0", "OFF", "False", "false"):
                break
            time.sleep(2)
            ro = read_only()
        ok = str(ro) in ("0", "OFF", "False", "false")
        (passed if ok else failed).append(
            f"切换失败后旧主只读已解除（read_only={ro}）"
            + ("" if ok else "  ← 业务被一次失败的切换永久卡死"))

        # ---- 尺子3：给足追平时间，切换必须成功且一行不丢 ----
        src_before = DR.fingerprint(a)
        r = switchover(token, task_id, timeout_ms=180000)
        ok = bool(r.get("success"))
        (passed if ok else failed).append(f"追平后计划内切换成功（{r.get('message')}）")

        if ok:
            F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=420)
            time.sleep(15)
            tgt_after = DR.fingerprint(b)
            same = (src_before == tgt_after)
            (passed if same else failed).append(
                f"切换前的数据一行不丢：切换前源 {DR.fmt(src_before)} vs 新主 {DR.fmt(tgt_after)}")

            # ---- 尺子4：旧主保持只读 ----
            ro2 = read_only()
            keep = str(ro2) in ("1", "ON", "True", "true")
            (passed if keep else failed).append(
                f"切换后旧主保持只读（read_only={ro2}）"
                + ("" if keep else "  ← 旧主还能写，两端各写各的就是双写分叉"))
    finally:
        # 用例把 A 置成了只读，收尾一定要还原，否则后面所有用 33320 的用例都会写不进去
        try:
            F.sql_exec(A_CFG, ["SET GLOBAL read_only = OFF"])
        except Exception:
            subprocess.run(["docker", "exec", "dr-mysql-a", "mysql", "-uroot", "-prootpassword",
                            "-e", "SET GLOBAL read_only=OFF"], capture_output=True)
        if prev_quota is not None:
            F.set_increment_quota(prev_quota)
        else:
            F.sql_exec(F.META_DB, ["UPDATE resource_quotas SET max_increment_rows_per_sec=NULL WHERE user_id=1"],
                       db="sync_task_db")
        DR.cleanup_task(token, task_id)

    for p in passed:
        print(f"   ✓ {p}")
    F.print_result(passed, failed)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
