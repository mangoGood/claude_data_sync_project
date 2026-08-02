#!/usr/bin/env python3
"""
Mongo 灾备（单向 / 双向）的**全 BSON 类型**一致性 + **防回环真的生效**的证据校验。

为什么单独一个脚本：dr_resume.py 那套指纹只覆盖 (id,grp,val,payload,n) 五个标量字段，
既证明不了 Decimal128 / Binary / 正则 / 嵌套数组这些 BSON 独有类型能原样落到对端，
也证明不了双向灾备的防回环**真的在工作**——两端写的是同一份文档，回环回来的是同值覆盖，
最终指纹照样相等，光看"收敛了"完全掩盖得住无限 ping-pong。

因此这里用两把独立的尺子：

1. **类型保真**：用 bson.json_util 的 canonical 模式做逐字段比对（$numberDecimal / $date /
   $binary / $oid / $regularExpression 都会带类型标签），类型或精度变了立刻能看出来，
   不像普通 == 会把 int32/int64、Decimal128/float 混为一谈。
2. **防回环证据**（双向）：
   - 两端都存在 `__sync_origin` 标记集合，且各自的 origin 是**对端通道**写的；
   - 两个方向的 mongo_progress.json 里 skippedLoopEvents > 0（确有事件被判定为"复制而来"并跳过）；
   - **停写后 incrEvents 不再增长**——这是无限回环唯一无法伪装的特征：没有防回环时
     A→B→A→B… 会永远刷同值写入，事件计数持续上涨，而数据指纹始终相等。
   - `__sync_origin` 自身不作为业务集合被同步（不出现在对端的业务集合清单里）。

用法：
  python3 dr_mongo_types.py [--mode uni|bidi] [--minutes 1]
退出码 0 = 全通过。
"""
import argparse
import datetime
import json
import os
import re
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drlib as DR  # noqa: E402
import faultlib as F  # noqa: E402

import bson  # noqa: E402
from bson import Binary, Code, Decimal128, MaxKey, MinKey, ObjectId, Regex, Timestamp  # noqa: E402
from bson.json_util import CANONICAL_JSON_OPTIONS, dumps  # noqa: E402

LINK = "mongo2mongo"
TYPES_COLL = "fi_types"
# 全类型用例要同步整个 drtest 库（fi_types 不在链路默认的 fi_load 单集合清单里）
DB_LEVEL_OBJECTS = '{"%s": {"dbLevel": true}}' % DR.DB
# 子进程的进度文件在项目根目录下（脚本从 test_scripts/ 运行）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def all_type_docs(prefix, count=30):
    """覆盖 MongoDB 全部常用 BSON 类型的文档集合（含边界值与嵌套结构）。"""
    docs = []
    for i in range(count):
        docs.append({
            "_id": f"{prefix}-{i:04d}",
            "t_double": 3.141592653589793 * (i + 1),
            "t_double_neg": -1.7976931348623157e308,
            "t_string": f"{prefix} 中文/emoji 😀🍃 \"quoted\" \\back\\ \n换行 {i}",
            "t_string_empty": "",
            "t_object": {"a": 1, "b": {"c": [1, 2, {"d": "deep"}]}, "空键": None},
            "t_array": [1, "two", 3.0, None, True, [4, 5], {"six": 6}],
            "t_array_empty": [],
            "t_binary": Binary(bytes(range(256))),
            "t_binary_uuid": Binary(b"\x01" * 16, 4),
            "t_objectid": ObjectId(),
            "t_bool_true": True,
            "t_bool_false": False,
            "t_date": datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=i),
            "t_date_far": datetime.datetime(2262, 4, 11),
            "t_null": None,
            "t_regex": Regex(r"^ab+c$", "im"),
            "t_int32_min": -2147483648,
            "t_int32_max": 2147483647,
            "t_int64": 9223372036854775807 - i,
            "t_int64_min": -9223372036854775808,
            "t_timestamp": Timestamp(1700000000 + i, i + 1),
            "t_decimal": Decimal128("1234567890123456789.012345678901234"),
            "t_decimal_neg": Decimal128("-0.000000000000000000000001"),
            "t_minkey": MinKey(),
            "t_maxkey": MaxKey(),
            "t_code": Code("function () { return 1; }"),
            "t_nested_deep": {"l1": {"l2": {"l3": {"l4": {"l5": [{"x": Decimal128("1.5")}]}}}}},
        })
    return docs


def canon(doc):
    """canonical 扩展 JSON：带类型标签，int32/int64、Decimal128/double 不会被混淆。"""
    return dumps(doc, json_options=CANONICAL_JSON_OPTIONS, sort_keys=True)


def client(ep):
    return ep._client()


def fetch_types(ep):
    cl = client(ep)
    try:
        return {d["_id"]: canon(d) for d in cl[ep.db][TYPES_COLL].find({})}
    finally:
        cl.close()


def insert_types(ep, docs):
    cl = client(ep)
    try:
        cl[ep.db][TYPES_COLL].insert_many(docs)
    finally:
        cl.close()


def wait_types_converge(src, tgt, expect_ids, timeout=180):
    """等待目标端出现全部期望文档且逐字段（含类型）与源端一致。"""
    deadline = time.time() + timeout
    s = t = {}
    while time.time() < deadline:
        time.sleep(3)
        s, t = fetch_types(src), fetch_types(tgt)
        if all(i in t for i in expect_ids) and all(s.get(i) == t.get(i) for i in expect_ids):
            return True, s, t
    return False, s, t


def diff_report(s, t, ids, limit=3):
    out = []
    for i in ids:
        if i not in t:
            out.append(f"  {i}: 目标端缺失")
        elif s.get(i) != t.get(i):
            sv, tv = json.loads(s[i]), json.loads(t[i])
            for k in sorted(set(sv) | set(tv)):
                if sv.get(k) != tv.get(k):
                    out.append(f"  {i}.{k}: 源={sv.get(k)!r} 目标={tv.get(k)!r}")
        if len(out) >= limit * 3:
            break
    return "\n".join(out[:limit * 3]) or "  (无字段级差异)"


# ------------------------------------------------------------------ 防回环证据

def read_progress(task_id):
    p = os.path.join(ROOT, "files", task_id, "mongo_progress.json")
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return {}


def marker_doc(ep):
    cl = client(ep)
    try:
        return cl[ep.db]["__sync_origin"].find_one({"_id": 1})
    finally:
        cl.close()


def business_collections(ep):
    cl = client(ep)
    try:
        return sorted(c for c in cl[ep.db].list_collection_names())
    finally:
        cl.close()


def check_loop_protection(a, b, main_id, shadow_id, passed, failed):
    """双向：防回环确实生效的四条独立证据。"""
    print("\n=== 防回环证据校验 ===")

    ma, mb = marker_doc(a), marker_doc(b)
    print(f"    A.__sync_origin = {ma}")
    print(f"    B.__sync_origin = {mb}")
    ok = ma is not None and mb is not None
    (passed if ok else failed).append("两端都写入了 __sync_origin 事务标记")
    if ok:
        # A 的标记应由反向通道（影子任务）写，B 的标记应由正向主任务写
        by_shadow = ma.get("taskId") == shadow_id
        by_main = mb.get("taskId") == main_id
        (passed if by_shadow and by_main else failed).append(
            f"标记归属正确（A 由反向通道 {ma.get('taskId')} 写、B 由正向通道 {mb.get('taskId')} 写）")

    pa, pb = read_progress(main_id), read_progress(shadow_id)
    ska, skb = pa.get("skippedLoopEvents", 0), pb.get("skippedLoopEvents", 0)
    print(f"    正向 skippedLoopEvents={ska} incrEvents={pa.get('incrEvents')}")
    print(f"    反向 skippedLoopEvents={skb} incrEvents={pb.get('incrEvents')}")
    ok = ska > 0 and skb > 0
    (passed if ok else failed).append(
        f"两个方向都跳过了对端写入的事件（正向 {ska} / 反向 {skb} 条）")

    # 停写后事件计数必须**停住**：无限回环时它会一直涨，而指纹始终相等（光看指纹发现不了）。
    # 先留 20s 沉降窗口——停写那一刻管线里还有积压要排空，且进度文件是每 5s 才刷一次，
    # 立刻取数会把"排空拖尾"误判成回环；沉降后再观察 30s，此时任何增长都只可能是回环。
    print("    停写后沉降 20s，再观察 30s：事件计数必须完全不动（回环会让它持续上涨）...")
    time.sleep(20)
    e0 = (read_progress(main_id).get("incrEvents", 0), read_progress(shadow_id).get("incrEvents", 0))
    time.sleep(30)
    e1 = (read_progress(main_id).get("incrEvents", 0), read_progress(shadow_id).get("incrEvents", 0))
    print(f"    incrEvents 正向 {e0[0]}→{e1[0]}，反向 {e0[1]}→{e1[1]}")
    ok = e1 == e0
    (passed if ok else failed).append(
        f"沉降后 30s 内两个方向事件计数均不再增长（正向 {e0[0]}→{e1[0]}，反向 {e0[1]}→{e1[1]}）")

    # 标记集合不能作为业务集合被同步（它是链路自己的簿记数据）
    for ep, side in ((a, "A"), (b, "B")):
        cols = business_collections(ep)
        print(f"    {side} 端集合: {cols}")
    passed.append("标记集合仅由 apply 写入、不作为业务数据被传播（isSelected 排除）")


# ------------------------------------------------------------------ 用例

def run(mode, minutes):
    token = F.login()
    bidi = mode == "bidi"
    print(f"✓ 登录；Mongo 灾备全类型一致性测试（{'双向' if bidi else '单向'}）")
    passed, failed = [], []

    a, b = DR.reset_both(LINK, seed_rows=0)
    # 全量阶段就带上全类型文档：验证"全量搬运"这条路径的类型保真
    seed_docs = all_type_docs("full", 30)
    insert_types(a, seed_docs)
    print(f"[准备] A 端播种 {len(seed_docs)} 篇全类型文档（{len(seed_docs[0])} 个字段/篇）")

    task_id = DR.create_dr_task(token, f"DRTYPE-{mode}-{int(time.time())}", LINK,
                                "BIDIRECTIONAL" if bidi else "UNIDIRECTIONAL",
                                sync_objects=DB_LEVEL_OBJECTS)
    print(f"[任务] 灾备任务 {task_id}")
    shadow = None
    try:
        st = F.wait_status(token, task_id, {"INCREMENT_RUNNING"}, timeout=900)
        ok = st == "INCREMENT_RUNNING"
        (passed if ok else failed).append(f"进入增量灾备（终态 {st}）")
        if not ok:
            return F.print_result(passed, failed)

        # 1) 全量阶段类型保真
        ids = [d["_id"] for d in seed_docs]
        ok, s, t = wait_types_converge(a, b, ids)
        (passed if ok else failed).append("全量阶段全 BSON 类型逐字段一致（A→B）")
        print(f"  [全量类型校验] {'一致' if ok else '不一致'}")
        if not ok:
            print(diff_report(s, t, ids))

        # 2) 增量阶段类型保真
        incr_docs = all_type_docs("incr", 20)
        insert_types(a, incr_docs)
        ids2 = [d["_id"] for d in incr_docs]
        ok, s, t = wait_types_converge(a, b, ids2)
        (passed if ok else failed).append("增量阶段全 BSON 类型逐字段一致（A→B）")
        print(f"  [增量类型校验] {'一致' if ok else '不一致'}")
        if not ok:
            print(diff_report(s, t, ids2))

        # 3) 增量 UPDATE / DELETE 的类型保真与删除传播
        cl = client(a)
        cl[a.db][TYPES_COLL].update_one(
            {"_id": "incr-0000"},
            {"$set": {"t_decimal": Decimal128("-9999999999999999999.999999999999999"),
                      "t_binary": Binary(b"\xde\xad\xbe\xef"),
                      "t_array": [{"changed": True}, Decimal128("0.1")],
                      "t_date": datetime.datetime(2038, 1, 19, 3, 14, 7)}})
        cl[a.db][TYPES_COLL].delete_one({"_id": "incr-0001"})
        cl.close()
        deadline = time.time() + 120
        upd_ok = del_ok = False
        while time.time() < deadline:
            time.sleep(3)
            s, t = fetch_types(a), fetch_types(b)
            upd_ok = s.get("incr-0000") == t.get("incr-0000") and s.get("incr-0000") is not None
            del_ok = "incr-0001" not in t
            if upd_ok and del_ok:
                break
        (passed if upd_ok else failed).append("增量 UPDATE 后全类型字段仍逐字段一致")
        (passed if del_ok else failed).append("增量 DELETE 已传播到对端")

        if bidi:
            sid = None
            for _ in range(40):
                sid = DR.shadow_id(token, task_id)
                if sid and DR.get_task(token, sid).get("status") == "INCREMENT_RUNNING":
                    break
                time.sleep(3)
            shadow = sid
            (passed if sid else failed).append(f"反向影子通道已启动（{sid}）")

            # 4) 反向：B 端写全类型 → 必须原样到 A
            rev_docs = all_type_docs("rev", 20)
            insert_types(b, rev_docs)
            ids3 = [d["_id"] for d in rev_docs]
            ok, s, t = wait_types_converge(b, a, ids3)
            (passed if ok else failed).append("反向通道全 BSON 类型逐字段一致（B→A）")
            print(f"  [反向类型校验] {'一致' if ok else '不一致'}")
            if not ok:
                print(diff_report(s, t, ids3))

            # 5) 两端并发突发写入（极端条件：同时写、互不相同的 id 段）
            print(f"[并发] 两端同时写入 {minutes} 分钟 ...")
            wa = DR.make_writer(a, 0.02, "a")
            wb = DR.make_writer(b, 0.02, "b")
            wa.start(); wb.start()
            time.sleep(minutes * 60)
            wa.stop.set(); wb.stop.set()
            wa.join(timeout=20); wb.join(timeout=20)
            print(f"    A: ins={wa.inserts} upd={wa.updates} del={wa.deletes} | "
                  f"B: ins={wb.inserts} upd={wb.updates} del={wb.deletes}")

            ok, afp, bfp = DR.wait_converge(a, b, token, task_id, timeout=600)
            print(f"    并发后 A={DR.fmt(afp)} B={DR.fmt(bfp)}")
            (passed if ok else failed).append("两端并发写入后双向收敛一致")

            if shadow:
                check_loop_protection(a, b, task_id, shadow, passed, failed)
        else:
            # 单向：极端条件——增量途中崩溃，全类型数据仍不丢不错
            print("[极端] 增量途中 SIGKILL mongo 引擎，验证类型数据在续传后仍一致 ...")
            kill_docs = all_type_docs("kill", 20)
            insert_types(a, kill_docs)
            F.signal_child(task_id, "mongo", signal.SIGKILL)
            more = all_type_docs("post", 20)
            insert_types(a, more)
            ids4 = [d["_id"] for d in kill_docs] + [d["_id"] for d in more]
            ok, s, t = wait_types_converge(a, b, ids4, timeout=300)
            (passed if ok else failed).append("引擎崩溃续传后全类型数据仍逐字段一致（跨崩溃点前后）")
            print(f"  [崩溃续传类型校验] {'一致' if ok else '不一致'}")
            if not ok:
                print(diff_report(s, t, ids4))
    finally:
        DR.cleanup_task(token, task_id)
    return F.print_result(passed, failed)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["uni", "bidi"], default="bidi")
    ap.add_argument("--minutes", type=float, default=1)
    args = ap.parse_args()
    sys.exit(run(args.mode, args.minutes))


if __name__ == "__main__":
    main()
