#!/usr/bin/env python3
# ============================================================
# 判据：指纹聚合口径必须是 SUM，不能是 XOR。
#
# 背景：本目录的判据脚本原先用 BIT_XOR(CRC32(...)) 做整表指纹。XOR 顺序无关、
# O(1) 内存，这两点都对；但它有一个致命性质——**任何成对出现的相同哈希互相抵消**。
# 于是"两组内容不同、但各自含一对重复行"的数据会得到完全相同的指纹，判据给绿灯。
#
# 这个形状在两条链路上真实可达：
#   1. 分片汇聚（同一行落进两片）——sharding/api_route_content_compare_e2e.py
#      早就因此改用了 SUM；
#   2. Mongo 指纹按业务 id 而非 _id 规范化，重放可造出同 id 的两份文档。
# 单表带主键的场景 XOR 抵消不了（主键禁止整行重复），那里的收益只是消掉
# crc32 碰撞对（200,000 行规模实测约 5 对）这一二阶风险。
#
# 无论哪种，SUM 与 XOR 同样顺序无关、同样 O(1) 内存，却没有抵消性质——零成本严格更强。
#
# 用法: python3 test_scripts/fault_injection/fingerprint_semantics.py
# 不依赖数据库，纯口径判据。
# ============================================================
import sys
import zlib

FAILURES = []


def record(name, ok, detail=""):
    print(("  ✓ " if ok else "  ✗ ") + name + (("  — " + detail) if detail else ""))
    if not ok:
        FAILURES.append(name)


def canon_key(row):
    """与 dblib.canon_key 同口径。"""
    rid, grp, val, payload, n = row
    return "|".join([
        str(int(rid)), str(int(grp)),
        "\x00" if val is None else str(val),
        "\x00" if payload is None else str(payload),
        "\x00" if n is None else str(int(n)),
    ])


def fp_sum(rows):
    t = 0
    for r in rows:
        t = (t + zlib.crc32(canon_key(r).encode("utf-8"))) & 0xFFFFFFFFFFFFFFFF
    return (len(rows), t)


def fp_xor(rows):
    x = 0
    for r in rows:
        x ^= zlib.crc32(canon_key(r).encode("utf-8"))
    return (len(rows), x)


def main():
    print("指纹口径判据")
    print("=" * 60)

    base = [(1, 1, "a", "p", 1), (2, 1, "b", "p", 2), (3, 1, "c", "p", 3)]

    print("\n[1] 顺序无关性（SUM 必须保留 XOR 的这个优点）")
    record("正序与逆序指纹相同", fp_sum(base) == fp_sum(list(reversed(base))))

    print("\n[2] 对每列敏感")
    for i, mutated in enumerate([
        [(1, 1, "a", "p", 1), (2, 1, "B", "p", 2), (3, 1, "c", "p", 3)],   # val 变
        [(1, 1, "a", "p", 1), (2, 1, "b", "P", 2), (3, 1, "c", "p", 3)],   # payload 变
        [(1, 1, "a", "p", 1), (2, 1, "b", "p", 9), (3, 1, "c", "p", 3)],   # n 变
        [(1, 2, "a", "p", 1), (2, 1, "b", "p", 2), (3, 1, "c", "p", 3)],   # grp 变
    ]):
        record("改动第 %d 类列能被发现" % (i + 1), fp_sum(base) != fp_sum(mutated))

    print("\n[3] NULL 与空串不能混淆（历史坑：PG 空串曾被写成字符串 'NULL'）")
    record("NULL 与空串指纹不同",
           fp_sum([(1, 1, None, "p", 1)]) != fp_sum([(1, 1, "", "p", 1)]))

    print("\n[4] 核心：成对重复不能被抵消掉")
    # 两组数据内容不同，但行数相同、各含一对重复行
    a = [(1, 1, "a", "p", 1), (2, 1, "b", "p", 2), (2, 1, "b", "p", 2)]
    b = [(1, 1, "a", "p", 1), (3, 1, "c", "p", 3), (3, 1, "c", "p", 3)]
    record("SUM 能区分（这是换掉 XOR 的理由）", fp_sum(a) != fp_sum(b),
           "sum(A)=%d sum(B)=%d" % (fp_sum(a)[1], fp_sum(b)[1]))
    record("XOR 确实区分不了（反证：口径退回去就会瞎）", fp_xor(a) == fp_xor(b),
           "xor 两侧同为 %d" % fp_xor(a)[1])

    print("\n[5] 源码里不得再出现 XOR 聚合")
    import os
    import re
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    offenders = []
    for fn in sorted(os.listdir(root)):
        if not fn.endswith(".py") or fn == os.path.basename(__file__):
            continue
        text = open(os.path.join(root, fn), encoding="utf-8").read()
        # 只看实际的聚合调用，注释里解释历史的不算
        for m in re.finditer(r"(BIT_XOR\s*\(|bit_xor\s*\(|\^=\s*zlib\.crc32)", text):
            line = text[:m.start()].count("\n") + 1
            src = text.splitlines()[line - 1].strip()
            if src.startswith("#"):
                continue
            offenders.append("%s:%d  %s" % (fn, line, src[:70]))
    record("fault_injection 下无 XOR 聚合残留", not offenders,
           ("; ".join(offenders) if offenders else ""))

    print("\n" + "=" * 60)
    if FAILURES:
        print("✗ 判据失败 %d 项: %s" % (len(FAILURES), ", ".join(FAILURES)))
        return 1
    print("✓ 判据全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
