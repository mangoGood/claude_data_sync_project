#!/usr/bin/env bash
# ============================================================
# 判据：THL 反序列化白名单不能锁死真实数据。
#
# 为什么要拿真文件跑：白名单最大的风险不是"漏放 gadget"，而是"漏放业务类型"——
# 后者的表现是任务跑到一半读不动 THL，且只在特定链路上出现。本判据首次运行时
# 就抓到两处只读代码发现不了的遗漏：
#   1. java.lang.Object（ArrayList 底层 Object[] 剥到元素类型）—— 74 个文件里 72 个被拒
#   2. com.migration.common.lob.LobRef（大字段链路放进 metadata，THL 模块无静态引用）
#
# 用法: ./test_scripts/security/thl_filter_realfiles.sh
# 前置: ./build.sh 已跑过；仓库 files/ 下有历史任务的 thl_output
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/../.."

if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
else
  export JAVA_HOME="/Users/finn/Library/Java/JavaVirtualMachines/ms-21.0.9/Contents/Home"
fi

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

( cd migration-thl && "$JAVA_HOME/bin/../../bin/mvn" -o -q dependency:build-classpath \
    -Dmdep.outputFile="$OUT/cp.txt" >/dev/null 2>&1 ) || \
  mvn -o -q -pl migration-thl dependency:build-classpath -Dmdep.outputFile="$OUT/cp.txt" >/dev/null 2>&1

CP="migration-thl/target/classes:$(cat "$OUT/cp.txt")"
"$JAVA_HOME/bin/javac" -cp "$CP" -d "$OUT" test_scripts/security/ScanRealThl.java
RESULT="$("$JAVA_HOME/bin/java" -cp "$CP:$OUT" ScanRealThl 2>/dev/null | grep -vE 'INFO|DEBUG')"
echo "$RESULT"

if echo "$RESULT" | grep -q "(无)"; then
  echo ""
  echo "✓ 判据通过：全部真实 THL 均可通过白名单读取"
  exit 0
fi
echo ""
echo "✗ 判据失败：有 THL 被白名单拒绝——白名单漏了业务类型，补进 ThlObjectInputFilter.ALLOWED"
exit 1
