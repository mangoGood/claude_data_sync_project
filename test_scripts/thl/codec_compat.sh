#!/usr/bin/env bash
# ============================================================
# 判据：ThlCodec 对真实历史数据逐字段无损，且体积确实降下来。
#
# 拿仓库 files/ 下全部历史 THL（THL1，Java 原生序列化）的每条事件做
#   原事件 → ThlCodec.encode → decode → 逐字段比对
# 并统计两种编码的字节数。
#
# 首次运行的结果（2026-08-19，74 文件 / 33,917 事件）：
#   字段不一致 0；32.56 MB → 21.60 MB，体积降 33.7%（单条 1006 B → 667 B）
#
# 用法: ./test_scripts/thl/codec_compat.sh
# 前置: ./build.sh 已跑过
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/../.."

if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
else
  export JAVA_HOME="/Users/finn/Library/Java/JavaVirtualMachines/ms-21.0.9/Contents/Home"
fi

OUT="$(mktemp -d)"; trap 'rm -rf "$OUT"' EXIT
mvn -o -q -pl migration-thl dependency:build-classpath -Dmdep.outputFile="$OUT/cp.txt" >/dev/null 2>&1
CP="migration-thl/target/classes:migration-common/target/classes:$(cat "$OUT/cp.txt")"

"$JAVA_HOME/bin/javac" -cp "$CP" -d "$OUT" test_scripts/thl/CodecCompat.java
"$JAVA_HOME/bin/java" -cp "$CP:$OUT" CodecCompat 2>/dev/null | grep -vE 'INFO|DEBUG'
