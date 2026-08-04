#!/usr/bin/env bash
# ============================================================
# 跑单元测试（不打包、不 clean，比 ./build.sh --tests 快得多）。
#
# 统一用 JDK 21 —— 与 start.sh / build.sh / restart_agent.sh 保持一致，
# 也就是生产实际运行的那个 JVM，测试环境别跟运行环境岔开。
#
# 本机可能装有多个 JDK，裸 `mvn test` 会跟着默认 JAVA_HOME 走。这里显式钉 21，
# 让"测试跑过的 JVM"等于"线上跑的 JVM"，而不是靠默认 JDK 碰运气。
#
# 用法: ./test.sh            # 只跑 java-backend 单测（最常用）
#       ./test.sh engine     # 只跑引擎聚合工程 (migration-*/thl) 单测
#       ./test.sh all        # 两个工程都跑
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

# ---- JDK 21（构建/运行/测试统一钉这一个版本，与线上一致）----
if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
else
  export JAVA_HOME="/Users/finn/Library/Java/JavaVirtualMachines/ms-21.0.9/Contents/Home"
fi
if [ ! -x "$JAVA_HOME/bin/java" ]; then
  echo "[test] ✗ 未找到 JDK 21：$JAVA_HOME"
  echo "[test]   装一个 21，或把上面的兜底路径改成本机实际路径。"
  exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"
echo "[test] JAVA_HOME=$JAVA_HOME"
java -version 2>&1 | head -1

run_backend() {
  echo "[test] java-backend 单测..."
  ( cd java-backend && mvn test )
}
run_engine() {
  echo "[test] 引擎聚合工程单测 (migration-*/thl)..."
  mvn test
}

case "${1:-backend}" in
  backend) run_backend ;;
  engine)  run_engine ;;
  all)     run_engine; run_backend ;;
  *) echo "用法: ./test.sh [backend|engine|all]"; exit 1 ;;
esac

echo ""
echo "============================================================"
echo "  单测通过"
echo "============================================================"
