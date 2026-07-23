#!/usr/bin/env bash
# ============================================================
# 一键构建整个项目。本仓库是【两个相互独立的 Maven 工程】：
#   1) 引擎聚合工程（根 pom，migration-* 十个模块，parent 为本 pom）
#   2) java-backend（独立 Spring Boot 工程，parent 为 spring-boot-starter-parent，
#      与引擎无编译依赖，运行期经 Kafka/HTTP 通信）
# 因此根目录直接 `mvn ... -pl java-backend` 会报
# "Could not find the selected project in the reactor: java-backend"——
# 那不是源码编译错误，而是 backend 不在根 reactor 里。用本脚本分别构建二者。
#
# 用法: ./build.sh            # 构建二者（跳过测试，最快）
#       ./build.sh --tests    # 跑测试再打包
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"

# ---- JDK 21（Spring Boot 3.2 用 21 最稳，不要用 24）----
if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
fi
export PATH="$JAVA_HOME/bin:$PATH"
echo "[build] JAVA_HOME=$JAVA_HOME"
java -version 2>&1 | head -1

if [ "${1:-}" = "--tests" ]; then
  SKIP=""
  echo "[build] 含测试"
else
  SKIP="-Dmaven.test.skip=true"
  echo "[build] 跳过测试（--tests 可开启）"
fi

echo "[build] ① 构建引擎聚合工程（根 reactor：migration-*）..."
mvn -q clean package $SKIP

echo "[build] ② 构建 java-backend（独立 Spring Boot 工程）..."
( cd java-backend && mvn -q clean package ${SKIP:+-DskipTests} )

echo ""
echo "============================================================"
echo "  构建完成"
echo "  引擎 agent:  migration-agent/target/migration-agent-1.0.0.jar"
echo "  后端:        java-backend/target/*.jar（spring-boot:run 亦可）"
echo "  启动整套:    ./start.sh"
echo "============================================================"
