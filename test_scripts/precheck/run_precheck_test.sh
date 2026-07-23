#!/usr/bin/env bash
# 编译并运行预检项测试（PrecheckValidationTest）。
# 依赖：已 mvn compile 生成 java-backend/target/classes；运行中的 synctask-mysql(33306) + mysql_db2(3307)。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BACKEND="$ROOT/java-backend"
CP_FILE="${CP_FILE:-/tmp/precheck_cp.txt}"
OUT="$(mktemp -d)"

echo "== 生成依赖 classpath =="
( cd "$BACKEND" && mvn -o -q dependency:build-classpath -Dmdep.outputFile="$CP_FILE" )

CP="$BACKEND/target/classes:$(cat "$CP_FILE")"

echo "== 编译 PrecheckValidationTest =="
javac -cp "$CP" -d "$OUT" "$ROOT/test_scripts/precheck/PrecheckValidationTest.java"

echo "== 运行 =="
java -cp "$OUT:$CP" com.synctask.service.PrecheckValidationTest
