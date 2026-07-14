#!/usr/bin/env bash
# ============================================================
# 关闭整个项目：
#   1. 停止后端 (spring-boot:run / 38080)
#   2. 停止数据同步进程 migration-agent 及其拉起的子进程
#   3. 停止 Docker 基础设施 (MySQL / Kafka / Zookeeper)，不删除容器
# 用法: ./stop.sh
# 如需彻底删除容器（连同容器内数据），请用 ./remove_env.sh。
# ============================================================
set -uo pipefail
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"
COMPOSE_FILE="docker-compose-synctask.yml"
DB_COMPOSE_FILE="docker-compose-synctask-db.yml"
LOG_DIR="$PROJECT_DIR/logs"

echo "[stop] 停止后端 (spring-boot:run / 38080)..."
pkill -f 'spring-boot:run' 2>/dev/null || true
pkill -f 'sync-task-backend' 2>/dev/null || true
backend_pids="$(lsof -ti tcp:38080 2>/dev/null || true)"
[ -n "$backend_pids" ] && kill $backend_pids 2>/dev/null || true

echo "[stop] 停止数据同步进程 migration-agent..."
# agent 会拉起 migration-full / capture / extract / increment 等子进程，一并清理
pkill -f 'migration-agent/target/migration-agent-1.0.0.jar' 2>/dev/null || true
pkill -f 'migration-full/target/migration-full-1.0.0.jar' 2>/dev/null || true
pkill -f 'migration-capture/target/migration-capture-1.0.0.jar' 2>/dev/null || true
pkill -f 'migration-extract/target/migration-extract-1.0.0.jar' 2>/dev/null || true
pkill -f 'migration-increment/target/migration-increment-1.0.0.jar' 2>/dev/null || true
if [ -f "$LOG_DIR/agent.pid" ]; then kill "$(cat "$LOG_DIR/agent.pid")" 2>/dev/null || true; fi

echo "[stop] 停止 Docker 基础设施 (mysql / kafka / zookeeper)，不删除容器..."
docker compose -f "$COMPOSE_FILE" stop

if docker inspect synctask-mongo-a >/dev/null 2>&1; then
  echo "[stop] 停止 Mongo/ES 基础设施 (mongo-a / mongo-b / es)，不删除容器..."
  docker compose -f "$DB_COMPOSE_FILE" stop
fi

rm -f "$LOG_DIR/agent.pid" "$LOG_DIR/backend.pid" 2>/dev/null || true

echo ""
echo "[stop] ✓ 已关闭：后端、agent 进程已停止，synctask-mysql / synctask-kafka / synctask-zk"
echo "[stop]   synctask-mongo-a / synctask-mongo-b / synctask-es 容器已停止（未删除）。"
echo "[stop]   如需彻底删除容器请用 ./remove_env.sh；重新启动请用 ./start.sh。"
