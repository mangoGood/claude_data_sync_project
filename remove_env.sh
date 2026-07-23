#!/usr/bin/env bash
# ============================================================
# 销毁基础设施环境：删除 synctask-mysql / synctask-kafka / synctask-zk 三个 container
# （docker-compose-synctask.yml 未定义持久化 volume，此操作会连同容器内数据一起清空）
# 用法: ./remove_env.sh
# 仅在需要彻底重建环境时使用；日常关闭请用 ./stop.sh（只停止不删除）。
# ============================================================
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE_FILE="docker-compose-synctask.yml"
DB_COMPOSE_FILE="docker-compose-synctask-db.yml"
MONGO_COMPOSE_FILE="docker-compose-synctask-mongo.yml"

echo "[remove_env] 停止并删除 Docker 基础设施 (mysql / kafka / zookeeper)..."
docker compose -f "$COMPOSE_FILE" down

echo "[remove_env] 停止并删除 Mongo 副本集 (mongo-a / mongo-b)..."
docker compose -f "$MONGO_COMPOSE_FILE" down

echo "[remove_env] 停止并删除 ES 基础设施 (es)..."
docker compose -f "$DB_COMPOSE_FILE" down

echo ""
echo "============================================================"
echo "  ✓ 已删除：synctask-mysql / synctask-kafka / synctask-zk"
echo "            synctask-mongo-a / synctask-mongo-b / synctask-es"
echo "  （无持久化 volume，容器内数据已一并清空）"
echo "  下一步: ./create_env.sh 重新创建环境"
echo "============================================================"
