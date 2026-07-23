#!/usr/bin/env bash
# ============================================================
# 创建基础设施环境：MySQL(33306) + Kafka(29092) + Zookeeper 容器
#   1. docker compose up -d 创建并启动三个 container
#   2. 等待 MySQL 健康检查通过 / 等待 Kafka 端口就绪
#   3. 创建空的 sync_task_db（表结构+种子数据交给 Flyway，在 backend 首次启动时建）
# 用法: ./create_env.sh
# 仅首次搭建环境、或执行过 ./remove_env.sh 之后需要运行；
# 日常启动/关闭进程请用 ./start.sh / ./stop.sh。
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE_FILE="docker-compose-synctask.yml"
DB_COMPOSE_FILE="docker-compose-synctask-db.yml"
MONGO_COMPOSE_FILE="docker-compose-synctask-mongo.yml"

# Mongo 副本集成员主机（固定 IP，见 docker-compose-synctask-mongo.yml）：rs.initiate 用固定 IP
# 作为 member host，重启不漂移。宿主机侧仍连 localhost:27117/27118（directConnection）。
MONGO_A_HOST="172.28.10.11:27017"
MONGO_B_HOST="172.28.10.12:27017"

echo "[create_env] 创建并启动 Docker 基础设施 (mysql:33306, kafka:29092, zookeeper)..."
docker compose -f "$COMPOSE_FILE" up -d

echo "[create_env] 创建并启动 Mongo 副本集 (mongo-a:27117, mongo-b:27118, 固定 IP)..."
docker compose -f "$MONGO_COMPOSE_FILE" up -d

echo "[create_env] 创建并启动 ES 基础设施 (es:9200)..."
docker compose -f "$DB_COMPOSE_FILE" up -d

echo "[create_env] 等待 MySQL 健康检查通过..."
status=""
for i in $(seq 1 60); do
  status="$(docker inspect -f '{{.State.Health.Status}}' synctask-mysql 2>/dev/null || echo starting)"
  [ "$status" = "healthy" ] && break
  sleep 2
done
if [ "$status" != "healthy" ]; then
  echo "[create_env] ✗ MySQL 未就绪，退出"; exit 1
fi

echo "[create_env] 等待 Kafka (localhost:29092)..."
for i in $(seq 1 30); do nc -z localhost 29092 2>/dev/null && break; sleep 2; done

echo "[create_env] 创建空数据库 sync_task_db（表结构+种子数据由 Flyway 在 backend 启动时创建）..."
docker exec -i synctask-mysql mysql -uroot -prootpassword \
  -e "CREATE DATABASE IF NOT EXISTS sync_task_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# ---- Mongo 副本集初始化（单成员副本集，rs.initiate 只需在首次创建时跑一次；
#      重复调用在已初始化的副本集上会报错，用 already initialized 判断幂等跳过）----
echo "[create_env] 等待 Mongo 副本集容器就绪..."
for i in $(seq 1 30); do
  docker exec synctask-mongo-a mongosh --quiet --eval 'db.runCommand("ping")' >/dev/null 2>&1 && \
  docker exec synctask-mongo-b mongosh --quiet --eval 'db.runCommand("ping")' >/dev/null 2>&1 && break
  sleep 2
done
echo "[create_env] 初始化 Mongo 副本集 rsA / rsB（幂等，已初始化则跳过；member host 用固定 IP）..."
docker exec synctask-mongo-a mongosh -u root -p rootpassword --quiet --eval \
  "try { rs.initiate({_id:\"rsA\", members:[{_id:0, host:\"${MONGO_A_HOST}\"}]}) } catch(e) { print(e.message) }" || true
docker exec synctask-mongo-b mongosh -u root -p rootpassword --quiet --eval \
  "try { rs.initiate({_id:\"rsB\", members:[{_id:0, host:\"${MONGO_B_HOST}\"}]}) } catch(e) { print(e.message) }" || true

echo "[create_env] 等待 Elasticsearch 就绪..."
for i in $(seq 1 40); do
  code="$(curl -s -m2 -o /dev/null -w '%{http_code}' -u elastic:espassword http://127.0.0.1:9200/ 2>/dev/null || echo 000)"
  [ "$code" = "200" ] && break
  sleep 3
done

# ---- 生成持久化主密钥（凭证 AES 加密用）----
# 落盘一次，start.sh 每次启动都注入同一个 SYNCTASK_MASTER_KEY，避免"用完就丢"导致
# 已加密的 connection string / config.properties 密码在下次重启后无法解密。
MASTER_KEY_FILE=".synctask_master_key"
if [ ! -f "$MASTER_KEY_FILE" ]; then
  echo "[create_env] 生成持久化主密钥 -> $MASTER_KEY_FILE ..."
  openssl rand -base64 32 > "$MASTER_KEY_FILE"
  chmod 600 "$MASTER_KEY_FILE"
else
  echo "[create_env] 主密钥已存在，跳过生成: $MASTER_KEY_FILE"
fi

echo ""
echo "============================================================"
echo "  ✓ 环境已创建：synctask-mysql / synctask-kafka / synctask-zk"
echo "              / synctask-mongo-a / synctask-mongo-b / synctask-es"
echo "  下一步: ./start.sh 启动进程"
echo "============================================================"
