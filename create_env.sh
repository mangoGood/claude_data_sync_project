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

echo "[create_env] 创建并启动 Docker 基础设施 (mysql:33306, kafka:29092, zookeeper)..."
docker compose -f "$COMPOSE_FILE" up -d

echo "[create_env] 创建并启动 Mongo/ES 基础设施 (mongo-a:27117, mongo-b:27118, es:9200)..."
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
echo "[create_env] 初始化 Mongo 副本集 rsA / rsB（幂等，已初始化则跳过）..."
docker exec synctask-mongo-a mongosh -u root -p rootpassword --quiet --eval \
  'try { rs.initiate({_id:"rsA", members:[{_id:0, host:"127.0.0.1:27017"}]}) } catch(e) { print(e.message) }' || true
docker exec synctask-mongo-b mongosh -u root -p rootpassword --quiet --eval \
  'try { rs.initiate({_id:"rsB", members:[{_id:0, host:"127.0.0.1:27017"}]}) } catch(e) { print(e.message) }' || true

# ---- Mongo 分片集群初始化（源：2 shard + 目标：1 shard；全部幂等可重复执行）----
# configsvr 默认端口 27019、shardsvr 默认 27018；集群内互访用服务名，宿主机只连 mongos。
init_sharded_cluster() {
  local prefix="$1" csrs="$2" mongos_host_port="$3"; shift 3
  local shards=("$@")   # 形如 srcSh1:synctask-mongos-src-sh1

  echo "[create_env] 初始化分片集群 ${prefix}（configsvr + ${#shards[@]} shard + mongos）..."
  for i in $(seq 1 30); do
    docker exec "${prefix}-cfg" mongosh --port 27019 --quiet --eval 'db.runCommand("ping")' >/dev/null 2>&1 && break
    sleep 2
  done
  docker exec "${prefix}-cfg" mongosh --port 27019 --quiet --eval \
    "try { rs.initiate({_id:\"${csrs}\", configsvr:true, members:[{_id:0, host:\"${prefix}-cfg:27019\"}]}) } catch(e) { print(e.message) }" || true

  local entry rsname host
  for entry in "${shards[@]}"; do
    rsname="${entry%%:*}"; host="${entry#*:}"
    for i in $(seq 1 30); do
      docker exec "$host" mongosh --port 27018 --quiet --eval 'db.runCommand("ping")' >/dev/null 2>&1 && break
      sleep 2
    done
    docker exec "$host" mongosh --port 27018 --quiet --eval \
      "try { rs.initiate({_id:\"${rsname}\", members:[{_id:0, host:\"${host}:27018\"}]}) } catch(e) { print(e.message) }" || true
  done

  echo "[create_env] 等待 mongos ${prefix} 就绪..."
  for i in $(seq 1 40); do
    docker exec "${prefix}" mongosh --quiet --eval 'db.runCommand("ping")' >/dev/null 2>&1 && break
    sleep 3
  done
  # localhost exception 建 root（已存在则捕获报错跳过）
  docker exec "${prefix}" mongosh --quiet --eval \
    'try { db.getSiblingDB("admin").createUser({user:"root", pwd:"rootpassword", roles:["root"]}) } catch(e) { print(e.message) }' || true
  for entry in "${shards[@]}"; do
    rsname="${entry%%:*}"; host="${entry#*:}"
    docker exec "${prefix}" mongosh -u root -p rootpassword --quiet --eval \
      "try { sh.addShard(\"${rsname}/${host}:27018\") } catch(e) { print(e.message) }" || true
  done
}

init_sharded_cluster synctask-mongos-src csrsSrc 27217 \
  srcSh1:synctask-mongos-src-sh1 srcSh2:synctask-mongos-src-sh2
init_sharded_cluster synctask-mongos-dst csrsDst 27218 \
  dstSh1:synctask-mongos-dst-sh1

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
