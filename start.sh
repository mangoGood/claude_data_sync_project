#!/usr/bin/env bash
# ============================================================
# 启动整个数据同步/灾备/订阅项目（假定环境已由 ./create_env.sh 创建过）：
#   1. 启动已存在的基础设施 container：MySQL(33306) + Kafka(29092) + Zookeeper
#   2. 需要时构建模块
#   3. 启动数据同步进程 migration-agent (AgentMain)
#   4. 启动后端 java-backend (spring-boot:run, 端口 38080)
# 用法: ./start.sh
# 首次搭建环境、或执行过 ./remove_env.sh 之后，请先运行 ./create_env.sh。
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"
COMPOSE_FILE="docker-compose-synctask.yml"
DB_COMPOSE_FILE="docker-compose-synctask-db.yml"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# ---- JDK 21（Spring Boot 3.1/3.2 用 21 最稳，不要用 24）----
if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
else
  export JAVA_HOME="/Users/finn/Library/Java/JavaVirtualMachines/ms-21.0.9/Contents/Home"
fi
export PATH="$JAVA_HOME/bin:$PATH"
echo "[start] JAVA_HOME=$JAVA_HOME"

# ---- 连接信息（agent 与 backend 共用）----
export DB_URL="jdbc:mysql://localhost:33306/sync_task_db?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true"
export DB_USERNAME="root"
export DB_PASSWORD="rootpassword"
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export MIGRATION_AGENT_KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export MIGRATION_AGENT_MYSQL_DB_URL="$DB_URL"
export MIGRATION_AGENT_MYSQL_DB_USER="root"
export MIGRATION_AGENT_MYSQL_DB_PASSWORD="rootpassword"

# ---- 凭证加密主密钥（agent 与 backend 共用，缺失则退化为内置开发默认密钥）----
# 由 create_env.sh 首次创建环境时生成并落盘，每次启动都注入同一份，
# 避免主密钥"只活在某次手工导出的 shell 里"，导致下次重启后旧的 ENC: 密文全部无法解密。
MASTER_KEY_FILE="$PROJECT_DIR/.synctask_master_key"
if [ -f "$MASTER_KEY_FILE" ]; then
  export SYNCTASK_MASTER_KEY="$(cat "$MASTER_KEY_FILE")"
else
  echo "[start] ⚠ 未找到 $MASTER_KEY_FILE，将使用内置开发默认主密钥（不安全，且与此前手工设置过自定义密钥加密的数据不兼容）。建议运行 ./create_env.sh 生成持久化主密钥。"
fi

# ---- 1. 启动已存在的 Docker 基础设施（不创建）----
if ! docker inspect synctask-mysql >/dev/null 2>&1; then
  echo "[start] ✗ 未找到 synctask-mysql 等 container，环境尚未创建。请先运行 ./create_env.sh"
  exit 1
fi
echo "[start] 启动 Docker 基础设施 (mysql:33306, kafka:29092, zookeeper)..."
docker compose -f "$COMPOSE_FILE" start

if docker inspect synctask-mongo-a >/dev/null 2>&1; then
  echo "[start] 启动 Mongo/ES 基础设施 (mongo-a:27117, mongo-b:27118, es:9200)..."
  docker compose -f "$DB_COMPOSE_FILE" start
fi

echo "[start] 等待 MySQL 健康检查通过..."
status=""
for i in $(seq 1 60); do
  status="$(docker inspect -f '{{.State.Health.Status}}' synctask-mysql 2>/dev/null || echo starting)"
  [ "$status" = "healthy" ] && break
  sleep 2
done
if [ "$status" != "healthy" ]; then
  echo "[start] ✗ MySQL 未就绪，退出"; exit 1
fi

echo "[start] 等待 Kafka (localhost:29092)..."
for i in $(seq 1 30); do nc -z localhost 29092 2>/dev/null && break; sleep 2; done
# 提示：若 Kafka 反复没有就绪，很可能是 stop/start 间隔太久导致 ZK ephemeral 节点未过期，
# 触发 KeeperException$NodeExistsException 崩溃循环；此时需 ./remove_env.sh 后再 ./create_env.sh 重建。

# ---- 2. 首次运行时构建（缺 jar 才构建）----
if [ ! -f migration-agent/target/migration-agent-1.0.0.jar ]; then
  echo "[start] 首次构建模块 (mvn package, 跳过测试)..."
  mvn -q -Dmaven.test.skip=true package
fi

# ---- 3. 启动数据同步进程 (AgentMain) ----
if pgrep -f 'migration-agent/target/migration-agent-1.0.0.jar' >/dev/null 2>&1; then
  echo "[start] agent 已在运行，跳过"
else
  echo "[start] 启动 migration-agent (AgentMain, HTTP:8083)..."
  nohup "$JAVA_HOME/bin/java" -jar migration-agent/target/migration-agent-1.0.0.jar \
    > "$LOG_DIR/agent.out" 2>&1 &
  echo $! > "$LOG_DIR/agent.pid"
fi

# ---- 4. 启动后端 (spring-boot:run, 38080) ----
if lsof -ti tcp:38080 >/dev/null 2>&1; then
  echo "[start] 38080 已被占用，后端可能已在运行，跳过"
else
  echo "[start] 启动后端 java-backend (spring-boot:run, 端口 38080)..."
  ( cd java-backend && nohup mvn -q -DskipTests spring-boot:run \
      > "$LOG_DIR/backend.out" 2>&1 & echo $! > "$LOG_DIR/backend.pid" )
fi

# ---- 5. 等待就绪 ----
echo "[start] 等待后端就绪 (localhost:38080)..."
for i in $(seq 1 60); do
  code="$(curl -s -m2 -o /dev/null -w '%{http_code}' http://localhost:38080/ 2>/dev/null || echo 000)"
  [ "$code" = "200" ] && break
  sleep 2
done

echo ""
echo "============================================================"
echo "  启动完成"
echo "  页面:     http://localhost:38080/login.html   (admin / admin123)"
echo "  后端API:  http://localhost:38080/api"
echo "  Agent:    http://localhost:8083/api/agent/health"
echo "  日志:     $LOG_DIR/agent.out , $LOG_DIR/backend.out"
echo "  关闭:     ./stop.sh"
echo "============================================================"
