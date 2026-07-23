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
MONGO_COMPOSE_FILE="docker-compose-synctask-mongo.yml"
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

# ---- 持久化密钥（agent 与 backend 共用）----
# 三者都落盘到项目目录的隐藏文件，每次启动注入同一份，避免密钥"只活在某次手工 shell 里"：
#   .synctask_master_key —— 凭证加密主密钥（缺失会让此前 ENC: 密文无法解密）
#   .synctask_jwt_secret —— JWT 签名密钥（缺失会让后端每次重启随机签名，已登录用户全被踢下线）
#   .synctask_agent_token —— agent 敏感接口鉴权 token（缺失会让 failover/start-increment 无鉴权）
# 缺失即生成（openssl 优先，退化到 /dev/urandom），生成后 chmod 600。
gen_secret() {  # $1=文件路径 $2=字节数
  local f="$1" n="${2:-32}"
  if [ ! -f "$f" ]; then
    if command -v openssl >/dev/null 2>&1; then
      openssl rand -base64 "$n" | tr -d '\n' > "$f"
    else
      head -c "$n" /dev/urandom | base64 | tr -d '\n' > "$f"
    fi
    chmod 600 "$f"
    echo "[start] 已生成持久化密钥: $(basename "$f")"
  fi
}
gen_secret "$PROJECT_DIR/.synctask_master_key" 32
gen_secret "$PROJECT_DIR/.synctask_jwt_secret" 48
gen_secret "$PROJECT_DIR/.synctask_agent_token" 32
export SYNCTASK_MASTER_KEY="$(cat "$PROJECT_DIR/.synctask_master_key")"
export JWT_SECRET="$(cat "$PROJECT_DIR/.synctask_jwt_secret")"
export AGENT_API_TOKEN="$(cat "$PROJECT_DIR/.synctask_agent_token")"
echo "[start] 已注入 SYNCTASK_MASTER_KEY / JWT_SECRET / AGENT_API_TOKEN（持久化，重启后 token 不失效）"

# ---- 1. 启动已存在的 Docker 基础设施（不创建）----
if ! docker inspect synctask-mysql >/dev/null 2>&1; then
  echo "[start] ✗ 未找到 synctask-mysql 等 container，环境尚未创建。请先运行 ./create_env.sh"
  exit 1
fi
echo "[start] 启动 Docker 基础设施 (mysql:33306, kafka:29092, zookeeper)..."
docker compose -f "$COMPOSE_FILE" start

if docker inspect synctask-mongo-a >/dev/null 2>&1; then
  echo "[start] 启动 Mongo 副本集 (mongo-a:27117, mongo-b:27118)..."
  docker compose -f "$MONGO_COMPOSE_FILE" start
fi

if docker inspect synctask-es >/dev/null 2>&1; then
  echo "[start] 启动 ES 基础设施 (es:9200)..."
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
kafka_up=0
for i in $(seq 1 30); do nc -z localhost 29092 2>/dev/null && { kafka_up=1; break; }; sleep 2; done
# Kafka 崩溃循环主动探测：stop/start 间隔过久时上一个 broker 的 ZK ephemeral znode 未过期，
# 新 broker 注册抛 KeeperException$NodeExistsException 后立即退出、被 docker 反复重启。
# 这种情况下端口探测会一直失败——直接检查容器状态并给出确切修复命令，别让用户对着
# "任务永远 PENDING" 猜半天。
if [ "$kafka_up" != "1" ]; then
  kstate="$(docker inspect -f '{{.State.Status}}' synctask-kafka 2>/dev/null || echo unknown)"
  krestarts="$(docker inspect -f '{{.RestartCount}}' synctask-kafka 2>/dev/null || echo 0)"
  echo "[start] ✗ Kafka 29092 未就绪 (容器状态=$kstate, 重启次数=$krestarts)"
  if docker logs --tail 50 synctask-kafka 2>&1 | grep -q "NodeExistsException"; then
    echo "[start] ✗ 检测到 ZooKeeper NodeExistsException 崩溃循环（stop/start 间隔过久导致）。"
    echo "[start]   修复：docker compose -f $COMPOSE_FILE down && docker compose -f $COMPOSE_FILE up -d"
    echo "[start]   （无持久卷，down+up 会清空 ZK/Kafka 状态；切勿只 restart 两个容器）"
  else
    echo "[start]   请检查: docker logs --tail 100 synctask-kafka"
  fi
  exit 1
fi

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
  # -Dh2.bindAddress=127.0.0.1：显式兜底（AgentMain.main 已在代码里设，此处防御，
  # 且子进程由 ProcessManager 各自透传）。本机 hostname 解析为 LAN IP 时 H2 跨进程轮询会挂起。
  nohup "$JAVA_HOME/bin/java" -Dh2.bindAddress=127.0.0.1 -jar migration-agent/target/migration-agent-1.0.0.jar \
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
