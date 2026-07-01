#!/usr/bin/env bash
# ============================================================
# 启动整个数据同步/灾备/订阅项目：
#   1. Docker 起 MySQL(33306) + Kafka(29092) + Zookeeper
#   2. 初始化数据库（幂等）并确保 admin/admin123 可登录
#   3. 需要时构建模块
#   4. 启动数据同步进程 migration-agent (AgentMain)
#   5. 启动后端 java-backend (spring-boot:run, 端口 38080)
# 用法: ./start.sh
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"
COMPOSE_FILE="docker-compose-synctask.yml"
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

# ---- 1. Docker 基础设施 ----
echo "[start] 启动 Docker 基础设施 (mysql:33306, kafka:29092, zookeeper)..."
docker compose -f "$COMPOSE_FILE" up -d

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

# ---- 2. 初始化数据库（幂等）+ 重置 admin 密码为 admin123 ----
echo "[start] 初始化数据库（database.sql, --force 容忍 MySQL 不兼容语句）..."
docker exec -i synctask-mysql mysql -uroot -prootpassword --force \
  < java-backend/src/main/resources/database.sql >/dev/null 2>&1 || true
if command -v htpasswd >/dev/null 2>&1; then
  HASH="$(htpasswd -bnBC 10 "" admin123 | tr -d '\n' | sed 's/^[^:]*://; s/^\$2y\$/\$2a\$/')"
  docker exec -i synctask-mysql mysql -uroot -prootpassword \
    -e "UPDATE sync_task_db.users SET password='$HASH' WHERE username IN ('admin','user1','user2');" \
    >/dev/null 2>&1 || true
fi

# ---- 3. 首次运行时构建（缺 jar 才构建）----
if [ ! -f migration-agent/target/migration-agent-1.0.0.jar ]; then
  echo "[start] 首次构建模块 (mvn package, 跳过测试)..."
  mvn -q -Dmaven.test.skip=true package
fi

# ---- 4. 启动数据同步进程 (AgentMain) ----
if pgrep -f 'migration-agent/target/migration-agent-1.0.0.jar' >/dev/null 2>&1; then
  echo "[start] agent 已在运行，跳过"
else
  echo "[start] 启动 migration-agent (AgentMain, HTTP:8083)..."
  nohup "$JAVA_HOME/bin/java" -jar migration-agent/target/migration-agent-1.0.0.jar \
    > "$LOG_DIR/agent.out" 2>&1 &
  echo $! > "$LOG_DIR/agent.pid"
fi

# ---- 5. 启动后端 (spring-boot:run, 38080) ----
if lsof -ti tcp:38080 >/dev/null 2>&1; then
  echo "[start] 38080 已被占用，后端可能已在运行，跳过"
else
  echo "[start] 启动后端 java-backend (spring-boot:run, 端口 38080)..."
  ( cd java-backend && nohup mvn -q -DskipTests spring-boot:run \
      > "$LOG_DIR/backend.out" 2>&1 & echo $! > "$LOG_DIR/backend.pid" )
fi

# ---- 6. 等待就绪 ----
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
