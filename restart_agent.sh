#!/usr/bin/env bash
# 只重启 migration-agent（不动后端与 Docker 基础设施）：改了 agent 侧代码重打 jar 后用它快速生效。
# 环境变量与 start.sh 保持一致（密钥、Kafka、元数据库、H2 bindAddress）。
set -euo pipefail
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
else
  export JAVA_HOME="/Users/finn/Library/Java/JavaVirtualMachines/ms-21.0.9/Contents/Home"
fi
export PATH="$JAVA_HOME/bin:$PATH"

export DB_URL="jdbc:mysql://localhost:33306/sync_task_db?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true"
export DB_USERNAME="root"
export DB_PASSWORD="rootpassword"
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export MIGRATION_AGENT_KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export MIGRATION_AGENT_MYSQL_DB_URL="$DB_URL"
export MIGRATION_AGENT_MYSQL_DB_USER="root"
export MIGRATION_AGENT_MYSQL_DB_PASSWORD="rootpassword"
export SYNCTASK_MASTER_KEY="$(cat "$PROJECT_DIR/.synctask_master_key")"
export JWT_SECRET="$(cat "$PROJECT_DIR/.synctask_jwt_secret")"
export AGENT_API_TOKEN="$(cat "$PROJECT_DIR/.synctask_agent_token")"

echo "[restart-agent] 停止旧 agent ..."
pkill -f 'migration-agent/target/migration-agent-1.0.0.jar' 2>/dev/null || true
# 子进程（capture/extract/increment/full）随 agent 退出不会自动收敛，一并清理
pkill -f 'migration-(capture|extract|increment|full|mongo|redis|elastic)/target' 2>/dev/null || true
sleep 3

echo "[restart-agent] 启动新 agent ..."
nohup "$JAVA_HOME/bin/java" -Dh2.bindAddress=127.0.0.1 \
  -jar migration-agent/target/migration-agent-1.0.0.jar > "$LOG_DIR/agent.out" 2>&1 &
echo $! > "$LOG_DIR/agent.pid"

for i in $(seq 1 30); do
  if curl -s -m2 http://localhost:8083/api/agent/health | grep -q UP; then
    echo "[restart-agent] agent 已就绪 (pid $(cat "$LOG_DIR/agent.pid"))"
    exit 0
  fi
  sleep 2
done
echo "[restart-agent] ✗ agent 未在 60s 内就绪，见 $LOG_DIR/agent.out"
exit 1
