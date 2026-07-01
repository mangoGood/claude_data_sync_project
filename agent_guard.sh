#!/bin/bash
#
# Agent 进程守护脚本
# 监控 migration-agent 进程，崩溃后自动重启
# 用法: ./agent_guard.sh [start|stop|status]
#

AGENT_JAR="migration-agent/target/migration-agent-1.0.0.jar"
PID_FILE="agent.pid"
LOG_FILE="logs/agent_guard.log"
MAX_RESTARTS=10
RESTART_INTERVAL=60
restart_count=0

mkdir -p logs

get_pid() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi
    echo ""
    return 1
}

start_agent() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] 启动 migration-agent..." | tee -a "$LOG_FILE"
    nohup java -jar "$AGENT_JAR" > logs/agent.out 2>&1 &
    echo $! > "$PID_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] Agent PID: $(cat $PID_FILE)" | tee -a "$LOG_FILE"
}

stop_agent() {
    local pid=$(get_pid)
    if [ -n "$pid" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] 停止 Agent (PID: $pid)..." | tee -a "$LOG_FILE"
        kill "$pid"
        sleep 3
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid"
            echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] 强制终止 Agent" | tee -a "$LOG_FILE"
        fi
        rm -f "$PID_FILE"
    else
        echo "Agent 未运行"
    fi
}

status_agent() {
    local pid=$(get_pid)
    if [ -n "$pid" ]; then
        echo "Agent 运行中 (PID: $pid)"
    else
        echo "Agent 未运行"
    fi
}

guard_loop() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] 启动进程守护模式，最大重启次数: $MAX_RESTARTS" | tee -a "$LOG_FILE"
    start_agent

    while true; do
        sleep 5
        local pid=$(get_pid)
        if [ -z "$pid" ]; then
            restart_count=$((restart_count + 1))
            if [ $restart_count -gt $MAX_RESTARTS ]; then
                echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] 已达最大重启次数 ($MAX_RESTARTS)，停止守护" | tee -a "$LOG_FILE"
                exit 1
            fi
            echo "$(date '+%Y-%m-%d %H:%M:%S') [GUARD] Agent 进程已退出，$RESTART_INTERVAL 秒后重启 (第 $restart_count 次)..." | tee -a "$LOG_FILE"
            sleep $RESTART_INTERVAL
            start_agent
        fi
    done
}

case "${1:-guard}" in
    start)
        start_agent
        ;;
    stop)
        stop_agent
        ;;
    status)
        status_agent
        ;;
    guard)
        guard_loop
        ;;
    *)
        echo "用法: $0 {start|stop|status|guard}"
        exit 1
        ;;
esac
