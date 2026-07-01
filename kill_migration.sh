#!/bin/bash

# 结束所有 migration 相关的 Java 进程

echo "查找 migration 相关的 Java 进程..."

# 查找所有包含 migration 关键字的 Java 进程
pids=$(ps aux | grep -i "migration" | grep "java" | grep -v grep | awk '{print $2}')

if [ -z "$pids" ]; then
    echo "没有找到 migration 相关的 Java 进程"
    exit 0
fi

echo "找到以下进程:"
ps aux | grep -i "migration" | grep "java" | grep -v grep

echo ""
echo "正在结束进程..."

for pid in $pids; do
    echo "结束进程 PID: $pid"
    kill $pid 2>/dev/null
done

sleep 2

# 检查是否还有残留进程，强制结束
remaining=$(ps aux | grep -i "migration" | grep "java" | grep -v grep | awk '{print $2}')
if [ -n "$remaining" ]; then
    echo "仍有残留进程，强制结束..."
    for pid in $remaining; do
        echo "强制结束进程 PID: $pid"
        kill -9 $pid 2>/dev/null
    done
fi

echo ""
echo "验证结果:"
ps aux | grep -i "migration" | grep "java" | grep -v grep || echo "所有 migration 相关进程已结束"