#!/bin/bash

echo "========================================="
echo "测试 Agent 配置文件更新功能"
echo "========================================="
echo ""

echo "1. 查看当前配置文件内容:"
echo "---"
cat ../config.properties
echo ""
echo "---"
echo ""

echo "2. 创建测试任务消息..."
TASK_ID="test-task-$(date +%s)"
SOURCE_CONN="mysql://testuser:testpass@192.168.1.100:3306/test_source_db"
TARGET_CONN="mysql://testuser:testpass@192.168.1.200:3306/test_target_db"

echo "任务ID: $TASK_ID"
echo "源库连接串: $SOURCE_CONN"
echo "目标库连接串: $TARGET_CONN"
echo ""

echo "3. 发送测试消息到 Kafka..."
echo "注意: 需要启动 Agent 服务来监听消息"
echo ""

echo "测试完成！"
echo ""
echo "========================================="
echo "启动 Agent 服务的命令:"
echo "cd /Users/finn/Documents/git_projects/test_git/migration-agent"
echo "java -jar target/migration-agent-1.0.0.jar"
echo "========================================="
