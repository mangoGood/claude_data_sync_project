#!/bin/bash
set -e
TOKEN="eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIyIiwidXNlcm5hbWUiOiJ1c2VyMSIsInJvbGUiOiJVU0VSIiwiaWF0IjoxNzgyMzkwMTY4LCJleHAiOjE3ODI0NzY1Njh9.Cr55ojQlB69Y6s5mdIi-2zjLZShSU2IC2p5-0o1YIIwae6Wj7jySb8V6sM5CNjHkioKoM-t9oP3nuyDQVqEn7Q"
BASE="http://localhost:8082"

# 创建 fullAndIncre workflow
echo "=== 创建 fullAndIncre workflow ==="
RESP=$(curl -s -m 10 -X POST "$BASE/api/workflows" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name":"oracle-pg-full-incre","sourceType":"oracle","targetType":"postgresql","taskType":"MIGRATION"}')
WID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['id'])")
echo "workflow id=$WID"

# 配置：fullAndIncre 模式
echo "=== 配置 fullAndIncre ==="
curl -s -m 10 -X PUT "$BASE/api/workflows/$WID/config" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"sourceConnection":"oracle://app_user:userpassword@192.168.137.11:1521/FREEPDB1","targetConnection":"postgresql://app_user:userpassword@127.0.0.1:5432/myapp_db","migrationMode":"fullAndIncre","syncObjects":"{\"APP_USER\":{\"tables\":[\"T1\"]}}","sourceDbName":"FREEPDB1","targetDbName":"myapp_db","sourceType":"oracle","targetType":"postgresql"}'
echo

# 启动
echo "=== 启动 ==="
curl -s -m 10 -X POST "$BASE/api/workflows/$WID/launch" -H "Authorization: Bearer $TOKEN"
echo

echo "=== 等待全量阶段完成(45s) ==="
sleep 45

echo "=== workflow 状态(全量后) ==="
curl -s -m 10 "$BASE/api/workflows/$WID" -H "Authorization: Bearer $TOKEN" | python3 -c "
import sys,json
d=json.load(sys.stdin)
w=d.get('data',{})
print('name=',w.get('name'))
print('status=',w.get('status'))
print('mode=',w.get('migration_mode'))
print('progress=',w.get('progress'))
print('error=',w.get('error_message'))
"

echo "=== agent 日志(capture 相关) ==="
tail -80 logs/agent.log 2>/dev/null | grep -oE '"message":"[^"]*(?:LogMiner|CDB|redo|capture|Oracle)[^"]*"' | tail -15

echo "WID=$WID" > /tmp/ora_pg_incre_wid.txt
echo "=== 增量任务已启动，workflow_id=$WID ==="
