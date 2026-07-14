import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='192.168.117.2:19092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

task_id = 'mysql-sync-test-001'

message = {
    'taskId': task_id,
    'taskName': 'MySQL-to-MySQL-Sync-Test',
    'userId': 1,
    'sourceType': 'mysql',
    'targetType': 'mysql',
    'migrationMode': 'fullAndIncre',
    'messageType': 'CREATE',
    'sourceConnection': 'jdbc:mysql://192.168.107.2:3306/myapp_db?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&allowPublicKeyRetrieval=true',
    'targetConnection': 'jdbc:mysql://192.168.107.6:3306/myapp_db?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&allowPublicKeyRetrieval=true',
    'sourceDbName': 'myapp_db',
    'source': {
        'host': '192.168.107.2',
        'port': 3306,
        'database': 'myapp_db',
        'username': 'root',
        'password': 'rootpassword'
    },
    'target': {
        'host': '192.168.107.6',
        'port': 3306,
        'database': 'myapp_db',
        'username': 'root',
        'password': 'rootpassword'
    },
    'syncObjects': {
        'myapp_db': {
            'tables': ['chinese_person_info', 'test1']
        }
    },
    'createdAt': '2026-04-25T15:15:00',
    'currentStatus': 'PENDING'
}

future = producer.send('sync-task-created', value=message)
result = future.get(timeout=10)
print(f'Message sent successfully to partition {result.partition} at offset {result.offset}')
producer.close()
