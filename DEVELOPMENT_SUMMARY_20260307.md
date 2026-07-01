# 开发记录总结 - 2026-03-07

## 一、状态流程重构

### 1. 问题背景
原有的任务状态流程中包含"运行中"(RUNNING)状态，但这个状态不够明确，无法区分任务处于哪个阶段。

### 2. 新的状态流程

移除了 `RUNNING` 状态，新增了更明确的状态：

| 状态 | 说明 | 颜色 |
|------|------|------|
| PENDING | 等待中 | 橙色 |
| STARTING | 启动中 | 蓝色 |
| FULL_MIGRATING | 全量同步中 | 蓝色 |
| FULL_COMPLETED | 全量同步完成 | 绿色 |
| INCREMENT_RUNNING | 增量同步中 | 绿色+闪烁绿点 |
| COMPLETED | 已完成 | 绿色 |
| FAILED | 失败 | 红色 |
| PAUSED | 已暂停 | 灰色 |

### 3. 状态流程图

```
增量同步任务状态流程：
┌─────────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────────┐
│   PENDING   │ →  │   STARTING   │ →  │ FULL_MIGRATING │ →  │  FULL_COMPLETED  │
│   等待中    │    │    启动中     │    │   全量同步中    │    │   全量同步完成    │
└─────────────┘    └──────────────┘    └───────────────┘    └──────────────────┘
                                                                    ↓
                                              ┌──────────────────┐
                                              │ INCREMENT_RUNNING│
                                              │    增量同步中     │
                                              │  (闪烁绿点)       │
                                              └──────────────────┘
```

### 4. 修改的文件

#### 后端 Java 代码

**WorkflowStatus.java** - 状态枚举定义
```java
public enum WorkflowStatus {
    PENDING,
    STARTING,           // 启动中
    FULL_MIGRATING,     // 全量同步中
    FULL_COMPLETED,     // 全量同步完成
    INCREMENT_RUNNING,  // 增量同步中
    COMPLETED,
    FAILED,
    PAUSED
}
```

**MigrationAgentThread.java** - 状态发送逻辑
- 任务开始：发送 `STARTING` 状态
- 全量迁移中：发送 `FULL_MIGRATING` 状态
- 全量完成：发送 `FULL_COMPLETED` 状态
- 增量同步中：每60秒发送 `INCREMENT_RUNNING` 状态

**MigrationTaskManager.java** - 全量迁移状态
- 启动时：`FULL_MIGRATING`
- 进度更新：`FULL_MIGRATING`
- 完成：`FULL_COMPLETED`

**KafkaConsumerService.java** - 日志消息处理
```java
case STARTING:
    return "任务启动中";
case FULL_MIGRATING:
    return "全量同步中";
case FULL_COMPLETED:
    return "全量同步完成";
case INCREMENT_RUNNING:
    return "增量同步中";
```

**WorkflowService.java** - 恢复任务状态
```java
workflow.setStatus(WorkflowStatus.STARTING);
```

#### 前端代码

**admin-dashboard.html** - 状态映射和样式

```javascript
const statusMap = {
    'PENDING': { text: '等待中', class: 'status-pending' },
    'STARTING': { text: '启动中', class: 'status-starting' },
    'FULL_MIGRATING': { text: '全量同步中', class: 'status-full-migrating' },
    'FULL_COMPLETED': { text: '全量同步完成', class: 'status-full-completed' },
    'INCREMENT_RUNNING': { text: '增量同步中', class: 'status-increment-running', dot: true },
    'COMPLETED': { text: '已完成', class: 'status-completed' },
    'FAILED': { text: '失败', class: 'status-failed' },
    'PAUSED': { text: '已暂停', class: 'status-paused' }
};
```

CSS 样式：
```css
.status-starting {
    background-color: #e6f7ff;
    color: #1890ff;
    border: 1px solid #91d5ff;
}

.status-full-migrating {
    background-color: #e6f7ff;
    color: #1890ff;
    border: 1px solid #91d5ff;
}

.status-full-completed {
    background-color: #f6ffed;
    color: #52c41a;
    border: 1px solid #b7eb8f;
}

.status-increment-running {
    background-color: #f6ffed;
    color: #52c41a;
    border: 1px solid #b7eb8f;
}

.status-increment-running .status-dot {
    width: 8px;
    height: 8px;
    background-color: #52c41a;
    border-radius: 50%;
    animation: blink 1s ease-in-out infinite;
}

@keyframes blink {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
}
```

---

## 二、WebSocket 实时状态更新

### 1. 添加的依赖

```html
<script src="https://cdn.jsdelivr.net/npm/sockjs-client@1/dist/sockjs.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/stompjs@2.3.3/lib/stomp.min.js"></script>
```

### 2. WebSocket 连接代码

```javascript
let stompClient = null;

function connectWebSocket() {
    const socket = new SockJS('http://localhost:8082/ws');
    stompClient = Stomp.over(socket);
    
    stompClient.connect({}, function(frame) {
        // 订阅全局任务状态更新
        stompClient.subscribe('/topic/task-status', function(message) {
            const data = JSON.parse(message.body);
            handleStatusUpdate(data);
        });
        
        // 订阅用户专属任务状态更新
        stompClient.subscribe('/user/queue/task-status', function(message) {
            const data = JSON.parse(message.body);
            handleStatusUpdate(data);
        });
    }, function(error) {
        // 5秒后重连
        setTimeout(connectWebSocket, 5000);
    });
}
```

### 3. 后端安全配置

**SecurityConfig.java** 添加 WebSocket 端点权限：
```java
.requestMatchers("/ws/**").permitAll()
```

---

## 三、日志路径配置

### 1. Binlog 模块

**BinlogMain.java** - 设置日志路径
```java
private static void setupLogging(String taskId) {
    if (taskId != null) {
        String logPath = "files/" + taskId + "/logs";
        File logDir = new File(logPath);
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        System.setProperty("LOG_PATH", logPath);
        System.setProperty("LOG_FILE", "binlog");
    }
}
```

**logback.xml** - 使用系统属性
```xml
<property name="LOG_PATH" value="${LOG_PATH:-logs}"/>
<property name="LOG_FILE" value="${LOG_FILE:-binlog}"/>
```

### 2. Increment 模块

**MainIncrement.java** - 设置日志路径
```java
private static void setupLogging(String taskId) {
    if (taskId != null) {
        String logPath = "files/" + taskId + "/logs";
        File logDir = new File(logPath);
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        System.setProperty("LOG_PATH", logPath);
        System.setProperty("LOG_FILE", "increment");
    }
}
```

### 3. 日志文件位置

| 模块 | 有 taskId | 无 taskId |
|------|-----------|-----------|
| binlog | `files/{task_id}/logs/binlog.log` | `logs/binlog.log` |
| increment | `files/{task_id}/logs/increment.log` | `logs/increment.log` |
| full | `files/{task_id}/logs/migration.log` | `logs/migration.log` |

---

## 四、数据库问题修复

### 1. 问题原因

数据库 `workflows` 表的 `status` 列是 ENUM 类型，只包含旧的状态值：
```sql
ENUM('PENDING','RUNNING','COMPLETED','FAILED','PAUSED')
```

修改 Java 枚举后，JPA 无法将数据库中的 `RUNNING` 值映射到新的枚举，导致查询失败。

### 2. 解决方案

执行以下 SQL 更新数据库：

```sql
-- 先改为 VARCHAR 类型
ALTER TABLE workflows MODIFY COLUMN status VARCHAR(50) DEFAULT 'PENDING' COMMENT '任务状态';

-- 更新旧数据
UPDATE workflows SET status = 'STARTING' WHERE status = 'RUNNING';

-- 再改回 ENUM 类型，包含新的状态值
ALTER TABLE workflows MODIFY COLUMN status ENUM(
    'PENDING',
    'STARTING',
    'FULL_MIGRATING',
    'FULL_COMPLETED',
    'INCREMENT_RUNNING',
    'COMPLETED',
    'FAILED',
    'PAUSED'
) DEFAULT 'PENDING' COMMENT '任务状态';
```

---

## 五、增量同步状态持续上报

### 1. 实现逻辑

**MigrationAgentThread.java** 中的 `incrementMonitorThread`：

```java
private static final long STATUS_REPORT_INTERVAL = 60000; // 60秒

incrementMonitorThread = new Thread(() -> {
    long lastReportTime = System.currentTimeMillis();
    
    while (running.get() && incrementProcess != null) {
        try {
            Thread.sleep(INCREMENT_MONITOR_INTERVAL);
            
            if (!running.get() || stopped.get()) {
                break;
            }
            
            if (!incrementProcess.isRunning()) {
                logger.error("[{}] 增量同步进程异常退出", threadName);
                sendStatus("FAILED", "增量同步进程异常退出", 100);
                running.set(false);
                break;
            }
            
            // 每60秒上报一次状态
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= STATUS_REPORT_INTERVAL) {
                sendStatus("INCREMENT_RUNNING", "增量同步中", 100);
                lastReportTime = currentTime;
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
        }
    }
}, "IncrementMonitor-" + taskId);
```

### 2. 状态更新流程

```
状态更新流程：
┌──────────────────────────────────────────────────────────────────┐
│ MigrationAgentThread                                              │
│   ↓ 每 60 秒发送 INCREMENT_RUNNING 状态                            │
│ Kafka sync-task-status Topic                                      │
│   ↓                                                               │
│ Java Backend KafkaConsumerService                                 │
│   ↓ 解析消息，更新数据库，推送 WebSocket                            │
│ SimpMessagingTemplate.convertAndSend()                            │
│   ↓                                                               │
│ 前端 STOMP 客户端                                                  │
│   ↓ 接收消息                                                       │
│ handleStatusUpdate()                                              │
│   ↓ 更新 UI                                                        │
│ 状态标签显示：绿色背景 + 闪烁绿点 + "增量同步中"                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 六、文件修改清单

### 后端文件

| 文件路径 | 修改内容 |
|----------|----------|
| `java-backend/src/main/java/com/synctask/entity/WorkflowStatus.java` | 重构状态枚举 |
| `java-backend/src/main/java/com/synctask/service/KafkaConsumerService.java` | 更新日志消息 |
| `java-backend/src/main/java/com/synctask/service/WorkflowService.java` | 更新恢复状态 |
| `java-backend/src/main/java/com/synctask/config/SecurityConfig.java` | 添加 WebSocket 权限 |
| `mysql-migration-agent/src/main/java/com/migration/agent/thread/MigrationAgentThread.java` | 状态发送逻辑 |
| `mysql-migration-agent/src/main/java/com/migration/agent/manager/MigrationTaskManager.java` | 全量迁移状态 |
| `mysql-migration-agent/src/main/java/com/migration/agent/AgentMain.java` | 恢复任务状态 |
| `mysql-migration-binlog/src/main/java/com/migration/binlog/BinlogMain.java` | 日志路径配置 |
| `mysql-migration-binlog/src/main/resources/logback.xml` | 日志配置 |
| `mysql-migration-increment/src/main/java/com/migration/increment/MainIncrement.java` | 日志路径配置 |
| `mysql-migration-increment/src/main/resources/logback.xml` | 日志配置 |

### 前端文件

| 文件路径 | 修改内容 |
|----------|----------|
| `admin-dashboard.html` | 状态映射、CSS样式、WebSocket连接 |

---

## 七、待办事项

- [ ] 无

---

## 八、注意事项

1. **数据库状态值**：修改状态枚举时，需要同步更新数据库中的 ENUM 类型定义
2. **日志路径**：确保 `files/{task_id}/logs/` 目录存在
3. **WebSocket 连接**：前端需要引入 SockJS 和 STOMP 库
4. **状态上报间隔**：当前设置为 60 秒，可根据需要调整
