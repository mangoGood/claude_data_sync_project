# 开发日志 - 2026-03-11

## 问题修复记录

### 1. 全量同步暂停后任务变为失败状态

**问题描述**：
全量同步过程中，点击暂停按钮，任务状态变为"失败"而非"暂停"。

**问题原因**：
1. 点击暂停时，`stopMigrationAgentThread` 调用 `agentThread.stop()`
2. `stop()` 方法设置 `stopped.set(true)` 并调用 `stopAllProcesses()`
3. `fullProcess.stop()` 强制终止进程，导致 `fullProcess.waitFor()` 返回非零退出码
4. `executeFullMigration` 方法检测到 `exitCode != 0`，发送 `FAILED` 状态
5. 监控线程检测到进程停止后，也会误报失败状态

**修复方案**：
在判断进程退出原因时，检查 `stopped` 标志。如果是被暂停停止的，不发送失败状态。

**修改文件**：
- `mysql-migration-agent/src/main/java/com/migration/agent/thread/MigrationAgentThread.java`

**关键代码修改**：

```java
// executeFullMigration 方法
if (stopped.get()) {
    logger.info("[{}] 全量迁移被暂停", threadName);
    return false;
}

// binlogMonitorThread
if (!binlogProcess.isRunning()) {
    if (stopped.get()) {
        logger.info("[{}] binlog 进程已停止（暂停）", threadName);
        break;
    }
    // ... 发送 FAILED 状态
}

// incrementMonitorThread
if (!incrementProcess.isRunning()) {
    if (stopped.get()) {
        logger.info("[{}] 增量同步进程已停止（暂停）", threadName);
        break;
    }
    // ... 发送 FAILED 状态
}
```

---

### 2. 前端全量同步进度无法正常显示

**问题描述**：
日志中的进度正常，但前端全量同步的进度无法正常显示（当前表显示为 null，进度为 0%）。

**问题原因**：

1. **字段名格式不匹配**：
   - 后端 REST API 返回的是 snake_case 格式（`total_tables`, `completed_tables`）
   - WebSocket 推送的 `TaskStatusUpdate` 对象序列化为 camelCase 格式（`totalTables`, `completedTables`）
   - 前端 `handleStatusUpdate` 函数使用 camelCase 字段名，无法正确读取数据

2. **JavaScript falsy 值判断问题**：
   - `if (data.totalTables)` 当 `totalTables=0` 时会判断为 `false`，导致初始进度不显示

**修复方案**：

1. 后端 `TaskStatusUpdate` 类添加 `@JsonProperty` 注解，使 WebSocket 消息序列化为 snake_case 格式
2. 前端 `handleStatusUpdate` 函数兼容两种字段名格式，并修复 falsy 值判断

**修改文件**：
- `java-backend/src/main/java/com/synctask/dto/TaskStatusUpdate.java`
- `admin-dashboard.html`

**关键代码修改**：

```java
// TaskStatusUpdate.java
@JsonProperty("total_tables")
private Integer totalTables;

@JsonProperty("completed_tables")
private Integer completedTables;

@JsonProperty("current_table")
private String currentTable;

@JsonProperty("current_table_progress")
private Integer currentTableProgress;
```

```javascript
// admin-dashboard.html - handleStatusUpdate 函数
const totalTables = data.total_tables !== undefined ? data.total_tables : data.totalTables;
const completedTables = data.completed_tables !== undefined ? data.completed_tables : data.completedTables;
const currentTable = data.current_table !== undefined ? data.current_table : data.currentTable;
const currentTableProgress = data.current_table_progress !== undefined ? data.current_table_progress : data.currentTableProgress;

if (data.status === 'FULL_MIGRATING' && totalTables !== undefined && totalTables !== null) {
    // 显示详细进度
}
```

---

## 数据流说明

### 全量同步进度上报流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ mysql-migration-full 模块                                                    │
│   ├─ ProgressManager 管理迁移进度（存储到 H2 数据库）                           │
│   └─ 每个表迁移时调用 startMigration()，更新进度时调用 updateProgress()         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ mysql-migration-agent 模块                                                   │
│   ├─ MigrationAgentThread.startFullMigrationMonitor()                       │
│   │   └─ 每 3 秒读取 H2 进度库                                                │
│   │       - totalTables: 总表数                                               │
│   │       - completedTables: 已完成表数                                       │
│   │       - currentTable: 当前同步的表                                        │
│   │       - currentTableProgress: 当前表进度百分比                             │
│   └─ sendStatus() 发送状态到 Kafka                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ java-backend 模块                                                            │
│   ├─ KafkaConsumerService 消费状态消息                                        │
│   ├─ 更新 Workflow 实体到 MySQL 数据库                                        │
│   └─ 通过 WebSocket 推送 TaskStatusUpdate 到前端                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ admin-dashboard.html                                                         │
│   ├─ WebSocket 接收 TaskStatusUpdate                                         │
│   └─ handleStatusUpdate() 更新进度显示                                        │
│       - 进度条百分比                                                          │
│       - 表: 已完成/总数                                                       │
│       - 当前表名和进度                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 状态流转说明

### 暂停操作的正确流程

```
用户点击暂停按钮
        ↓
前端调用 pauseWorkflow API
        ↓
Java Backend 发送 Kafka 消息 (type=stop)
        ↓
mysql-migration-agent 接收消息
        ↓
handleStopMessage() 处理
        ├─ pausedTasks.add(taskId)
        ├─ 保存任务状态到 H2
        ├─ stopTaskById() 停止 binlog/全量迁移进程
        └─ stopMigrationAgentThread() 停止监控线程
                ↓
        MigrationAgentThread.stop()
                ├─ stopped.set(true)
                └─ stopAllProcesses()
                        ↓
                各监控线程检测到 stopped=true
                        ├─ 不发送 FAILED 状态
                        └─ 正常退出
        ↓
sendStatus("PAUSED", ...) 发送暂停状态
        ↓
前端显示任务为"已暂停"状态
```

---

## 注意事项

1. **字段命名一致性**：后端 REST API 和 WebSocket 消息应使用相同的字段命名格式（建议统一使用 snake_case）

2. **JavaScript falsy 值**：在 JavaScript 中判断数值时，`0` 会被视为 `false`，应使用 `!== undefined && !== null` 进行判断

3. **进程状态判断**：在判断进程异常退出时，需要区分是正常停止（暂停/结束）还是真正的异常退出
