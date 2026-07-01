# MySQL 数据迁移系统开发总结

## 项目概述

本项目是一个完整的 MySQL 数据迁移系统，支持全量迁移和增量同步，包含以下核心功能：
- 基于 binlog 的实时数据同步
- 全量数据迁移
- 任务管理和监控
- Web 管理界面
- Kafka 消息队列集成

## 项目架构

```
test_git/
├── java-backend/              # Java 后端服务（Spring Boot）
│   ├── src/main/java/
│   │   └── com/synctask/
│   │       ├── config/        # 配置类
│   │       ├── controller/    # REST API 控制器
│   │       ├── entity/        # 数据库实体
│   │       ├── repository/    # JPA 仓库
│   │       ├── security/      # JWT 认证
│   │       └── service/       # 业务逻辑
│   └── pom.xml
├── mysql-migration-agent/     # 迁移代理服务
│   └── src/main/java/
│       └── com/migration/agent/
│           ├── manager/       # 任务管理器
│           ├── service/       # Kafka 服务
│           └── AgentMain.java
├── mysql-migration-binlog/    # Binlog 解析模块
│   └── src/main/java/
│       └── com/migration/binlog/
│           ├── handler/       # 事件处理器
│           └── BinlogMain.java
├── mysql-migration-full/      # 全量迁移模块
│   └── src/main/java/
│       └── com/migration/full/
│           └── Main.java
├── mysql-migration-common/    # 公共模块
├── mysql-migration-increment/ # 增量迁移模块
├── login.html                 # 登录页面
├── admin-dashboard.html       # 管理面板
└── pom.xml                    # 父 POM
```

## 主要功能模块

### 1. Java 后端服务（端口 8082）
- 用户认证和授权（JWT）
- 任务管理 API
- 静态文件服务
- Kafka 消息生产
- WebSocket 实时通信

### 2. 迁移代理服务
- 监听 Kafka 任务创建消息
- 启动和监控迁移进程
- 持久化迁移进度
- 上报任务状态

### 3. Binlog 解析模块
- 实时监听 MySQL binlog
- 解析 DDL/DML/DCL 事件
- 生成对应的 SQL 语句
- 从源库读取表结构

### 4. 全量迁移模块
- 迁移表结构
- 迁移数据
- 进度跟踪
- 断点续传

## 关键技术实现

### 1. 静态文件服务配置

**文件**: `java-backend/src/main/java/com/synctask/config/WebMvcConfig.java`

```java
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/**")
                .addResourceLocations("file:../")
                .setCachePeriod(0);
    }
}
```

**作用**: 让 Java 后端能够提供前端静态文件（login.html、admin-dashboard.html）

### 2. 安全配置

**文件**: `java-backend/src/main/java/com/synctask/config/SecurityConfig.java`

```java
.authorizeHttpRequests(auth -> 
    auth.requestMatchers("/api/auth/**").permitAll()
        .requestMatchers("/api/health").permitAll()
        .requestMatchers("/error").permitAll()
        .requestMatchers("/", "/login.html", "/admin-dashboard.html").permitAll()
        .requestMatchers("/*.css", "/*.js", "/*.png", "/*.jpg", "/*.ico").permitAll()
        .anyRequest().authenticated()
);
```

**作用**: 允许未认证用户访问登录页面和静态资源

### 3. 任务列表排序

**文件**: `java-backend/src/main/java/com/synctask/repository/WorkflowRepository.java`

```java
@Query("SELECT w FROM Workflow w WHERE w.userId = :userId ORDER BY w.createdAt DESC")
Page<Workflow> findByUserId(@Param("userId") Long userId, Pageable pageable);
```

**作用**: 
- 按创建时间降序排列任务
- 只显示当前用户的任务
- 支持分页（每页 10 条）

### 4. 迁移模式支持

**前端**: 添加了迁移模式选择（全量/全量+增量）

```javascript
const migrationMode = document.querySelector('input[name="migrationMode"]:checked').value;
// full: 仅全量迁移
// fullAndIncre: 全量 + 增量同步
```

**后端**: Agent 根据模式决定是否启动 binlog 进程

```java
if ("fullAndIncre".equals(migrationMode)) {
    processManager.startBinlogProcess(taskId);
}
```

### 5. 进程日志配置

**文件**: `mysql-migration-full/src/main/resources/logback.xml`

```xml
<appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>logs/migration.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <fileNamePattern>logs/migration.%d{yyyy-MM-dd}.log</fileNamePattern>
        <maxHistory>30</maxHistory>
    </rollingPolicy>
    <encoder>
        <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
</appender>
```

**作用**: 确保进程日志正确输出到文件

### 6. 依赖版本统一

**所有模块统一使用**:
- logback.version: 1.4.11
- slf4j.version: 2.0.9

**Maven Shade Plugin 配置**:
```xml
<transformers>
    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
        <mainClass>com.migration.agent.AgentMain</mainClass>
    </transformer>
</transformers>
<filters>
    <filter>
        <artifact>*:*</artifact>
        <excludes>
            <exclude>META-INF/*.SF</exclude>
            <exclude>META-INF/*.DSA</exclude>
            <exclude>META-INF/*.RSA</exclude>
        </excludes>
    </filter>
</filters>
```

**作用**: 解决 logback 类找不到的问题

## 问题解决方案

### 问题 1: 前端页面无法访问

**现象**: 启动 Java 后端后，访问 http://localhost:8082/login.html 返回 404

**原因**: Java 后端没有配置静态文件服务

**解决方案**:
1. 创建 WebMvcConfig.java 配置静态资源路径
2. 修改 SecurityConfig.java 允许访问静态文件
3. 静态文件路径配置为 `file:../`（相对于 java-backend 目录）

### 问题 2: 任务列表排序混乱

**现象**: 任务列表不按创建时间排序，显示顺序随机

**原因**: 
1. 前端连接的是 Java 后端（8082），而不是 Node.js 后端（3000）
2. Java 后端的查询没有指定排序

**解决方案**:
1. 修改 WorkflowRepository 添加 `ORDER BY createdAt DESC`
2. 确保查询按 user_id 过滤
3. 验证分页正确性

### 问题 3: Logback 类找不到

**现象**: 
```
java.lang.NoClassDefFoundError: ch/qos/logback/core/joran/action/AbstractEventEvaluatorAction
```

**原因**: 各模块 logback 版本不一致，shade plugin 配置不完整

**解决方案**:
1. 统一所有模块的 logback 和 slf4j 版本
2. 添加 ServicesResourceTransformer
3. 排除签名文件

### 问题 4: 进程日志不输出

**现象**: Agent 启动的进程没有日志输出

**原因**: 
1. 缺少 logback.xml 配置文件
2. 工作目录设置不正确

**解决方案**:
1. 在 migration-full 和 migration-binlog 模块添加 logback.xml
2. ProcessManager 设置正确的工作目录

### 问题 5: Git Detached HEAD 状态

**现象**: 
```
fatal: You are not currently on a branch
```

**解决方案**:
```bash
git checkout -b feature/migration-agent
git push -u origin feature/migration-agent
```

## 启动命令

### 启动后端服务

```bash
# 编译（如果需要）
cd /Users/finn/Documents/git_projects/test_git/java-backend
mvn clean package -DskipTests

# 启动服务
java -jar target/sync-task-backend-1.0.0.jar

# 后台运行
nohup java -jar target/sync-task-backend-1.0.0.jar > /tmp/java-backend.log 2>&1 &
```

### 访问前端

启动 Java 后端后，通过浏览器访问：
- 登录页面: http://localhost:8082/login.html
- 管理面板: http://localhost:8082/admin-dashboard.html

### 停止服务

```bash
# 查找进程
lsof -i :8082

# 停止进程
kill -9 <PID>
```

### 启动 Agent 服务

```bash
cd /Users/finn/Documents/git_projects/test_git/mysql-migration-agent
mvn clean package -DskipTests
java -jar target/mysql-migration-agent-1.0.0.jar
```

## 数据库配置

### MySQL 连接信息
- Host: 192.168.107.2
- Port: 3306
- Database: sync_task_db
- Username: root
- Password: rootpassword

### Kafka 连接信息
- Bootstrap Servers: 192.168.117.2:19092
- Topics:
  - sync-task-created: 任务创建消息
  - sync-task-status: 任务状态更新消息

## 用户账号

### 测试账号
- 用户名: test1
- 密码: 123456
- 角色: USER

## API 接口

### 认证接口
- POST /api/auth/login - 用户登录
- POST /api/auth/register - 用户注册

### 任务接口
- GET /api/workflows - 获取任务列表（分页）
- POST /api/workflows - 创建新任务
- GET /api/workflows/{id} - 获取任务详情
- PUT /api/workflows/{id} - 更新任务
- DELETE /api/workflows/{id} - 删除任务

### 健康检查
- GET /api/health - 服务健康状态

## 注意事项

1. **不需要启动 Node.js 后端**: 前端由 Java 后端直接提供
2. **端口冲突**: 确保 8082 端口未被占用
3. **数据库连接**: 确保 MySQL 和 Kafka 服务正常运行
4. **日志查看**: 
   - Java 后端日志: 控制台输出或 /tmp/java-backend.log
   - 迁移日志: logs/migration.log
5. **代码修改后**: 需要重新编译并重启服务

## 开发环境

- Java: 17
- Spring Boot: 3.2.0
- MySQL: 8.0
- Kafka: 2.8+
- Maven: 3.9.9

## 下一步计划

1. 添加任务暂停/恢复功能
2. 实现任务失败重试机制
3. 添加迁移性能监控
4. 实现多任务并发执行
5. 添加邮件通知功能
6. 实现数据校验功能

## 联系方式

如有问题，请联系开发团队。

---

**文档生成时间**: 2026-03-04 20:32
**项目路径**: /Users/finn/Documents/git_projects/test_git
