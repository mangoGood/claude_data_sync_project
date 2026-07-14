-- ============================================================
-- Sync Task Backend 基线表结构
--
-- 直接取自当前生产/开发环境实际 schema（此前由 database.sql + ddl-auto:update
-- 双轨制维护多时）：database.sql 里的 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
-- 是 MariaDB 语法，在 MySQL 8 上是非法语法，从未真正执行成功；那些"扩展字段"
-- 实际全部由 Hibernate ddl-auto:update 按实体类型自动建出（例如 Boolean 字段建成
-- bit(1) 而不是 database.sql 里写的 tinyint(1)，日期字段建成 datetime(6) 而不是
-- timestamp）。此文件以 SHOW CREATE TABLE 实际结果为准，保证与现有数据库、以及
-- 之后 ddl-auto:validate 的校验完全一致。
--
-- 全部使用 CREATE TABLE IF NOT EXISTS：对已存在（已由 ddl-auto:update 建好）的库
-- 是安全的空操作；对全新库则完整建表。
-- ============================================================

-- ============================================================
-- 1. 用户表
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    password VARCHAR(255) NOT NULL COMMENT '密码（BCrypt加密）',
    email VARCHAR(100) DEFAULT NULL COMMENT '邮箱',
    role VARCHAR(20) DEFAULT 'USER' COMMENT '角色: USER/ADMIN',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY username (username),
    UNIQUE KEY email (email),
    INDEX idx_username (username),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

-- ============================================================
-- 2. 同步任务工作流表
-- ============================================================
CREATE TABLE IF NOT EXISTS workflows (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL COMMENT '任务名称',
    source_connection VARCHAR(255) DEFAULT NULL COMMENT '源连接信息',
    target_connection VARCHAR(255) DEFAULT NULL COMMENT '目标连接信息',
    status ENUM('CONFIGURING', 'PENDING', 'RECEIVED', 'STARTING', 'FULL_MIGRATING',
                 'FULL_COMPLETED', 'INCREMENT_RUNNING', 'SUBSCRIBE_RUNNING',
                 'SWITCHING', 'COMPLETED', 'FAILED', 'PAUSED') DEFAULT 'CONFIGURING' COMMENT '任务状态',
    progress INT DEFAULT 0 COMMENT '进度百分比',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    migration_mode VARCHAR(255) DEFAULT NULL COMMENT '迁移模式: full/fullAndIncre/subscribe',
    is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否软删除',
    sync_objects TEXT COMMENT '同步对象JSON',
    source_db_name VARCHAR(255) DEFAULT NULL COMMENT '源数据库名',
    source_type VARCHAR(20) DEFAULT 'mysql' COMMENT '源数据库类型',
    target_type VARCHAR(20) DEFAULT 'mysql' COMMENT '目标数据库类型',
    total_tables INT DEFAULT NULL COMMENT '全量同步总表数',
    completed_tables INT DEFAULT 0 COMMENT '全量同步已完成表数',
    current_table VARCHAR(255) DEFAULT NULL COMMENT '当前正在同步的表名',
    current_table_progress INT DEFAULT 0 COMMENT '当前表同步进度百分比',
    current_table_rows BIGINT DEFAULT 0 COMMENT '当前表已同步行数',
    current_table_total_rows BIGINT DEFAULT 0 COMMENT '当前表总行数',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    completed_at TIMESTAMP NULL DEFAULT NULL COMMENT '完成时间',
    error_message TEXT COMMENT '错误信息',
    is_billing TINYINT(1) DEFAULT 0 COMMENT '是否计费中',
    error_code VARCHAR(10) DEFAULT NULL COMMENT '错误码',
    rpo_ms BIGINT DEFAULT NULL COMMENT 'RPO延迟(毫秒)',
    rto_ms BIGINT DEFAULT NULL COMMENT 'RTO延迟(毫秒)',
    task_type VARCHAR(20) DEFAULT NULL COMMENT '任务类型: SYNC-同步, DR-灾备',
    dr_status VARCHAR(20) DEFAULT NULL COMMENT '灾备状态',
    dr_switch_count INT DEFAULT NULL COMMENT '灾备倒换次数',
    dr_switch_start_time DATETIME(6) DEFAULT NULL COMMENT '灾备倒换开始时间',
    kafka_bootstrap_servers VARCHAR(500) DEFAULT NULL COMMENT 'Kafka连接地址',
    kafka_topic_prefix VARCHAR(100) DEFAULT NULL COMMENT 'Kafka主题前缀',
    kafka_topic_strategy VARCHAR(20) DEFAULT NULL COMMENT 'Kafka主题策略',
    subscribe_format VARCHAR(20) DEFAULT NULL COMMENT '订阅消息格式',
    target_db_name VARCHAR(255) DEFAULT NULL COMMENT '目标数据库名',
    increment_started BIT(1) DEFAULT NULL COMMENT '是否已开始增量同步',
    target_connections TEXT COMMENT '多目标库连接串JSON数组（fan-out）',
    fanout_enabled BIT(1) DEFAULT NULL COMMENT '是否启用多目标库分发',
    fanout_target_count INT DEFAULT NULL COMMENT 'fan-out目标库数量',
    INDEX idx_status (status),
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at),
    INDEX idx_is_deleted (is_deleted),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步任务工作流表';

-- ============================================================
-- 3. 工作流日志表
-- ============================================================
CREATE TABLE IF NOT EXISTS workflow_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL,
    level ENUM('INFO', 'WARNING', 'ERROR') DEFAULT 'INFO' COMMENT '日志级别',
    message TEXT NOT NULL COMMENT '日志内容',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    INDEX idx_workflow_id (workflow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='工作流日志表';

-- ============================================================
-- 4. 数据对比任务表
-- ============================================================
CREATE TABLE IF NOT EXISTS validation_tasks (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL COMMENT '对比任务名称',
    workflow_id VARCHAR(36) NOT NULL COMMENT '关联的同步任务ID',
    workflow_name VARCHAR(255) DEFAULT NULL COMMENT '关联的同步任务名称',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    source_connection TEXT COMMENT '源数据库连接',
    target_connection TEXT COMMENT '目标数据库连接',
    sync_objects TEXT COMMENT '同步对象JSON',
    compare_type VARCHAR(20) DEFAULT 'ROW_COUNT' COMMENT '对比类型: ROW_COUNT/CONTENT',
    compare_result LONGTEXT COMMENT '对比结果JSON',
    status ENUM('PENDING', 'RUNNING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING' COMMENT '对比状态',
    total_tables INT DEFAULT 0 COMMENT '总表数',
    passed_tables INT DEFAULT 0 COMMENT '通过对比的表数',
    failed_tables INT DEFAULT 0 COMMENT '对比失败的表数',
    total_rows BIGINT DEFAULT 0 COMMENT '总行数',
    mismatched_rows BIGINT DEFAULT 0 COMMENT '不一致的行数',
    error_message TEXT COMMENT '错误信息',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    started_at TIMESTAMP NULL DEFAULT NULL COMMENT '开始时间',
    completed_at TIMESTAMP NULL DEFAULT NULL COMMENT '完成时间',
    is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否软删除',
    task_type VARCHAR(20) DEFAULT NULL COMMENT '任务类型: SYNC/DR',
    repair_status VARCHAR(20) DEFAULT NULL COMMENT '差异修复状态',
    repair_summary LONGTEXT COMMENT '差异修复结果摘要JSON',
    repaired_at DATETIME(6) DEFAULT NULL COMMENT '修复完成时间',
    source_db_name VARCHAR(255) DEFAULT NULL COMMENT '源数据库名',
    target_db_name VARCHAR(255) DEFAULT NULL COMMENT '目标数据库名',
    INDEX idx_workflow_id (workflow_id),
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_is_deleted (is_deleted),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据对比任务表';

-- ============================================================
-- 5. 校验任务日志表
-- ============================================================
CREATE TABLE IF NOT EXISTS validation_task_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    validation_task_id VARCHAR(36) NOT NULL COMMENT '校验任务ID',
    level ENUM('INFO', 'WARNING', 'ERROR') DEFAULT 'INFO' COMMENT '日志级别',
    message TEXT COMMENT '日志内容',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_validation_task_id (validation_task_id),
    FOREIGN KEY (validation_task_id) REFERENCES validation_tasks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='校验任务日志表';

-- ============================================================
-- 6. 审计日志表
-- ============================================================
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL COMMENT '操作用户ID',
    username VARCHAR(100) DEFAULT NULL COMMENT '操作用户名',
    action ENUM('CREATE_TASK','UPDATE_CONFIG','LAUNCH_TASK','PAUSE_TASK','RESUME_TASK',
                'STOP_TASK','DELETE_TASK','RETRY_TASK','FAILOVER_TASK','LOGIN','LOGOUT',
                'CHANGE_PASSWORD') NOT NULL COMMENT '操作类型',
    workflow_id VARCHAR(36) DEFAULT NULL COMMENT '关联工作流ID',
    workflow_name VARCHAR(200) DEFAULT NULL COMMENT '工作流名称',
    details TEXT COMMENT '操作详情(JSON)',
    result ENUM('SUCCESS','FAILURE') NOT NULL DEFAULT 'SUCCESS' COMMENT '操作结果',
    error_message VARCHAR(1000) DEFAULT NULL COMMENT '错误信息',
    client_ip VARCHAR(50) DEFAULT NULL COMMENT '客户端IP',
    user_agent VARCHAR(500) DEFAULT NULL COMMENT 'User-Agent',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '操作时间',
    INDEX idx_audit_user_id (user_id),
    INDEX idx_audit_workflow_id (workflow_id),
    INDEX idx_audit_action (action),
    INDEX idx_audit_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='审计日志表';

-- ============================================================
-- 7. 慢SQL记录表
-- ============================================================
CREATE TABLE IF NOT EXISTS slow_sql_records (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL COMMENT '工作流ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    sql_text TEXT NOT NULL COMMENT 'SQL文本',
    execution_time_ms BIGINT NOT NULL COMMENT '执行耗时(毫秒)',
    table_name VARCHAR(200) DEFAULT NULL COMMENT '表名',
    sql_type VARCHAR(20) DEFAULT NULL COMMENT 'SQL类型: SELECT/INSERT/UPDATE/DELETE',
    threshold_ms BIGINT DEFAULT NULL COMMENT '慢SQL阈值(毫秒)',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    INDEX idx_slow_sql_workflow_id (workflow_id),
    INDEX idx_slow_sql_user_id (user_id),
    INDEX idx_slow_sql_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='慢SQL记录表';

-- ============================================================
-- 8. 数据校验结果表（自动校验与差异修复）
-- ============================================================
CREATE TABLE IF NOT EXISTS data_validations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL COMMENT '工作流ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    table_name VARCHAR(200) DEFAULT NULL COMMENT '表名',
    validation_type VARCHAR(20) DEFAULT 'ROW_COUNT' COMMENT '校验类型: ROW_COUNT/CONTENT/CHECKSUM',
    source_db_name VARCHAR(200) DEFAULT NULL COMMENT '源库名',
    target_db_name VARCHAR(200) DEFAULT NULL COMMENT '目标库名',
    source_count BIGINT DEFAULT NULL COMMENT '源行数',
    target_count BIGINT DEFAULT NULL COMMENT '目标行数',
    mismatched_count BIGINT DEFAULT NULL COMMENT '不一致行数',
    status VARCHAR(20) DEFAULT 'PENDING' COMMENT '状态: PENDING/RUNNING/PASSED/FAILED/REPAIRING/REPAIRED',
    repair_sql TEXT COMMENT '修复SQL',
    repair_status VARCHAR(20) DEFAULT NULL COMMENT '修复状态',
    error_message TEXT COMMENT '错误信息',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    completed_at TIMESTAMP NULL DEFAULT NULL COMMENT '完成时间',
    INDEX idx_validation_workflow_id (workflow_id),
    INDEX idx_validation_user_id (user_id),
    INDEX idx_validation_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据校验结果表';

-- ============================================================
-- 9. 资源配额表（多租户隔离）
-- ============================================================
CREATE TABLE IF NOT EXISTS resource_quotas (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL COMMENT '用户ID',
    max_tasks INT DEFAULT 50 COMMENT '最大任务数',
    max_concurrent_tasks INT DEFAULT 5 COMMENT '最大并发运行任务数',
    max_storage_mb BIGINT DEFAULT 10240 COMMENT '最大存储空间(MB)',
    api_rate_limit_per_min INT DEFAULT 100 COMMENT 'API调用频率限制(次/分钟)',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    max_full_sync_concurrent_tables INT DEFAULT NULL COMMENT '全量同步最大并发表数',
    max_increment_rows_per_sec INT DEFAULT NULL COMMENT '增量同步限速(行/秒)',
    UNIQUE KEY user_id (user_id),
    INDEX idx_quota_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资源配额表';

-- ============================================================
-- 10. 配置版本管理表（支持回滚）
-- ============================================================
CREATE TABLE IF NOT EXISTS config_versions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL COMMENT '工作流ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    version_number INT NOT NULL COMMENT '版本号',
    config_snapshot TEXT NOT NULL COMMENT '配置快照JSON',
    change_description VARCHAR(500) DEFAULT NULL COMMENT '变更描述',
    created_by VARCHAR(100) DEFAULT NULL COMMENT '创建人',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_config_version_workflow_id (workflow_id),
    INDEX idx_config_version_user_id (user_id),
    INDEX idx_config_version_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='配置版本管理表';

-- ============================================================
-- 11. 告警规则表
-- ============================================================
CREATE TABLE IF NOT EXISTS alert_rules (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL COMMENT '用户ID',
    workflow_id VARCHAR(36) DEFAULT NULL COMMENT '工作流ID',
    rule_name VARCHAR(200) NOT NULL COMMENT '规则名称',
    metric_type VARCHAR(30) NOT NULL COMMENT '指标类型: RPO_MS/RTO_MS/SYNC_LATENCY/PROCESS_DOWN/SYNC_FAILED/SLOW_SQL',
    operator VARCHAR(5) DEFAULT 'GT' COMMENT '比较操作符: GT/LT/EQ/GTE/LTE',
    threshold DOUBLE DEFAULT NULL COMMENT '阈值',
    duration_seconds INT DEFAULT 0 COMMENT '持续时间(秒)',
    notify_channels VARCHAR(100) DEFAULT 'WEBHOOK' COMMENT '通知渠道: EMAIL/DINGTALK/WEBHOOK',
    webhook_url VARCHAR(500) DEFAULT NULL COMMENT 'Webhook地址',
    email_recipients VARCHAR(500) DEFAULT NULL COMMENT '邮件收件人',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    last_triggered_at TIMESTAMP NULL DEFAULT NULL COMMENT '最后触发时间',
    trigger_count INT DEFAULT 0 COMMENT '触发次数',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_alert_user_id (user_id),
    INDEX idx_alert_workflow_id (workflow_id),
    INDEX idx_alert_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警规则表';

-- ============================================================
-- 12. 告警事件记录表
-- ============================================================
CREATE TABLE IF NOT EXISTS alert_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    rule_id BIGINT DEFAULT NULL COMMENT '规则ID',
    user_id BIGINT DEFAULT NULL COMMENT '用户ID',
    workflow_id VARCHAR(36) DEFAULT NULL COMMENT '工作流ID',
    rule_name VARCHAR(200) DEFAULT NULL COMMENT '规则名称',
    metric_type VARCHAR(30) DEFAULT NULL COMMENT '指标类型',
    metric_value DOUBLE DEFAULT NULL COMMENT '指标值',
    threshold DOUBLE DEFAULT NULL COMMENT '阈值',
    message TEXT COMMENT '告警消息',
    status VARCHAR(20) DEFAULT 'NOTIFIED' COMMENT '状态: PENDING/NOTIFIED/RESOLVED',
    notify_channels VARCHAR(100) DEFAULT NULL COMMENT '通知渠道',
    notify_result TEXT COMMENT '通知结果',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    resolved_at TIMESTAMP NULL DEFAULT NULL COMMENT '解决时间',
    INDEX idx_alert_event_rule_id (rule_id),
    INDEX idx_alert_event_workflow_id (workflow_id),
    INDEX idx_alert_event_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警事件记录表';

-- ============================================================
-- 13. 任务依赖编排表
-- ============================================================
CREATE TABLE IF NOT EXISTS task_dependencies (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    upstream_workflow_id VARCHAR(36) NOT NULL COMMENT '上游工作流ID',
    downstream_workflow_id VARCHAR(36) NOT NULL COMMENT '下游工作流ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    trigger_condition VARCHAR(20) DEFAULT 'ON_SUCCESS' COMMENT '触发条件: ON_SUCCESS/ON_COMPLETION/ON_FAILURE',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    last_triggered_at TIMESTAMP NULL DEFAULT NULL COMMENT '最后触发时间',
    trigger_count INT DEFAULT 0 COMMENT '触发次数',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_dep_upstream (upstream_workflow_id),
    INDEX idx_dep_downstream (downstream_workflow_id),
    INDEX idx_dep_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务依赖编排表';

-- ============================================================
-- 14. 重试策略配置表
-- ============================================================
CREATE TABLE IF NOT EXISTS retry_policies (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) DEFAULT NULL COMMENT '工作流ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    policy_name VARCHAR(200) DEFAULT NULL COMMENT '策略名称',
    error_type VARCHAR(20) DEFAULT 'ALL' COMMENT '错误类型: CONNECTION/SQL/TIMEOUT/ALL',
    max_retries INT DEFAULT 3 COMMENT '最大重试次数',
    retry_interval_ms BIGINT DEFAULT 5000 COMMENT '重试间隔(毫秒)',
    backoff_strategy VARCHAR(20) DEFAULT 'EXPONENTIAL' COMMENT '退避策略: FIXED/EXPONENTIAL',
    max_interval_ms BIGINT DEFAULT 60000 COMMENT '最大间隔(毫秒)',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    current_retry_count INT DEFAULT 0 COMMENT '当前重试次数',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_retry_workflow_id (workflow_id),
    INDEX idx_retry_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='重试策略配置表';

-- ============================================================
-- 15. 定时调度配置表
-- ============================================================
CREATE TABLE IF NOT EXISTS task_schedules (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL COMMENT '工作流ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    cron_expression VARCHAR(100) NOT NULL COMMENT 'Cron表达式',
    schedule_name VARCHAR(200) DEFAULT NULL COMMENT '调度名称',
    schedule_type VARCHAR(20) DEFAULT 'FULL_SYNC' COMMENT '调度类型: FULL_SYNC/VALIDATION',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    last_triggered_at TIMESTAMP NULL DEFAULT NULL COMMENT '最后触发时间',
    next_trigger_at TIMESTAMP NULL DEFAULT NULL COMMENT '下次触发时间',
    trigger_count INT DEFAULT 0 COMMENT '触发次数',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_schedule_workflow_id (workflow_id),
    INDEX idx_schedule_user_id (user_id),
    INDEX idx_schedule_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='定时调度配置表';
