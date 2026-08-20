-- ============================================================
-- V21 (2026-08-19)　数据治理三件套
--
-- 按 markdown/DATA_GOVERNANCE_DESIGN_20260819.md：
--   阶段 1/2　字段级血缘
--   阶段 4　　数据分级与标签
--   阶段 5　　Schema 演进审批
--
-- 三者有依赖：分级的策略校验（"目标端级别不得低于源端"）与
-- 审批的触发条件（"改动涉及 RESTRICTED 列必须审批"）都要靠血缘。
-- ============================================================

-- ------------------------------------------------------------
-- 1. 字段级血缘
--
-- 节点是"某个字段的一个版本"，边是"变换"。
-- 有效区间用 seqno 而不是墙上时间：与 schema-timeline 同一把尺子，
-- 才能回答"某个位点时这一列从哪来"。valid_to_seqno 为 NULL = 当前有效。
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lineage_node (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    side ENUM('SOURCE','TARGET') NOT NULL COMMENT '该节点属于源端还是目标端',
    db_name VARCHAR(128) NOT NULL COMMENT '库名',
    table_name VARCHAR(128) NOT NULL COMMENT '表名',
    column_name VARCHAR(128) NOT NULL COMMENT '列名',
    valid_from_seqno BIGINT NOT NULL DEFAULT 0 COMMENT '生效位点',
    valid_to_seqno BIGINT DEFAULT NULL COMMENT '失效位点；NULL=当前有效',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_node (side, db_name, table_name, column_name, valid_from_seqno),
    INDEX idx_lookup (db_name, table_name, column_name),
    INDEX idx_current (valid_to_seqno)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='字段级血缘节点';

CREATE TABLE IF NOT EXISTS lineage_edge (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    src_node_id BIGINT NOT NULL,
    dst_node_id BIGINT NOT NULL,
    -- IDENTITY  原样搬运          RENAME    列名映射
    -- MASK      脱敏              FILTER    行过滤（整表口径，记在任一列上）
    -- DEFAULT   附加列（无源）    ROUTE_SPLIT / ROUTE_MERGE  分片路由
    -- DROP      不同步
    operator VARCHAR(32) NOT NULL COMMENT '变换算子',
    operator_detail JSON DEFAULT NULL COMMENT '算子参数（脱敏规则、分片键等）',
    workflow_id VARCHAR(36) NOT NULL COMMENT '产生这条边的任务',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_edge (src_node_id, dst_node_id, workflow_id),
    INDEX idx_src (src_node_id),
    INDEX idx_dst (dst_node_id),
    INDEX idx_workflow (workflow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='字段级血缘边';

-- ------------------------------------------------------------
-- 2. 数据分级与标签
--
-- 粒度到列。级别单调可比（PUBLIC < INTERNAL < SENSITIVE < RESTRICTED），
-- 策略校验"目标端级别不得低于源端"靠这个序。
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_classification (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    db_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    column_name VARCHAR(128) NOT NULL,
    level ENUM('PUBLIC','INTERNAL','SENSITIVE','RESTRICTED') NOT NULL DEFAULT 'INTERNAL',
    tags VARCHAR(512) DEFAULT NULL COMMENT '逗号分隔的自由标签（如 PII,财务）',
    source ENUM('MANUAL','RULE') NOT NULL DEFAULT 'MANUAL' COMMENT '人工打标还是规则命中',
    matched_rule VARCHAR(128) DEFAULT NULL COMMENT 'source=RULE 时命中的规则名',
    updated_by BIGINT DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_column (db_name, table_name, column_name),
    INDEX idx_level (level),
    INDEX idx_table (db_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据分级与标签';

-- 自动打标规则：按列名正则命中。人工打标优先级高于规则，重跑规则不覆盖人工值。
CREATE TABLE IF NOT EXISTS classification_rule (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    column_pattern VARCHAR(256) NOT NULL COMMENT '列名正则（不区分大小写）',
    level ENUM('PUBLIC','INTERNAL','SENSITIVE','RESTRICTED') NOT NULL,
    tags VARCHAR(512) DEFAULT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    priority INT NOT NULL DEFAULT 100 COMMENT '数值小的先匹配',
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_name (name),
    INDEX idx_enabled (enabled, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='自动分级规则';

-- ------------------------------------------------------------
-- 3. Schema 演进审批
--
-- DDL 策略的 MANUAL 档此前只是"停下来记一条日志"，没有后续。
-- 这张表把它升级成"停下来 + 建审批单 + 通过后继续"。
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_change_request (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL,
    db_name VARCHAR(128) DEFAULT NULL COMMENT 'DDL 作用的库',
    table_name VARCHAR(128) DEFAULT NULL,
    ddl_type VARCHAR(64) DEFAULT NULL COMMENT 'ALTER_TABLE / CREATE_TABLE / DROP_COLUMN …',
    ddl_sql TEXT NOT NULL COMMENT '原始 DDL',
    -- 事件定位：审批通过后引擎要能找回这条 DDL 并应用
    seqno BIGINT DEFAULT NULL,
    event_id VARCHAR(256) DEFAULT NULL,
    -- 风险提示：涉及的列里最高的敏感级别（来自 data_classification）
    max_level ENUM('PUBLIC','INTERNAL','SENSITIVE','RESTRICTED') DEFAULT NULL,
    affected_columns VARCHAR(1024) DEFAULT NULL COMMENT '受影响的列（含下游，来自血缘）',
    status ENUM('PENDING','APPROVED','REJECTED','APPLIED','FAILED') NOT NULL DEFAULT 'PENDING',
    reviewer_id BIGINT DEFAULT NULL,
    review_comment VARCHAR(1024) DEFAULT NULL,
    reviewed_at TIMESTAMP NULL DEFAULT NULL,
    applied_at TIMESTAMP NULL DEFAULT NULL,
    error_message VARCHAR(1024) DEFAULT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    -- 同一任务同一 seqno 只建一张单：引擎重启后会重放这段 THL，
    -- 不去重会把同一条 DDL 反复建成新单
    UNIQUE KEY uk_event (workflow_id, seqno),
    INDEX idx_status (status),
    INDEX idx_workflow (workflow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Schema 演进审批单';

-- 审计动作补齐
ALTER TABLE audit_logs
    MODIFY COLUMN action ENUM(
        'CREATE_TASK','UPDATE_CONFIG','LAUNCH_TASK','PAUSE_TASK','RESUME_TASK',
        'STOP_TASK','DELETE_TASK','RETRY_TASK','FAILOVER_TASK','LOGIN','LOGOUT',
        'CHANGE_PASSWORD',
        'UPLOAD_CERTIFICATE','DELETE_CERTIFICATE','UPDATE_CREDENTIAL',
        'BROWSE_METADATA','RUN_VALIDATION','DOWNLOAD_DIAGNOSTICS',
        'VIEW_DEADLETTER','SKIP_EVENT','MANAGE_USER',
        -- V21 新增
        'REBUILD_LINEAGE','UPDATE_CLASSIFICATION','APPROVE_SCHEMA_CHANGE',
        'REJECT_SCHEMA_CHANGE'
    ) NOT NULL COMMENT '操作类型';

-- 常见敏感列的内置规则。priority 小的先匹配，因此更具体的规则排前面。
INSERT INTO classification_rule (name, column_pattern, level, tags, priority) VALUES
    ('身份证号', '(id_?card|identity_?no|sfzh)', 'RESTRICTED', 'PII', 10),
    ('银行卡号', '(bank_?card|card_?no|account_?no)', 'RESTRICTED', 'PII,财务', 10),
    ('手机号',   '(phone|mobile|tel|telephone)', 'SENSITIVE', 'PII', 20),
    ('邮箱',     '(email|mail_?addr)', 'SENSITIVE', 'PII', 20),
    ('姓名',     '(^name$|user_?name|real_?name|full_?name)', 'SENSITIVE', 'PII', 30),
    ('地址',     '(address|addr$|住址)', 'SENSITIVE', 'PII', 30),
    ('口令',     '(password|passwd|pwd|secret|token)', 'RESTRICTED', '凭证', 10),
    ('金额',     '(amount|balance|salary|price|fee)', 'INTERNAL', '财务', 50)
ON DUPLICATE KEY UPDATE name = name;
