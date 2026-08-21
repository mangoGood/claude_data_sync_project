-- ============================================================
-- 流量复制与回放任务（仅 MySQL）
--
-- 与实时同步/灾备/数据订阅并列的第四类任务，复用 workflows 表的整套生命周期
-- （启动/暂停/停止/派发/集群租约/加密/审计/配额/预检门禁），只把它<b>特有的配置</b>
-- 放进这里的侧表。
--
-- 为什么不像 dr_* / kafka_* 那样直接往 workflows 加列：
--   workflows 已有 60+ 列，而这批字段只对 6 种 task_type 里的 2 种有意义，
--   再加 20 列会让每次查任务列表都拖着一大片恒为 NULL 的字段。
--
-- 为什么录制文件要单独一张表、而不是靠"去 agent 上列目录"：
--   1) 回放任务要在<b>建任务时</b>就能选录制文件，那时还没有 agent 归属；
--   2) 平台是多 agent 集群，录制文件躺在<b>产出它的那台</b> agent 上，
--      不记 agent_id 就不知道该去哪台机器取；
--   3) 录制文件可以被上传进来（根本不来自任何捕获任务）。
-- ============================================================

-- workflows.status 是 MySQL ENUM：新增取值必须改列定义，
-- 否则写入被静默截断成空串（V7 加 RECONNECTING 时踩过一次）。
ALTER TABLE workflows
    MODIFY COLUMN status ENUM('CONFIGURING', 'PENDING', 'RECEIVED', 'STARTING', 'FULL_MIGRATING',
                              'FULL_COMPLETED', 'INCREMENT_RUNNING', 'SUBSCRIBE_RUNNING',
                              'TRAFFIC_CAPTURING', 'TRAFFIC_REPLAYING',
                              'SWITCHING', 'RECONNECTING', 'COMPLETED', 'FAILED', 'PAUSED')
        DEFAULT 'CONFIGURING' COMMENT '任务状态';

CREATE TABLE IF NOT EXISTS traffic_task_config (
    task_id                 VARCHAR(36)  PRIMARY KEY COMMENT '= workflows.id',

    -- ===== 捕获侧 =====
    capture_backend         VARCHAR(20)  DEFAULT 'GENERAL_LOG'
        COMMENT 'GENERAL_LOG（默认，完整）/ PERF_SCHEMA（低侵入但会截断 >1KB 的 SQL 且高负载丢事件）',
    capture_databases       TEXT         DEFAULT NULL COMMENT 'JSON 数组，空=不限库',
    capture_classes         VARCHAR(100) DEFAULT 'SELECT,DML,DDL'
        COMMENT '负载类白名单。SET/USE/TCL 是会话状态，永远录，不受此项影响',
    capture_users           TEXT         DEFAULT NULL COMMENT 'JSON 数组，按来源账号过滤',
    capture_sample_rate     DECIMAL(5,4) DEFAULT 1.0000 COMMENT '按会话哈希采样（不是按语句：按语句会把事务切碎）',
    capture_enrich          TINYINT(1)   DEFAULT 0 COMMENT '是否用 performance_schema 富化错误码/行数/耗时（尽力而为）',
    capture_max_duration_ms BIGINT       DEFAULT 7200000  COMMENT '体量护栏：到顶自动封口并转 COMPLETED',
    capture_max_bytes       BIGINT       DEFAULT 21474836480,
    capture_max_records     BIGINT       DEFAULT 100000000,

    -- ===== 源库开关原值（兜底还原的依据）=====
    -- 必须落库而不是只放内存：捕获进程被 kill -9 / agent 硬崩时，
    -- 没有任何进程记得该把 general_log 还原成什么，而不还原会把源库磁盘写满。
    src_general_log_before  VARCHAR(10)  DEFAULT NULL,
    src_log_output_before   VARCHAR(30)  DEFAULT NULL,
    src_restore_pending     TINYINT(1)   DEFAULT 0 COMMENT '1=源库开关尚未确认还原，需人工/扫尾处理',

    -- ===== 回放侧 =====
    replay_recording_id     VARCHAR(36)  DEFAULT NULL COMMENT '要回放哪份录制（traffic_recordings.id）',
    replay_speed            DECIMAL(6,3) DEFAULT 1.000,
    replay_classes          VARCHAR(100) DEFAULT 'SELECT,DML',
    replay_lag_policy       VARCHAR(20)  DEFAULT 'WAIT'     COMMENT 'WAIT / SKIP / STRETCH',
    replay_lag_skip_ms      BIGINT       DEFAULT 5000,
    replay_gap_policy       VARCHAR(20)  DEFAULT 'PRESERVE' COMMENT 'PRESERVE / COMPRESS',
    replay_max_sessions     INT          DEFAULT 200,
    replay_compare          VARCHAR(20)  DEFAULT 'NONE'     COMMENT 'NONE / ROWCOUNT',
    replay_allow_dcl        TINYINT(1)   DEFAULT 0,
    replay_allow_dangerous  TINYINT(1)   DEFAULT 0 COMMENT '放行 DROP DATABASE / SET GLOBAL 等破坏性语句',
    replay_allow_same_instance TINYINT(1) DEFAULT 0 COMMENT '放行"回放到录制源库自己"——会把源库操作再做一遍',
    replay_abort_error_rate DECIMAL(5,4) DEFAULT 0.5000,

    created_at              DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) COMMENT='流量复制/回放任务配置';

CREATE TABLE IF NOT EXISTS traffic_recordings (
    id                 VARCHAR(36)  PRIMARY KEY,
    capture_task_id    VARCHAR(36)  DEFAULT NULL COMMENT '产出它的捕获任务；上传进来的录制为 NULL',
    user_id            BIGINT       NOT NULL,
    agent_id           VARCHAR(64)  DEFAULT NULL COMMENT '文件实际躺在哪台 agent 上（集群里必须记）',
    name               VARCHAR(200) NOT NULL,
    t0_wall            DATETIME(6)  DEFAULT NULL COMMENT '时间轴原点（绝对时刻，仅展示）',
    end_wall           DATETIME(6)  DEFAULT NULL,
    duration_ms        BIGINT       DEFAULT 0,
    record_count       BIGINT       DEFAULT 0,
    byte_size          BIGINT       DEFAULT 0,
    session_count      INT          DEFAULT 0,
    gap_count          INT          DEFAULT 0 COMMENT '时间轴空洞数：捕获停摆期间的语句永久丢失',
    stats_json         TEXT         DEFAULT NULL COMMENT 'manifest.stats 原样，供列表展示语句类别分布',
    source_fingerprint TEXT         DEFAULT NULL COMMENT 'manifest.source 原样：回放前逐项比对的依据',
    sha256             VARCHAR(64)  DEFAULT NULL,
    sealed             TINYINT(1)   DEFAULT 0 COMMENT '未封口的录制不允许回放',
    is_deleted         TINYINT(1)   DEFAULT 0,
    created_at         DATETIME     DEFAULT CURRENT_TIMESTAMP,
    KEY idx_capture_task (capture_task_id),
    KEY idx_user (user_id, is_deleted)
) COMMENT='流量录制文件目录';

-- 审计动作补齐：下载录制文件 = 导出业务数据，必须留痕
ALTER TABLE audit_logs
    MODIFY COLUMN action ENUM(
        'CREATE_TASK','UPDATE_CONFIG','LAUNCH_TASK','PAUSE_TASK','RESUME_TASK',
        'STOP_TASK','DELETE_TASK','RETRY_TASK','FAILOVER_TASK','LOGIN','LOGOUT',
        'CHANGE_PASSWORD',
        'UPLOAD_CERTIFICATE','DELETE_CERTIFICATE','UPDATE_CREDENTIAL',
        'BROWSE_METADATA','RUN_VALIDATION','DOWNLOAD_DIAGNOSTICS',
        'VIEW_DEADLETTER','SKIP_EVENT','MANAGE_USER',
        'REBUILD_LINEAGE','UPDATE_CLASSIFICATION','APPROVE_SCHEMA_CHANGE',
        'REJECT_SCHEMA_CHANGE',
        -- V22 新增
        'DOWNLOAD_TRAFFIC_RECORDING'
    ) NOT NULL COMMENT '操作类型';
