-- ============================================================
-- 表结构时序库的中心存储：每张表在每个 binlog 位点上的结构版本。
--
-- 解决的是"用<b>现在</b>的表结构解释<b>过去</b>的 binlog 事件"这个错位。抽取端今天的列名/
-- 类型/主键全部来自源库 information_schema 的当前定义，而 extract 是在全量做完之后才开始
-- 消化几小时前的 .cap——链路有延迟时源库做了 ALTER，按当前定义解析会让整行的值与列错位，
-- 写进目标库的是合法值、看不出异常。设计见 markdown/SCHEMA_TIMELINE_DESIGN_20260811.md。
--
-- <b>为什么不塞进 task_checkpoints.payload</b>：那一列是 TEXT（64KB）。一张 50 列表的结构
-- JSON 就有 5~6KB，几十张表直接撑爆。这里用 MEDIUMTEXT，且一行只放一个版本。
--
-- 写入方是 agent（extract 子进程够不着元数据库，它只写本地 schema_history.jsonl，
-- 由 agent 增量上传）。payload 对 agent 是<b>不透明的原始行</b>——结构格式的知识只存在于
-- migration-extract 一处，agent 只认 (db, table, 位点) 这几个键，回灌时原样写回。
--
-- 两条规则：
--   ① 幂等：唯一键 (task_id, db_name, table_name, monotonic_key)。重放同一段 binlog 会
--      重新算出同位点的同一版本，靠唯一键吸收，不需要应用层判重。
--   ② fencing：写入带 lease_epoch，低于该任务已记录的最高 epoch 一律整批拒绝——
--      网络分区下没死透的老 agent 手里的时序库是旧的，让它写进来就是把错的结构固化下去。
-- ============================================================
CREATE TABLE IF NOT EXISTS task_schema_versions (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id       VARCHAR(36)  NOT NULL,
    db_name       VARCHAR(64)  NOT NULL COMMENT '源库名（小写）',
    table_name    VARCHAR(64)  NOT NULL COMMENT '源表名（小写）',
    binlog_file   VARCHAR(255) NOT NULL COMMENT '该版本的生效位点：文件',
    binlog_pos    BIGINT       NOT NULL COMMENT '该版本的生效位点：位置',
    monotonic_key BIGINT       NOT NULL COMMENT '文件号<<32|位置，排序与裁剪用；RESET MASTER 会让它回退（与 task_checkpoints 同一局限）',
    change_kind   VARCHAR(16)  NOT NULL COMMENT 'CREATED/ALTERED/DROPPED',
    payload       MEDIUMTEXT   NULL COMMENT 'schema_history.jsonl 的原始行；DROPPED 也有行（payload 里 schema 为 null）',
    agent_id      VARCHAR(64)  NOT NULL COMMENT '写入方 agent',
    lease_epoch   INT          NOT NULL DEFAULT 0 COMMENT 'fencing token，取自 workflows.lease_epoch',
    created_at    DATETIME(3)  NOT NULL COMMENT '写入时刻；一律 JVM 侧绑定，禁止 SQL NOW()（容器 UTC 与 JVM 时区差 8h）',
    UNIQUE KEY uk_schema_version (task_id, db_name, table_name, monotonic_key),
    INDEX idx_schema_task_key (task_id, monotonic_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表结构时序库（按 binlog 位点索引的表结构版本）';
