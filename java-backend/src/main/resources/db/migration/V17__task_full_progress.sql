-- 全量表级断点的中心化。
--
-- 位点中心化（V13）解决的是**增量**的跨机接管；全量的表级断点一直只在
-- files/<taskId>/migration_progress.mv.db 这个本机 H2 里。接管方的目录是空的，
-- 于是已经搬完的表也要从头再搬一遍——全量提到 38K 行/秒之后，一张 10 亿行的表仍要 ~7 小时，
-- 因为一次 agent 崩溃重来一遍是产品级的问题。
--
-- 原设计（CHECKPOINT_DURABILITY_DESIGN §9）把这条明确划在范围外，理由是"全量重跑是幂等的、
-- 代价可接受"。它保证的是**正确性**，不是 RTO——本轮实测后改为要做。
CREATE TABLE IF NOT EXISTS task_full_progress (
    task_id          VARCHAR(64)  NOT NULL,
    table_key        VARCHAR(255) NOT NULL COMMENT '与本地 migration_progress.table_name 一致（汇聚下带源库名前缀）',
    status           VARCHAR(32)  NOT NULL,
    total_rows       BIGINT       NOT NULL DEFAULT 0,
    migrated_rows    BIGINT       NOT NULL DEFAULT 0,
    last_migrated_id BIGINT       DEFAULT NULL,
    agent_id         VARCHAR(64)  DEFAULT NULL,
    updated_at       DATETIME(3)  NOT NULL,
    PRIMARY KEY (task_id, table_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全量表级断点（跨机接管用）';
