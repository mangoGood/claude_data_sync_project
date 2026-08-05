-- ============================================================
-- 位点持续持久化：把位点从"只活在某台 agent 的本地磁盘上"搬到元数据库。
--
-- 要解决的是 V8 集群化留下的一个静默数据丢失：V8 假设"接管方走既有的崩溃恢复路径
-- （从各自 checkpoint 续传）"，而这个前提只在<b>同一台机器</b>上成立。换一台 agent 接管时
-- files/<taskId>/ 是空的，AbstractTaskExecutor.initMysqlCheckpoint 找不到 checkpoint 就去
-- 取"源库此刻的位点"——崩溃时刻到接管时刻之间的全部变更被直接跳过，不报错、不告警、
-- 进度条一路 100%。故障转移越成功，丢得越干净。
--
--   task_checkpoints         —— 每条链路段的当前位点（(task_id, stage, stream_key) 一行）
--   task_checkpoint_history  —— 位点采样与重置审计，位点回溯（PITR）的数据来源
--
-- 两条写入规则（都压在 SQL 里，不靠应用层判断，并发下才拦得住）：
--   ① fencing：低于当前 lease_epoch 的写入一律拒绝——网络分区下没死透的老 agent 还在写位点
--   ② 单调：同一 epoch 内位点不许回退；monotonic_key=0 表示"该形态折不出可比标量"
--      （GTID 集、Mongo resume token），此时守卫自动降级为不校验
-- 更高的 epoch 无条件放行：它是租约的合法新主，且它的位点本来就是从中心库回灌来的，
-- 即便偏旧也只意味着多重放（安全方向），而拦住它会让位点永久卡死。
-- ============================================================
CREATE TABLE IF NOT EXISTS task_checkpoints (
    task_id       VARCHAR(36)  NOT NULL COMMENT '任务 id（跨实例汇聚的每条 MERGE_LEG 是独立任务，天然一 leg 一行）',
    stage         VARCHAR(16)  NOT NULL COMMENT 'CAPTURE/EXTRACT/APPLY/FULL/SUBSCRIBE',
    stream_key    VARCHAR(128) NOT NULL DEFAULT '-' COMMENT '单流固定 "-"；订阅按 topic-partition 时才有别的值',
    engine        VARCHAR(32)  NOT NULL COMMENT '源引擎类型',
    kind          VARCHAR(32)  NOT NULL COMMENT 'BINLOG_FILE_POS/GTID_SET/LSN/SCN/TSO/RESUME_TOKEN/REPL_OFFSET/SEQNO/KAFKA_OFFSET',
    payload       TEXT         NOT NULL COMMENT '引擎原生位点（properties 文本），原样保存不做归一',
    monotonic_key BIGINT       NOT NULL DEFAULT 0 COMMENT '可比标量，仅用于单调守卫；0=该形态不可比，跳过校验',
    source_ts     DATETIME(3)  DEFAULT NULL COMMENT '该位点对应的源端事件时刻（算 RPO 用）',
    agent_id      VARCHAR(64)  NOT NULL COMMENT '写入方 agent',
    lease_epoch   INT          NOT NULL DEFAULT 0 COMMENT 'fencing token，取自 workflows.lease_epoch',
    updated_at    DATETIME(3)  NOT NULL COMMENT '写入时刻；一律 JVM 侧绑定，禁止用 SQL NOW()（容器 UTC 与 JVM 时区差 8h）',
    PRIMARY KEY (task_id, stage, stream_key),
    INDEX idx_ckpt_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务位点中心存储';

CREATE TABLE IF NOT EXISTS task_checkpoint_history (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id       VARCHAR(36)  NOT NULL,
    stage         VARCHAR(16)  NOT NULL,
    stream_key    VARCHAR(128) NOT NULL DEFAULT '-',
    engine        VARCHAR(32)  NOT NULL DEFAULT '',
    kind          VARCHAR(32)  NOT NULL DEFAULT '',
    payload       TEXT         NOT NULL,
    monotonic_key BIGINT       NOT NULL DEFAULT 0,
    source_ts     DATETIME(3)  DEFAULT NULL,
    recorded_at   DATETIME(3)  NOT NULL,
    reason        VARCHAR(32)  NOT NULL COMMENT 'SAMPLE=周期采样 / RESET=人工重置 / FAILOVER=倒换作废 / SNAPSHOT=全量快照点',
    operator      VARCHAR(64)  DEFAULT NULL COMMENT 'RESET 时的操作人',
    INDEX idx_ckpt_hist (task_id, stage, recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务位点历史（回溯与审计）';
