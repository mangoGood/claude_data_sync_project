-- ============================================================
-- 新增任务状态 RECONNECTING（长期重连中）。
--
-- 背景：子进程崩溃后 agent 只有「短期指数退避重试」一档，预算耗尽即 FAILED，
-- 且熔断器打开后守护线程直接退出、进程永不再被拉起——目标库一次超过 ~2.5 分钟的
-- 计划内维护窗口就能把任务永久打死，必须人工 retry。
--
-- 补上「长期重连」一档后需要一个与 FAILED 区分的状态：
--   RECONNECTING = 可自愈、不算失败，重连成功会自动回到 INCREMENT_RUNNING / SUBSCRIBE_RUNNING；
--   FAILED       = 长期重连也用尽，需人工介入。
--
-- workflows.status 是 MySQL ENUM，新增取值必须改列定义（否则写入被截断成空串）。
-- ============================================================
ALTER TABLE workflows
    MODIFY COLUMN status ENUM('CONFIGURING', 'PENDING', 'RECEIVED', 'STARTING', 'FULL_MIGRATING',
                              'FULL_COMPLETED', 'INCREMENT_RUNNING', 'SUBSCRIBE_RUNNING',
                              'SWITCHING', 'RECONNECTING', 'COMPLETED', 'FAILED', 'PAUSED')
        DEFAULT 'CONFIGURING' COMMENT '任务状态';
