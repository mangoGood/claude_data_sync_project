-- 启动前预检的留档。
--
-- 此前预检只在前端弹窗里跑一次、点确定就过，既不阻断也不留痕：事后没有任何办法回答
-- "这个任务当初是在什么前提下启动的""是谁忽略了哪条 FAIL"。现在每次启动都落一行，
-- 无论放行、拦截还是强启。
CREATE TABLE IF NOT EXISTS task_precheck_results (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    workflow_id VARCHAR(36)  NOT NULL,
    user_id     BIGINT       DEFAULT NULL,
    overall     VARCHAR(16)  NOT NULL COMMENT 'PASS / WARNING / FAIL / ERROR',
    forced      TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1=预检 FAIL 但被强制启动',
    summary     VARCHAR(1000) DEFAULT NULL,
    checks_json TEXT         DEFAULT NULL COMMENT '检查项明细，原样存 schemaPrecheck 的 checks',
    created_at  DATETIME(3)  NOT NULL,
    INDEX idx_precheck_workflow (workflow_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='启动前预检结果留档';
