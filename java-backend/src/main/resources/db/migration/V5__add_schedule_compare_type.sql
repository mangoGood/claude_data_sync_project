-- ============================================================
-- 对比任务定时化：VALIDATION 类型的调度需记录对比类型（行数/内容）。
-- 到点由 TaskScheduleService 复用 ValidationTaskService 自动创建并执行对比任务。
-- 仅 schedule_type=VALIDATION 时有意义；FULL_SYNC 调度忽略此列。
-- ============================================================
ALTER TABLE task_schedules
    ADD COLUMN compare_type VARCHAR(20) DEFAULT NULL COMMENT '对比类型(VALIDATION调度用): ROW_COUNT/CONTENT';
