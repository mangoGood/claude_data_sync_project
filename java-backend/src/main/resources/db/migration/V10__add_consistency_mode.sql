-- ============================================================
-- 增量投递的一致性语义（创建任务时由用户选定，之后不可修改）：
--
--   TRANSACTIONAL 事务一致性：目标库的提交顺序与源库事务提交顺序一致，
--                 每个源事务在目标端仍是一个事务（内容一致、不拆不并）。
--                 增量应用串行执行，吞吐低于最终一致性。
--   EVENTUAL      最终一致性：源事务可被打散/合并，只保证目标库最终数据一致。
--                 增量按「表 + 主键」冲突矩阵并发应用（不同表、同表不同主键可并发；
--                 同表同主键严格保序），吞吐远高于事务一致性。
--
-- 默认值按任务类型（在后端 createWorkflow 里判定）：订阅/灾备=TRANSACTIONAL，同步=EVENTUAL。
--
-- 存量任务一律回填 EVENTUAL —— 它才是这些任务此前<b>实际</b>跑的语义
-- （apply.transaction.mode 历史默认 EVENT）。回填成 TRANSACTIONAL 会让升级后
-- 正在跑的灾备/订阅任务突然改变投递语义，这不是升级该做的事。
-- ============================================================
ALTER TABLE workflows
    ADD COLUMN consistency_mode VARCHAR(20) NOT NULL DEFAULT 'EVENTUAL'
        COMMENT '一致性语义: TRANSACTIONAL-事务一致, EVENTUAL-最终一致（创建后不可修改）';

UPDATE workflows SET consistency_mode = 'EVENTUAL' WHERE consistency_mode IS NULL;
