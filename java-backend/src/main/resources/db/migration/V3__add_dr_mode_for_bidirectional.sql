-- ============================================================
-- 双向灾备支持：
--   dr_mode              灾备方向（UNIDIRECTIONAL 单向 / BIDIRECTIONAL 双向，仅 DR 任务使用）
--   dr_peer_workflow_id  双向灾备对端任务ID（正向主任务 与 反向影子任务 互相指向）
--
-- 双向灾备实现为「一显一隐」两条同步通道：用户可见的 DR 主任务负责 A→B（全量+增量），
-- 隐藏的 DR_SHADOW 影子任务负责 B→A（仅增量，主方向进入增量同步后由 backend 自动启动）。
-- 环路防护依赖 apply 端事务内 origin 打标 + capture 端见标记跳过（见 migration-common bidi 包）。
-- ============================================================
ALTER TABLE workflows
    ADD COLUMN dr_mode VARCHAR(20) DEFAULT NULL COMMENT '灾备方向: UNIDIRECTIONAL-单向, BIDIRECTIONAL-双向（仅DR任务）',
    ADD COLUMN dr_peer_workflow_id VARCHAR(36) DEFAULT NULL COMMENT '双向灾备对端任务ID（主任务与影子反向任务互指）';
