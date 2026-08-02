-- ============================================================
-- 控制面/执行面集群化（P1-1）：agent 注册表 + 任务租约。
--
-- 现状是「Kafka 广播 + 谁抢到算谁」：所有 agent 都消费同一个 topic、同一个消费组，
-- 谁拿到分区谁执行；agent 一旦硬崩，它名下的任务就<b>没有人接管</b>——必须等这台机器
-- 被人肉拉起来。恢复路径本身早就有了（子进程各自的 checkpoint 续传），缺的只是
-- 「谁来负责这个任务」这条信息。
--
--   agents                 —— 每个 agent 进程注册一行，心跳刷新 heartbeat_at
--   workflows.agent_id     —— 任务当前归属哪个 agent（NULL = 尚未指派，兼容旧广播语义）
--   workflows.lease_*      —— 租约到期时刻与代次；心跳超时即作废租约、改派给其它存活 agent，
--                             lease_epoch 单调递增便于排查"谁在什么时候接管过"
-- ============================================================
CREATE TABLE IF NOT EXISTS agents (
    agent_id VARCHAR(64) PRIMARY KEY COMMENT 'agent 稳定标识（进程重启后不变）',
    host VARCHAR(255) NOT NULL COMMENT 'agent HTTP 主机',
    port INT NOT NULL COMMENT 'agent HTTP 端口',
    capacity INT NOT NULL DEFAULT 10 COMMENT '可并发承载的任务数',
    running_tasks INT NOT NULL DEFAULT 0 COMMENT '当前在跑的任务数（心跳时刷新）',
    status VARCHAR(20) NOT NULL DEFAULT 'ONLINE' COMMENT 'ONLINE/OFFLINE',
    version VARCHAR(50) DEFAULT NULL COMMENT 'agent 版本',
    started_at DATETIME DEFAULT NULL COMMENT '本次进程启动时刻',
    heartbeat_at DATETIME DEFAULT NULL COMMENT '最近一次心跳',
    INDEX idx_agents_heartbeat (status, heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='执行面 agent 注册表';

ALTER TABLE workflows
    ADD COLUMN agent_id VARCHAR(64) DEFAULT NULL COMMENT '当前归属的 agent（NULL=未指派）',
    ADD COLUMN lease_expire_at DATETIME DEFAULT NULL COMMENT '租约到期时刻，过期即可被抢占',
    ADD COLUMN lease_epoch INT NOT NULL DEFAULT 0 COMMENT '租约代次，每次改派 +1';

CREATE INDEX idx_workflows_agent ON workflows (agent_id, status);
