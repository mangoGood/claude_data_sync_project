-- P2-4 可观测性与 SLA 闭环：把 agent 侧已有但只落在文件/日志里的信号沉到任务表，
-- 让 alert_rules 能像 RPO_MS/RTO_MS 一样对它们设阈值。
--
-- replication_lag_ms 与已有的 rpo_ms 不是一回事：rpo 是"最新捕获事件 − 已应用事件"，
-- 源库空闲时恒为 0（链路卡死也是 0）；replication_lag_ms 用源库当前时钟做分子，卡多久涨多久。

ALTER TABLE workflows
    ADD COLUMN replication_lag_ms  BIGINT NULL COMMENT '绝对复制延迟：源库当前时刻-已应用事件源端时刻(ms)',
    ADD COLUMN capture_replay_bytes BIGINT NULL COMMENT 'capture 重启后重放的字节数',
    ADD COLUMN restart_count_10m   INT    NULL COMMENT '近 10 分钟子进程重启次数',
    ADD COLUMN conflict_count      BIGINT NULL COMMENT '双向冲突裁决累计条数',
    ADD COLUMN deadletter_count    BIGINT NULL COMMENT '死信累计条数',
    ADD COLUMN disk_usage_bytes    BIGINT NULL COMMENT '任务工作目录占用字节数';
