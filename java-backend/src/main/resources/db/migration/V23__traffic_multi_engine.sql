-- 流量复制与回放：PostgreSQL / Oracle 扩展。
--
-- 录制文件从此自带"我是哪种引擎录的"（format 升到 synctask-traffic/2）。
-- 库里也要记一份：回放向导要按引擎过滤可选的录制——一份 PG 录制被选去回放到 MySQL，
-- 只会在目标库上制造一堆半成功的破坏（SQL 方言不可能自动翻译）。

-- 任务配置：引擎 + 引擎特有的还原载荷
ALTER TABLE traffic_task_config
    ADD COLUMN engine VARCHAR(20) NOT NULL DEFAULT 'mysql'
        COMMENT '源端/目标端引擎: mysql / postgresql / oracle';

-- 源端被改动的状态。MySQL 只有两个全局变量（老的两列保留兼容），
-- PG 是四个 GUC 的原值与来源、Oracle 是两条审计策略名与启用范围 —— 形状完全不同，
-- 再往表上加列没有意义，统一放 JSON。
ALTER TABLE traffic_task_config
    ADD COLUMN src_state_before TEXT DEFAULT NULL
        COMMENT '源端原始状态（JSON，引擎自定义键值），兜底还原依据';

-- 录制目录：引擎 + 捕获通道
ALTER TABLE traffic_recordings
    ADD COLUMN engine VARCHAR(20) NOT NULL DEFAULT 'mysql'
        COMMENT '录制来自哪种引擎';
ALTER TABLE traffic_recordings
    ADD COLUMN capture_backend VARCHAR(32) DEFAULT NULL
        COMMENT '捕获通道: GENERAL_LOG / PG_JSONLOG / PG_CSVLOG / ORA_UNIFIED_AUDIT';

-- 已有数据都是 MySQL 录的（v1 格式），默认值已经把它们标对了
