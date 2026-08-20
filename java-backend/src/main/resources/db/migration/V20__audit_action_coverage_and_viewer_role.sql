-- ============================================================
-- V20 (2026-08-19)
--
-- 两件事，都是 2026-08-19 缺口整改的落库部分：
--
-- 1) audit_logs.action 扩容
--    此前 96 个端点里 58 个零审计，而漏掉的恰是碰数据最深的几类：
--    证书私钥上传/删除、元数据探查、逐行内容对比、排障包下载、死信查看。
--    AuditLog.Action 补了 9 个枚举值，而这列是 **MySQL ENUM**（不是 VARCHAR）——
--    不同步扩就会在插入时报 Data truncated（非严格模式下更糟：静默写成空串）。
--
-- 2) users.role 放开 VIEWER
--    此前全系统零权限模型：只有 anyRequest().authenticated()，任何账号都等价于超管。
--    现在分 ADMIN / USER / VIEWER 三级。role 是 VARCHAR(20)，容得下 VIEWER，
--    这里只更新注释让 schema 自解释，不改类型、不动存量行。
-- ============================================================

ALTER TABLE audit_logs
    MODIFY COLUMN action ENUM(
        'CREATE_TASK','UPDATE_CONFIG','LAUNCH_TASK','PAUSE_TASK','RESUME_TASK',
        'STOP_TASK','DELETE_TASK','RETRY_TASK','FAILOVER_TASK','LOGIN','LOGOUT',
        'CHANGE_PASSWORD',
        -- 以下为本次新增
        'UPLOAD_CERTIFICATE','DELETE_CERTIFICATE','UPDATE_CREDENTIAL',
        'BROWSE_METADATA','RUN_VALIDATION','DOWNLOAD_DIAGNOSTICS',
        'VIEW_DEADLETTER','SKIP_EVENT','MANAGE_USER'
    ) NOT NULL COMMENT '操作类型';

ALTER TABLE users
    MODIFY COLUMN role VARCHAR(20) DEFAULT 'USER'
        COMMENT '角色: ADMIN(全权) / USER(操作任务) / VIEWER(只读)';
