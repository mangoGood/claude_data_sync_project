-- ============================================================
-- 账号同步（仅 MySQL→MySQL）：
--   sync_account                    是否同步账号（不勾选=与原有行为一致，不同步任何账号）
--   sync_account_super_privilege    是否同步超级账号权限（false 时全局 GRANT 剔除 SUPER/管理权限）
--
-- 勾选后：全量阶段同步源库存量账号（含口令哈希与授权），增量阶段同步 binlog 里的账号
-- 管理语句（新建账号、改口令、改权限、删账号等）。系统账号（root/mysql.*）与目标连接
-- 账号永不同步。配置随任务下发写入 config.properties 的 sync.account.enabled / sync.account.super。
-- ============================================================
ALTER TABLE workflows
    ADD COLUMN sync_account TINYINT(1) DEFAULT 0 COMMENT '是否同步账号（仅MySQL→MySQL）',
    ADD COLUMN sync_account_super_privilege TINYINT(1) DEFAULT 0 COMMENT '是否同步超级账号权限';
