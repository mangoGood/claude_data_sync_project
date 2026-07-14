-- ============================================================
-- JWT 令牌版本：改密/登出使旧 token 立即失效。
-- 签发 token 时把 users.token_version 写入 claim；校验时比对用户当前版本，
-- 不一致即拒绝。changePassword 递增该字段，所有旧 token 随之作废。
-- ============================================================
ALTER TABLE users
    ADD COLUMN token_version INT NOT NULL DEFAULT 0 COMMENT 'JWT 令牌版本，改密时递增使旧 token 失效';
