-- ============================================================
-- 初始化默认数据
--
-- 密码哈希改用真实对应明文密码的 BCrypt 值（此前 database.sql 里的哈希与其自身
-- 注释的明文密码对不上，create_env.sh 一直靠 htpasswd 在每次建环境时运行时重置一遍；
-- 这里直接用正确的哈希，重置脚本不再需要）。
-- ============================================================

-- 默认管理员账号 (密码: admin123)
INSERT INTO users (username, password, email, role, enabled)
VALUES ('admin', '$2a$10$mg17vHM7fyFglPzzEjpV1OMdv3Hqapgw8yN/gquLU0UxCRb0GFQyS', 'admin@example.com', 'ADMIN', 1)
ON DUPLICATE KEY UPDATE username=username;

-- 测试用户账号 (密码: 123456，与前端登录页一致)
INSERT INTO users (username, password, email, role, enabled)
VALUES ('user1', '$2a$10$0wtaadjL8u7UGzgGVkijPORq1lgOANVcIwwbPTxyhqCYmRDY8z3ZO', 'user1@example.com', 'USER', 1)
ON DUPLICATE KEY UPDATE username=username;

-- 测试用户账号2 (密码: 123456)
INSERT INTO users (username, password, email, role, enabled)
VALUES ('user2', '$2a$10$0wtaadjL8u7UGzgGVkijPORq1lgOANVcIwwbPTxyhqCYmRDY8z3ZO', 'user2@example.com', 'USER', 1)
ON DUPLICATE KEY UPDATE username=username;

-- 为所有现有用户初始化默认资源配额
INSERT INTO resource_quotas (user_id, max_tasks, max_concurrent_tasks, max_storage_mb, api_rate_limit_per_min)
SELECT u.id, 50, 5, 10240, 100
FROM users u
WHERE NOT EXISTS (SELECT 1 FROM resource_quotas rq WHERE rq.user_id = u.id);
