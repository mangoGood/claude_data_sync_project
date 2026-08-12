-- ============================================================
-- 任务级传输加密（SSL/TLS）：证书库 + 任务对源/目标端各自的档位与证书引用
--
-- 为什么证书要单独一张表、而不是塞进连接串或 sync_objects：
--   1) source_connection 是 VARCHAR(255) 且 ConnectionStringParser 的正则不吃查询串；
--   2) **测试连接发生在任务保存之前**，那时 sync_objects 还不存在，但测连就已经需要证书了；
--   3) 一套证书通常要给多个任务用（同一个数据库集群），复制粘贴 N 份必然出现
--      "换了证书但只换了其中三个任务"。
--
-- 为什么证书内容存库、而不是只落后端磁盘：
--   平台已经是多 agent 集群（agent_registry + 租约）。证书只在后端磁盘上，
--   任务一旦调度到别的 agent 主机就连不上——而这个错误要等**任务已经跑起来之后**才暴露。
--   存库则任何 agent 都能用它已有的元数据库连接把证书取下来，落到本地再用。
--
-- 私钥用与连接串口令同一套 AES-GCM（CredentialCipher，SYNCTASK_MASTER_KEY）加密后存放。
--
-- 存量任务一律 DISABLED + NULL —— 这正是它们此前**实际**在跑的档位（明文）。
-- 回填成别的值等于在升级时悄悄改变正在跑的任务的行为。
-- ============================================================

CREATE TABLE IF NOT EXISTS db_certificates (
    id            VARCHAR(36)  PRIMARY KEY,
    name          VARCHAR(128) NOT NULL COMMENT '证书名称（用户可读，用于在向导里选择）',
    user_id       BIGINT       NOT NULL COMMENT '归属用户',
    ca_cert       TEXT         DEFAULT NULL COMMENT 'CA 证书 PEM（VERIFY_CA 及以上必需）',
    client_cert   TEXT         DEFAULT NULL COMMENT '客户端证书 PEM（mTLS 可选）',
    client_key    TEXT         DEFAULT NULL COMMENT '客户端私钥（ENC: AES-GCM 密文）',
    subject_cn    VARCHAR(255) DEFAULT NULL COMMENT 'CA 证书的 CN，仅用于展示',
    fingerprint   VARCHAR(128) DEFAULT NULL COMMENT 'CA 证书 SHA-256 指纹，用于展示与去重',
    not_after     DATETIME     DEFAULT NULL COMMENT 'CA 证书到期时间（到期告警用）',
    has_client_cert TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否配了客户端证书（双向认证）',
    created_at    DATETIME(3)  NOT NULL,
    updated_at    DATETIME(3)  DEFAULT NULL,
    INDEX idx_cert_user (user_id, created_at),
    UNIQUE KEY uk_cert_user_name (user_id, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据库连接的 TLS 证书库';

-- 同步 / 灾备 / 订阅任务共用 workflows 表，因此四列覆盖三类任务。
-- 订阅任务的"目标端"是 Kafka，target_ssl_mode 在那里表达的是 Kafka 的
-- security.protocol（DISABLED=PLAINTEXT / REQUIRED=SSL），语义由引擎侧按 target_type 分派。
ALTER TABLE workflows
    ADD COLUMN source_ssl_mode VARCHAR(20) NOT NULL DEFAULT 'DISABLED'
        COMMENT '源端传输加密档位: DISABLED/PREFERRED/REQUIRED/VERIFY_CA/VERIFY_IDENTITY',
    ADD COLUMN source_ssl_cert_id VARCHAR(36) DEFAULT NULL
        COMMENT '源端证书（db_certificates.id）；REQUIRED 以下可为空',
    ADD COLUMN target_ssl_mode VARCHAR(20) NOT NULL DEFAULT 'DISABLED'
        COMMENT '目标端传输加密档位（订阅任务=下游 Kafka）',
    ADD COLUMN target_ssl_cert_id VARCHAR(36) DEFAULT NULL
        COMMENT '目标端证书（db_certificates.id）';

-- 数据校验 / 内容对比任务也直连用户库，同样要能走 TLS——
-- 否则"同步链路加密了、对比任务把同一批数据又明文拉了一遍"。
ALTER TABLE validation_tasks
    ADD COLUMN source_ssl_mode VARCHAR(20) NOT NULL DEFAULT 'DISABLED'
        COMMENT '源端传输加密档位',
    ADD COLUMN source_ssl_cert_id VARCHAR(36) DEFAULT NULL COMMENT '源端证书',
    ADD COLUMN target_ssl_mode VARCHAR(20) NOT NULL DEFAULT 'DISABLED'
        COMMENT '目标端传输加密档位',
    ADD COLUMN target_ssl_cert_id VARCHAR(36) DEFAULT NULL COMMENT '目标端证书';
