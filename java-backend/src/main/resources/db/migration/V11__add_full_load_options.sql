-- ============================================================
-- 全量装载通道 + 全量一致性快照（任务级可选，未启动前可改）
--
-- bulk_load_enabled  关掉即退回各引擎的逐条写入路径（排障用；正常不该关）
-- bulk_load_mode     装载档位：
--                      AUTO        各目标端选零协议风险的那条（JDBC 走驱动语句重写，
--                                  Mongo/ES/Redis 走各自原生批量 API）——升级前的行为
--                      BATCH       显式指定驱动语句重写
--                      COPY        PostgreSQL 目标：COPY ... WITH (FORMAT binary)
--                      DIRECT_PATH Oracle 目标：INSERT /*+ APPEND_VALUES */ 直接路径装载
-- snapshot_mode      全量一致性快照档位：
--                      NONE        不记位点也不开快照
--                      GTID_ONLY   只记位点（GTID/binlog、LSN、SCN、TSO、clusterTime），不加锁
--                      CONSISTENT  真快照（各源端手法不同，见 ConsistentSnapshot）
--
-- 默认值按<b>源端</b>给（在后端 createWorkflow 里判定）：
--   MySQL 源默认 GTID_ONLY —— 它的真快照要 RELOAD 权限 + 一段全局读锁，代价不该默认承担；
--   TiDB / PostgreSQL / Oracle / MongoDB / Redis 源默认 CONSISTENT —— 这几家的快照
--   分别是 MVCC 历史读、导出快照、闪回查询、快照会话与 RDB，都不需要全局锁。
--
-- 存量任务一律回填 AUTO + GTID_ONLY —— 这正是它们此前<b>实际</b>在跑的档位
-- （引擎侧 MigrationConfig 的编译期默认值）。回填成别的值等于在升级时悄悄改变
-- 正在跑的任务的行为，这不是升级该做的事。
-- ============================================================
ALTER TABLE workflows
    ADD COLUMN bulk_load_enabled BOOLEAN NOT NULL DEFAULT TRUE
        COMMENT '全量批量装载是否启用（关闭=逐条写入，仅排障用）',
    ADD COLUMN bulk_load_mode VARCHAR(20) NOT NULL DEFAULT 'AUTO'
        COMMENT '批量装载档位: AUTO/BATCH/COPY(PG)/DIRECT_PATH(Oracle)',
    ADD COLUMN snapshot_mode VARCHAR(20) NOT NULL DEFAULT 'GTID_ONLY'
        COMMENT '全量一致性快照档位: NONE/GTID_ONLY/CONSISTENT（任务启动后不可改）';

UPDATE workflows SET bulk_load_mode = 'AUTO' WHERE bulk_load_mode IS NULL;
UPDATE workflows SET snapshot_mode = 'GTID_ONLY' WHERE snapshot_mode IS NULL;
