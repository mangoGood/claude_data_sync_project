-- 全量装载限速配额。
--
-- 增量早就有 max_increment_rows_per_sec，全量一直没有——而第 5 批把全量从 291 行/秒
-- 提到 38,365 行/秒之后，"链路上一个阀门都没有"本身就成了风险：一个没人看着的全量任务
-- 可以把源库 IO 打满。默认 NULL = 不限速，与提速前的行为一致。
ALTER TABLE resource_quotas
    ADD COLUMN max_full_sync_rows_per_sec INT DEFAULT NULL COMMENT '全量装载限速(行/秒)，NULL=不限速';
