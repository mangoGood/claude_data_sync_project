-- ============================================================
-- 聚合路由（分库分表汇聚 / 拆分）
--
-- route_config      路由配置 JSON，形如：
--                     {"mode":"MERGE",
--                      "merge":[{"match":"shard_db_*.order_*","target":"dw.order_all",
--                                "pkStrategy":"COMPOSITE_SOURCE","ddlPolicy":"FIRST_WINS"}],
--                      "legs":[{"nodeId":"inst-b","host":"10.0.0.2","port":3306,
--                               "username":"u","password":"p","syncObjects":{...}}]}
--                     拆分则是 {"mode":"SPLIT","split":[{"match":"app.orders",
--                       "shardKey":"user_id","algo":"HASH_MOD","count":16,
--                       "targetDb":"dw_<分片号除以2>","targetTable":"orders_<分片号>"}]}
--                     （目标库/表模板里的分片号占位符见引擎侧 ShardTemplate；
--                      这里不写它的字面形式——Flyway 会把它当成自己的占位符解析）
--                     legs 里带库口令，与 target_connections 同样落库前加密。
--                     未配置（NULL）= 现状 1:1 同步，全链路行为不变。
--
-- merge_parent_id   跨实例汇聚的父任务 id。每个额外的源实例会派生一个隐藏的
--                   MERGE_LEG 子任务（各自一条采集管线、各自的位点），父任务聚合它们的
--                   进度与状态。与双向灾备的 DR_SHADOW 是同一套"隐藏子任务"模式。
-- ============================================================
ALTER TABLE workflows
    ADD COLUMN route_config TEXT
        COMMENT '聚合路由配置 JSON（汇聚/拆分规则 + 跨实例源；含口令，落库前加密）',
    ADD COLUMN merge_parent_id VARCHAR(36)
        COMMENT '跨实例汇聚的父任务 id（本行是隐藏的 MERGE_LEG 子任务）';

CREATE INDEX idx_workflows_merge_parent ON workflows (merge_parent_id);
