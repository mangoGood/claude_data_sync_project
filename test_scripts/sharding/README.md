# 分库分表汇聚 / 拆分 E2E

直驱引擎子进程（不经 backend/agent），在 `synctask-mysql`(33306) 上跑真库验证。
方案见 [../../markdown/SHARDING_ROUTE_DESIGN_20260803.md](../../markdown/SHARDING_ROUTE_DESIGN_20260803.md)。

## 前置

```bash
mvn clean install -DskipTests -pl migration-common,migration-full -am
```

改了 `migration-common` 一定要 `clean install`——fat jar 里打的是安装到本地仓库的那份，
只 `package` 会带着旧的 common 跑。

## 脚本

| 脚本 | 覆盖 |
|---|---|
| `mysql_merge_full_e2e.py` | 汇聚全量（B2）：3 分库 × 2 分表 → 1 张目标表；来源标识列与复合主键；幂等重跑；崩溃重启不清表；结构不一致 fail-stop；未命中规则的表仍走 1:1 |
| `mysql_merge_increment_e2e.py` | 汇聚增量（B3a）：直驱 capture/extract/increment 三进程；跨来源同主键 INSERT 并存；UPDATE/DELETE 只作用于本来源那一行；同一条 ALTER 只应用一次；分表被 DROP 不影响汇聚表 |
| `mysql_split_full_e2e.py` | 拆分全量（B4a）：1 表 → 2 库 × 4 表；目标表预建并剥 AUTO_INCREMENT；逐片核对落点；NULL 分片键按 BROADCAST；10 万行搬到一半 SIGKILL 后续跑一行不少 |
| `mysql_split_increment_e2e.py` | 拆分增量（B4b）：按分片键路由 INSERT/UPDATE/DELETE；改分片键触发跨分片搬迁（旧片删、新片插、全局只剩一份）；ALTER 广播到全部分片；源表 DROP 不连带删分片表 |
| `mysql_merge_column_processing_e2e.py` | 汇聚 × 列处理叠加：三个分库配**不同**过滤阈值（各源保留行数必须不同）；列名映射后结构校验通过；自定义附加列按各自来源取值；映射不一致 fail-stop |
| `mysql2pg_route_e2e.py` | **异构库对** mysql→pg：汇聚的来源标识列进得了翻译器产出的 DDL、复合主键、幂等重跑；拆分由翻译器预建分片表并按分片键落点。需 `postgres_db`(5432, app_user/userpassword) |
| `mongo_route_e2e.py` | **MongoDB** 集合级路由：汇聚换 `_id` 防撞、拆分落点；增量改分片键的跨分片搬迁、DELETE 按 `_id` 广播删。需 `synctask-mongo-a/b` |
| `es_route_e2e.py` | **Elasticsearch** 索引级路由：汇聚 `_id` 带来源前缀、拆分落点；增量 INSERT/改分片键/DELETE（binlog 有前镜像，精确删）。需 `synctask-es`(9200, elastic/espassword) |
| `api_merge_legs_e2e.py` | 跨实例汇聚的后端链路（B3b+B5）：路由配置存取与校验、派生隐藏 MERGE_LEG 子任务、route.* 下发到 config.properties、两条通道数据都汇进目标表。需先 `./start.sh` |
| `mysql_split_cross_instance_e2e.py` | 跨实例拆分：分片表建到两个实例上、行按分片键落到对的实例、改分片键触发跨实例搬迁、与事务一致档位互斥。第二个实例 `synctask-mysql-b`(33307) 由脚本自动拉起 |
| `api_route_compare_e2e.py` | 路由感知的行数对比：汇聚按来源标识切片、拆分按分片求和。需先 `./start.sh` |
| `api_route_guard_e2e.py` | 适用边界：已支持的库对（关系库任意组合含异构、mongo↔mongo、mysql→es）与叠加列处理放行；Redis / Oracle / TiDB 源 / 灾备订阅任务仍在保存或改配置时被拒。不落数据，需先 `./start.sh` |
| `api_route_content_compare_e2e.py` | 路由任务的**内容对比**：汇聚/拆分各造 SOURCE_ONLY / CONTENT_DIFF / TARGET_ONLY / WRONG_SHARD 四种差异，逐条核对定位与一键修复收敛；含"错片是唯一问题"与"同一行在两片各一份"两个摘要看不出来的用例。需先 `./start.sh` |

```bash
python3 test_scripts/sharding/mysql_merge_full_e2e.py
python3 test_scripts/sharding/mysql_merge_increment_e2e.py
python3 test_scripts/sharding/mysql_split_full_e2e.py
python3 test_scripts/sharding/mysql_split_increment_e2e.py
python3 test_scripts/sharding/mysql_split_cross_instance_e2e.py
python3 test_scripts/sharding/mysql_merge_column_processing_e2e.py
python3 test_scripts/sharding/mysql2pg_route_e2e.py     # 需 postgres_db
python3 test_scripts/sharding/mongo_route_e2e.py        # 需 synctask-mongo-a/b
python3 test_scripts/sharding/es_route_e2e.py           # 需 synctask-es
# 下面四个需要先 ./start.sh（后端 38080 + agent）
python3 test_scripts/sharding/api_merge_legs_e2e.py
python3 test_scripts/sharding/api_route_compare_e2e.py
python3 test_scripts/sharding/api_route_guard_e2e.py
python3 test_scripts/sharding/api_route_content_compare_e2e.py
```

走 API 的脚本会占用任务配额（每用户 50 个，软删除才释放），除 `api_merge_legs_e2e.py` 外
都会在开跑前把自己上一轮的任务停掉并删除。**只停不删不够**：上一轮的增量管线还活着的话，
本轮 setup 的 `DROP/CREATE DATABASE` 会被它重放到目标端，把刚建好的表连库一起删掉
（现象是目标端查出来是空，看着像功能坏了）。

增量脚本会拉起三个后台 JVM，跑完自行收尸；日志留在 `files/mrg-mysql-inc/{capture,extract,increment}.log`。

脚本自己建/删 `mrg_shard_1..3` 与 `mrg_dw` 库，跑完不清理，方便出问题时直接查目标表。
