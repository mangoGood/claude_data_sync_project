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

| `api_merge_legs_e2e.py` | 跨实例汇聚的后端链路（B3b+B5）：路由配置存取与校验、派生隐藏 MERGE_LEG 子任务、route.* 下发到 config.properties、两条通道数据都汇进目标表。需先 `./start.sh` |

```bash
python3 test_scripts/sharding/mysql_merge_full_e2e.py
python3 test_scripts/sharding/mysql_merge_increment_e2e.py
python3 test_scripts/sharding/mysql_split_full_e2e.py
python3 test_scripts/sharding/mysql_split_increment_e2e.py
```

增量脚本会拉起三个后台 JVM，跑完自行收尸；日志留在 `files/mrg-mysql-inc/{capture,extract,increment}.log`。

脚本自己建/删 `mrg_shard_1..3` 与 `mrg_dw` 库，跑完不清理，方便出问题时直接查目标表。
