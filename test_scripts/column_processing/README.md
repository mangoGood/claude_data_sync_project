# 列处理（列过滤 / 列名映射 / 附加列）端到端测试

覆盖 **mysql→mysql、pg→pg、mongodb→mongodb** 同引擎同步任务的列处理三能力：

- **列过滤**：命中条件的行/文档不同步（排除语义）。过滤列限数值/日期/bit 类型，`amount < 100` 排除。
- **列名映射**：`note → remark`，只改名不改类型/值。
- **附加列**：`sync_time`(CREATE_TIME) + `src_tag`(CUSTOM → `prod@<库>@<表>`)。

这些脚本**直接驱动同步引擎子进程**（读 `files/<taskId>/config.properties`），不经后端/agent，
因而确定性强、可反复运行、可作为回归用例长期保留。

## 用例与覆盖矩阵

| 脚本 | 引擎 | 全量 | 增量 | 边界/特殊值 | 大数据量 |
|---|---|:-:|:-:|:-:|:-:|
| `mongo_column_processing_e2e.py`     | mongodb→mongodb（MongoSyncMain） | ✓ | ✓ | ✓ | ✓ |
| `mysql_column_processing_full_e2e.py`| mysql→mysql（migration-full）    | ✓ | — | ✓ | ✓ |
| `pg_column_processing_full_e2e.py`   | pg→pg（migration-full）          | ✓ | — | ✓ | ✓ |

- **边界/特殊值**：过滤阈值边界（99/100）、零、负数、NULL（保留）、DECIMAL/Decimal128 小数边界、
  unicode 值保真；mongo 另含缺失字段、嵌套文档、数组、以及索引 key 随列名映射改写。
- **大数据量**：5000 行/文档全量过滤 + 映射 + 附加列；mongo 增量另含 2000 文档批量 CRUD。
- **增量（仅 mongo，Change Streams）**：insert（符合/被过滤）、update（改名值更新、
  后镜像被过滤→目标删除、前过滤后符合→目标 upsert）、delete。

### SQL 增量列处理的覆盖

mysql/pg 的**增量**列处理走 capture→extract→increment 三进程管线（binlog/WAL），
其转换逻辑（`TypedDmlConverter` / `THLToSqlConverter`）由单元测试覆盖，随工程构建执行：

```bash
mvn -pl migration-common,migration-increment test \
  -Dtest=ColumnProcessingConfigTest,TypedDmlConverterColumnProcessingTest
```

mongo 增量列处理由 `MongoDocumentProcessorTest`（migration-mongo）+ 上表的 mongo E2E 覆盖。

## 前置条件

1. 容器就绪：
   - mysql：`synctask-mysql`（:33306，root/rootpassword）— `docker-compose-synctask.yml`
   - pg：`postgres_db`（:5432，app_user/userpassword）
   - mongo：`synctask-mongo-a`(:27117)/`synctask-mongo-b`(:27118) 副本集，固定 IP，已 `rs.initiate`
     — `docker-compose-synctask-mongo.yml` + `create_env.sh`（或见下）
2. 引擎 fat jar 已构建：
   ```bash
   mvn -pl migration-mongo,migration-full -am package -DskipTests
   ```

手动拉起并初始化 mongo 副本集（create_env.sh 已自动化，等价命令）：
```bash
docker compose -f docker-compose-synctask-mongo.yml up -d
docker exec synctask-mongo-a mongosh -u root -p rootpassword --quiet --eval \
  'rs.initiate({_id:"rsA", members:[{_id:0, host:"172.28.10.11:27017"}]})'
docker exec synctask-mongo-b mongosh -u root -p rootpassword --quiet --eval \
  'rs.initiate({_id:"rsB", members:[{_id:0, host:"172.28.10.12:27017"}]})'
```

## 运行

```bash
python3 test_scripts/column_processing/mongo_column_processing_e2e.py
python3 test_scripts/column_processing/mysql_column_processing_full_e2e.py
python3 test_scripts/column_processing/pg_column_processing_full_e2e.py
```

每个脚本自建/清理测试库（mongo: `testdb`；mysql/pg: `cp_src`/`cp_tgt`），退出码 0=全通过。

## 清理

```bash
# mysql / pg 测试库
docker exec synctask-mysql mysql -uroot -prootpassword -e "DROP DATABASE IF EXISTS cp_src; DROP DATABASE IF EXISTS cp_tgt;"
docker exec postgres_db psql -U app_user -d myapp_db -c "DROP DATABASE IF EXISTS cp_src;" -c "DROP DATABASE IF EXISTS cp_tgt;"
# 生成的任务目录
rm -rf files/cp-mongo-full files/cp-mongo-incre files/cp-mysql-full files/cp-pg-full
```
