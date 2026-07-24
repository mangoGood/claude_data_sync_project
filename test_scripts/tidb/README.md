# TiDB → MySQL 同步 e2e

`tidb_e2e.py` 走真实 backend + agent + Kafka 全链路，覆盖 TiDB 源的两种任务形态。

## 前置

```bash
docker compose -f docker-compose-synctask-tidb.yml up -d
```

拉起独立的 TiDB 集群（固定 IP，见 compose 文件顶部注释）：

| 容器 | 角色 | 固定 IP | 宿主端口 |
|------|------|---------|----------|
| `synctask-tidb-pd`   | PD          | 172.28.30.11 | 12379 |
| `synctask-tidb-tikv` | TiKV        | 172.28.30.12 | — |
| `synctask-tidb`      | TiDB server | 172.28.30.13 | 14000 (MySQL 协议) / 11080 (status) |
| `synctask-tidb-init` | 一次性设置 root 口令 | 172.28.30.15 | — |
| `synctask-tidb-cdc`  | TiCDC       | 172.28.30.14 | 18300 (OpenAPI) |

TiCDC 同时接入 `synctask-net`（Kafka 所在网络），changefeed 直接投递到 `synctask-kafka:9092`；
宿主上的 capture 进程从 `localhost:29092` 消费同一 topic。因此本 compose 依赖
`docker-compose-synctask.yml` 已经起过。

源连接串：`mysql://root:tidbpassword@127.0.0.1:14000`（TiDB 讲 MySQL 协议，
协议名就是 `mysql://`，源库类型在向导里选 **TiDB**）。

再启动应用侧：

```bash
./start.sh
```

## 用法

```bash
python3 test_scripts/tidb/tidb_e2e.py
```

依赖：`mysql-connector-python`、`requests`。退出码 0 = 全通过。

## 覆盖

| 场景 | 内容 |
|------|------|
| 元数据接口 | `test-connection` / `databases` / `tables` / `validate`（TiDB 源不跑 binlog 检查，改查 TiCDC 服务可达性） |
| 场景 A 全量 | `migration.mode=full` → `FULL_COMPLETED`，源/目标逐行逐列比对 |
| 场景 B 全量+增量 | `fullAndIncre` → `INCREMENT_RUNNING`，写入 INSERT/UPDATE/DELETE/主键变更/NULL 互转后再比对 |

数据集 `t_all_types` 覆盖 TiDB 支持的全部 MySQL 类型，每列都取极端形态：

- 整数：各宽度的有符号最小/最大值 + 无符号 0/最大值（含 `BIGINT UNSIGNED` 的 2^64-1）
- `DECIMAL(65,30)` 的正负满精度值、`FLOAT`/`DOUBLE` 的量级边界
- `DATE`/`DATETIME(6)`/`TIMESTAMP(6)`/`TIME(6)`/`YEAR` 的取值区间两端（含负 TIME、闰日、微秒）
- `BIT(1)/BIT(8)/BIT(64)`、`TINYINT(1)` 布尔语义
- 文本：空串、定长满长、多字节中文、emoji、引号/反斜杠/换行/制表符
- 二进制：全 `\x00`、全 `\xff`、`0..255` 全字节、非法 UTF-8 字节序列（验证按裸字节保真）
- `ENUM`/`SET`/`JSON`，以及整行全 NULL

## 说明

TiDB 不提供 binlog dump 协议，增量走 TiCDC changefeed（canal-json → Kafka）。
纯全量任务不会创建 changefeed——changefeed 会持有源集群 GC safepoint，
建了不用只会拖住 TiDB 的 GC。
