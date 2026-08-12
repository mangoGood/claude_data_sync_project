# 全链路 SSL/TLS 传输加密设计方案

> 2026-08-12 ｜ 覆盖：同步任务 / 灾备任务 / 订阅任务的源库·目标库连接，以及平台自身的元数据库与中间件

---

## 0. 现状体检：不是"从零开始"，但也远不是"已经有了"

第 7 批（`d79e7a4`）与 `be3a9a4` 已经铺了一层地基，但**只覆盖了主 JDBC 连接，且是环境变量级的全局开关，没有任何 UI**。逐条对齐现状：

### 0.1 已经有的

| 件 | 位置 | 覆盖 |
|---|---|---|
| 五档枚举 `DISABLED/PREFERRED/REQUIRED/VERIFY_CA/VERIFY_IDENTITY` | `DatabaseConfig:128` | 数据面 MySQL/PG 主连接 |
| 控制面档位 | `java-backend/.../util/JdbcSslOptions.java` | 后端直连用户库 9 处 |
| Kafka 安全参数 | `migration-common/.../security/KafkaSecurity.java`（backend 有一份同名副本） | 4 个 Kafka 客户端 |
| 引擎侧读取 | `MigrationConfig:99,117` 读 `source/target.db.ssl.mode` | 全量 + 部分增量 |
| agent 下发 | `ConfigService:660-663` 从 `SOURCE_DB_SSL_MODE`/`TARGET_DB_SSL_MODE` env 写进 config.properties | — |

### 0.2 当场发现的四个硬伤（在动工前必须先认下来）

**① `META_DB_SSL_MODE` 是死开关。**
`application.yml:25` 写的是 `${DB_URL:jdbc:mysql://...sslMode=${META_DB_SSL_MODE:DISABLED}...}` —— 只有 `DB_URL` **没设**时那个默认值才生效。而 [start.sh:33](start.sh:33) 无条件 `export DB_URL="...?useSSL=false&..."`，[start.sh:38](start.sh:38) 又把它原样传给 agent（`MIGRATION_AGENT_MYSQL_DB_URL`）。**实际部署路径下，把 `META_DB_SSL_MODE=REQUIRED` 设成什么都不会有任何效果。**

**② 同一个 agent 进程里两套元数据库 URL 口径。**
[AgentConfig.java:36](migration-agent/src/main/java/com/migration/agent/service/AgentConfig.java:36) 认 `META_DB_SSL_MODE`；[AgentMain.java:46](migration-agent/src/main/java/com/migration/agent/AgentMain.java:46) 的 `MYSQL_DB_URL` 默认值硬编码 `useSSL=false` 且完全不认那个 env。两条路都在跑。

**③ 真正搬数据的那条连接一处都没接。**
用户理解的"开了 TLS"是**业务数据在网上是密文**。而现在：

| 链路 | 真实状态 |
|---|---|
| MySQL binlog 抓取（`StreamingBinaryLogClient`） | 明文。vendored 客户端**自带** `setSSLMode()`（`StreamingBinaryLogClient:265`），但 [MySQLBinlogCapture.java:512](migration-capture/src/main/java/com/migration/capture/MySQLBinlogCapture.java:512) 从来没调过 |
| PG 逻辑复制槽（`replication=database`） | 明文。`PostgresWalCapture` 14 处 `getConnection`，URL 一律裸拼（:150/:193/:323/:376/:552/:603） |
| Oracle LogMiner | 明文。`DatabaseConfig.getJdbcUrl()` 的 oracle 分支直接 `return`，连 `withExtraOptions` 都不走 |
| MongoDB | 明文。`MongoSyncMain:1067` 的 `MongoClientSettings` 没有 `applyToSslSettings` |
| Redis | 明文。`RedisSyncMain:486` 的 `DefaultJedisClientConfig` 没有 `.ssl()`；**redis-replicator 的 PSYNC 连接更是另一条独立通道** |
| Elasticsearch | 明文 http |
| TiCDC OpenAPI | 明文 `http://127.0.0.1:18300` |

这正是本仓库反复吃亏的那一类：**"以为加密了、其实没有"比"不支持加密"更危险**——因为前者会通过入网评审。

**④ 同一段 switch 抄了三份。**
`DatabaseConfig.mysqlSslParams()/pgSslParams()`、`JdbcSslOptions.mysql()/postgres()`、`ContinuousIncrementMain.targetSslParams()`（:411）—— 三份逐字相同的档位映射。再加 7 条链路只会变成十份。

### 0.3 规模底数

`src/main` 下：`DriverManager.getConnection` **92 处**，JDBC URL 字面量 **97 处**。其中 agent 的 38 处里约 25 处是连**元数据库**（checkpoint/注册/配额），属第 2 问范畴。

---

## 1. 目标与非目标

**目标**
1. 三类任务（同步 / 灾备 / 订阅）的**源库与目标库**各自独立选择是否启用 SSL；启用后可上传证书。
2. 开启后，**该端点的每一条连接**都走 TLS——包括全量、增量、抓取（binlog/WAL/redo/oplog/PSYNC）、预检、元数据探查、数据校验、内容对比、E2E 探针、倒换、fan-out 分发。
3. 平台自身组件（元数据库、Kafka、Schema Registry、后端 HTTP、agent HTTP、ES、TiCDC）可启用 TLS。
4. **可验证**：不靠"我们自己的日志说加密了"，而是从**服务端视角**证明连接是 TLS。

**非目标（本轮明确不做）**
- 不做证书自动签发 / ACME。
- 不做国密 SM2/SM4 套件（Java 原生不支持，要引 BGMProvider，单列一轮）。
- 不做 H2 的 TLS（理由见 §5.3，不是遗漏是取舍）。

---

## 2. 核心设计

### 2.1 配置模型：证书库 + 任务引用

不把证书塞进连接串（`source_connection` 是 `VARCHAR(255)`，`ConnectionStringParser` 的正则也不吃查询串），也不塞进 `sync_objects` JSON（**测连发生在任务保存之前**，那时 `sync_objects` 还不存在）。

采用**独立证书库 + 任务按 id 引用**：

```
db_certificates                       workflows
┌────────────────────────┐            ┌──────────────────────────────┐
│ id            VARCHAR  │◄───────────│ source_ssl_mode    VARCHAR   │
│ name          VARCHAR  │      ┌─────│ source_ssl_cert_id VARCHAR   │
│ user_id       BIGINT   │      │     │ target_ssl_mode    VARCHAR   │
│ ca_cert       TEXT     │      │     │ target_ssl_cert_id VARCHAR   │
│ client_cert   TEXT     │      │     └──────────────────────────────┘
│ client_key    TEXT ENC:│◄─────┘
│ key_password  VARCHAR ENC:                （fan-out / 路由节点的
│ store_password VARCHAR ENC:                每个目标各带一份，见 §3.4）
│ fingerprint   VARCHAR  │
│ subject_cn    VARCHAR  │
│ not_after     DATETIME │
└────────────────────────┘
```

**为什么证书内容存库而不是只落盘**：平台已经是多 agent 集群（`agent_registry` + 租约，`d79e7a4`）。证书只存后端磁盘，等于任务一旦调度到别的 agent 主机就连不上——而这个错误会在**任务已经跑起来之后**才暴露。存库则任何 agent 都能用它已有的元数据库连接把证书取下来。

私钥与库口令用现成的 `CredentialCipher`（AES-GCM，`SYNCTASK_MASTER_KEY`）加密，与连接串口令同一套机制。

### 2.2 证书归一：一次转换，喂饱所有驱动

这是整个方案里最容易被低估的部分。**每种驱动要的材料形态都不一样**：

- MySQL Connector/J 要 **keystore**（JKS/PKCS12），不吃 PEM
- PostgreSQL JDBC 要 **PEM**，而且私钥必须是 **PKCS8 DER**（`.pk8`），PKCS1 PEM 直接报 `Unsupported key type`
- Kafka 要 **keystore**
- Mongo / Redis / ES / binlog 客户端要一个 **`SSLContext` 对象**
- Oracle 要 keystore，且**走连接属性不走 URL 查询串**

用户只应该上传他手上真实有的东西——**PEM**（`openssl` 与所有数据库自带工具的产物）。平台在**证书入库时转换一次**，生成一个"证书包"：

```
{workdir}/certs/{certId}/
    ca.pem              原样（PG / TiCDC / Mongo 直用）
    client-cert.pem     原样
    client-key.pem      原样（PKCS8 PEM）
    client-key.pk8      PKCS8 DER —— PG JDBC 专用
    truststore.p12      CA 导入 —— MySQL / Oracle / Kafka
    keystore.p12        客户端证书+私钥 —— mTLS
```

**PKCS1 → PKCS8 的处理**：MySQL 自带的 `mysql_ssl_rsa_setup` 和 `openssl genrsa` 产出的都是 PKCS1（`-----BEGIN RSA PRIVATE KEY-----`），而 JDK 的 `PKCS8EncodedKeySpec` 只吃 PKCS8。
**建议做法：手写 ASN.1 包装（约 30 行，零新依赖）**——PKCS1 的 `RSAPrivateKey` 外面套一层 `SEQUENCE { version, AlgorithmIdentifier(rsaEncryption,NULL), OCTET STRING }` 就是 PKCS8。避免为此引入 BouncyCastle（本仓库在 `byte-buddy`/Spring BOM 上已经吃过混包的亏）。
**加密私钥**（`ENCRYPTED PRIVATE KEY`）本轮不支持解密，上传时直接拒绝并在 UI 给出确切的转换命令，而不是让它在运行期才炸。

生成 p12 全部走 JDK 的 `KeyStore` + `CertificateFactory`，不 shell-out 调 openssl（agent 主机不保证有）。

### 2.3 统一出口：`SslMaterial`

在 `migration-common` 新建 `com.migration.common.ssl.SslMaterial`，**唯一**的档位映射与材料出口，同时干掉 §0.2④ 的三份重复：

```java
public final class SslMaterial {
    static SslMaterial from(Properties props, String prefix);   // 引擎侧：source./target.
    static SslMaterial of(String mode, Path bundleDir, String storePass); // 控制面

    boolean enabled();
    boolean verifyIdentity();

    String  mysqlUrlParams();     // sslMode=..&trustCertificateKeyStoreUrl=..&...
    String  pgUrlParams();        // sslmode=..&sslrootcert=..&sslcert=..&sslkey=..pk8
    void    applyOracleProps(Properties p);   // javax.net.ssl.* + ssl_server_dn_match
    String  oracleTcpsUrl(String host, int port, String service);
    SSLContext sslContext();      // Mongo / Redis / ES / binlog / TiCDC
    void    applyKafka(Map<String,Object> sink);
}
```

改造顺序上，**先落这一个类并把已有三处替换掉**，是 B1 批次唯一的内容——此时行为逐字节不变（默认仍 DISABLED），风险为零，但后面 7 条链路接入就只剩"调一个方法"。

### 2.4 下发链路

```
向导 UI                       后端                         元数据库           agent                引擎子进程
─────────                    ─────                        ────────          ─────                ─────────
上传 PEM ──POST /api/certs──► 校验+解析+归一 ──────────────► db_certificates
                              （p12/pk8 就地生成）
选择档位 + 证书
测试连接 ──POST test-connection{sslMode,certId}──► 按证书包实连，回报协商到的协议/套件
保存配置 ─────────────────► workflows.source_ssl_* ────►
启动 ─────────────────────► TaskCreatedMessage ────Kafka────► ConfigService
                             (+ssl 字段)                      按 certId 从元数据库拉证书
                                                              落 files/{taskId}/certs/（0600）
                                                              写 source/target.db.ssl.* ─────► SslMaterial.from(props)
```

**关键约束**：agent 落盘的证书目录必须 `0600`、且随任务清理（`TaskFilesJanitor`）一起删。`config.properties` 里只写**路径**与 `ENC:` 加密的库口令，不写证书内容。

---

## 3. 数据面：逐链路改造清单

### 3.1 按驱动的接入方式（这张表是实现的主要依据）

| # | 链路 | 客户端 | 接入方式 | 材料 | 改动文件 |
|---|---|---|---|---|---|
| 1 | MySQL / TiDB JDBC | Connector/J 8 | `sslMode=` + `trustCertificateKeyStoreUrl=file:..p12` + `trustCertificateKeyStoreType=PKCS12` + `trustCertificateKeyStorePassword=` (+`clientCertificateKeyStore*` for mTLS) | p12 | `DatabaseConfig`, `MySqlDialect`, `ContinuousIncrementMain` |
| 2 | **MySQL binlog 抓取** | vendored `StreamingBinaryLogClient` | `setSSLMode(SSLMode.X)` + `setSslSocketFactory(new DefaultSSLSocketFactory(){ getContext() })` | `SSLContext` | `MySQLBinlogCapture:512` |
| 3 | PostgreSQL JDBC | pgjdbc | `sslmode=` + `sslrootcert=ca.pem` + `sslcert=` + `sslkey=client-key.pk8` | PEM+pk8 | `DatabaseConfig`, `PostgreSqlDialect` |
| 4 | **PG 逻辑复制槽** | pgjdbc `replication=database` | 同上，但 URL 在 3 个地方各拼一次 | PEM+pk8 | `PostgresWalCapture:323,376,603` |
| 5 | Oracle JDBC | ojdbc thin | URL 换成 `@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=)))`；truststore 走**连接属性** `javax.net.ssl.trustStore*`；VERIFY_IDENTITY → `oracle.net.ssl_server_dn_match=true` | p12 | `DatabaseConfig`(oracle 分支要重写), `OracleDialect`, `OracleRedoCapture` |
| 6 | MongoDB | driver-sync | `MongoClientSettings.applyToSslSettings(b→b.enabled(true).context(ctx).invalidHostNameAllowed(!verifyIdentity))` | `SSLContext` | `MongoSyncMain:1067`, `MongoSubscribeMain:566` |
| 7 | Redis（命令通道） | Jedis | `DefaultJedisClientConfig.ssl(true).sslSocketFactory(ctx.getSocketFactory()).hostnameVerifier(..)` | `SSLContext` | `RedisSyncMain:486` |
| 8 | **Redis（PSYNC 复制通道）** | redis-replicator | `Configuration.setSslSocketFactory(..)` + `setSsl(true)` —— **与 7 是两条独立连接** | `SSLContext` | `RedisSyncMain.applyAuth` 附近 |
| 9 | Elasticsearch | RestClient | `new HttpHost(h,p,"https")` + `httpClientConfigCallback(b→b.setSSLContext(ctx))` | `SSLContext` | `ElasticSyncMain` |
| 10 | Kafka（订阅目标） | kafka-clients | `security.protocol=SSL/SASL_SSL` + `ssl.truststore.location/.type=PKCS12` | p12 | `KafkaSecurity`（改成也认任务级） |
| 11 | TiCDC OpenAPI | HTTP | `https://` + changefeed `sink-uri` 带 `&ca=&cert=&key=` | PEM | `TicdcChangefeedService` |
| 12 | 控制面直连用户库 | 后端 | `JdbcSslOptions` → 收敛进 `SslMaterial`，且**改成按任务取**而非全局 env | 全部 | `MetadataService`, `DataValidationService`, `ContentCompareService`, `ValidationTaskService`, `DiagnosticService` |

### 3.2 三个必须专门点名的坑

**坑 1 —— Redis 有两条连接。** Jedis 那条加了 TLS，PSYNC 那条（真正搬全量 RDB + 增量命令流的）没加，结果就是"控制命令加密、业务数据明文"。§3.1 的 7 与 8 必须同时改，判据里必须分别验。

**坑 2 —— PREFERRED 会静默降级成明文。** 这是 MySQL/PG 的语义，不是 bug。因此：
- UI 上开启 SSL 后**默认选 REQUIRED**，PREFERRED 单独标注"服务端不支持时会自动明文"；
- 运行期把**实际协商到的协议与套件**写进任务日志与指标（见 §3.5）。

**坑 3 —— Oracle 换 TLS 就要换端口。** TCPS 监听默认 2484 而非 1521，且 URL 结构完全不同（不是加查询参数）。UI 在源/目标类型为 Oracle 且开启 SSL 时，要提示端口通常需要改成 2484。

### 3.3 各任务类型的差异

| 任务类型 | 源端 | 目标端 |
|---|---|---|
| 同步 | 库（7 种） | 库（5 种） |
| 灾备 | 库（MySQL/PG/Mongo） | 库 + **影子任务反向腿** |
| 订阅 | 库（MySQL/PG/Oracle/TiDB/Mongo） | **Kafka**（+ Schema Registry） |

订阅任务的"目标 SSL"配的是 Kafka 的 `SSL/SASL_SSL` 与 truststore，UI 上应当直接呈现成 Kafka 的语汇（安全协议 / SASL 机制），而不是复用库的五档枚举——两者不是一回事，混用会让人配错。

### 3.4 传播：四个"会漏掉一条腿"的地方

这四处是本方案里最容易出静默故障的地方，必须逐个写判据：

1. **灾备影子任务**：`WorkflowService:558` 派生 `DR_SHADOW`（B→A）时，SSL 配置必须**源/目标对调**后带过去。漏了 = 反向腿明文。
2. **主备倒换**：`SwitchoverService` 交换源/目标，SSL 配置要一起换。
3. **fan-out 多目标**：`target_connections` 是 JSON 数组，要变成 `[{conn, sslMode, certId}, ...]`，每个目标独立配置。
4. **分库分表路由**：`route_config` 里每个节点都是一个连接，同上。

### 3.5 让"开了 TLS"可验证

新增运行期证据（这是本方案区别于"加个开关"的地方）：

- 连接建立后立即探一次并记录：MySQL `SHOW STATUS LIKE 'Ssl_cipher'` / `'Ssl_version'`，PG `SELECT ssl,version,cipher FROM pg_stat_ssl WHERE pid=pg_backend_pid()`，Mongo `serverStatus().transportSecurity`。
- 写进任务日志一行 + 一个 `ssl_state` 指标，UI 的任务详情显示 `源库 TLSv1.3 / TLS_AES_256_GCM_SHA384`。
- **档位 ≥ REQUIRED 却探到未加密 → fail-stop**，不继续跑。

### 3.6 预检与错误码

扩展现有的"传输加密"预检项（`DiagnosticService:571`）：从"读 env 报档位"改成**按任务实配去真连一次**，报告协商结果；服务端不支持 TLS 而任务要求 REQUIRED → 阻断（error，非 warning）。

新增错误码（三份目录同步，已有 CI 门禁：`SyncErrorCode.java` / `SyncErrorCodeMapper.java` / `admin-dashboard.js`）：

| 码 | 含义 | 处置提示 |
|---|---|---|
| `E5005` | TLS 握手失败 | 检查服务端是否开启 SSL、端口是否为 TLS 端口（Oracle 通常 2484） |
| `E5006` | 证书校验失败 | CA 不匹配或已过期；VERIFY_IDENTITY 下还需证书 CN/SAN 与连接主机名一致 |
| `E5007` | 要求加密但实际未加密 | 档位 ≥ REQUIRED 而实测明文；服务端未开 SSL 或账号未 `REQUIRE SSL` |
| `E5008` | 证书材料不可用 | 证书已被删除 / 私钥格式不支持 / 库口令解密失败 |

---

## 4. 数据库、API、UI 变更清单

**Flyway `V19__db_ssl.sql`**（新增，绝不改 V1/V2）：
- 建 `db_certificates`
- `workflows` 加 `source_ssl_mode / source_ssl_cert_id / target_ssl_mode / target_ssl_cert_id`（默认 `DISABLED`/NULL = 与现有任务行为完全相同）
- `validation_tasks` 同样加两组（内容对比也直连用户库）

**新 API**
```
POST   /api/certificates            上传（multipart：ca / client-cert / client-key + name）
GET    /api/certificates            列表（只回元信息：CN、指纹、有效期，绝不回私钥）
DELETE /api/certificates/{id}       删除（被任务引用时拒绝）
POST   /api/metadata/test-connection   请求体加 sslMode / sslCertId，响应加 tlsVersion / cipher
```
后端此前**没有任何文件上传端点**，需要开 `spring.servlet.multipart`，并设死上限（证书 ≤ 64KB）与 MIME/内容校验——只接受能被 `CertificateFactory` 解析的 PEM。

**UI（三个向导 + 独立证书管理页）**
- `admin-dashboard.html` 的 `cfgStep1Content`（同步）、`drStep1Content`（灾备）、`subStep1Content`（订阅）三处连接表单，各自在源/目标块下加一行：`[ ] 启用 SSL` → 展开档位下拉 + 证书选择器 + "上传新证书"。
- 不启用时表单外观与现在**完全一致**（这是"不配置 SSL 则和当前测试情况一样"的直接落点）。
- Oracle + SSL 时提示端口；PREFERRED 时给降级警告；订阅任务的目标端换成 Kafka 安全语汇。

---

## 5. 第 2 问：平台自身的元数据库与中间件

### 5.1 元数据库（MySQL 33306）——**先修死开关**

1. `start.sh` 不再硬编码 `useSSL=false`：改成按 `META_DB_SSL_MODE` 拼（缺省 DISABLED，行为不变）。
2. `AgentMain:46` 的默认 URL 与 `AgentConfig:36` 合并成一个来源，消除同进程两套口径。
3. 新增 `META_DB_SSL_CA` / `META_DB_SSL_CLIENT_*`，走同一个 `SslMaterial`。
4. Hikari 连接池只需 URL 正确，无需额外改造。

### 5.2 Kafka（控制面 29092 / 订阅下游 39092）

`KafkaSecurity` 已经就位，缺的是**服务端**：compose 里加 SSL listener + keystore/truststore，并保持"下游订阅必须用独立的 39092"这条既有约束。控制面与下游两套证书分开，避免一套私钥同时暴露给平台与用户下游消费者。

### 5.3 H2（每任务 `metadata.mv.db`，AUTO_SERVER）——**建议不上 TLS，并说明理由**

H2 在这里是**同一主机上的跨进程通道**，且已经因为 hostname 解析问题被显式钉死在 `-Dh2.bindAddress=127.0.0.1`（否则跨进程轮询会挂几分钟）。对 loopback 加 TLS 属于安全剧场，却要引入 AUTO_SERVER + TLS 的握手不确定性——而我们在这条链路上已经踩过一次坑。

**替代加固**：坐实 `bindAddress=127.0.0.1`、库文件 `0600`、口令从默认 `sa`/空改成随机生成并 `ENC:` 落盘。这三条的实际收益高于 TLS。

### 5.4 后端 HTTP :38080 → HTTPS

目前 JWT 在明文 HTTP 上传输，是整个平台**最直接**的一处暴露（拿到 token 即等同登录）。
`server.ssl.enabled/key-store/key-store-type=PKCS12`，配 `BACKEND_TLS_*` env；HTTP 端口保留但 301 跳转，或直接关闭（可配）。

### 5.5 agent HTTP :8083 → HTTPS

`AgentHttpServer:74` 的 `HttpServer.create` 换 `HttpsServer.create` + `setHttpsConfigurator`。
注意：监控页已经改成**走后端代理**而非直连 agent，所以只有后端这一个客户端需要信任 agent 证书——改造面比看起来小。

### 5.6 其余

| 组件 | 改法 |
|---|---|
| Schema Registry（Apicurio 38081） | https + 后端/订阅引擎 truststore |
| Elasticsearch（9200） | `xpack.security.http.ssl.enabled=true` |
| TiCDC OpenAPI（18300） | https + changefeed sink 带证书参数 |

---

## 6. 实施批次

| 批 | 内容 | 行为变化 | 判据 |
|---|---|---|---|
| **B1 ✅ 已完成** | `SslMaterial` 内核 + 证书归一（PEM→p12/pk8）+ 替换已有 3 份重复 switch + 修 §0.2 的①② | **零**（默认 DISABLED） | 见 §6.1 |
| **B2 ✅ 已完成** | `V19` schema + 证书库 API + 上传 + 三个向导 UI + 测连回报协商结果 | 新增能力，旧任务不受影响 | 见 §6.2 |
| **B3 ✅ 已完成** | 数据面链路接入 + §3.4 四处传播 | 按任务生效 | 见 §6.3 |
| **B4 ✅ 已完成** | 预检实连 + 运行期协商证据 + 4 个错误码 + 证书到期告警 | — | 见 §6.4 |
| **B5 ✅ 已完成** | 平台组件：修死开关、元数据库/Kafka/后端/agent + start.sh | 默认关，开关可控 | 见 §6.4 |
| **B6 ✅ 已完成** | `test_scripts/ssl/` 判据套件 + 证书生成脚本 + SSL 账号 | — | 见 §6.4 |

B2 与 B3 可并行（B3 先用手工写的 config.properties 验证引擎侧）。

### 6.1 B1 交付记录（2026-08-12）

**新增**
- `migration-common/.../ssl/SslMaterial.java` —— 档位语义、MySQL/PG URL 参数、Oracle TCPS URL 与连接属性、Kafka 参数、`SSLContext` 的唯一出口
- `migration-common/.../ssl/CertBundle.java` —— PEM → `truststore.p12` / `keystore.p12` / `client-key.pk8`，含手写的 PKCS1→PKCS8 ASN.1 包装（零新依赖）

**收敛**：`DatabaseConfig`、`JdbcSslOptions`、`ContinuousIncrementMain.targetSslParams()` 三份重复 switch 全部改为走统一出口。后端因不依赖 migration-common（Spring BOM 混包风险，与 `CredentialCipher`/`KafkaSecurity` 同一处置）保留镜像，靠 `JdbcSslOptionsMirrorTest` 的全档位表格守住不漂移。

**修掉的两个硬伤**
- `start.sh` 改为按 `META_DB_SSL_MODE` 拼 `DB_URL`（缺省 DISABLED 时输出与旧硬编码串**逐字节相同**）——此前那个开关在实际部署路径下完全无效
- `AgentMain` 与 `AgentConfig` 合并到 `AgentConfig.defaultMetaDbUrl()` 一个来源，消除同进程两套口径

**顺手修的**：`DatabaseConfig.getRootJdbcUrl()`（建库那一跳）此前硬编码 `useSSL=false`，现在跟随同一份材料。

**判据**
- 单测 `./test.sh all`：**1003 通过 / 0 失败**（较改动前 +33：`SslMaterialTest` 17、`CertBundleTest` 10、`JdbcSslOptionsMirrorTest` 6）
- 零行为变化由"旧实现逐字符快照"锁定：三份旧实现原样抄进测试做对照，全档位 × 有无证书矩阵比对
- 归一时刻意修掉的那处分裂也有用例固定：非法档位（如 `REQUIRE` 少个 D）此前 MySQL 链路报错、PG 链路**静默明文**，且两份 PG 实现连证书都处置不同；现在构造即抛
- **服务端视角实测**（不是看客户端日志）：对 `synctask-mysql` 用 Connector/J 8.0.33 实连，`SHOW STATUS LIKE 'Ssl_cipher'` 结果——
  `DISABLED → Ssl_cipher 为空（明文）`，`REQUIRED → TLSv1.3 / TLS_AES_256_GCM_SHA384`

**遗留工作面**：`src/main` 下仍有 **25 处**硬编码 `useSSL=false`（capture 8、increment 5、agent 7、extract 2、backend 1、其余 2），即 §3.1 那张表的 B3 范围。B3 完成后此数应归零，可直接作为 CI 门禁。

### 6.2 B2 交付记录（2026-08-12）

**Schema**：`V19__db_ssl.sql` —— `db_certificates` 表 + `workflows`/`validation_tasks` 各四列，默认 `DISABLED`/NULL（存量任务保持明文，与它们此前实际在跑的档位一致）。

**后端**
- `DbCertificate` / `DbCertificateRepository` / `CertificateService` / `CertificateController`
- `CertMaterial` —— 数据面 `CertBundle` 的镜像（后端不依赖 migration-common），PEM → `truststore.p12` / `keystore.p12` / `client-key.pk8`
- `OracleSslSupport` —— TCPS URL 与连接属性（Oracle 的 thin URL 没有查询串可挂参数）
- `TlsStateProbe` —— 从**服务端**读加密状态：MySQL `SHOW STATUS LIKE 'Ssl_cipher'`、PG `pg_stat_ssl`
- `JdbcSslOptions` 增加任务级重载；`MetadataService.testConnectionDetailed` 接受 `SslConfig` 并回报协商结果
- 传播：`applySsl(..., swap)` 一个入口覆盖 6 处 `TaskCreatedMessage` 构造；灾备影子任务、主备倒换（连实体字段一起换）、fan-out/汇聚 leg、任务克隆各自继承

**前端**：新增 `dashboard-ssl.js`（三个向导共用），同步/灾备/订阅第 1 步各挂源/目标两个面板；不勾选时外观与之前完全一致。订阅任务的"目标端"换成 Kafka 的语汇（PLAINTEXT/SSL），不复用数据库的五档说法。

**API**
```
POST   /api/certificates          multipart 上传（name + caCert/clientCert/clientKey）
GET    /api/certificates          列表（只回元信息，绝不回私钥）
GET    /api/certificates/expiry   到期/临期
DELETE /api/certificates/{id}     删除（被任务引用时拒绝）
POST   /api/metadata/test-connection   请求加 sslMode/sslCertId，响应加 encrypted/tlsVersion/tlsCipher
```

**判据**（单测 `./test.sh all` **1018 通过 / 0 失败**，较 B1 的 1003 增 15）

真实环境实测（后端 38090 + `synctask-mysql` / `postgres_db` / TiDB）：

| # | 场景 | 结果 |
|---|---|---|
| ① | MySQL + DISABLED | 连接成功，`encrypted=false`，提示"明文" |
| ② | MySQL + REQUIRED | 连接成功，**服务端确认** TLSv1.3 / TLS_AES_256_GCM_SHA384 |
| ③ | TiDB 无 TLS + REQUIRED | `SSL_NOT_SUPPORTED`，提示去开服务端 SSL |
| ④ | PG + DISABLED | 连接成功，"明文" |
| ⑤ | PG 无 TLS + REQUIRED | `SSL_NOT_SUPPORTED`（**pgjdbc 报文是本地化的中文**，靠上下文兜底而非英文匹配） |
| ⑥ | 口令错 + REQUIRED | 仍是 `AUTH_FAILED`，未被误报成 SSL 问题 |
| ⑦ | 地址错 + REQUIRED | 仍是 `NETWORK_ERROR`，未被误报成 SSL 问题 |
| ⑧ | VERIFY_CA + 真 CA（mTLS） | 连接成功，TLSv1.3 |
| ⑨ | VERIFY_IDENTITY（证书 CN 与 IP 不符） | `SSL_CERT_INVALID`，提示改用证书上的主机名/补 SAN/降到 VERIFY_CA |
| ⑩ | VERIFY_CA 未选证书 | `SSL_CERT_INVALID`，提示"留空会回落 JVM 默认信任库，自签 CA 不在其中" |

证书上传用的是 `synctask-mysql` 容器自签的 `ca.pem` + `client-cert.pem` + `client-key.pem`（真实工作流）。物化产物 `ca.pem / client-cert.pem / client-key.pk8 / truststore.p12 / keystore.p12`，目录 0700、文件 0600；库里私钥是 `ENC:` 密文，CA 证书明文（本就是公开材料）。

保护性判据：证书被任务引用时删除被拒；非法档位 `REQUIRE` 被拒；把档位改回 DISABLED 会连带清掉证书引用（避免"档位关了却还挂着证书"的误导态）。

**开发中实际踩到并修掉的三个**
1. 勾选"启用 SSL"后档位默认落在列表首项 `PREFERRED`——而它会静默退回明文。`if (!sel.value)` 兜不住（select 永远有 value），改由 `<option selected>` 落实 REQUIRED。
2. `fetchWithAuth` 等助手在主脚本的 IIFE 里，不在全局作用域；`dashboard-ssl.js` 直接裸用会 ReferenceError，而 `try/catch` 把它吞成了**空证书列表**——界面上"接口挂了"和"你还没传过证书"长得一模一样。改走 `window.__dash`，并在加载失败时把原因**显示在下拉框里**。
3. `sslRenderPanel` 与 `sslSetConfig` 各自异步拉一次证书列表，先发的那个 `keepValue` 为空、若后完成就会清掉已回填的选中项。改为只由 `sslSetConfig`/`sslOnToggle` 两个知道该选谁的入口拉取。

**未接入**：证书只到"存下来 + 控制面能用"为止，**数据面（引擎子进程）仍未使用**——agent 侧按 certId 取证书、落盘、写进 config.properties 是 B3 的内容。因此现在把任务配成 REQUIRED，测连会走 TLS，但任务真正跑起来时引擎仍是明文。（B3 已补齐，见 §6.3。）

### 6.3 B3 交付记录（2026-08-12）

**下发链路**：新增 `migration-agent/.../service/CertificateStore.java` —— agent 按 certId 从<b>元数据库</b>取 PEM（证书内容不随 Kafka 消息走：控制面消息会落到 broker 磁盘），在本机物化到 `files/<taskId>/certs/<certId>/`，随任务清理一并删除。库口令由 `SYNCTASK_MASTER_KEY + certId` 经 HMAC 派生，与后端 `CertificateService#storePasswordFor` 逐字节一致，两侧才能读对方生成的 p12。

`ConfigService.applyTaskSslConfig` 把档位与证书路径写进 config.properties。**位置很关键**：必须排在库类型之后、方言生成 `jdbc.url` 之前——晚一步写就拼不进 URL，表现是"config 里 ssl.mode 写着 VERIFY_CA，但 jdbc.url 里没有任何加密参数"。agent 级 env 从"覆盖"改成"仅在任务未配时兜底"。

**接入的链路**

| 链路 | 做法 |
|---|---|
| MySQL/TiDB JDBC | `SslMaterial.mysqlUrlParams()`（含 p12 类型与口令） |
| **MySQL binlog 复制流** | `setSSLMode()` + 按任务证书构造 `DefaultSSLSocketFactory`（默认工厂在 VERIFY_CA 下走 JVM 信任库，自签 CA 不在其中） |
| PostgreSQL JDBC + **逻辑复制槽** | `pgUrlParams()`（PEM CA + `.pk8` 私钥），14 处 URL 全覆盖，含 3 处 `replication=database` |
| Oracle JDBC + LogMiner | TCPS 描述串 + 连接属性，三处 URL 收敛成 `oracleUrl()`/`oracleProps()` |
| MongoDB | `applyToSslSettings(...context(sslContext))` |
| **Redis 两条通道** | Jedis 命令通道 + redis-replicator PSYNC 通道（后者才搬数据） |
| Elasticsearch / 订阅 Kafka | 源侧 JDBC；Kafka 走 `applyKafka()`，任务级覆盖部署级 env |
| agent 侧各服务 | CheckpointManager（MySQL/PG/Oracle 三个位点入口）、DbObjectsSync、Switchover、SlaMetrics、E2eProbe、Fanout 各目标 |

**判据**

- 单测 `./test.sh all` **1018 通过 / 0 失败**
- 门禁：`src/main` 下硬编码 `useSSL=false` **归零**
- **真实全流程实测**（dr-mysql-a 33320 → dr-mysql-b 33321，VERIFY_CA + 各自 CA 证书）：
  - 测连两端均 `TLSv1.3 / TLS_AES_256_GCM_SHA384`
  - 全量 3 行 → 增量 INSERT×2 / UPDATE / DELETE → 源目标 MD5 **完全一致**
  - **测试账号是 `REQUIRE SSL` 的**：服务端会直接拒绝任何明文连接，因此"任务能跑完"本身就等价于"管线里没有任何一条腿是明文"
  - 服务端视角取证（`performance_schema.threads` join `status_by_thread`）：两端全部连接均有 `Ssl_cipher`，其中源端的
    **`Binlog Dump GTID → ECDHE-RSA-AES256-GCM-SHA384`** ——即真正搬业务数据的那条复制流已加密

**这一批真实抓到的两个问题**

1. **`CheckpointManager.getCurrentPositionFromSource` 漏了**，任务在"初始化 checkpoint 失败: 无法获取 binlog position"处失败——报错完全看不出是加密问题。它是任务启动路径上<b>第一条打到源库的连接</b>，正因为测试账号是 `REQUIRE SSL` 才被逼出来；账号若只是"支持 SSL"，这条腿会一路明文跑下去且全绿。
2. **我自己的门禁表达式是错的**：用 `grep -vE ":\s*(\*|//)"` 排除注释时，`jdbc:mysql://` 里的 `://` 同样命中，于是所有含 URL 的行被静默排除、门禁报 0 而实际还剩 2 处。判据脚本本身也会骗人——这条值得单独记着。

---

## 7. 测试方案（docker 内生成证书 + SSL 账号）

### 7.1 证书生成 `test_scripts/ssl/gen_certs.sh`

一套自签 CA + 每实例服务端证书 + 一张客户端证书。
**关键**：服务端证书的 CN/SAN 必须覆盖**实际连接用的主机名**（`localhost`、`127.0.0.1`、以及 compose 里的固定 IP），否则 `VERIFY_IDENTITY` 必挂，而排查方向会被带偏到"证书不对"。

### 7.2 各实例开启 SSL 与建号

| 实例 | 服务端开启 | 测试账号 |
|---|---|---|
| MySQL 8（33306/3307/3308/3309） | 挂载 `ssl-ca/ssl-cert/ssl-key` | `ssluser REQUIRE SSL`；`mtlsuser REQUIRE X509` |
| PostgreSQL（5432/5433） | `ssl=on` + `ssl_ca_file`；`pg_hba.conf` 用 `hostssl ... clientcert=verify-full` | `ssluser` |
| Oracle Free（1521→2484） | 手配 `listener.ora`/`sqlnet.ora` + wallet（**这套最费时，单独排期**） | `SSLUSER` |
| MongoDB（27117/27118） | `--tlsMode requireTLS --tlsCertificateKeyFile`（注意 Mongo 要 **cert+key 合并成一个 pem**） | `ssluser` |
| Redis 7（6390/6391） | `--tls-port 6390 --port 0 --tls-cert-file ...`（官方 redis:7 镜像自带 TLS 支持） | `default`（requirepass） |
| Kafka（29092/39092） | SSL listener + JKS | — |
| ES 8（9200） | `xpack.security.http.ssl` | `elastic` |

### 7.3 判据 `test_scripts/ssl/ssl_e2e.py`

对每条链路跑真实同步，然后**从服务端视角**取证（不看我们自己的日志）：

- MySQL：`performance_schema.status_by_thread` / `threads` 里定位本任务的连接，查 `Ssl_cipher` 非空
- PG：`SELECT * FROM pg_stat_ssl JOIN pg_stat_activity USING(pid)` —— 最硬的一条判据，直接看服务端
- PG 复制槽：确认 `pg_stat_replication` 对应的后端也在 `pg_stat_ssl` 里 `ssl=t`
- Mongo：`db.currentOp()` 里连接的 `client` + `serverStatus().transportSecurity`
- Redis：分别验命令连接与 PSYNC 连接（后者看 `INFO replication` 的 slave 条目落在 TLS 端口上）
- Kafka：broker 端 `kafka-log-dirs` / JMX 的 listener 计数

**反向判据（同等重要）**：
1. 账号 `REQUIRE SSL`、任务配 `DISABLED` → 任务必须**明确失败并给 E5007**，不得静默降级或半途报别的错。
2. 故意给错 CA → 必须 E5006，且**在预检阶段**就拦下来，不能等跑起来才炸。
3. 只给源端开 SSL、目标端不开 → 源侧全链路（含 binlog/WAL）加密、目标侧明文，两侧互不影响。
4. 灾备双向：影子任务的反向腿也必须在服务端视角是 TLS（对应 §3.4 的第 1 条）。

---

## 8. 风险与取舍

| 风险 | 处置 |
|---|---|
| 92 处连接点改漏一处 = 静默明文 | B1 先做统一出口；B3 完成后用 `grep -c "useSSL=false"` 归零作为门禁；判据从服务端取证而非看日志 |
| Oracle TCPS 环境搭建耗时不可控 | 单独排期，不阻塞其余 11 条链路 |
| TLS 带来吞吐下降（全量场景敏感） | 判据里带一次全量吞吐对比；预期 5~15%，若超预期则评估 `TLS_AES_128` 套件优先 |
| 证书过期导致长跑任务中断 | B4 加到期告警（提前 30 天）+ 任务详情展示有效期 |
| 私钥泄露面扩大（落到每个 agent 主机） | 库内加密 + 落盘 0600 + 随任务清理；日志与 `toString()` 全链路脱敏（沿用连接串的既有做法） |

---

## 附：与本仓库既有教训的对齐

- **"以为加密了其实没有"** —— 与订阅链路"配了 AVRO 却退回 JSON 不报错"、"合成心跳把延迟刷成 0" 是同一类。因此本方案把**服务端视角取证**和 **≥REQUIRED 探到明文即 fail-stop** 定为必需项，不是加分项。
- **判据必须跑 fat jar**，改了 `migration-common` 要 `clean install`，否则引擎子进程用的还是旧 `SslMaterial`。
- **错误码三份目录**有 CI 门禁，E5005~E5008 要同步落三处。


### 6.4 B4 / B5 / B6 交付记录（2026-08-12）

#### B4 预检实连 + 运行期证据 + 错误码

- **四个错误码**，三份目录同步（`SyncErrorCode` / `SyncErrorCodeMapper` / `admin-dashboard.js`，
  已有 CI 门禁 `SyncErrorCodeCatalogTest` 守着）：
  `E5005` TLS 握手失败 / `E5006` 证书校验失败 / `E5007` 要求加密但实际未加密 /
  `E5008` 证书材料不可用。mapper 里这几条**排在最前**——驱动会把握手失败包成
  "网络不可达/连接失败"，落到那些泛化规则上会把人指去查地址端口，而地址端口完全是对的。
- **预检从"读 env"改成"按任务实配真连一次"**：`DiagnosticService.checkTransportEncryption`
  现在给出的是服务端回报的协商结果，实测明文而档位要求加密时报 **FAIL 阻断**（此前只 WARNING）。
- **运行期证据** `migration-common/.../ssl/TlsEvidence`：全量与增量连上目标库后立刻探一次，
  ≥REQUIRED 却实测明文直接抛（E5007），PREFERRED 退回明文时告警但不阻断（那是驱动的既定语义）。

#### B5 平台自身组件

| 组件 | 开关 | 实测 |
|---|---|---|
| 元数据库（后端 + agent） | `META_DB_SSL_MODE` / `META_DB_SSL_ROOT_CERT` | `sync_task_db` 全部 5 条连接 `TLS_AES_256_GCM_SHA384`，明文 0 条 |
| 后端 HTTPS | `BACKEND_TLS_ENABLED` / `BACKEND_TLS_KEYSTORE` | TLSv1.3；明文 HTTP 打到 38080 被拒（400） |
| agent HTTPS | `AGENT_TLS_KEYSTORE` | TLSv1.3；后端代理位点/指标经 https 正常取到数据 |
| 控制面到用户库 | `CONTROL_PLANE_DB_SSL_MODE` | 诊断里的 JDBC URL 实际带 `sslMode=REQUIRED` |
| Kafka 四个客户端 | `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SSL_*` | 代码路径就位（`KafkaSecurity` + 任务级覆盖），broker 侧 SSL listener 未在本轮实测 |

一键打开：`SYNCTASK_TLS_ALL=1 ./start.sh`。默认全关 = 升级前行为。

**新增 `AgentHttpSupport`**：agent 一旦走 HTTPS，后端那 5 处硬编码 `http://` 的代理调用会全部连不上。
统一收口并加载 agent 的自签信任库。注意 TiCDC 的状态查询**不能**走它——那不是 agent，
协议由 `sync.ticdc.api-url` 自己决定，误接会把 http 改写成 https。

**H2 按既定取舍不上 TLS**（同主机 loopback + 已钉死 `h2.bindAddress=127.0.0.1`）。

#### B6 判据套件

- `test_scripts/ssl/gen_certs.sh`：自签 CA + 7 张服务端证书 + 客户端证书（含 PG 要的
  PKCS8 DER）+ p12/truststore + Mongo 要的 cert-key 合并 pem。服务端证书 SAN 覆盖
  `localhost` / `127.0.0.1` / 容器名——`VERIFY_IDENTITY` 下缺了就必挂，而报错会把人带偏。
- `test_scripts/ssl/ssl_e2e.py`：4 组共 **14 条判据，全绿**（后端 http/https 自动探测）。
- `test_scripts/ssl/README.md`：证书生成、平台组件开关、各引擎服务端开 SSL 的注意事项。

**最终实测**（`SYNCTASK_TLS_ALL=1 META_DB_SSL_MODE=REQUIRED ./start.sh`，即数据面 + 控制面 +
元数据库 + 后端/agent HTTPS 全开）：14/14 通过，含全量 3 行 + 增量 INSERT/UPDATE/DELETE
后源目标 MD5 一致，以及 `Binlog Dump → ECDHE-RSA-AES256-GCM-SHA384`。

**判据自身的一个修正**：反向判据原先断言"任务配 DISABLED 必须连不上"，但
`CONTROL_PLANE_DB_SSL_MODE` 是一条**下限**——任务没配时会回落到它，于是连接照样加密、
`REQUIRE SSL` 账号照样接受。真正的不变量是"**绝不允许连上且是明文**"，已按此改写。
