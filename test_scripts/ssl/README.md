# SSL/TLS 判据与联调

方案见 [`markdown/SSL_TLS_DESIGN_20260812.md`](../../markdown/SSL_TLS_DESIGN_20260812.md)。

## 一、生成证书

```bash
./test_scripts/ssl/gen_certs.sh
```

产物在 `test_scripts/ssl/out/`（已在 `.gitignore` 里），口令统一 `synctask`。

**服务端证书的 CN/SAN 必须覆盖实际连接用的主机名**——`VERIFY_IDENTITY` 档位下证书里没有
`127.0.0.1` 就一定失败，而报错会把人带偏到"证书不对"。生成脚本已给每张服务端证书写上
`localhost` / `127.0.0.1` / 容器名。

## 二、跑判据

```bash
python3 test_scripts/ssl/ssl_e2e.py
```

前置：`./start.sh` 已跑起来，且 `dr-mysql-a`(33320) / `dr-mysql-b`(33321) 在线。

四组判据：

| 组 | 内容 |
|---|---|
| accounts | 把测试账号建成 `REQUIRE SSL`，确认服务端确实会拒绝明文 |
| conn | 测连正向加密 / 反向拒绝 / 口令错与地址错不被误报成 SSL 问题 |
| task | 建任务跑全量 + 增量，比对源目标 MD5 |
| server | `performance_schema` 取证：所有连接都有 `Ssl_cipher`，含 `Binlog Dump` |

**为什么账号要建成 `REQUIRE SSL`**：这样服务端会拒绝任何明文连接，于是"任务能跑完"
本身就等价于"管线里没有任何一条腿是明文"，比逐条去查可靠得多。B3 就是靠它逼出了
漏掉的 `CheckpointManager` 那一跳——账号若只是"支持 SSL"，那条腿会一路明文跑下去且全绿。

## 三、平台自身组件开 TLS

```bash
SYNCTASK_TLS_ALL=1 ./start.sh
```

打开：后端 HTTPS（38080）、agent HTTPS（8083）、控制面到用户库的 TLS。
默认全关，等于升级前行为。单项开关：

| 变量 | 作用 |
|---|---|
| `META_DB_SSL_MODE` / `META_DB_SSL_ROOT_CERT` | 后端与 agent 到元数据库 |
| `CONTROL_PLANE_DB_SSL_MODE` / `..._ROOT_CERT` | 后端直连用户库（元数据探查/校验/内容对比） |
| `BACKEND_TLS_ENABLED` / `BACKEND_TLS_KEYSTORE` / `..._PASSWORD` | 后端 HTTPS |
| `AGENT_TLS_KEYSTORE` / `AGENT_TLS_KEYSTORE_PASSWORD` | agent HTTPS |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SSL_*` / `KAFKA_SASL_*` | 四个 Kafka 客户端 |

**H2（每任务 `metadata.mv.db`）刻意不上 TLS**：它是同主机 loopback 通道，且已被钉死
`-Dh2.bindAddress=127.0.0.1`。对 loopback 加 TLS 是安全剧场，却要引入 AUTO_SERVER + TLS
的握手不确定性——这条链路我们已经踩过一次坑。加固手段是 loopback 绑定 + 文件权限。

## 四、数据库侧开 SSL（联调用）

MySQL 8 / PostgreSQL 16 的官方镜像**默认就带自签证书并开着 SSL**，容器内路径：

- MySQL：`/var/lib/mysql/ca.pem`、`server-cert.pem`、`server-key.pem`
- PostgreSQL：需显式 `ssl=on`

所以最省事的联调方式是直接把容器自带的 `ca.pem` 掏出来当证书上传：

```bash
docker exec dr-mysql-a cat /var/lib/mysql/ca.pem > /tmp/ca-a.pem
```

要用本套自签证书替换服务端证书时（例如验 `VERIFY_IDENTITY`），把 `out/server-mysql*.pem`
挂进容器并加启动参数 `--ssl-ca --ssl-cert --ssl-key`；私钥必须 `chmod 600`，
否则 MySQL 会以 "has group or world access" 拒绝启动。

其它引擎的服务端开关：

| 引擎 | 开法 | 注意 |
|---|---|---|
| MongoDB | `--tlsMode requireTLS --tlsCertificateKeyFile` | 要 **cert+key 合并成一个 pem**（脚本已生成 `mongo-combined.pem`） |
| Redis | `--tls-port 6380 --port 0 --tls-cert-file ...` | Redis 的 TLS 是**独立端口**，不是同端口协商——连接串里的端口要跟着改 |
| Kafka | SSL listener + keystore | 控制面 29092 与订阅下游 39092 两套证书分开 |
| Oracle | `listener.ora`/`sqlnet.ora` + wallet，TCPS 端口 2484 | URL 结构整个换成 DESCRIPTION，不是加参数 |
