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
| Oracle | 见下方「Oracle TCPS」 | URL 结构整个换成 DESCRIPTION，不是加参数 |

## 五、Oracle TCPS

Oracle 是唯一一个"开 TLS ≠ 加参数"的引擎，实测踩到三个各自独立的坑，按顺序处理：

**1）钱包（服务端证书）。** `gvenzl/oracle-free` 精简镜像里 `orapki` 跑不起来——既没有 JRE，
也没有 `oraclepki.jar` / `osdt_core.jar` / `osdt_cert.jar`。两样都要补进容器：

```bash
# JRE（容器是 aarch64 Linux，不能直接拷宿主 macOS 的 JDK）
docker pull docker.1ms.run/eclipse-temurin:11-jre
CID=$(docker create docker.1ms.run/eclipse-temurin:11-jre)
docker cp "$CID:/opt/java/openjdk" /tmp/jre11 && docker rm -f "$CID"
docker exec -u 0 oracle_db mkdir -p /opt/jre11 && docker cp /tmp/jre11/. oracle_db:/opt/jre11/
docker exec -u 0 oracle_db chmod -R a+rx /opt/jre11

# PKI jar（Maven Central 上就有，ojdbc 的同门）
M=~/.m2/repository/com/oracle/database/security
docker exec -u 0 oracle_db mkdir -p /opt/pki
for j in oraclepki osdt_core osdt_cert; do
  docker cp "$M/$j/21.1.0.0/$j-21.1.0.0.jar" oracle_db:/opt/pki/
done
docker exec -u 0 oracle_db chmod -R a+r /opt/pki

# 建自签钱包。CN 必须是客户端连接用的主机名
docker exec oracle_db bash -lc '
CP=/opt/pki/oraclepki-21.1.0.0.jar:/opt/pki/osdt_core-21.1.0.0.jar:/opt/pki/osdt_cert-21.1.0.0.jar
W=/opt/oracle/oradata/dbconfig/FREE/wallet
mkdir -p $W
/opt/jre11/bin/java -cp $CP oracle.security.pki.textui.OraclePKITextUI wallet create -wallet $W -auto_login_only
/opt/jre11/bin/java -cp $CP oracle.security.pki.textui.OraclePKITextUI wallet add -wallet $W \
  -dn "CN=localhost,O=synctask,C=CN" -keysize 2048 -self_signed -validity 3650 -auto_login_only'
```

钱包落在 `/opt/oracle/oradata/...`，那是**命名卷**，容器重建不丢。

**2）监听器。** `listener.ora` 加 TCPS ADDRESS，`sqlnet.ora` 与 `listener.ora` 都要有
`WALLET_LOCATION` 与 `SSL_CLIENT_AUTHENTICATION = FALSE`；改完必须 `lsnrctl stop && lsnrctl start`
（`reload` **不会**加载新的 ADDRESS），再 `ALTER SYSTEM REGISTER` 让服务注册上去。
compose 里还要把 2484 映射出来，否则宿主上的 agent/后端连不到。

**3）客户端两个坑（已在 `SslMaterial` 里处理，这里说明为什么）。**

- **TLS 版本**：JDK 17+ 默认先提 TLS 1.3，而这套 Oracle 只做到 1.2，握手以
  `TNS-00542 SSL Handshake failed` / `Connection closed` 告终——报文里没有半个字提到版本。
  所以 `oracle.net.ssl_version` 默认钉 `1.2`，服务端支持 1.3 时用
  `SOURCE_DB_SSL_ORACLE_VERSION=1.3`（或 `TARGET_...`）覆盖。
- **DN 匹配**：Oracle 的"身份校验"比的是**完整 DN**，不是主机名对 CN/SAN。
  `VERIFY_IDENTITY` 必须把期望 DN 写进描述串，否则即便证书 CN 正是所连主机名，
  也会报 `Mismatch with the server cert DN`。用
  `SOURCE_DB_SSL_ORACLE_SERVER_DN="CN=localhost,O=synctask,C=CN"` 给出；
  取值：`openssl s_client -connect host:2484 </dev/null | openssl x509 -noout -subject -nameopt RFC2253`。

判据：`python3 test_scripts/ssl/ssl_e2e.py --only oracle`（2484 没监听则自动跳过）。
