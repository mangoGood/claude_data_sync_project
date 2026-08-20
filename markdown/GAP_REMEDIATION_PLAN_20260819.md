# 缺口优化方案（2026-08-19）

审查报告见会话产出的 20 项清单（S-01~S-09 安全 / P-01~P-05 性能 / F-01~F-06 特性）。
本文件是**实施方案**，按"可利用性 + 改动耦合度"分五批，每批列出改哪些文件、判据怎么验。

## 排序依据

不按严重度排，按**攻击链的完整性**排。S-01 单独看只是"权限模型缺失"，S-02 单独看需要认证，
两者叠加才是完整的未授权 RCE，所以必须同批出。同理 P-01 与 S-03 是同一处代码
（THL 的 Java 原生序列化），拆成两批会改两遍。

| 批次 | 覆盖项 | 主题 | 状态 |
|---|---|---|---|
| A | S-01 S-02 S-03 S-08 | 接入控制：把门装上 | ✅ 完成 |
| B | S-04 S-05 S-06 | 日志行值门控 + CSP + 前端转义 + 落盘权限 | ✅ 完成（.cap 加密除外） |
| C | F-04 | 指纹口径 XOR → SUM | ✅ 完成（定性有修正，见下） |
| D | S-07 S-09 P-05 | 密钥轮转 / 审计切面 / 指标出口 | ✅ 完成 |
| E | P-01 | THL 编码替换 | ✅ 完成，实测降 22~34% |
| F | F-01 F-02 F-05 | 容器化 / API 契约 / CI | ✅ 完成 |

未纳入本轮：P-02（增量并行，需要压测环境定并行度）、P-03（93 处裸连接，机械但量大）、
P-04（前端构建体系）、F-03（PITR，需要独立设计）、F-06（数据治理，属新特性而非缺口修复）。

---

## 批次 A —— 接入控制

### A1　关掉自助注册 + 三级 RBAC（S-01）

现状：`/api/auth/**` 整段 permitAll 含 `register`；全仓 `@PreAuthorize` 为 0，
唯一授权规则是 `anyRequest().authenticated()`。

改法：
- `Role` 常量类，三级：`ADMIN`（全权）> `USER`（操作任务，不碰凭证/证书/用户）> `VIEWER`（只读）。
  沿用库里已有的 `ADMIN`/`USER` 取值，不动存量数据；`VIEWER` 为新增。
- `register` 端点改为**需要 ADMIN**。
  > 实施时否掉了原计划的 `app.registration.open`+"用户表为空才放行"引导分支：那是个
  > TOCTOU 缺口（攻击者可以抢在部署完成前建号）。首个管理员由 Flyway V2 种子建出，
  > 本来就不需要走这个接口。
- `SecurityConfig` 按路径分层授权；401（未登录）与 403（角色不够）分开返回，
  否则前端会把"权限不足"误判成"登录失效"而反复跳登录页。
- `V20` 迁移：`users.role` 注释补上 VIEWER（列本身是 VARCHAR(20)，容得下，不改类型）。

判据：`AuthorizationMatrixTest`（7 项，第二轮补做）——读 `SecurityConfig` 源码断言
授权规则没被改回去。做过变异验证：把 register 改回 `permitAll` 后 2 项立刻转红。
> 不起 Spring 上下文是因为那要连元数据库，而本仓库有"单测不连外部库"的既有约定。
> 代价是守不住运行期行为，只守得住"规则有没有被人删掉"——但实际发生过的退化正是后者。

### A2　连接串解析加固（S-02）

现状：`MetadataService.CONNECTION_PATTERN` 的 database 组是 `(.*)`，问号与 `&` 直接进 JDBC URL。

改法：
- 正则收紧：host 限 `[A-Za-z0-9._\-]`，database 限 `[A-Za-z0-9_$\-.]`，端口范围校验。
- 新增 `JdbcUrlSafety` 工具：`assertSafeHost` / `assertSafeDatabase`，在**所有**拼 JDBC URL
  的入口调用（后端 5 处 + 引擎侧 DatabaseConfig / 各 Dialect）。
- 危险参数黑名单：`allowLoadLocalInfile` `allowUrlInLocalInfile` `autoDeserialize`
  `queryInterceptors` `socketFactory` `socketFactoryArg` `init` 等 —— 出现即拒。
- 引擎侧同样要挡：`DatabaseConfig` 的两个构造函数上闸门（那三段有四条拼装路径，
  逐条加校验必漏；构造期报错也比连接期报错更指得准问题）。镜像类放在
  `migration-common/.../security/JdbcUrlSafety`，沿用本仓库"后端刻意不依赖 migration-common"的惯例。
- **参数改走 Properties**（第二轮补做）：新增 `JdbcConnections`，URL 只放
  `jdbc:mysql://host:port/db`，驱动参数全进 `Properties`。校验是"挡住"，
  这一步是"结构上不可能"——URL 里根本没有 `?` 可供接管。
  加密档位不在这里重新推导，而是解析 `JdbcSslOptions` 自己的输出，
  保证按构造等价（重新推导必然漂移：PG 的 DISABLED 是 `sslmode=disable` 不是
  `ssl=false`，还有 rootCert→truststore 那一串分支）。

判据：`JdbcUrlSafetyTest`（闸门本身，8 项）+ `ConnectionStringInjectionTest`
（闸门确实装在解析路径上，6 项）+ `JdbcConnectionsTest`（URL 无查询串、
参数一项不丢、覆盖项过黑名单，9 项）。

### A3　THL 反序列化白名单（S-03）

现状：四处 `readObject()` 无 `ObjectInputFilter`。

改法：`ThlObjectInputFilter` 工具，白名单只放 `THLEvent` 及其字段类型
（`String` `Timestamp` `HashMap` `byte[]` 及装箱基本类型），深度/引用数/字节数一并设限。
四处 `new ObjectInputStream(...)` 后统一 `setObjectInputFilter`。

判据：`ThlObjectInputFilterTest`（4 项）+ `test_scripts/security/thl_filter_realfiles.sh`。
后者拿仓库里全部真实 THL 实扫——**白名单最大的风险不是漏放 gadget，而是漏放业务类型**，
而后者只有真数据才照得出来（实测抓到两处）。

### A4　认证边界收口（S-08）

- 登录限流：`LoginAttemptGuard`，按用户名 + 客户端 IP 双维度，10 次失败锁 15 分钟。
- agent token 改 `MessageDigest.isEqual` 恒定时间比较。
- `/api/advanced/metrics` 与 `/ws/**` 从 permitAll 移除。
- agent 侧 9 个只读端点在未配 token 时 fail-open（`checkAuthOptional`）**本轮未改**：
  改成 fail-close 会让未配 token 的部署监控页直接空掉，需要先给部署方一个迁移期。

---

## 批次 B —— 数据泄露面

### B1　CSP + 前端统一转义（S-04）
- `SecurityHeadersFilter`：CSP、`X-Content-Type-Options`、`X-Frame-Options`、`Referrer-Policy`。
- 前端补 `safeHtml` 标签模板函数，对插值自动转义；把报告里点名的高危注入点（错误消息回显）切过去。
  209 处全量迁移量太大且回归面广，本轮先建机制 + 改**用户可控数据**那批。

### B2　错误路径行值门控（S-06）
`ContinuousIncrementMain` :881 / :938 两处 `logger.error` 过 `logRowValues` 开关，
默认只出 seqno / 库表名 / 错误码。

---

## 批次 C —— 判据盲区（F-04）

`BIT_XOR(CRC32(...))` → `SUM(CRC32(...))`。XOR 对成对重复会抵消，而成对重复正是
崩溃续传最容易产生的错误形态。涉及 `drlib.py` `dr_resume.py` `agent_failover.py`
`write_amplification.py` `selfheal_reconnect.py` `sql_resume.py` 及 dblib 的 Python 版指纹。

---

## 批次 D —— 密钥 / 审计 / 指标

- **S-07**：`CredentialCipher` 与 `ThlEncryptionService` 换 PBKDF2-HMAC-SHA256（310k 轮）+ 盐；
  密文格式加 keyId 字节，支持新旧双密钥并存的轮转窗口，旧密文（无 keyId）按 legacy 解。
- **S-09**：`@Audited` 注解 + `AuditAspect` 切面，替代各 controller 手写；
  补齐证书 / 凭证 / 内容对比 / 排障包 / 死信五类动作。
- **P-05**：后端加 actuator + micrometer-prometheus，与 agent 同一套 scrape 约定。

---

## 批次 E —— THL 编码替换（P-01）

实测：单条 741 B 里 278 B（38%）是重复写入的 Java 类描述符；整文件 gzip 37×。
根因 `THLFileWriter.writeEvent` 每条 new 一个 `ObjectOutputStream`，且每条 `flush()`。

改法：`ThlCodec` —— 定长头 + 手写字段编解码，描述符不再入流；写侧改按批 flush。
读侧按 magic 三态兼容：`THL2`（新）/ `THL1`（framed 旧）/ 无 magic（最旧）。
存量文件必须仍可读——跨机接管会回灌历史 THL。

判据：`test_scripts/thl/codec_compat.sh`——把全部历史 THL 的每条事件过一遍
`encode→decode` 并逐字段比对，同时给出体积对比。

> **降幅是 22%~34% 的区间，不是一个定值**：省掉的主要是每条事件固定的类描述符开销
> （约 250~340 B），所以事件越小、占比越高。两次测量（33.7% 与 22.3%）用的是不同语料
> ——中间 `TaskFilesJanitor` 的 72h 保留期清掉了一批终结任务的文件，剩下的平均事件更大。
> 引用这个数字时要带上语料口径。

---

## 批次 F —— 交付形态

- **F-01**：backend / agent 两个 Dockerfile + `docker-compose-synctask-app.yml` + `.dockerignore`。
  `start.sh` 的 macOS 硬编码路径**本轮未动**——容器化之后它退化成本机开发脚本，
  改它的收益不如把精力放在镜像上。
- **F-02**：springdoc-openapi，96 个端点出 `/v3/api-docs` 与 Swagger UI。
- **F-05**：GitHub Actions —— 构建 + 双工程单测 + 三个离线判据 + 镜像构建验证。
  需要真实库的判据（`autotest/`、`fault_injection/`）不放进来：它们要先起七个
  docker-compose 依赖栈，属于夜间流水线。

---

## 回归底线

每批改完跑 `./build.sh` + `./test.sh all`；批次 A/B 额外跑 `autotest/` 全量 22 条，
确认没碰坏同步链路本身。

---

# 实施记录（2026-08-19）

## 实测数据

| 项 | 口径 | 结果 |
|---|---|---|
| ThlCodec 体积 | 74 个真实 THL / 33,917 条事件 | 32.56 MB → 21.60 MB，**降 33.7%**（单条 1006 B → 667 B） |
| ↑ 复测（语料被 janitor 清理后变成 29 文件 / 18,760 事件） | 同一判据 | 19.69 MB → 15.31 MB，**降 22.3%**（单条 1100 B → 855 B） |
| ThlCodec 保真 | 同上，逐字段比对（含 metadata 深比） | **0 处不一致** |
| THL 白名单 | 同上，全部读通 | 74/74 |
| JDBC 注入 | 报告里 3 个实测可通的 payload | 全部拒绝 |
| 回归 | 引擎 127 + 后端 128 单测 | 全绿 |

## 三个只靠读代码发现不了的坑

1. **白名单漏 `java.lang.Object`**——`ArrayList` 底层是 `Object[]`，剥到元素类型就是它。
   漏了它，74 个真实 THL 里 **72 个读不动**。是拿真文件实扫才暴露的。
2. **白名单漏 `LobRef`**——大字段链路把落盘引用放进 metadata，而 migration-thl
   对这个类<b>没有任何静态引用</b>，grep 源码永远找不到。
3. **`audit_logs.action` 是 MySQL ENUM 不是 VARCHAR**——加 9 个 Java 枚举值不配迁移，
   插入会 Data truncated（非严格模式下静默写空串）。已补 `V20`。

另有一个工程坑值得记：`public static final String` 是**编译期常量，会被内联进调用方字节码**。
把 `CredentialCipher.PREFIX` 从 `ENC:` 改成 `ENC2:` 后，未重新编译的测试类仍带着旧字面量，
表现为两个莫名其妙的失败（`Illegal base64 character 3a` 就是 `:`）。改这类常量必须 clean 重建，
与"改 common 后 fat jar 要 clean install"是同一类问题。

## 一处定性修正：F-04 我在报告里说重了

原报告称判据"对重复行是瞎的"。核查后：这批脚本的判据表**都有主键**，整行重复根本
无法出现；且 `count` 本身就是指纹二元组的一半，任何基数变化都会被抓到。
XOR 真正的残余风险只有 crc32 碰撞对（200,000 行规模实测约 5 对），
且要恰好命中那几行才误判，是二阶事件。

换成 SUM 仍然做了——它与 XOR 同样顺序无关、同样 O(1) 内存，却没有抵消性质，
零成本严格更强；且 `sharding/api_route_content_compare_e2e.py` 早就因为分片汇聚场景
用了 SUM，口径本来就该统一。新增判据 `fingerprint_semantics.py` 里第 [4] 项用
**等量 + 成对重复** 的构造给出了 XOR 确实区分不了、SUM 能区分的对照。

## 新增判据

| 脚本 | 守什么 |
|---|---|
| `test_scripts/security/thl_filter_realfiles.sh` | 白名单不锁死真实 THL |
| `test_scripts/thl/codec_compat.sh` | 编解码逐字段无损 + 体积对比 |
| `test_scripts/fault_injection/fingerprint_semantics.py` | 指纹口径不得退回 XOR（含源码扫描防回退） |
| `JdbcUrlSafetyTest` / `ConnectionStringInjectionTest` | 注入闸门本身 + 闸门确实装在解析路径上 |
| `ThlObjectInputFilterTest` | 白名单放行业务类型、拒绝 gadget、限额生效 |
| `CredentialCipherTest`（扩） | 旧密文仍可解、新密文带 keyId、KDF 强度 |

## 部署注意

1. **升级顺序**：`CredentialCipher` 新写入的是 `ENC2:`。后端与 agent 必须**同时**升级——
   新后端写 `ENC2:`、旧 agent 读不懂。两者共用同一 fat jar 构建，正常发布不会分叉。
2. **密钥轮转**：设 `SYNCTASK_MASTER_KEY_ID`（新）+ `SYNCTASK_MASTER_KEY_PREV` /
   `_PREV_ID`（旧，只解密）。存量密文照读，新写入自动用新密钥；重写完存量即可摘掉 PREV。
3. **首个管理员**由 Flyway V2 种子建出，注册接口已收成 ADMIN-only，没有引导后门。
4. **Actuator** 默认绑 `127.0.0.1:8091`；容器里要被抓取时用 `MANAGEMENT_ADDRESS=0.0.0.0`
   覆盖，并只在内网暴露。

## 未做的部分（明确留下）

| 项 | 为什么没做 |
|---|---|
| S-04 前端 209 处转义 | CSP 已上（挡外传），但逐处改注入点回归面太大，需要按页推进 + 人工验证 |
| P-02 增量并行 | 并行度要靠压测定，本机没有可复现的负载环境 |
| P-03 93 处裸连接 | 机械但量大，且每处的生命周期语义要逐个确认 |
| P-04 前端构建体系 | 引入 esbuild 会改变部署形态，应与前端重构一起做 |
| F-03 PITR | 需要独立设计（归档层 + 回放入口），不是缺口修复 |
| F-06 数据治理 | 属新特性 |

---

# 第三轮实施记录（2026-08-19，S-03 ~ S-08）

S-03 / S-06 / S-07 前两轮已完成，本轮核实无遗漏；实际工作在 S-04、S-05、S-08。

## 实测发现：我自己在第二轮引入的两个回归

这两个都是**只看代码看不出来、必须加载真实页面才能发现**的：

1. **CSP 打断了整个前端。** 页面从 `cdn.jsdelivr.net` 加载 SockJS / STOMP（实时任务状态推送）
   与 Chart.js（全部图表），而我写的 `script-src 'self' 'unsafe-inline'` 把三个全拦了。
   浏览器控制台三条 CSP 拦截 + `SockJS is not defined`。
   已放行该 CDN 并做成可配置；**更好的终局是把三个库落到本地再收回 `'self'`**——
   第三方 CDN 既是供应链风险，也让产品无法在离线/内网部署。

2. **`/ws/**` 收成 authenticated 后 WebSocket 全挂。** 浏览器的 WebSocket/SockJS 握手
   没有设置自定义请求头的 API，前端只能把 token 放进 `?token=`，而 JWT 过滤器只读
   `Authorization` 头 → 握手 401，实时推送整个断掉。
   已让过滤器在 `/ws` 路径下接受查询参数（只对该路径放开，token 在 URL 里会被日志留存）。

## S-04　前端转义

先修了 `escapeHtml` 自身两个 bug——不修就不能大规模套用：

| 问题 | 后果 |
|---|---|
| `if (!str) return ''` | `escapeHtml(0)` / `escapeHtml(false)` 返回空串，数字 0 在页面上凭空消失 |
| 定义在非全局作用域 | 三个 `type="module"` 的 dashboard 调用它会 ReferenceError |

然后写了改写器：只处理 `.innerHTML` 赋值里的模板字面量，**跳过产出 HTML 的插值**。
"哪些函数返回 HTML"是逐个读定义核实的，不是靠命名猜的——启发式把
`advTaskName`（返回任务名，纯文本）误判成 HTML 函数，而它恰恰是一个真实 XSS 点。

结果：**包上 198 处，跳过 63 处**（已转义的、产 HTML 的、含字面量 `<` 的、`.map().join()` 的）。

> 顺带更正上一份报告的一个数字：我当时写"209 个注入点只有 5 个转义"。
> 那是按**行** grep 的，而 `escapeHtml` 跨多行模板串使用。真实口径是
> **537 个插值里 75 个已转义**（14%），不是 2.4%。缺口仍然大，但没那么夸张。

浏览器实测：任务列表、状态徽章、一致性徽章、调度页全部正常渲染；
`escapeHtml(0)` → `"0"`；XSS payload 被转义；SockJS/Stomp/Chart 均加载成功。

## S-05　落盘数据

| 做了 | 说明 |
|---|---|
| 任务目录 0700 / 文件 0600 | 一处覆盖 `.cap` `.thl` `sql_output` checkpoint `config.properties`。收目录而不是逐个收文件——全仓 54 处创建点，逐处加必然漏，而目录没有 `x` 权限就进不去 |
| 去掉硬编码兜底口令 | 原来回退到源码里的公开常量 `default-thl-encryption-key-please-change`，任何人都能解开"已加密"的 THL。改为从主密钥派生；主密钥也没有则拒绝启动 |
| KDF 换 PBKDF2 | 与 `CredentialCipher` 一致，310k 轮 |
| 覆盖范围显式告警 | 开启 THL 加密时明确打出"`.cap` 仍是明文" |

**`.cap` 加密没有做**，这是本轮最大的保留项。`.cap` 是按行的文本格式，逐行加密在原理上
可行且能保住续传语义，但要改 4 个 capture 写入点 + extract 读取点 + 存量文件格式识别，
而正确性依赖 `.cap` 的行边界与断点续传——仓库里"`.cap` 半行"是修过的静默丢数缺陷。
验证需要跑故障注入判据（真实库、耗时长）。**目录 0700 已经挡住同机其它用户**；
剩余暴露面是备份 / 磁盘镜像 / 共享存储，那些场景当前应依赖卷加密。

## S-08　认证边界

- agent 9 个只读端点从 **fail-open 改为默认 fail-close**。原理由是"保证监控页可用"，
  但监控页现在走后端代理不直连 agent，理由已不成立；且这些端点暴露位点、表级延迟、
  路由分片与双向冲突记录（含行数据）。标准部署不受影响（`start.sh` 一直会注入 token），
  受影响的只有"裸跑 agent 且不配 token"，给它留了 `AGENT_READONLY_ALLOW_ANONYMOUS=true`
  逃生开关 + 启动大声告警。
- `start.sh` 两处弱默认（元数据库明文、keystore 口令 `synctask`）加了启动告警。
  没有改默认值：那是本机开发脚本，改了会让谁都起不来；生产路径
  （`docker-compose-synctask-app.yml`）里默认本来就是 REQUIRED + 强制注入口令。

## 新增判据

| 判据 | 守什么 |
|---|---|
| `TaskDirPermissionsTest`（4 项） | 目录 0700 / 文件 0600；0700 意味着其它用户进不去 |
| `ThlEncryptionKeyTest`（6 项） | 无口令无主密钥必须拒绝启动、不同口令派生不同密钥、KDF 强度、默认关闭时透传 |

---

# 第四轮：剩余工作按序实施（2026-08-19）

## S-05　`.cap` 逐行加密　✅ 完成

上一轮保留的那块补上了。做法是<b>逐行加密</b>而不是整文件：

| 约束 | 为什么逐行能守住 |
|---|---|
| extract 按<b>已读行数</b>记断点 | 一行一条记录不变，行数不变 |
| 半行检测靠"文件是否以换行结尾" | 换行摘下来、加密记录体、再接回去 |
| 升级瞬间断点不能错 | 用<b>行首标记</b>（`\002`）而不是文件头——文件头会让行计数整体偏移一位；带标记则同一文件里明文行与密文行可以共存 |

行首标记不会与明文歧义：明文行首字符恒为事件类型名的大写字母（ROTATE / QUERY / WRITE_ROWS…）。

接入面只改了<b>创建那一行</b>：`CapFileWriter extends BufferedWriter`，四个 capture 的十几处
`write/flush/close` 一个字都没动。

### 实测（真实端到端，不是单测）

```
autotest --suite sync_mysql2mysql   不加密 9/9 通过（49s）
                                     加密   9/9 通过（52s）  两端指纹一致

.cap 内容对比（同一条链路）
  明文文件: at_load 出现 159 次 | QUERY 195 | WRITE_ROWS 60 | UPDATE 13 | CREATE 4
  加密文件: 全部 0 次
  行结构 : 895 行，末字节 0x0a（换行）——半行检测仍然成立
```

## P-02　增量并行　⚠️ 部分完成

### 量出来的三个数

| 配置 | INSERT 档 | UPDATE 档 |
|---|---|---|
| 基线（`increment.apply.parallelism=1`） | 2,715 行/秒 | 2,047 行/秒 |
| 并行度默认改为 4（最终一致档位） | 3,743 / 3,552 / 3,307 行/秒 | 2,245 / 2,213 / 2,085 行/秒 |

三次重复跑给出噪声带约 ±400 行/秒。**并行度改动在噪声带之外（约 +30%），是真实提升；
而语句缓存（`cachePrepStmts`）在噪声带之内，量不出来。**

### 做了

- 并行度默认值<b>随一致性档位走</b>：最终一致档位默认 4，事务一致档位仍为 1
  （那个档位的口径是"目标提交顺序 = 源事务提交顺序"，并发提交必然打乱）。
  并行应用的机制（冲突矩阵分片、同键保序）本来就写好了，默认 1 意味着没人会用上它。
- 目标连接开启 `cachePrepStmts`。**实测量不出提升**，保留是因为机制成立、可配、无害：
  本机源/目标/agent 全在 localhost，一次往返只有几十微秒，省两次往返约等于零；
  真实部署里 agent 与目标库之间通常有 0.5~2ms 延迟，那时才会显出来。
  <b>不要拿它当性能承诺。</b>

### 没做

extract 侧的解析/转换并行化。理由是<b>测量不支持先做它</b>：判据测的是端到端追平时间
（源提交 → 目标行数追平），apply 并行度从 1 提到 4 只涨了 30%，说明瓶颈已经不主要在 apply；
但要确定是 capture 还是 extract，需要分段埋点，那是下一步该做的事——
在没有分段数据之前动 extract 的并发是拿最脆弱的路径赌。

## P-03　裸连接归池　⚠️ 部分完成（后端）

逐处确认了生命周期语义，结论是**不该全转**：

| 位置 | 处置 | 理由 |
|---|---|---|
| `DiagnosticService` 5 处短查询 | ✅ 归池 | SHOW VARIABLES / SHOW GRANTS 之类一两条查询，每次一个完整 TCP+TLS+认证往返全是白付 |
| `DiagnosticService:135` 连接测试 | ❌ 保留裸连接 | 语义是"验证这组凭证能不能连上"，复用池中连接会掩盖凭证错误——换错密码照样"连接成功" |
| `MetadataService:320` 连接测试 | ❌ 保留裸连接 | 同上（原代码已有注释说明） |
| `DataValidationService` 4 处 | ❌ 保留裸连接 | **转池有害**：校验持有源+目标两条连接跨越整个比对（可达数分钟），池容量 10，5 个并发校验就占满，第 6 个阻塞 30s 后失败 |

引擎侧约 82 处未动——它们大多在子进程里、生命周期与进程同寿，池化收益接近零。

## P-04　前端构建体系　✅ 完成

```
npm run build

第三方库 → static/vendor/          （版本与原 CDN 逐一对应，附 SOURCES.json 与 sha256）
  sockjs.min.js       56.1 KB
  stomp.min.js         7.7 KB
  chart.umd.min.js   200.8 KB

压缩 dashboard → static/js/
  admin-dashboard.js       369.1 KB → 185.0 KB  (-50%)
  dashboard-subscribe.js    68.2 KB →  35.2 KB  (-48%)
  dashboard-dr.js           46.8 KB →  25.9 KB  (-45%)
  dashboard-advanced.js     44.0 KB →  26.0 KB  (-41%)
  dashboard-ssl.js          20.8 KB →  12.4 KB  (-40%)
  合计                     549.0 KB → 284.5 KB  (-48%)
```

几个刻意的取舍：

- **不 bundle，只 minify**：`admin-dashboard.js` 里的函数是靠全局作用域被三个
  `type="module"` 的 dashboard 调到的（`escapeHtml` 就是），打进一个 bundle 会改变作用域语义。
  构建只负责"更小"，不负责"更现代"。
- **落本地不升级版本**：三个库装的就是原 CDN 上那几个版本。我第一次装成了
  `@stomp/stompjs@7`——那是完全不同的 API（页面用的是 `Stomp.over()` 老式全局），
  会直接打断 WebSocket。落本地不该改变行为。
- **`static/vendor/` 入库、`static/js/` 不入库**：落本地的意义是部署时不依赖外网与 npm，
  不入库就等于把 CDN 依赖换成了 npm 依赖，白做。产物则由构建生成，源文件才是正本。
- **默认仍下发源文件**：`app.frontend.dist=false`（缺省）与改造前逐字节相同，
  开发时改一行就能刷新看到；生产设 `true`。

**主要收益是 CSP 收回 `'self'`**——外部脚本源一个都不放，供应链风险与公网依赖一并去掉。

### 实测（浏览器）

```
CSP           script-src 'self' 'unsafe-inline'     ← 已无 cdn.jsdelivr.net
加载的脚本     static/vendor/{sockjs,stomp,chart.umd}.min.js + 五个 dashboard
SockJS/Stomp/Chart/escapeHtml   全部 typeof 正确
admin-dashboard.js 下发         189,416 B（产物）而非 369 KB（源文件）
页面渲染       任务列表/状态徽章/一致性徽章 全部正常
```

途中修掉一个自己引入的问题：`/static/vendor/*.js` 返回 401——`SecurityConfig` 的
`/*.js` 只匹配根级单段路径，多段路径匹配不上。已补 `/static/**` 放行。

## F-03 / F-06　设计文档　✅ 完成（按约定不写实现）

- `markdown/PITR_DESIGN_20260819.md`
- `markdown/DATA_GOVERNANCE_DESIGN_20260819.md`

两份都写到"改哪个类、判据怎么写、分几阶段"的粒度，并各自列了**实施前必须先查清的风险**：

- PITR：`.cap` 不归档，若某些边界形态（压缩 binlog、PARTIAL_JSON）的信息只在 `.cap` 里，
  回放会失真——这一条必须在阶段 1 之前查清。
- 治理：脱敏与数据校验、脱敏与双向灾备是互斥的，语义必须先定，
  否则用户会看到一堆"差异"而不知道是设计如此。

---

# 第五轮：F-06 数据脱敏（2026-08-19）

按 `DATA_GOVERNANCE_DESIGN_20260819.md` 的阶段 3 实施。设计文档里标为"实施前必须先定"的
脱敏与内容对比的互斥语义，由使用方定为：**提示用户 + 脱敏列自动排除在内容对比之外**。

## 语义

> 如果选择了脱敏，则无法完成脱敏列内容对比。

这句话在三处出现，口径一致：向导第 3 步的脱敏页签、添加规则后的汇总条、以及保存配置时
落进任务日志的一条 WARNING。**行数对比与其余列的内容对比不受影响**——这是与既有
"配置了列处理的任务完全不能对比"最关键的区别。

为此 `columnMask` <b>刻意不加进</b> `RouteConfigValidator.COLUMN_PROCESSING_KEYS`：
那三项（过滤/映射/附加列）会改变行的存在与否和列的构成，只能整表放弃对比；
而脱敏只改被脱敏那几列的值。已在该常量上写了注释钉住，防止有人"顺手"加进去。

## 五种规则

| 规则 | 语义 |
|---|---|
| `MASK_ALL` | 整值替换 |
| `MASK_PARTIAL` | 保留前 N 后 M，中间<b>固定 6 个 *</b>——按原长度填会泄露原值长度，对定长字段等于泄露格式 |
| `HASH` | 不可逆但<b>保留可连接性</b>：同值恒同结果，跨表 JOIN 仍成立 |
| `NULLIFY` | 置空 |
| `FAKE` | 同型假数据（姓名/邮箱/手机号/地址/身份证） |

**所有规则都是确定性的**。这不是美观要求：增量重放、断点续传、以及全量与增量对同一行的
两次处理都依赖"再算一遍还是那个值"。带随机性的话，幂等 upsert 会把每次重放都看成一次
真实变更，两端将永远对不齐且不报任何错。

## 挂载点

| 侧 | 位置 | 为什么是这里 |
|---|---|---|
| 全量 | `DataMigration#readColumnValue` | 行值的单一出口，分页扫描与瘦行扫描都经过它 |
| 增量 | `TypedDmlConverter` 取到 `rowsTyped` 之后 | 一处覆盖 INSERT/UPDATE/DELETE/拆分/汇聚全部下游路径 |

增量的 **before 镜像（UPDATE/DELETE 的 WHERE 值）必须同样脱敏**：目标端存的是脱敏后的值，
拿原值做 WHERE 一行都匹配不上——UPDATE 影响 0 行、DELETE 删不掉，而两者都不报错。

## 端到端判据抓到的一个洞

`test_scripts/masking/mask_e2e.py` 首次运行结果：**全量 20 行脱敏全对，增量 5 行完全没脱敏。**

根因：`ColumnProcessingConfig.isEmpty()` 只看 filters/mappings/extras，不认识 masks。
于是"只配了脱敏"的任务被判定为无列处理，`columnProcessingActive` 为 false，
增量整条脱敏逻辑被跳过；而全量走的是另一条判断，照常脱敏——
形成"全量脱了、增量没脱"这种最难察觉的半脱敏状态。

**单测碰不到这个洞**（单测直接构造配置对象，不走 isEmpty 那条判断），只有端到端能抓。
已修，并在 `isEmpty()` 上写明原因。

## 实测

```
全量 20 行 + 增量 5 行，全部脱敏正确；未脱敏列 amount 原样
确定性：同一 email 原值的两行 → 目标端得到同一脱敏值  ✓

源端  (1, '用户1',  '13800130001', 'u1@corp.com',  '备注1')
目标端 (1, '黄磊',  '138******0001', '8dfc48fb64c365015ae149fd70eb372b', NULL)
```

## 判据

| 判据 | 项 | 守什么 |
|---|---|---|
| `ColumnMaskTest` | 16 | 确定性、跨实例一致、HASH 可连接、遮蔽不泄露长度、保留位数≥原长时整值遮蔽、未知 FAKE 类型退化为哈希而非原样返回 |
| `ColumnMaskValidatorTest` | 10 | 提示语必须含"无法完成脱敏列内容对比"、主键不得脱敏、同列多规则被拒、FAKE 必须带类型 |
| `mask_e2e.py` | 端到端 | 全量与增量都脱敏、未脱敏列不受影响、确定性 |

## 未做（阶段 1/2 的血缘）

本轮只做了设计文档里的阶段 3（脱敏）。字段级血缘（阶段 1~2）未实现——
设计文档里它排在第一优先，理由是"元数据已在手上、投入产出比最高"，那个判断不变。

---

# 第六轮：F-06 剩余三块（2026-08-20）

字段级血缘（阶段 1/2）、数据分级与标签（阶段 4）、Schema 演进审批（阶段 5）。
按设计文档的依赖顺序做——分级的策略校验与审批的风险标注都要读血缘图。

## 血缘：采集不需要新探针

这是设计文档里最关键的那个判断，实施后成立。血缘要的信息平台已经全部掌握：

| 已有的东西 | 血缘拿它做什么 |
|---|---|
| `syncObjects` 表清单 / `tableMapping` / `targetDb` | 表级与库级的边 |
| `columnMapping` | RENAME |
| `columnMask` | MASK（并标 `comparable:false`） |
| `columnFilter` | FILTER（行级，记在被引用的列上） |
| `extraColumns` | DEFAULT_VALUE（目标端有、源端没有，挂虚拟源节点） |
| `routeConfig` | ROUTE_SPLIT / ROUTE_MERGE（记在分片键列上） |

所以实现是**一个把配置归一成图的转换层**，不是一套采集系统。
生成时机是<b>保存任务配置时</b>而非运行期——血缘描述的是"数据会怎么流"，那由配置决定；
运行期采集只会把同一件事算很多遍，还得处理"任务没跑过就没有血缘"的空洞。

一条边只记<b>一个</b>算子，按改动强度取最强：脱敏 ＞ 改名 ＞ 原样。
同时改名又脱敏时，脱敏才是使用者需要知道的那件事——改名看目标列名就知道，
而"值被改过"看不出来。

## 分级：价值在联动

单独打标用处有限。三条策略都是与另两块能力接起来才能给出的结论：

| 策略 | 依赖 |
|---|---|
| 目标端级别不得低于源端（数据流动中不该被降级） | 血缘 |
| RESTRICTED 列必须脱敏 | 血缘 + 脱敏 |
| 某级别的列都流到了哪里（合规审计常问） | 血缘 |

人工打标优先于规则，重跑规则**不覆盖**人工值——人工打标代表有人看过并作了判断，
被一条正则悄悄改掉是最糟的：既丢了判断，又没人知道它变了。

内置 8 条规则（身份证/银行卡/口令 → RESTRICTED，手机/邮箱/姓名/地址 → SENSITIVE，
金额 → INTERNAL），priority 小的先匹配。

## Schema 审批：把 MANUAL 补完

`MANUAL` 档此前只是"停下来记一条日志"——运维自己去目标库敲，平台**不知道敲没敲**。
现在：引擎写结构化待审批记录 → 控制面同步成单（带风险标注）→ **批准即由平台应用并留痕**。

"批准"与"生效"是同一个动作，审计链才完整。单子上的两项风险标注分别来自血缘
（受影响的下游字段）与分级（最高敏感级别）——这正是审批排在它们之后的原因，
单独做审批只能给出一句 SQL。

## 实测（`test_scripts/governance/governance_e2e.py`，19 项全通过）

```
血缘  配置保存后自动生成 7 条边；MASK/RENAME/IDENTITY 三种算子判定正确
      影响面 phone → gov_tgt.members.phone 且标出 hasMask
      上游追溯 mail_addr → 找回源列 email
分级  内置规则打标 5 列：id_card=RESTRICTED / phone,email,real_name=SENSITIVE / amount=INTERNAL
      重跑规则不覆盖人工打标（note 保持 MANUAL/RESTRICTED）
      策略校验发现 2 处 "RESTRICTED 列未脱敏"
审批  待审批同步按 (task,seqno) 去重（重复行 created=1）
      单子标注 maxLevel=RESTRICTED
      批准 → status=APPLIED，目标库真的多出 id_card_ext 列
      已处理的单不能重复审批
```

## 途中修掉的四个问题

1. **`ddl-auto: validate` 拒绝启动**：Hibernate 6 默认把 Java 枚举映射成 MySQL 原生 ENUM，
   而我把 `lineage_edge.operator` 声明成了 VARCHAR。这里 **VARCHAR 才是对的**——
   算子集合以后会长，用 ENUM 意味着每加一个算子都要一次 ALTER。
   用 `@JdbcTypeCode(VARCHAR)` 让 Hibernate 按 VARCHAR 对待。
2. **任务目录路径解析错**：后端跑在 `java-backend/` 下，`files/...` 解析到
   `java-backend/files/`，找不到东西且不报错，表现为"同步了 0 条"。
   改成可配 + 两种布局都试（容器里是 `/app/files`）。
3. **`maxLevel` 恒为 null**：`ADD COLUMN id_card_ext` 是全新列，库里当然没有它的分级记录。
   但"新增了一个名字像身份证的列"正是审批人最该看到的——补了<b>按列名套规则</b>的回退。
   这是功能设计的问题，不是判据写错。
4. **`fetchWithAuth` 把 403 当 token 过期**：那是加 RBAC 之前的假设。
   现在 403 = 角色不够，把只读用户从无权访问的按钮上强制踢出登录，且重新登录也没用。
   已改为弹提示而不是跳登录页。

## 顺带修正一条判据

`AuthorizationMatrixTest.viewerIsReadOnly` 原来断言 "VIEWER 全文只出现一次"——
那是写判据时恰好只有一条 GET 规则，把当时的现状误当成了不变量，
新增一条只读授权（治理查询）就误报。真正的不变量是"VIEWER 不能出现在写规则上"，
已改为**逐处检查**。

## 授权划分

| 路径 | 角色 | 理由 |
|---|---|---|
| `schema-changes/*/approve|reject` | ADMIN | 批准即改目标库结构，与"改配置"是两个量级 |
| `classification/rules/**` | ADMIN | 一条正则影响所有库表的打标 |
| `GET /api/governance/**` | + VIEWER | 合规审计要看"敏感数据流到哪儿"，正是只读角色该有的能力 |
| 其余写操作 | USER 及以上 | |
