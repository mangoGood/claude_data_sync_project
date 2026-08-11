package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一键诊断服务
 * 自动检查源库/目标库连接、权限、binlog配置、磁盘空间等前置条件
 */
@Service
public class DiagnosticService {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticService.class);

    @Autowired
    private WorkflowRepository workflowRepository;

    /**
     * 执行一键诊断
     */
    public Map<String, Object> diagnose(String workflowId, Long userId) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此任务");
        }

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> checks = new ArrayList<>();
        int passed = 0;
        int failed = 0;
        int warnings = 0;

        // 1. 检查源库连接
        Map<String, Object> sourceConnCheck = checkDatabaseConnection("源库连接", workflow.getSourceConnection());
        checks.add(sourceConnCheck);
        String srcStatus = (String) sourceConnCheck.get("status");
        if ("PASS".equals(srcStatus)) passed++; else if ("FAIL".equals(srcStatus)) failed++; else warnings++;

        // 2. 检查目标库连接
        Map<String, Object> targetConnCheck = checkDatabaseConnection("目标库连接", workflow.getTargetConnection());
        checks.add(targetConnCheck);
        String tgtStatus = (String) targetConnCheck.get("status");
        if ("PASS".equals(tgtStatus)) passed++; else if ("FAIL".equals(tgtStatus)) failed++; else warnings++;

        // 3. 检查源库binlog配置
        if ("PASS".equals(srcStatus)) {
            Map<String, Object> binlogCheck = checkBinlogConfig(workflow.getSourceConnection());
            checks.add(binlogCheck);
            String bStatus = (String) binlogCheck.get("status");
            if ("PASS".equals(bStatus)) passed++; else if ("FAIL".equals(bStatus)) failed++; else warnings++;
        }

        // 4. 检查源库权限
        if ("PASS".equals(srcStatus)) {
            Map<String, Object> permCheck = checkDatabasePrivileges("源库权限", workflow.getSourceConnection());
            checks.add(permCheck);
            String pStatus = (String) permCheck.get("status");
            if ("PASS".equals(pStatus)) passed++; else if ("FAIL".equals(pStatus)) failed++; else warnings++;
        }

        // 5. 检查目标库权限
        if ("PASS".equals(tgtStatus)) {
            Map<String, Object> permCheck = checkDatabasePrivileges("目标库权限", workflow.getTargetConnection());
            checks.add(permCheck);
            String pStatus = (String) permCheck.get("status");
            if ("PASS".equals(pStatus)) passed++; else if ("FAIL".equals(pStatus)) failed++; else warnings++;
        }

        // 6. 检查磁盘空间
        Map<String, Object> diskCheck = checkDiskSpace();
        checks.add(diskCheck);
        String dStatus = (String) diskCheck.get("status");
        if ("PASS".equals(dStatus)) passed++; else if ("FAIL".equals(dStatus)) failed++; else warnings++;

        // 7. 检查任务配置完整性
        Map<String, Object> configCheck = checkTaskConfig(workflow);
        checks.add(configCheck);
        String cStatus = (String) configCheck.get("status");
        if ("PASS".equals(cStatus)) passed++; else if ("FAIL".equals(cStatus)) failed++; else warnings++;

        result.put("checks", checks);
        result.put("total", checks.size());
        result.put("passed", passed);
        result.put("failed", failed);
        result.put("warnings", warnings);
        result.put("overall", failed > 0 ? "FAIL" : (warnings > 0 ? "WARNING" : "PASS"));
        result.put("workflowId", workflowId);
        result.put("workflowName", workflow.getName());

        return result;
    }

    private Map<String, Object> checkDatabaseConnection(String checkName, String connectionStr) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", checkName);

        if (connectionStr == null || connectionStr.trim().isEmpty()) {
            result.put("status", "FAIL");
            result.put("message", "连接字符串为空");
            return result;
        }

        Connection conn = null;
        try {
            // 解析 mysql://user:pass@host:port/db 格式
            String[] parsed = parseConnectionUrl(connectionStr);
            String jdbcUrl = parsed[0];
            String username = parsed[1];
            String password = parsed[2];

            conn = DriverManager.getConnection(jdbcUrl, username, password);
            result.put("status", "PASS");
            result.put("message", "连接成功");
            result.put("detail", "JDBC: " + jdbcUrl.replaceAll(password, "***"));
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", "连接失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> checkBinlogConfig(String connectionStr) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", "源库Binlog配置");

        Connection conn = null;
        try {
            String[] parsed = parseConnectionUrl(connectionStr);
            conn = DriverManager.getConnection(parsed[0], parsed[1], parsed[2]);

            // 检查 log_bin 是否开启
            try (PreparedStatement stmt = conn.prepareStatement("SHOW VARIABLES LIKE 'log_bin'");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String logBin = rs.getString(2);
                    if ("ON".equalsIgnoreCase(logBin)) {
                        result.put("status", "PASS");
                        result.put("message", "log_bin=ON");
                    } else {
                        result.put("status", "FAIL");
                        result.put("message", "log_bin=OFF，增量同步需要开启binlog");
                    }
                }
            }

            // 检查 binlog_format
            try (PreparedStatement stmt = conn.prepareStatement("SHOW VARIABLES LIKE 'binlog_format'");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String format = rs.getString(2);
                    if (!"ROW".equalsIgnoreCase(format)) {
                        result.put("status", "WARNING");
                        result.put("message", "binlog_format=" + format + "，建议设置为ROW");
                    }
                }
            }

            // 检查 binlog_row_image
            try (PreparedStatement stmt = conn.prepareStatement("SHOW VARIABLES LIKE 'binlog_row_image'");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String image = rs.getString(2);
                    if (!"FULL".equalsIgnoreCase(image)) {
                        String msg = "binlog_row_image=" + image + "，建议设置为FULL";
                        result.put("status", "WARNING");
                        result.put("message", result.get("message") + "; " + msg);
                    }
                }
            }
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", "检查失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> checkDatabasePrivileges(String checkName, String connectionStr) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", checkName);

        Connection conn = null;
        try {
            String[] parsed = parseConnectionUrl(connectionStr);
            conn = DriverManager.getConnection(parsed[0], parsed[1], parsed[2]);

            try (PreparedStatement stmt = conn.prepareStatement("SHOW GRANTS FOR CURRENT_USER()");
                 ResultSet rs = stmt.executeQuery()) {
                StringBuilder grants = new StringBuilder();
                boolean hasAllPrivileges = false;
                while (rs.next()) {
                    String grant = rs.getString(1);
                    grants.append(grant).append("; ");
                    if (grant.contains("ALL PRIVILEGES")) hasAllPrivileges = true;
                }
                if (hasAllPrivileges) {
                    result.put("status", "PASS");
                    result.put("message", "拥有ALL PRIVILEGES权限");
                } else {
                    result.put("status", "WARNING");
                    result.put("message", "权限: " + grants);
                }
            }
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", "权限检查失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> checkDiskSpace() {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", "本地磁盘空间");

        java.io.File disk = new java.io.File(".");
        long freeSpace = disk.getUsableSpace();
        long totalSpace = disk.getTotalSpace();
        long freeGB = freeSpace / (1024 * 1024 * 1024);

        if (freeGB > 1) {
            result.put("status", "PASS");
            result.put("message", "可用空间: " + freeGB + "GB");
        } else {
            result.put("status", "FAIL");
            result.put("message", "磁盘空间不足: 仅剩 " + freeGB + "GB");
        }
        return result;
    }

    private Map<String, Object> checkTaskConfig(Workflow workflow) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", "任务配置完整性");

        List<String> issues = new ArrayList<>();
        if (workflow.getName() == null || workflow.getName().trim().isEmpty()) {
            issues.add("任务名称为空");
        }
        if (workflow.getSourceConnection() == null) {
            issues.add("源库连接未配置");
        }
        if (workflow.getTargetConnection() == null) {
            issues.add("目标库连接未配置");
        }
        if (workflow.getSyncObjects() == null || workflow.getSyncObjects().isEmpty()) {
            issues.add("同步对象未选择");
        }
        if (workflow.getMigrationMode() == null) {
            issues.add("迁移模式未选择");
        }

        if (issues.isEmpty()) {
            result.put("status", "PASS");
            result.put("message", "配置完整");
        } else {
            result.put("status", "FAIL");
            result.put("message", String.join("; ", issues));
        }
        return result;
    }

    // ==================== 启动前 schema 预检 ====================
    private static final com.google.gson.Gson PRECHECK_GSON = new com.google.gson.Gson();

    /**
     * 启动前 schema 预检：把"跑起来才暴露"的结构问题挡在 launch 之前。
     * 源表/库存在性（FAIL）、增量任务主键缺失（WARNING，无 PK 增量 UPDATE/DELETE 无法定位行）、
     * 列处理引用的源列存在性（FAIL）、目标同名表已存在（WARNING，全量可能冲突/重复）。
     * 目前仅 MySQL 源库；其它源类型返回单条 WARNING 跳过（不阻断启动）。
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> schemaPrecheck(String workflowId, Long userId) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此任务");
        }

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> checks = new ArrayList<>();

        String srcConn = workflow.getSourceConnection();
        // TiDB 讲 MySQL 协议，连接串同为 mysql://，走同一套 information_schema 查询
        boolean mysqlSource = srcConn != null && srcConn.startsWith("mysql://");
        boolean pgSource = srcConn != null && srcConn.startsWith("postgresql://");
        boolean oracleSource = srcConn != null && srcConn.startsWith("oracle://");
        boolean mongoSource = srcConn != null && srcConn.startsWith("mongodb://");
        if (!mysqlSource && !pgSource && !oracleSource && !mongoSource) {
            checks.add(check("schema 预检", "WARNING",
                    "对象级预检暂不支持该源类型（当前支持 MySQL/TiDB/PostgreSQL/Oracle/MongoDB），已跳过（不影响启动）", null));
            return summarize(result, checks, workflow);
        }

        List<DbEntry> entries;
        try {
            entries = parseSyncEntries(workflow);
        } catch (Exception e) {
            checks.add(check("同步对象解析", "FAIL", "无法解析同步对象配置: " + e.getMessage(), null));
            return summarize(result, checks, workflow);
        }
        if (entries.isEmpty()) {
            checks.add(check("同步对象", "FAIL", "未选择任何同步对象", null));
            return summarize(result, checks, workflow);
        }

        if (pgSource) {
            return summarize(result, pgSchemaPrecheck(workflow, srcConn, entries, checks), workflow);
        }
        if (oracleSource) {
            return summarize(result, oracleSchemaPrecheck(workflow, srcConn, entries, checks), workflow);
        }
        if (mongoSource) {
            return summarize(result, mongoSchemaPrecheck(workflow, srcConn, entries, checks), workflow);
        }

        String mode = workflow.getMigrationMode();
        boolean needsIncrement = mode != null &&
                (mode.toLowerCase().contains("incre") || mode.equalsIgnoreCase("subscribe"));
        String tgtConn = workflow.getTargetConnection();
        boolean relationalTarget = tgtConn != null &&
                (tgtConn.startsWith("mysql://") || tgtConn.startsWith("postgresql://"));

        try (Connection src = openConn(srcConn)) {
            checks.add(checkSourceObjectsExist(src, entries));
            if (needsIncrement) {
                if ("tidb".equalsIgnoreCase(workflow.getSourceType())) {
                    checks.add(checkTidbReplicableKeys(src, entries));
                } else {
                    checks.add(checkPrimaryKeys(src, entries));
                }
            }
            checks.add(checkColumnRefs(src, entries));
            checks.add(checkForeignKeyIntegrity(src, entries));
            checks.add(checkLargeObjectSupport(src, srcConn, tgtConn, entries, needsIncrement));
        } catch (Exception e) {
            checks.add(check("源库 schema 检查", "FAIL", "连接源库失败: " + e.getMessage(), null));
        }

        checks.add(checkTransportEncryption(workflow));

        // 目标同名表预存在（仅关系型目标；异构/kafka 跳过）——目标库名按 per-db 映射解析
        if (relationalTarget && tgtConn.startsWith("mysql://")) {
            try (Connection tgt = openConn(tgtConn); Connection src2 = openConn(srcConn)) {
                checks.add(checkTargetConflicts(tgt, entries, workflow));
                checks.add(checkTargetUniqueIndexes(src2, tgt, entries));
            } catch (Exception e) {
                checks.add(check("目标库 schema 检查", "WARNING", "连接目标库失败，跳过目标冲突检查: " + e.getMessage(), null));
            }
        }

        return summarize(result, checks, workflow);
    }

    /**
     * 大字段（LONGBLOB/LONGTEXT/MEDIUM*）能不能搬得动。
     *
     * <p>这一项拦的都是"跑到一半才炸、且炸得看不懂"的情况：
     * <ul>
     *   <li><b>max_allowed_packet 不够</b>：MySQL 的这个参数同时限制单值上限与
     *       {@code CONCAT()} 结果上限，分块追加也绕不过去。撞上时服务端只回一句
     *       "Result of concat() was larger than max_allowed_packet - truncated"，
     *       既不说是哪张表哪一列，也不说该调多大。它的上限就是 1GB，
     *       所以超过 1GB 的单值<b>根本无法通过 SQL 协议写入</b>；</li>
     *   <li><b>大字段表没有主键</b>：增量靠主键定位行，没有主键就要拿 1GB 的值去做全列匹配，
     *       既不可行也没有意义；</li>
     *   <li><b>binlog 事务压缩开着</b>：压缩事务在连接器里是整块解压进堆的，
     *       大字段的流式改造对这条路径完全无效；</li>
     *   <li><b>binlog_row_image=FULL</b>：UPDATE 会把没改动的大字段前后镜像都写进 binlog，
     *       网络与磁盘各多一倍。改 NOBLOB 能直接省掉，但不阻断。</li>
     * </ul>
     */
    private Map<String, Object> checkLargeObjectSupport(Connection src, String srcConn, String tgtConn,
                                                        List<DbEntry> entries, boolean needsIncrement) {
        List<String> lobTables = new ArrayList<>();
        List<String> noPkLobTables = new ArrayList<>();
        long maxValueBytes = 0;
        String maxValueWhere = null;
        try {
            for (DbEntry entry : entries) {
                for (String table : entry.tables) {
                    List<String> lobCols = new ArrayList<>();
                    try (PreparedStatement ps = src.prepareStatement(
                            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
                                    + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                                    + "AND DATA_TYPE IN ('longblob','longtext','mediumblob','mediumtext')")) {
                        ps.setString(1, entry.sourceDb);
                        ps.setString(2, table);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                lobCols.add(rs.getString(1));
                            }
                        }
                    }
                    if (lobCols.isEmpty()) {
                        continue;
                    }
                    lobTables.add(entry.sourceDb + "." + table);
                    if (!hasPrimaryKey(src, entry.sourceDb, table)) {
                        noPkLobTables.add(entry.sourceDb + "." + table + "(" + String.join(",", lobCols) + ")");
                    }
                    // 实际最大值：OCTET_LENGTH 会让服务端把 LOB 读一遍，所以只在
                    // 确实存在大字段列的表上做，且一张表一条语句。
                    for (String col : lobCols) {
                        try (java.sql.Statement st = src.createStatement();
                             ResultSet rs = st.executeQuery("SELECT IFNULL(MAX(OCTET_LENGTH(`"
                                     + col.replace("`", "``") + "`)),0) FROM `"
                                     + entry.sourceDb.replace("`", "``") + "`.`" + table.replace("`", "``") + "`")) {
                            if (rs.next() && rs.getLong(1) > maxValueBytes) {
                                maxValueBytes = rs.getLong(1);
                                maxValueWhere = entry.sourceDb + "." + table + "." + col;
                            }
                        } catch (Exception ignored) {
                            // 单列量不到不影响其余判据
                        }
                    }
                }
            }
        } catch (Exception e) {
            return check("大字段搬运能力", "WARNING", "大字段检查失败，已跳过: " + e.getMessage(), null);
        }

        if (lobTables.isEmpty()) {
            return check("大字段搬运能力", "PASS", "所选对象里没有 LONGBLOB/LONGTEXT/MEDIUM* 列", null);
        }

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        StringBuilder detail = new StringBuilder("含大字段的表: " + String.join(", ", lobTables));
        if (maxValueWhere != null) {
            detail.append("；实测最大单值 ").append(maxValueBytes).append(" 字节（").append(maxValueWhere).append("）");
        }

        long srcPacket = globalLong(src, "max_allowed_packet");
        long tgtPacket = -1;
        if (tgtConn != null && tgtConn.startsWith("mysql://")) {
            try (Connection tgt = openConn(tgtConn)) {
                tgtPacket = globalLong(tgt, "max_allowed_packet");
            } catch (Exception e) {
                warnings.add("读不到目标端 max_allowed_packet: " + e.getMessage());
            }
        }
        detail.append("；max_allowed_packet 源=").append(srcPacket).append(" 目标=").append(tgtPacket);

        if (maxValueBytes > 1073741824L) {
            errors.add("存在超过 1GB 的单值（" + maxValueBytes + " 字节 @ " + maxValueWhere
                    + "）。MySQL 的 max_allowed_packet 上限就是 1GB，这种值无法通过 SQL 协议写入目标端");
        } else if (maxValueBytes > 0) {
            if (srcPacket > 0 && srcPacket < maxValueBytes) {
                errors.add("源端 max_allowed_packet=" + srcPacket + " 小于最大单值 " + maxValueBytes
                        + "，读取会被截断，请调到 >= " + maxValueBytes);
            }
            if (tgtPacket > 0 && tgtPacket < maxValueBytes) {
                errors.add("目标端 max_allowed_packet=" + tgtPacket + " 小于最大单值 " + maxValueBytes
                        + "，写入必失败（该参数同时限制 CONCAT 结果上限，分块追加也绕不过），请调到 >= " + maxValueBytes);
            }
        }

        if (needsIncrement && !noPkLobTables.isEmpty()) {
            errors.add("下列表含大字段却没有主键，增量无法按行定位: " + String.join(", ", noPkLobTables));
        }

        if (needsIncrement && maxValueBytes > 0) {
            // 复制协议的硬上限：单个 binlog 事件不能超过 replica_max_allowed_packet（最大 1GB）。
            // 实测过：1GB 的 LONGBLOB 加上其余 9 列，行事件是 1073742110 字节——比 1GB 上限多 286 字节，
            // 源端的 dump 线程直接断开连接，capture 侧表现为"读取线程无声无息地没了"。
            // 注意这与全量无关：全量走的是普通 SQL，1GB 是搬得动的（已验证）。
            long replicaLimit = globalLong(src, "replica_max_allowed_packet");
            if (replicaLimit <= 0) {
                replicaLimit = globalLong(src, "slave_max_allowed_packet");
            }
            String rowImage0 = globalString(src, "binlog_row_image");
            boolean fullImage = rowImage0 == null || "FULL".equalsIgnoreCase(rowImage0);
            // UPDATE 的前后镜像在<b>同一个事件</b>里；FULL 下未改动的大字段也会两份都写进去
            long worstEvent = fullImage ? maxValueBytes * 2 : maxValueBytes;
            detail.append("；replica_max_allowed_packet=").append(replicaLimit)
                    .append("，最坏事件约 ").append(worstEvent).append(" 字节")
                    .append(fullImage ? "（FULL 下 UPDATE 带前后两份镜像）" : "（NOBLOB）");
            if (replicaLimit > 0 && worstEvent >= replicaLimit) {
                errors.add("增量搬不动：单值 " + maxValueBytes + " 字节，在 binlog_row_image="
                        + (rowImage0 == null ? "FULL" : rowImage0) + " 下一个 UPDATE 事件约 " + worstEvent
                        + " 字节，超过 replica_max_allowed_packet=" + replicaLimit
                        + "（MySQL 上限就是 1GB）。这是复制协议本身的限制，与同步工具无关——"
                        + "源端 dump 线程会直接断开。可行的做法：把 binlog_row_image 改成 NOBLOB "
                        + "（UPDATE 只带一份后镜像，上限翻倍）、把单值控制在 1GB 以内并留出行开销余量，"
                        + "或该表只做全量不做增量（全量走普通 SQL，不受这条限制）");
            }
        }

        if (needsIncrement) {
            String compression = globalString(src, "binlog_transaction_compression");
            if (compression != null && ("ON".equalsIgnoreCase(compression) || "1".equals(compression))) {
                errors.add("源端 binlog_transaction_compression=ON：压缩事务在连接器里是整块解压进内存的，"
                        + "大字段的流式读取对这条路径无效，请关闭");
            }
            String rowImage = globalString(src, "binlog_row_image");
            if (rowImage != null && "FULL".equalsIgnoreCase(rowImage)) {
                warnings.add("源端 binlog_row_image=FULL：UPDATE 会把未改动的大字段前后镜像都写进 binlog，"
                        + "网络与磁盘各多一倍；改成 NOBLOB 可直接省掉（不影响正确性）");
            }
        }

        if (!errors.isEmpty()) {
            return check("大字段搬运能力", "FAIL", String.join("；", errors), detail.toString());
        }
        if (!warnings.isEmpty()) {
            return check("大字段搬运能力", "WARNING", String.join("；", warnings), detail.toString());
        }
        return check("大字段搬运能力", "PASS", "大字段可搬运（单值未超过两端 max_allowed_packet，且均有主键）",
                detail.toString());
    }

    private long globalLong(Connection conn, String var) {
        String v = globalString(conn, var);
        try {
            return v == null ? -1 : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private String globalString(Connection conn, String var) {
        try (java.sql.Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT @@GLOBAL." + var)) {
            return rs.next() ? rs.getString(1) : null;
        } catch (Exception e) {
            return null;   // 变量不存在（版本差异）不算失败
        }
    }

    /**
     * 传输层加密。
     *
     * <p>平台把凭证 AES-GCM 落库加密、THL 文件也能加密，唯独<b>真正流动的业务数据</b>
     * 长期是明文的（全仓 34 处硬编码 {@code useSSL=false}，且没有任何 SSL 配置项）。
     * 金融政企的入网评审基本过不了这一关。
     *
     * <p>只报 WARNING 不阻断：内网环境不开 TLS 是常见且合理的选择，
     * 但"当初知不知道自己没开"必须留下痕迹（预检结果现在会落 {@code task_precheck_results}）。
     */
    private Map<String, Object> checkTransportEncryption(Workflow workflow) {
        String src = System.getenv("SOURCE_DB_SSL_MODE");
        String tgt = System.getenv("TARGET_DB_SSL_MODE");
        boolean srcOn = on(src);
        boolean tgtOn = on(tgt);
        // 控制面：后端自己也直连用户库（元数据探查、连接校验、数据校验、**内容对比逐行读业务数据**），
        // 以及后端/agent 到元数据库与 Kafka 的那几跳。数据面加密而控制面明文，
        // 等于同一批数据换条路又明文走了一遍——所以四项一起判，只报"全开"才算 PASS。
        String control = com.synctask.util.JdbcSslOptions.mode();
        String meta = System.getenv("META_DB_SSL_MODE");
        String kafka = com.synctask.util.KafkaSecurity.protocol();
        boolean controlOn = on(control);
        boolean metaOn = on(meta);
        boolean kafkaOn = com.synctask.util.KafkaSecurity.enabled();

        String detail = String.format(
                "数据面 源=%s 目标=%s；控制面 到用户库=%s 到元数据库=%s Kafka=%s",
                srcOn ? src : "DISABLED", tgtOn ? tgt : "DISABLED",
                controlOn ? control : "DISABLED", metaOn ? meta : "DISABLED", kafka);

        if (srcOn && tgtOn && controlOn && metaOn && kafkaOn) {
            return check("传输加密", "PASS", "数据面与控制面均已启用 TLS", detail);
        }
        return check("传输加密", "WARNING",
                "以下链路未启用 TLS，数据在网络上是明文传输", detail
                        + "。开关：SOURCE_DB_SSL_MODE / TARGET_DB_SSL_MODE（数据面）、"
                        + "CONTROL_PLANE_DB_SSL_MODE（后端直连用户库）、META_DB_SSL_MODE（元数据库）、"
                        + "KAFKA_SECURITY_PROTOCOL=SSL|SASL_SSL（Kafka）");
    }

    private static boolean on(String v) {
        return v != null && !v.isEmpty() && !"DISABLED".equalsIgnoreCase(v);
    }

    /**
     * PostgreSQL 源的对象级预检。
     *
     * <p>与 MySQL 版是<b>同一批判据、不同的目录表</b>：PG 里 syncObjects 的 key 是 schema，
     * 所以查的是 {@code pg_catalog}/{@code information_schema} 而不是 {@code information_schema.schemata} 那套。
     * 之前这条链路整个被一句"仅支持 MySQL 源库，已跳过"打发掉——对象级预检覆盖率为 0，
     * 而"跑起来才炸"的问题恰恰大多在对象级。
     *
     * <p>多一条 MySQL 没有的检查：<b>REPLICA IDENTITY</b>。PG 的逻辑复制默认只在 WAL 里带主键列，
     * 无主键表若不设 {@code REPLICA IDENTITY FULL}，UPDATE/DELETE 根本没有前镜像可用，
     * 增量会直接哑掉——这是 PG 特有、且必炸的一条。
     */
    private List<Map<String, Object>> pgSchemaPrecheck(Workflow workflow, String srcConn,
                                                       List<DbEntry> entries,
                                                       List<Map<String, Object>> checks) {
        String mode = workflow.getMigrationMode();
        boolean needsIncrement = mode != null
                && (mode.toLowerCase().contains("incre") || mode.equalsIgnoreCase("subscribe"));
        try (Connection src = openPgConn(srcConn)) {
            List<String> missing = new ArrayList<>();
            List<String> noPk = new ArrayList<>();
            List<String> weakIdentity = new ArrayList<>();
            List<String> missingCols = new ArrayList<>();

            for (DbEntry de : entries) {
                if (!pgSchemaExists(src, de.sourceDb)) {
                    missing.add("schema " + de.sourceDb);
                    continue;
                }
                if (de.dbLevel) continue;
                for (String t : de.tables) {
                    if (!pgTableExists(src, de.sourceDb, t)) {
                        missing.add(de.sourceDb + "." + t);
                        continue;
                    }
                    if (needsIncrement) {
                        if (!pgHasPrimaryKey(src, de.sourceDb, t)) {
                            noPk.add(de.sourceDb + "." + t);
                            if (!"f".equalsIgnoreCase(pgReplicaIdentity(src, de.sourceDb, t))) {
                                weakIdentity.add(de.sourceDb + "." + t);
                            }
                        }
                    }
                }
                for (Map.Entry<String, java.util.Set<String>> te : de.referencedColumns.entrySet()) {
                    if (!pgTableExists(src, de.sourceDb, te.getKey())) continue;
                    java.util.Set<String> cols = pgTableColumns(src, de.sourceDb, te.getKey());
                    for (String ref : te.getValue()) {
                        if (!cols.contains(ref.toLowerCase())) {
                            missingCols.add(de.sourceDb + "." + te.getKey() + "." + ref);
                        }
                    }
                }
            }

            checks.add(missing.isEmpty()
                    ? check("源库对象存在性", "PASS", "所有同步对象均存在于源库", null)
                    : check("源库对象存在性", "FAIL",
                            "源库不存在以下对象（" + missing.size() + " 个）", String.join(", ", missing)));

            if (needsIncrement) {
                checks.add(noPk.isEmpty()
                        ? check("增量主键", "PASS", "增量同步的表均有主键", null)
                        : check("增量主键", "FAIL",
                                "以下表无主键，增量 UPDATE/DELETE 只能按整行匹配定位（存在完全重复行时无法区分是哪一条，"
                                        + "引擎默认限量成只影响一行）。建议加主键或唯一索引（" + noPk.size() + " 个）",
                                String.join(", ", noPk)));
                checks.add(weakIdentity.isEmpty()
                        ? check("REPLICA IDENTITY", "PASS", "无主键表均已设置 REPLICA IDENTITY FULL", null)
                        : check("REPLICA IDENTITY", "FAIL",
                                "以下无主键表未设置 REPLICA IDENTITY FULL，逻辑复制不会记录前镜像，"
                                        + "UPDATE/DELETE 无法同步（" + weakIdentity.size() + " 个）",
                                String.join(", ", weakIdentity)));
            }

            checks.add(missingCols.isEmpty()
                    ? check("列处理引用列", "PASS", "列过滤/映射引用的源列均存在", null)
                    : check("列处理引用列", "FAIL",
                            "列处理引用了不存在的源列（" + missingCols.size() + " 个）", String.join(", ", missingCols)));
        } catch (Exception e) {
            checks.add(check("源库 schema 检查", "FAIL", "连接源库失败: " + e.getMessage(), null));
        }
        return checks;
    }

    /**
     * Oracle 源的对象级预检。
     *
     * <p>与 MySQL/PG 同一批判据（对象存在性、增量主键、列处理引用列），换成 {@code ALL_*} 数据字典。
     * 两条 Oracle 特有的：
     *
     * <ul>
     *   <li><b>补充日志</b>：LogMiner 默认只记录被改的列，没有最小补充日志时
     *       UPDATE/DELETE 拿不到行标识，增量根本定位不到目标行。这是 Oracle 源最常见的
     *       "任务起得来但增量一条都不同步"的原因，且只有跑起来才会暴露。</li>
     *   <li><b>标识符大小写</b>：Oracle 的对象名默认大写存储，用户在向导里填小写表名
     *       会查不到——但这不是"表不存在"，只是大小写问题，所以单独提示，
     *       否则用户看到"源库不存在以下对象"会去建一张本来就有的表。</li>
     * </ul>
     */
    private List<Map<String, Object>> oracleSchemaPrecheck(Workflow workflow, String srcConn,
                                                           List<DbEntry> entries,
                                                           List<Map<String, Object>> checks) {
        String mode = workflow.getMigrationMode();
        boolean needsIncrement = mode != null
                && (mode.toLowerCase().contains("incre") || mode.equalsIgnoreCase("subscribe"));
        try (Connection src = openOracleConn(srcConn)) {
            List<String> missing = new ArrayList<>();
            List<String> caseHint = new ArrayList<>();
            List<String> noPk = new ArrayList<>();
            List<String> missingCols = new ArrayList<>();

            for (DbEntry de : entries) {
                if (de.dbLevel) continue;
                String owner = de.sourceDb == null ? "" : de.sourceDb.toUpperCase();
                for (String t : de.tables) {
                    String table = t.toUpperCase();
                    if (!oracleTableExists(src, owner, table)) {
                        missing.add(de.sourceDb + "." + t);
                        continue;
                    }
                    if (!table.equals(t)) {
                        caseHint.add(de.sourceDb + "." + t + " → " + table);
                    }
                    if (needsIncrement && !oracleHasPrimaryKey(src, owner, table)) {
                        noPk.add(owner + "." + table);
                    }
                }
                for (Map.Entry<String, java.util.Set<String>> te : de.referencedColumns.entrySet()) {
                    String table = te.getKey().toUpperCase();
                    if (!oracleTableExists(src, owner, table)) continue;
                    java.util.Set<String> cols = oracleTableColumns(src, owner, table);
                    for (String ref : te.getValue()) {
                        if (!cols.contains(ref.toUpperCase())) {
                            missingCols.add(owner + "." + table + "." + ref);
                        }
                    }
                }
            }

            checks.add(missing.isEmpty()
                    ? check("源库对象存在性", "PASS", "所有同步对象均存在于源库", null)
                    : check("源库对象存在性", "FAIL",
                            "源库不存在以下对象（" + missing.size() + " 个）", String.join(", ", missing)));

            if (!caseHint.isEmpty()) {
                checks.add(check("标识符大小写", "WARNING",
                        "Oracle 对象名默认以大写存储，以下对象按大写匹配成功（" + caseHint.size() + " 个）；"
                                + "若源端确实建的是带引号的小写名，请在同步对象里填写实际大小写",
                        String.join(", ", caseHint)));
            }

            if (needsIncrement) {
                checks.add(noPk.isEmpty()
                        ? check("增量主键", "PASS", "增量同步的表均有主键", null)
                        : check("增量主键", "FAIL",
                                "以下表无主键，增量 UPDATE/DELETE 只能按整行匹配定位（存在完全重复行时无法区分是哪一条，"
                                        + "引擎默认限量成只影响一行）。建议加主键或唯一索引（" + noPk.size() + " 个）",
                                String.join(", ", noPk)));
                checks.add(checkOracleSupplementalLog(src));
            }

            checks.add(missingCols.isEmpty()
                    ? check("列处理引用列", "PASS", "列过滤/映射引用的源列均存在", null)
                    : check("列处理引用列", "FAIL",
                            "列处理引用了不存在的源列（" + missingCols.size() + " 个）", String.join(", ", missingCols)));
        } catch (Exception e) {
            checks.add(check("源库 schema 检查", "FAIL", "连接源库失败: " + e.getMessage(), null));
        }
        return checks;
    }

    /**
     * 最小补充日志。没有它，LogMiner 的 UPDATE/DELETE 记录里没有行标识，
     * 增量表现为"任务健康、位点在推进、目标端一行不动"——是 Oracle 源最难查的一类故障。
     */
    private Map<String, Object> checkOracleSupplementalLog(Connection conn) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT supplemental_log_data_min FROM v$database");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                String v = rs.getString(1);
                if (v != null && !"NO".equalsIgnoreCase(v)) {
                    return check("补充日志", "PASS", "supplemental_log_data_min=" + v, null);
                }
                return check("补充日志", "FAIL",
                        "源库未开启最小补充日志（supplemental_log_data_min=NO）：LogMiner 的 UPDATE/DELETE "
                                + "记录里不带行标识，增量会表现为「位点一直推进、目标端一行不动」",
                        "执行 ALTER DATABASE ADD SUPPLEMENTAL LOG DATA; 后重试");
            }
        } catch (Exception e) {
            return check("补充日志", "WARNING", "无法查询补充日志状态（需要 v$database 读权限）: " + e.getMessage(), null);
        }
        return check("补充日志", "WARNING", "无法确定补充日志状态", null);
    }

    private Connection openOracleConn(String connStr) throws Exception {
        // oracle://user:pass@host:port/service
        String rest = connStr.substring("oracle://".length());
        int at = rest.indexOf('@');
        String[] up = rest.substring(0, at).split(":", 2);
        String hostPortService = rest.substring(at + 1);
        String service = hostPortService.contains("/")
                ? hostPortService.substring(hostPortService.indexOf('/') + 1) : "ORCL";
        String hostPort = hostPortService.contains("/")
                ? hostPortService.substring(0, hostPortService.indexOf('/')) : hostPortService;
        return DriverManager.getConnection("jdbc:oracle:thin:@" + hostPort + "/" + service,
                up[0], up.length > 1 ? up[1] : "");
    }

    private boolean oracleTableExists(Connection c, String owner, String table) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM all_tables WHERE owner = ? AND table_name = ?")) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean oracleHasPrimaryKey(Connection c, String owner, String table) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM all_constraints WHERE owner = ? AND table_name = ? "
                        + "AND constraint_type = 'P' AND status = 'ENABLED'")) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private java.util.Set<String> oracleTableColumns(Connection c, String owner, String table) throws Exception {
        java.util.Set<String> cols = new java.util.HashSet<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT column_name FROM all_tab_columns WHERE owner = ? AND table_name = ?")) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1).toUpperCase());
            }
        }
        return cols;
    }

    /**
     * MongoDB 源的对象级预检。
     *
     * <p>关系库那套"主键 / 列存在性"在这里没有对应物（{@code _id} 必然存在、文档无固定列），
     * 所以判据换成 Mongo 自己会炸的那几条：
     *
     * <ul>
     *   <li><b>副本集</b>：Change Streams 只在副本集/分片集群可用。单机 mongod 上增量任务
     *       起得来但一条变更都收不到。</li>
     *   <li><b>集合存在性</b>与<b>集合类型</b>：视图（view）与 capped 集合都不能作为同步源，
     *       但表现各不相同——视图直接读不出 change stream，capped 集合会在写满回卷时丢事件。</li>
     *   <li><b>oplog 窗口</b>：窗口太短时，全量搬运还没跑完 resume token 就已经滚出窗口，
     *       增量接不上只能重做全量。这条只有事后才发现，所以要在启动前量一次。</li>
     * </ul>
     */
    private List<Map<String, Object>> mongoSchemaPrecheck(Workflow workflow, String srcConn,
                                                          List<DbEntry> entries,
                                                          List<Map<String, Object>> checks) {
        String mode = workflow.getMigrationMode();
        boolean needsIncrement = mode != null
                && (mode.toLowerCase().contains("incre") || mode.equalsIgnoreCase("subscribe"));
        com.mongodb.client.MongoClient client = null;
        try {
            client = com.mongodb.client.MongoClients.create(srcConn);
            org.bson.Document hello = client.getDatabase("admin")
                    .runCommand(new org.bson.Document("hello", 1));

            if (needsIncrement) {
                boolean replicaSet = hello.get("setName") != null || hello.getBoolean("isreplicaset", false)
                        || "isdbgrid".equals(hello.getString("msg"));
                checks.add(replicaSet
                        ? check("副本集/分片集群", "PASS",
                                "源端是副本集或分片集群（setName=" + hello.getString("setName") + "），Change Streams 可用", null)
                        : check("副本集/分片集群", "FAIL",
                                "源端不是副本集/分片集群：Change Streams 不可用，增量任务会起得来但一条变更都收不到",
                                "把源端 mongod 配成副本集（哪怕单节点 rs.initiate()）后重试"));
            }

            List<String> missing = new ArrayList<>();
            List<String> notCollections = new ArrayList<>();
            for (DbEntry de : entries) {
                com.mongodb.client.MongoDatabase db = client.getDatabase(de.sourceDb);
                Map<String, String> types = new java.util.HashMap<>();
                for (org.bson.Document d : db.listCollections()) {
                    types.put(d.getString("name"), d.getString("type"));
                }
                if (types.isEmpty() && !de.dbLevel && !de.tables.isEmpty()) {
                    missing.add("库 " + de.sourceDb);
                    continue;
                }
                if (de.dbLevel) continue;
                for (String t : de.tables) {
                    if (!types.containsKey(t)) {
                        missing.add(de.sourceDb + "." + t);
                    } else if (types.get(t) != null && !"collection".equals(types.get(t))) {
                        notCollections.add(de.sourceDb + "." + t + "(" + types.get(t) + ")");
                    }
                }
            }
            checks.add(missing.isEmpty()
                    ? check("源库对象存在性", "PASS", "所有同步集合均存在于源库", null)
                    : check("源库对象存在性", "FAIL",
                            "源库不存在以下集合（" + missing.size() + " 个）", String.join(", ", missing)));
            checks.add(notCollections.isEmpty()
                    ? check("集合类型", "PASS", "同步对象均为普通集合", null)
                    : check("集合类型", "FAIL",
                            "以下对象不是普通集合（视图不产生 change stream，无法作为同步源）（"
                                    + notCollections.size() + " 个）", String.join(", ", notCollections)));

            if (needsIncrement) {
                checks.add(checkMongoOplogWindow(client));
            }
        } catch (Exception e) {
            checks.add(check("源库 schema 检查", "FAIL", "连接源库失败: " + e.getMessage(), null));
        } finally {
            if (client != null) {
                try { client.close(); } catch (Exception ignored) { }
            }
        }
        return checks;
    }

    /** oplog 窗口：小于 1 小时时全量还没搬完 resume token 就可能已经滚出去了。 */
    private Map<String, Object> checkMongoOplogWindow(com.mongodb.client.MongoClient client) {
        try {
            com.mongodb.client.MongoCollection<org.bson.Document> oplog =
                    client.getDatabase("local").getCollection("oplog.rs");
            org.bson.Document first = oplog.find().sort(new org.bson.Document("$natural", 1)).first();
            org.bson.Document last = oplog.find().sort(new org.bson.Document("$natural", -1)).first();
            if (first == null || last == null) {
                return check("oplog 窗口", "WARNING", "无法读取 oplog（需要 local.oplog.rs 读权限）", null);
            }
            org.bson.BsonTimestamp t0 = first.get("ts", org.bson.BsonTimestamp.class);
            org.bson.BsonTimestamp t1 = last.get("ts", org.bson.BsonTimestamp.class);
            long windowSec = (long) t1.getTime() - t0.getTime();
            String human = (windowSec / 3600) + "h" + ((windowSec % 3600) / 60) + "m";
            if (windowSec >= 3600) {
                return check("oplog 窗口", "PASS", "oplog 覆盖约 " + human, null);
            }
            return check("oplog 窗口", "WARNING",
                    "oplog 窗口只有约 " + human + "：全量搬运耗时超过这个窗口时，"
                            + "resume token 会滚出 oplog，增量接不上只能重做全量",
                    "调大 replSetResizeOplog 的 size，或先在业务低峰期跑全量");
        } catch (Exception e) {
            return check("oplog 窗口", "WARNING", "无法评估 oplog 窗口: " + e.getMessage(), null);
        }
    }

    private Connection openPgConn(String connStr) throws Exception {
        String url = connStr.replace("postgresql://", "");
        int at = url.indexOf('@');
        String[] up = url.substring(0, at).split(":", 2);
        String hostDb = url.substring(at + 1);
        if (!hostDb.contains("/")) {
            hostDb = hostDb + "/postgres";
        }
        return DriverManager.getConnection("jdbc:postgresql://" + hostDb, up[0], up.length > 1 ? up[1] : "");
    }

    private boolean pgSchemaExists(Connection c, String schema) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean pgTableExists(Connection c, String schema, String table) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean pgHasPrimaryKey(Connection c, String schema, String table) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid "
                        + "JOIN pg_namespace n ON n.oid = t.relnamespace "
                        + "WHERE n.nspname = ? AND t.relname = ? AND i.indisprimary LIMIT 1")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    /** relreplident: d=default(主键) / f=full / i=index / n=nothing */
    private String pgReplicaIdentity(Connection c, String schema, String table) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT t.relreplident FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace "
                        + "WHERE n.nspname = ? AND t.relname = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? rs.getString(1) : "d"; }
        }
    }

    private java.util.Set<String> pgTableColumns(Connection c, String schema, String table) throws Exception {
        java.util.Set<String> cols = new java.util.HashSet<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1).toLowerCase());
            }
        }
        return cols;
    }

    /** 单库同步 entry（预检用）。tables 为空 + dbLevel=true 表示整库同步。 */
    private static final class DbEntry {
        String sourceDb;
        String targetDb;
        boolean dbLevel;
        List<String> tables = new ArrayList<>();
        Map<String, String> tableMapping = new HashMap<>();   // src表 -> tgt表
        // 表 -> 引用的源列集合（columnFilter 的 column、columnMapping 的 key）
        Map<String, java.util.Set<String>> referencedColumns = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    private List<DbEntry> parseSyncEntries(Workflow workflow) {
        List<DbEntry> out = new ArrayList<>();
        Map<String, Object> raw = PRECHECK_GSON.fromJson(workflow.getSyncObjects(), Map.class);
        if (raw == null) return out;
        String globalTargetDb = workflow.getTargetDbName();
        for (Map.Entry<String, Object> e : raw.entrySet()) {
            DbEntry de = new DbEntry();
            de.sourceDb = e.getKey();
            Object v = e.getValue();
            if (!(v instanceof Map)) {
                if (v instanceof List) for (Object t : (List<?>) v) de.tables.add(String.valueOf(t));
                de.targetDb = globalTargetDb != null ? globalTargetDb : de.sourceDb;
                out.add(de);
                continue;
            }
            Map<String, Object> m = (Map<String, Object>) v;
            Object td = m.get("targetDb");
            de.targetDb = (td instanceof String && !((String) td).isEmpty()) ? (String) td
                    : (globalTargetDb != null && !globalTargetDb.isEmpty() ? globalTargetDb : de.sourceDb);
            de.dbLevel = Boolean.TRUE.equals(m.get("dbLevel"));
            Object tables = m.get("tables");
            if (tables instanceof List) for (Object t : (List<?>) tables) de.tables.add(String.valueOf(t));
            Object tm = m.get("tableMapping");
            if (tm instanceof Map) ((Map<String, Object>) tm).forEach((k, val) -> de.tableMapping.put(k, String.valueOf(val)));
            // 收集列处理引用的源列
            Object cf = m.get("columnFilter");
            if (cf instanceof Map) ((Map<String, Object>) cf).forEach((table, items) -> {
                if (items instanceof List) for (Object it : (List<?>) items) {
                    if (it instanceof Map) {
                        Object col = ((Map<?, ?>) it).get("column");
                        if (col != null) de.referencedColumns.computeIfAbsent(table, x -> new java.util.HashSet<>()).add(String.valueOf(col));
                    }
                }
            });
            Object cm = m.get("columnMapping");
            if (cm instanceof Map) ((Map<String, Object>) cm).forEach((table, mp) -> {
                if (mp instanceof Map) ((Map<?, ?>) mp).keySet().forEach(srcCol ->
                        de.referencedColumns.computeIfAbsent(table, x -> new java.util.HashSet<>()).add(String.valueOf(srcCol)));
            });
            out.add(de);
        }
        return out;
    }

    private Map<String, Object> checkSourceObjectsExist(Connection src, List<DbEntry> entries) throws Exception {
        List<String> missing = new ArrayList<>();
        for (DbEntry de : entries) {
            if (!schemaExists(src, de.sourceDb)) {
                missing.add("库 " + de.sourceDb);
                continue;
            }
            if (de.dbLevel) continue; // 整库同步，不逐表校验
            for (String t : de.tables) {
                if (!tableExists(src, de.sourceDb, t)) missing.add(de.sourceDb + "." + t);
            }
        }
        if (missing.isEmpty()) {
            return check("源库对象存在性", "PASS", "所有同步对象均存在于源库", null);
        }
        return check("源库对象存在性", "FAIL",
                "源库不存在以下对象（" + missing.size() + " 个）", String.join(", ", missing));
    }

    private Map<String, Object> checkPrimaryKeys(Connection src, List<DbEntry> entries) throws Exception {
        List<String> noPk = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.dbLevel || !schemaExists(src, de.sourceDb)) continue;
            for (String t : de.tables) {
                if (tableExists(src, de.sourceDb, t) && !hasPrimaryKey(src, de.sourceDb, t)) {
                    noPk.add(de.sourceDb + "." + t);
                }
            }
        }
        if (noPk.isEmpty()) {
            return check("增量主键", "PASS", "增量同步的表均有主键", null);
        }
        // 从 WARNING 升级为 FAIL：无主键表的增量 UPDATE/DELETE 只能按整行前镜像定位，
        // 而无主键表允许完全重复的行——源端删 1 条，目标端会把所有重复行一起删掉（实测源剩 2/目标剩 0）。
        // 引擎侧已默认限量成"只影响一行"（increment.nopk.row.match=LIMIT_ONE），
        // 但"删哪一条"仍然是不确定的，行序也无法保证，所以这依旧是需要人明确知情并确认的事，
        // 不该是一条划过去就没了的黄字。
        return check("增量主键", "FAIL",
                "以下表无主键，增量 UPDATE/DELETE 只能按整行匹配定位（存在完全重复行时无法区分是哪一条，"
                        + "引擎默认限量成只影响一行）。建议加主键或唯一索引；确需继续请强制启动并知悉风险（"
                        + noPk.size() + " 个）",
                String.join(", ", noPk));
    }

    /**
     * TiDB 增量的可复制性检查。与 MySQL 源的差别在于严重级别：MySQL 的 binlog 带完整行镜像，
     * 无主键表还能靠全列 WHERE 兜底（WARNING）；而 TiCDC 直接<b>拒绝复制</b>既无主键也无
     * NOT NULL 唯一索引的表，这类表的增量变更一条都不会到达目标端，因此按阻断项报。
     */
    private Map<String, Object> checkTidbReplicableKeys(Connection src, List<DbEntry> entries) throws Exception {
        List<String> notReplicable = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.dbLevel || !schemaExists(src, de.sourceDb)) continue;
            for (String t : de.tables) {
                if (!tableExists(src, de.sourceDb, t)) continue;
                if (!hasPrimaryKey(src, de.sourceDb, t) && !hasNotNullUniqueIndex(src, de.sourceDb, t)) {
                    notReplicable.add(de.sourceDb + "." + t);
                }
            }
        }
        if (notReplicable.isEmpty()) {
            return check("增量可复制性", "PASS", "增量同步的表均有主键或非空唯一索引，TiCDC 可复制", null);
        }
        return check("增量可复制性", "FAIL",
                "以下表既无主键也无非空唯一索引，TiCDC 不会复制其变更（" + notReplicable.size() + " 个）",
                String.join(", ", notReplicable));
    }

    /** 表上是否存在全部列均 NOT NULL 的唯一索引（TiCDC 认可的复制键之一）。 */
    private boolean hasNotNullUniqueIndex(Connection conn, String schema, String table) throws Exception {
        String sql = "SELECT s.INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS s "
                + "JOIN INFORMATION_SCHEMA.COLUMNS c ON c.TABLE_SCHEMA = s.TABLE_SCHEMA "
                + " AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME "
                + "WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 0 "
                + "GROUP BY s.INDEX_NAME "
                + "HAVING SUM(CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END) = 0";
        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private Map<String, Object> checkColumnRefs(Connection src, List<DbEntry> entries) throws Exception {
        List<String> missing = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.referencedColumns.isEmpty() || !schemaExists(src, de.sourceDb)) continue;
            for (Map.Entry<String, java.util.Set<String>> te : de.referencedColumns.entrySet()) {
                String table = te.getKey();
                if (!tableExists(src, de.sourceDb, table)) continue; // 表不存在已由存在性检查覆盖
                java.util.Set<String> cols = tableColumns(src, de.sourceDb, table);
                for (String ref : te.getValue()) {
                    if (!cols.contains(ref.toLowerCase())) {
                        missing.add(de.sourceDb + "." + table + "." + ref);
                    }
                }
            }
        }
        if (missing.isEmpty()) {
            return check("列处理引用列", "PASS", "列过滤/映射引用的源列均存在", null);
        }
        return check("列处理引用列", "FAIL",
                "列处理引用了不存在的源列（" + missing.size() + " 个）", String.join(", ", missing));
    }

    /**
     * 约束完整性检查（对象级）：对已选表，若其外键父表不在同步范围内，
     * 增量/全量写入子表时可能因外键约束失败——提示（WARNING）用户把父表纳入同步范围。
     * 整库同步（dbLevel）的库视为整库在范围内，其库内表引用同库父表不告警。
     */
    private Map<String, Object> checkForeignKeyIntegrity(Connection src, List<DbEntry> entries) throws Exception {
        List<String[]> selected = new ArrayList<>();
        Map<String, java.util.Set<String>> inScope = new HashMap<>();
        java.util.Set<String> dbLevelSchemas = new java.util.HashSet<>();
        for (DbEntry de : entries) {
            if (de.dbLevel) { dbLevelSchemas.add(de.sourceDb); continue; }
            java.util.Set<String> set = inScope.computeIfAbsent(de.sourceDb, x -> new java.util.HashSet<>());
            for (String t : de.tables) {
                set.add(t.toLowerCase());
                if (tableExists(src, de.sourceDb, t)) selected.add(new String[]{de.sourceDb, t});
            }
        }
        if (selected.isEmpty()) {
            return check("约束完整性", "PASS", "无需检查外键约束（无显式选表）", null);
        }
        List<String> oos = findOutOfScopeForeignKeys(src, selected, inScope, dbLevelSchemas);
        if (oos.isEmpty()) {
            return check("约束完整性", "PASS", "已选表的外键父表均在同步范围内", null);
        }
        return check("约束完整性", "WARNING",
                "以下外键的父表不在同步范围内，可能导致外键约束失败（" + oos.size() + " 个）",
                String.join(", ", oos));
    }

    /**
     * 对每张已选源表查其外键父表；父表既不属于整库同步的库、也不在已选表集合中，则判为"越界外键"。
     * 抽成包级静态方法便于直接对 Connection 做单元测试。
     */
    static List<String> findOutOfScopeForeignKeys(Connection src, List<String[]> selectedTables,
            Map<String, java.util.Set<String>> inScopeTables,
            java.util.Set<String> dbLevelSchemas) throws Exception {
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = src.prepareStatement(
                "SELECT DISTINCT referenced_table_schema, referenced_table_name " +
                "FROM information_schema.key_column_usage " +
                "WHERE table_schema = ? AND table_name = ? AND referenced_table_name IS NOT NULL")) {
            for (String[] st : selectedTables) {
                ps.setString(1, st[0]);
                ps.setString(2, st[1]);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String refSchema = rs.getString(1);
                        String refTable = rs.getString(2);
                        boolean inScope = dbLevelSchemas.contains(refSchema)
                                || inScopeTables.getOrDefault(refSchema, java.util.Collections.emptySet())
                                        .contains(refTable == null ? null : refTable.toLowerCase());
                        if (!inScope) {
                            out.add(st[0] + "." + st[1] + " → " + refSchema + "." + refTable);
                        }
                    }
                }
            }
        }
        return out;
    }

    private Map<String, Object> checkTargetConflicts(Connection tgt, List<DbEntry> entries, Workflow workflow) throws Exception {
        List<String> existing = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.dbLevel) continue;
            if (!schemaExists(tgt, de.targetDb)) continue; // 目标库不存在→全量会建，无冲突
            for (String t : de.tables) {
                String tgtTable = de.tableMapping.getOrDefault(t, t);
                if (tableExists(tgt, de.targetDb, tgtTable)) {
                    existing.add(de.targetDb + "." + tgtTable);
                }
            }
        }
        if (existing.isEmpty()) {
            return check("目标表冲突", "PASS", "目标库无同名表，全量将新建", null);
        }
        return check("目标表冲突", "WARNING",
                "目标库已存在同名表，全量同步可能产生重复或冲突数据，请确认（" + existing.size() + " 个）",
                String.join(", ", existing));
    }

    /**
     * 目标端存在源端没有的唯一索引。
     *
     * <p>这条比它看起来严重得多，而且<b>只能在这里拦</b>。MySQL 目标的增量 INSERT 是
     * {@code INSERT ... ON DUPLICATE KEY UPDATE 全部列}，撞上一个源端没有的唯一索引时它
     * <b>不报错</b>——而是把那条冲突的旧行整行改掉，<b>连主键一起改成新行的主键</b>。
     *
     * <p>实测：目标表加 {@code UNIQUE(email)} 后，源端插入一条 email 重复的新行（id=99991），
     * 目标端原来的 id=0 那一行直接变成了 id=99991——<b>一条语句毁掉一行、又把两行并成一行</b>，
     * 全程没有任何错误或告警。PG 目标则是 {@code ON CONFLICT (pk) DO NOTHING} 兜不住唯一约束，
     * 抛异常后被"重复键忽略"吞掉（那条已由 E3017 修）。
     *
     * <p>运行期分辨不了"主键冲突（幂等重放，该忽略）"与"唯一键冲突（该停）"——
     * MySQL 的 upsert 两种情况都返回成功。所以必须在启动前把结构差异摆出来。
     */
    private Map<String, Object> checkTargetUniqueIndexes(Connection src, Connection tgt,
                                                         List<DbEntry> entries) throws Exception {
        List<String> extra = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.dbLevel || !schemaExists(tgt, de.targetDb)) continue;
            for (String t : de.tables) {
                String tgtTable = de.tableMapping.getOrDefault(t, t);
                if (!tableExists(tgt, de.targetDb, tgtTable) || !tableExists(src, de.sourceDb, t)) continue;
                java.util.Set<String> srcKeys = uniqueIndexSignatures(src, de.sourceDb, t);
                for (Map.Entry<String, String> e : uniqueIndexColumns(tgt, de.targetDb, tgtTable).entrySet()) {
                    if (!srcKeys.contains(e.getValue())) {
                        extra.add(de.targetDb + "." + tgtTable + "." + e.getKey() + "(" + e.getValue() + ")");
                    }
                }
            }
        }
        if (extra.isEmpty()) {
            return check("目标唯一索引", "PASS", "目标端没有源端不存在的唯一索引", null);
        }
        return check("目标唯一索引", "FAIL",
                "目标端存在源端没有的唯一索引：增量 upsert 撞上它时不会报错，"
                        + "而是把冲突的旧行整行改掉（连主键一起改），等于毁掉一行又把两行并成一行（"
                        + extra.size() + " 个）",
                String.join(", ", extra));
    }

    /** 表上所有唯一索引（不含主键）的列签名集合，用于两端对比。 */
    private java.util.Set<String> uniqueIndexSignatures(Connection conn, String schema, String table) throws Exception {
        return new java.util.HashSet<>(uniqueIndexColumns(conn, schema, table).values());
    }

    /** 索引名 → 列签名（按 seq_in_index 排序后小写逗号拼接）。 */
    private Map<String, String> uniqueIndexColumns(Connection conn, String schema, String table) throws Exception {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT index_name, GROUP_CONCAT(LOWER(column_name) ORDER BY seq_in_index) cols "
                        + "FROM information_schema.statistics "
                        + "WHERE table_schema = ? AND table_name = ? AND non_unique = 0 "
                        + "AND index_name <> 'PRIMARY' GROUP BY index_name")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString("index_name"), rs.getString("cols"));
                }
            }
        }
        return out;
    }

    // ---- information_schema 查询辅助（标识符经参数化，避免注入与 LIKE 通配符误匹配）----
    private boolean schemaExists(Connection conn, String schema) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean tableExists(Connection conn, String schema, String table) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean hasPrimaryKey(Connection conn, String schema, String table) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? " +
                "AND index_name = 'PRIMARY' LIMIT 1")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private java.util.Set<String> tableColumns(Connection conn, String schema, String table) throws Exception {
        java.util.Set<String> cols = new java.util.HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1).toLowerCase());
            }
        }
        return cols;
    }

    private Connection openConn(String connectionStr) throws Exception {
        String[] parsed = parseConnectionUrl(connectionStr);
        return DriverManager.getConnection(parsed[0], parsed[1], parsed[2]);
    }

    private Map<String, Object> check(String name, String status, String message, String detail) {
        Map<String, Object> m = new HashMap<>();
        m.put("checkName", name);
        m.put("status", status);
        m.put("message", message);
        if (detail != null) m.put("detail", detail);
        return m;
    }

    private Map<String, Object> summarize(Map<String, Object> result, List<Map<String, Object>> checks, Workflow workflow) {
        int passed = 0, failed = 0, warnings = 0;
        for (Map<String, Object> c : checks) {
            String s = (String) c.get("status");
            if ("PASS".equals(s)) passed++; else if ("FAIL".equals(s)) failed++; else warnings++;
        }
        result.put("checks", checks);
        result.put("total", checks.size());
        result.put("passed", passed);
        result.put("failed", failed);
        result.put("warnings", warnings);
        result.put("overall", failed > 0 ? "FAIL" : (warnings > 0 ? "WARNING" : "PASS"));
        result.put("workflowId", workflow.getId());
        result.put("workflowName", workflow.getName());
        return result;
    }

    /**
     * 解析 mysql://user:pass@host:port/db 格式为JDBC连接串
     */
    private String[] parseConnectionUrl(String connStr) {
        // mysql://root:rootpassword@192.168.107.6:3306/test_db1
        String url = connStr.replace("mysql://", "");
        int atIdx = url.indexOf('@');
        String userPass = url.substring(0, atIdx);
        String hostDb = url.substring(atIdx + 1);

        String[] up = userPass.split(":", 2);
        String username = up[0];
        String password = up.length > 1 ? up[1] : "";

        String jdbcUrl = "jdbc:mysql://" + hostDb + "?" + com.synctask.util.JdbcSslOptions.mysql() + "&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";

        return new String[]{jdbcUrl, username, password};
    }
}
