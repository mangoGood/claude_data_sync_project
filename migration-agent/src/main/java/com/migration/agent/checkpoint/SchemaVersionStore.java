package com.migration.agent.checkpoint;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 表结构时序库的中心存储读写（表 {@code task_schema_versions}）。
 *
 * <p>时序库本身活在 extract 里（它是唯一算得出版本的人），落盘在
 * {@code files/<taskId>/schema_history.jsonl}。agent 只做两件事：把这个文件<b>增量上传</b>
 * 到中心库，以及跨机接管时把中心库<b>回灌</b>成这个文件。
 *
 * <p><b>payload 对本类是不透明的</b>：一行 jsonl 原样存、原样取回。表结构格式的知识只存在于
 * {@code migration-extract} 一处——agent 只解析 {@code db/tbl/f/p/kind} 这几个键来做行定位。
 * 两边各存一份格式理解迟早会漂，而漂了之后回灌出来的结构是错的、还看不出来。
 *
 * <p>与 {@link CentralCheckpointStore} 一样：未初始化（单机部署 / 中心位点关掉）时
 * {@link #getInstance()} 返回 null，调用方按"只有本地历史"处理，回到单机行为而不是报错。
 */
public class SchemaVersionStore {

    private static final Logger logger = LoggerFactory.getLogger(SchemaVersionStore.class);

    private static volatile SchemaVersionStore instance;

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public SchemaVersionStore(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public static synchronized SchemaVersionStore initialize(String dbUrl, String dbUser, String dbPassword) {
        if (instance == null) {
            instance = new SchemaVersionStore(dbUrl, dbUser, dbPassword);
        }
        return instance;
    }

    public static SchemaVersionStore getInstance() {
        return instance;
    }

    public static synchronized void reset() {
        instance = null;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    /** 本地历史文件路径（与 extract 的 {@code extract.schema.timeline.history.path} 默认值一致）。 */
    public static Path historyPath(String taskId) {
        return Paths.get("./files/" + taskId + "/schema_history.jsonl");
    }

    // ---- 上传 ----

    /**
     * 把本地历史里位点大于 {@code afterKey} 的行传到中心库。
     *
     * @return 实际写入的行数；-1 表示被 fencing 拒绝（整批不写）
     */
    public int uploadSince(String taskId, long afterKey, String agentId, int leaseEpoch) {
        List<String> lines = readLines(historyPath(taskId));
        if (lines.isEmpty()) {
            return 0;
        }

        try (Connection conn = connect()) {
            Integer maxEpoch = maxLeaseEpoch(conn, taskId);
            if (maxEpoch != null && leaseEpoch < maxEpoch) {
                // 没死透的老 agent：它手里的时序库是旧的，写进来就是把错的结构固化下去
                logger.warn("[{}] 表结构版本上传被 fencing 拒绝：本机 epoch={} < 中心 epoch={}",
                        taskId, leaseEpoch, maxEpoch);
                return -1;
            }

            String sql = "INSERT INTO task_schema_versions "
                    + "(task_id, db_name, table_name, binlog_file, binlog_pos, monotonic_key, "
                    + " change_kind, payload, agent_id, lease_epoch, created_at) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                    + "ON DUPLICATE KEY UPDATE id=id";   // 版本一旦写下就不可变，撞唯一键=重放，忽略即可

            int written = 0;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (String line : lines) {
                    Row row = parse(line);
                    if (row == null || row.monotonicKey <= afterKey) {
                        continue;
                    }
                    int i = 1;
                    ps.setString(i++, taskId);
                    ps.setString(i++, row.db);
                    ps.setString(i++, row.table);
                    ps.setString(i++, row.file);
                    ps.setLong(i++, row.pos);
                    ps.setLong(i++, row.monotonicKey);
                    ps.setString(i++, row.kind);
                    ps.setString(i++, line);
                    ps.setString(i++, agentId);
                    ps.setInt(i++, leaseEpoch);
                    ps.setTimestamp(i, now);
                    ps.addBatch();
                    written++;
                }
                if (written > 0) {
                    ps.executeBatch();
                }
            }
            if (written > 0) {
                logger.info("[{}] 上传表结构版本 {} 条（位点 > {}）", taskId, written, afterKey);
            }
            return written;
        } catch (SQLException e) {
            logger.warn("[{}] 上传表结构版本失败: {}", taskId, e.getMessage());
            return 0;
        }
    }

    /** 中心库里该任务最大的版本位点，上传时用它做增量起点。 */
    public long maxUploadedKey(String taskId) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COALESCE(MAX(monotonic_key), -1) FROM task_schema_versions WHERE task_id = ?")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1L;
            }
        } catch (SQLException e) {
            logger.warn("[{}] 读表结构版本最大位点失败: {}", taskId, e.getMessage());
            return -1L;
        }
    }

    // ---- 回灌 ----

    /**
     * 把中心库里的版本写回本地历史文件——<b>必须在 extract 启动之前完成</b>，
     * 否则 extract 会以为自己是首次启动、只拿 capture 的基线当权威。
     *
     * <p>本地已有历史时按"两边合并、按位点去重"处理：接管方本地可能有上一轮残留，
     * 直接覆盖会丢掉中心库还没收到的那几条。
     *
     * @return 写回的行数；-1 表示中心库里没有该任务的任何版本
     */
    public int hydrate(String taskId) {
        List<String> central = new ArrayList<>();
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT payload FROM task_schema_versions WHERE task_id = ? "
                             + "ORDER BY monotonic_key, id")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String payload = rs.getString(1);
                    if (payload != null && !payload.isEmpty()) {
                        central.add(payload);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("[{}] 回灌表结构版本失败: {}", taskId, e.getMessage());
            return -1;
        }
        if (central.isEmpty()) {
            return -1;
        }

        Path path = historyPath(taskId);
        java.util.LinkedHashMap<String, String> merged = new java.util.LinkedHashMap<>();
        for (String line : central) {
            Row r = parse(line);
            if (r != null) {
                merged.put(r.dedupKey(), line);
            }
        }
        for (String line : readLines(path)) {
            Row r = parse(line);
            if (r != null) {
                merged.putIfAbsent(r.dedupKey(), line);
            }
        }

        List<String> out = new ArrayList<>(merged.values());
        out.sort((a, b) -> {
            Row ra = parse(a);
            Row rb = parse(b);
            long ka = ra == null ? 0 : ra.monotonicKey;
            long kb = rb == null ? 0 : rb.monotonicKey;
            return Long.compare(ka, kb);
        });

        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".hydrate");
            Files.write(tmp, out, StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            logger.error("[{}] 写回本地表结构历史失败: {}", taskId, e.getMessage());
            return -1;
        }
        logger.info("[{}] 表结构时序库已回灌：中心 {} 条，合并后 {} 条", taskId, central.size(), out.size());
        return out.size();
    }

    /** 中心库里有没有这个任务的时序库——决定接管时是"回灌"还是"没有可回灌的"。 */
    public boolean hasVersions(String taskId) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM task_schema_versions WHERE task_id = ? LIMIT 1")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            logger.warn("[{}] 查表结构版本失败: {}", taskId, e.getMessage());
            return false;
        }
    }

    // ---- 清理 ----

    /**
     * 作废该任务的全部版本。
     *
     * <p>由 {@link CheckpointCleaner} 在位点全作废时调用。位点作废意味着任务会从一个
     * <b>新的、更靠后的</b>位点重启，而时序库最新版本停在很久以前——中间那段的 DDL 它一条都
     * 没见过。留着会被当权威用，正是要防的静默错，所以必须一起清掉、重新打基线。
     *
     * <p>反过来，PITR 把位点往<b>前</b>调（{@code reset_at}）时不能清：时序库已经覆盖那段范围，
     * 信息比重新打一份基线更全，重放时同位点版本按唯一键幂等重入。
     */
    public void deleteTask(String taskId, String reason) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "DELETE FROM task_schema_versions WHERE task_id = ?")) {
            ps.setString(1, taskId);
            int n = ps.executeUpdate();
            if (n > 0) {
                logger.info("[{}] 已作废中心表结构版本 {} 行（{}）", taskId, n, reason);
            }
        } catch (SQLException e) {
            logger.warn("[{}] 作废中心表结构版本失败: {}", taskId, e.getMessage());
        }
    }

    /**
     * 裁剪：删掉位点低于 {@code minKey} 的版本，<b>但每张表要留住 ≤ minKey 的最后一个</b>——
     * 它是回答该位点查询的唯一依据，删了从那里重放的第一个事件就查不到结构。
     */
    public int pruneBelow(String taskId, long minKey) {
        String sql = "DELETE v FROM task_schema_versions v "
                + "JOIN (SELECT db_name, table_name, MAX(monotonic_key) AS keep "
                + "        FROM task_schema_versions WHERE task_id = ? AND monotonic_key <= ? "
                + "       GROUP BY db_name, table_name) k "
                + "  ON v.db_name = k.db_name AND v.table_name = k.table_name "
                + "WHERE v.task_id = ? AND v.monotonic_key < k.keep";
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setLong(2, minKey);
            ps.setString(3, taskId);
            int n = ps.executeUpdate();
            if (n > 0) {
                logger.info("[{}] 裁剪表结构版本 {} 行（位点 < 各表在 {} 处的生效版本）", taskId, n, minKey);
            }
            return n;
        } catch (SQLException e) {
            logger.warn("[{}] 裁剪表结构版本失败: {}", taskId, e.getMessage());
            return 0;
        }
    }

    // ---- 工具 ----

    private Integer maxLeaseEpoch(Connection conn, String taskId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT MAX(lease_epoch) FROM task_schema_versions WHERE task_id = ?")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int v = rs.getInt(1);
                    return rs.wasNull() ? null : v;
                }
            }
        }
        return null;
    }

    private static List<String> readLines(Path path) {
        if (!Files.isRegularFile(path)) {
            return List.of();
        }
        try {
            return Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.warn("读本地表结构历史失败 {}: {}", path, e.getMessage());
            return List.of();
        }
    }

    /** jsonl 行里 agent 需要认识的那几个键。payload 本体保持不透明。 */
    static class Row {
        String db;
        String table;
        String file;
        long pos;
        long monotonicKey;
        String kind;

        String dedupKey() {
            return db + "." + table + "#" + monotonicKey;
        }
    }

    static Row parse(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        try {
            JsonObject o = JsonParser.parseString(line).getAsJsonObject();
            Row r = new Row();
            r.db = o.has("db") && !o.get("db").isJsonNull()
                    ? o.get("db").getAsString().toLowerCase(Locale.ROOT) : "";
            r.table = o.has("tbl") && !o.get("tbl").isJsonNull()
                    ? o.get("tbl").getAsString().toLowerCase(Locale.ROOT) : "";
            r.file = o.has("f") && !o.get("f").isJsonNull() ? o.get("f").getAsString() : "";
            r.pos = o.has("p") ? o.get("p").getAsLong() : 0L;
            r.kind = o.has("kind") ? o.get("kind").getAsString() : "ALTERED";
            r.monotonicKey = monotonicKey(r.file, r.pos);
            return r.table.isEmpty() ? null : r;
        } catch (Exception e) {
            // 崩溃留下的半条：跳过，别让一行坏数据挡住整批上传
            return null;
        }
    }

    /**
     * 与 {@code SchemaTimeline.monotonicKey} 必须完全一致。
     *
     * <p>刻意复制一份而不是共享代码：这是 agent 与 extract 之间唯一的格式约定，
     * 放进 migration-common 会让"改 common 后 fat jar 必须 clean install"那个坑
     * 波及到这条链路上的每一次改动。两处都有单测钉住同一组样例。
     */
    static long monotonicKey(String binlogFile, long pos) {
        long fileNo = 0;
        if (binlogFile != null && !binlogFile.isEmpty()) {
            int end = binlogFile.length();
            int start = end;
            while (start > 0 && Character.isDigit(binlogFile.charAt(start - 1))) {
                start--;
            }
            if (start < end) {
                try {
                    fileNo = Long.parseLong(binlogFile.substring(start, end));
                } catch (NumberFormatException ignored) {
                    fileNo = 0;
                }
            }
        }
        return (fileNo << 32) | (pos & 0xFFFFFFFFL);
    }
}
