package com.migration.agent.checkpoint;

import com.migration.common.position.CheckpointRecord;
import com.migration.common.position.MonotonicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 位点的中心存储（元数据库 {@code task_checkpoints}）。
 *
 * <p>它存在的唯一理由：位点不能只活在某台 agent 的本地磁盘上。V8 的故障转移假设
 * "接管方走既有的崩溃恢复路径"，而那条路径读的是 {@code files/<taskId>/}——换台机器就是空的，
 * 于是接管方会去取"源库此刻的位点"，把崩溃到接管之间的变更整段跳过（不报错、不告警）。
 *
 * <p><b>写入的两条守卫都压在 SQL 里</b>，不在应用层判断（并发下判不住）：
 * <ol>
 *   <li><b>fencing</b>：低于当前 {@code lease_epoch} 的写入一律拒绝——网络分区下没死透的老 agent
 *       仍在写位点，它的 epoch 一定更低。</li>
 *   <li><b>单调</b>：同一 epoch 内位点不许回退。{@code monotonic_key=0} 表示该形态折不出可比标量
 *       （GTID 集、resume token），守卫对它自动降级为不校验。</li>
 * </ol>
 * 更高的 epoch <b>无条件放行</b>：它是租约的合法新主，位点本来就是从这里回灌走的，
 * 偏旧只意味着多重放（安全方向），拦住它反而会让位点永久卡死。
 */
public class CentralCheckpointStore {

    private static final Logger logger = LoggerFactory.getLogger(CentralCheckpointStore.class);

    /** 写入结果，只为把"被守卫拒绝"与"正常写入"区分开，好计数告警。 */
    public enum WriteResult { ACCEPTED, INSERTED, REJECTED, FAILED }

    private static volatile CentralCheckpointStore instance;

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public CentralCheckpointStore(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public static synchronized CentralCheckpointStore initialize(String dbUrl, String dbUser, String dbPassword) {
        if (instance == null) {
            instance = new CentralCheckpointStore(dbUrl, dbUser, dbPassword);
        }
        return instance;
    }

    /**
     * 未初始化（{@code checkpoint.central.enabled=false} 或单机部署）时返回 null。
     * 调用方一律按"没有中心位点"处理，回到本地位点的老行为，而不是报错。
     */
    public static CentralCheckpointStore getInstance() {
        return instance;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    /**
     * 时间戳一律 JVM 侧绑定，绝不用 SQL 的 {@code NOW()}：元数据库在容器里跑 UTC，
     * 判活/比对都在 JVM 时区，用 NOW() 会差一个时区偏移（V8 的故障转移就栽在这上面）。
     */
    private static Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * 写入一条位点。
     *
     * <p>先条件 UPDATE、失败再 INSERT：这样"被守卫拒绝"与"这行还不存在"能明确区分开，
     * 而单条 {@code ON DUPLICATE KEY UPDATE ... IF(...)} 只能看到一个含糊的 affected rows。
     */
    public WriteResult upsert(CheckpointRecord record, String agentId, int leaseEpoch) {
        String updateSql =
                "UPDATE task_checkpoints SET engine=?, kind=?, payload=?, monotonic_key=?, source_ts=?, " +
                "agent_id=?, lease_epoch=?, updated_at=? " +
                "WHERE task_id=? AND stage=? AND stream_key=? " +
                "  AND (? > lease_epoch OR (? = lease_epoch AND ? >= monotonic_key))";
        String insertSql =
                "INSERT INTO task_checkpoints " +
                "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, source_ts, agent_id, lease_epoch, updated_at) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) " +
                "ON DUPLICATE KEY UPDATE task_id=task_id";   // 撞上并发插入就当没写成，下一拍再来
        long key = MonotonicKey.toColumn(record.getMonotonicKey());
        try (Connection conn = connect()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                int i = 1;
                ps.setString(i++, record.getEngine());
                ps.setString(i++, record.getKind().name());
                ps.setString(i++, record.payloadText());
                ps.setLong(i++, key);
                ps.setTimestamp(i++, record.getSourceTs() > 0 ? new Timestamp(record.getSourceTs()) : null);
                ps.setString(i++, agentId);
                ps.setInt(i++, leaseEpoch);
                ps.setTimestamp(i++, now());
                ps.setString(i++, record.getTaskId());
                ps.setString(i++, record.getStage().name());
                ps.setString(i++, record.getStreamKey());
                ps.setInt(i++, leaseEpoch);
                ps.setInt(i++, leaseEpoch);
                ps.setLong(i, key);
                if (ps.executeUpdate() > 0) {
                    return WriteResult.ACCEPTED;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                int i = 1;
                ps.setString(i++, record.getTaskId());
                ps.setString(i++, record.getStage().name());
                ps.setString(i++, record.getStreamKey());
                ps.setString(i++, record.getEngine());
                ps.setString(i++, record.getKind().name());
                ps.setString(i++, record.payloadText());
                ps.setLong(i++, key);
                ps.setTimestamp(i++, record.getSourceTs() > 0 ? new Timestamp(record.getSourceTs()) : null);
                ps.setString(i++, agentId);
                ps.setInt(i++, leaseEpoch);
                ps.setTimestamp(i, now());
                if (ps.executeUpdate() > 0) {
                    return WriteResult.INSERTED;
                }
            }
            // 走到这里有两种可能，必须查一眼分清楚：
            //   ① 守卫真的拒绝了（epoch 更低 / 位点回退）——要计数告警
            //   ② UPDATE 因为"值一个字节都没变"返回 0 行，而 INSERT 撞了主键
            // 后者取决于驱动怎么算 affected rows（MySQL 驱动默认返回 matched 行，
            // 但 useAffectedRows=true 或换个引擎就不是了），不能拿它当判据去报警。
            return acceptedByGuard(conn, record, leaseEpoch, key)
                    ? WriteResult.ACCEPTED : WriteResult.REJECTED;
        } catch (SQLException e) {
            logger.warn("[{}] 位点上卷失败（本地位点不受影响）: {}", record.getTaskId(), e.getMessage());
            return WriteResult.FAILED;
        }
    }

    /** 当前行是否本来就接受这次写入（用来把"守卫拒绝"与"值没变所以 0 行"区分开）。 */
    private boolean acceptedByGuard(Connection conn, CheckpointRecord record, int leaseEpoch, long key)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT lease_epoch, monotonic_key FROM task_checkpoints " +
                "WHERE task_id=? AND stage=? AND stream_key=?")) {
            ps.setString(1, record.getTaskId());
            ps.setString(2, record.getStage().name());
            ps.setString(3, record.getStreamKey());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
                int currentEpoch = rs.getInt(1);
                long currentKey = rs.getLong(2);
                return leaseEpoch > currentEpoch || (leaseEpoch == currentEpoch && key >= currentKey);
            }
        }
    }

    /** 读取该任务在中心库里的全部位点。元数据库不可达时抛出，由调用方决定是否 fail-stop。 */
    public List<CheckpointRecord> loadAll(String taskId) throws SQLException {
        List<CheckpointRecord> out = new ArrayList<>();
        String sql = "SELECT stage, stream_key, engine, kind, payload, monotonic_key, source_ts, " +
                "updated_at, reset_at FROM task_checkpoints WHERE task_id=?";
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    CheckpointRecord.Stage stage;
                    CheckpointRecord.Kind kind;
                    try {
                        stage = CheckpointRecord.Stage.valueOf(rs.getString("stage"));
                        kind = CheckpointRecord.Kind.valueOf(rs.getString("kind"));
                    } catch (IllegalArgumentException e) {
                        // 更高版本写进来的枚举值：跳过而不是猜，猜错就是拿错位点续传
                        logger.warn("[{}] 中心位点含未知 stage/kind，跳过该行", taskId);
                        continue;
                    }
                    Timestamp sourceTs = rs.getTimestamp("source_ts");
                    Timestamp updatedAt = rs.getTimestamp("updated_at");
                    Timestamp resetAt = rs.getTimestamp("reset_at");
                    out.add(new CheckpointRecord(taskId, stage, rs.getString("stream_key"),
                            rs.getString("engine"), kind,
                            CheckpointRecord.parsePayload(rs.getString("payload")),
                            rs.getLong("monotonic_key"),
                            sourceTs != null ? sourceTs.getTime() : 0L,
                            updatedAt != null ? updatedAt.getTime() : 0L,
                            resetAt != null ? resetAt.getTime() : 0L));
                }
            }
        }
        return out;
    }

    /** 该任务在中心库里有没有位点。用于区分"真·首启"与"接管"——两者的失败处理完全相反。 */
    public boolean hasAny(String taskId) throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM task_checkpoints WHERE task_id=? LIMIT 1")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** 记一条位点历史（采样 / 重置 / 倒换作废 / 全量快照点）。 */
    public void recordHistory(CheckpointRecord record, String reason, String operator) {
        String sql = "INSERT INTO task_checkpoint_history " +
                "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, source_ts, recorded_at, reason, operator) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            ps.setString(i++, record.getTaskId());
            ps.setString(i++, record.getStage().name());
            ps.setString(i++, record.getStreamKey());
            ps.setString(i++, record.getEngine());
            ps.setString(i++, record.getKind().name());
            ps.setString(i++, record.payloadText());
            ps.setLong(i++, MonotonicKey.toColumn(record.getMonotonicKey()));
            ps.setTimestamp(i++, record.getSourceTs() > 0 ? new Timestamp(record.getSourceTs()) : null);
            ps.setTimestamp(i++, now());
            ps.setString(i++, reason);
            ps.setString(i, operator);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.warn("[{}] 位点历史写入失败: {}", record.getTaskId(), e.getMessage());
        }
    }

    /**
     * 作废该任务的中心位点（主备倒换 / 重做全量）。
     *
     * <p><b>这是不能省的一步</b>：倒换后源库已换成原目标实例，旧实例的 GTID/LSN/SCN 在新源上毫无意义
     * ——GTID 尤其致命，服务端会从新源 binlog 最开头整段重放冲垮备库。本地位点早就在清了，
     * 中心位点要是留着，接管方一回灌就把刚清掉的旧位点又请回来。删之前先留一份 history 便于事后对账。
     */
    public void deleteTask(String taskId, String reason) {
        try {
            for (CheckpointRecord r : loadAll(taskId)) {
                recordHistory(r, reason, null);
            }
        } catch (SQLException e) {
            logger.warn("[{}] 作废中心位点前留档失败（继续删除）: {}", taskId, e.getMessage());
        }
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM task_checkpoints WHERE task_id=?")) {
            ps.setString(1, taskId);
            int rows = ps.executeUpdate();
            if (rows > 0) {
                logger.info("[{}] 已作废中心位点 {} 行（{}）", taskId, rows, reason);
            }
        } catch (SQLException e) {
            logger.warn("[{}] 作废中心位点失败: {}", taskId, e.getMessage());
        }
    }

    /** 取任务当前的租约代次，作为 fencing token。查不到按 0 处理（未指派 = 单机语义）。 */
    public int leaseEpoch(String taskId) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT lease_epoch FROM workflows WHERE id=?")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            logger.debug("[{}] 读取 lease_epoch 失败，按 0 处理: {}", taskId, e.getMessage());
        }
        return 0;
    }
}
