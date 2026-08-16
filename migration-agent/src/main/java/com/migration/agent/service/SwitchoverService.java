package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 计划内主备切换（Switchover）的"停写 → 追平"两步。
 *
 * <p>此前平台只有一种倒换：交换连接串 → 清位点与中间态 → 重新拉起。它既不停旧主的写、
 * 也不等链路追平，而 {@code FailoverService.cleanFailoverFiles()} 还会把 {@code thl_output/}
 * 整个清掉——<b>已捕获但尚未应用的变更就此消失</b>。计划外接管这么做是 RPO 的固有代价，
 * 计划内切换这么做就是白丢数据。
 *
 * <p>这个类只负责把"能不能安全切"这件事判出来，判完了才轮到原来那套倒换动作：
 *
 * <ol>
 *   <li><b>停写</b>：把旧主置为只读（MySQL {@code SET GLOBAL read_only}，
 *       PG {@code ALTER SYSTEM SET default_transaction_read_only}）。拿不到权限就<b>如实报错</b>，
 *       让人自己停业务再来——绝不"装作停了"继续往下走。</li>
 *   <li><b>记界</b>：停写之后读一次源端当前位点。这个位点就是"最后一笔业务写入"的上界。</li>
 *   <li><b>追平</b>：等 capture 位点越过这个上界，并且 THL 里没有未应用的事件
 *       （{@code pending_events == 0}）。</li>
 * </ol>
 *
 * <p>超时没追平就<b>解除只读并放弃切换</b>：宁可这次切不过去，也不能切过去之后才发现丢了数据。
 * 切换成功时旧主<b>保持只读</b>——它现在是备库，让它继续接受业务写入才是真正的危险
 * （两端各写各的，双写分叉）。
 */
public class SwitchoverService {

    private static final Logger logger = LoggerFactory.getLogger(SwitchoverService.class);

    private final CheckpointVisualizationService checkpointView = new CheckpointVisualizationService();

    /** 追平判定结果。{@code ok=false} 时调用方必须放弃切换。 */
    public static class DrainResult {
        public final boolean ok;
        public final String reason;
        public final Map<String, Object> details = new LinkedHashMap<>();

        DrainResult(boolean ok, String reason) {
            this.ok = ok;
            this.reason = reason;
        }
    }

    /**
     * 停写 + 等追平。
     *
     * @param taskId    任务
     * @param timeoutMs 追平超时；超时即解除只读并放弃
     * @param fence     是否真的去停旧主的写。没有权限的环境可以传 false，
     *                  由人工先停业务——但那样"业务是否真的停了"就不再是本方法能保证的了
     */
    public DrainResult drainAndFence(String taskId, long timeoutMs, boolean fence) {
        Properties cfg = loadTaskConfig(taskId);
        if (cfg == null) {
            return new DrainResult(false, "读不到任务配置 files/" + taskId + "/config.properties");
        }
        boolean sourceIsPg = "postgresql".equalsIgnoreCase(cfg.getProperty("source.db.type", "mysql"));
        boolean fenced = false;
        try (Connection src = openSource(cfg, sourceIsPg)) {
            if (fence) {
                String err = setReadOnly(src, sourceIsPg, true);
                if (err != null) {
                    DrainResult r = new DrainResult(false,
                            "无法把旧主置为只读（" + err + "）。请先自行停止业务写入后重试，"
                                    + "或在明确知悉风险的前提下改用计划外接管");
                    r.details.put("fenced", false);
                    return r;
                }
                fenced = true;
                logger.warn("[{}] 旧主已置为只读，开始等待链路追平", taskId);
            }

            String boundary = currentPosition(src, sourceIsPg);
            long deadline = System.currentTimeMillis() + Math.max(1000L, timeoutMs);
            Map<String, Object> last = null;
            while (System.currentTimeMillis() < deadline) {
                last = drainState(taskId);
                // 两把尺子都要过：
                //   1) capture 已经读过了停写那一刻的源端位点（reachedBoundary）
                //   2) 读到的东西已经全部应用完（caughtUp：pending_events==0 且无未应用 THL 文件）
                // 只看 2) 会漏掉最危险的那一段：**capture 还没把最后一批写入读出来**时，
                // THL 里根本没有这些事件，pending_events 自然是 0，于是判成"已追平"就切了，
                // 方向一对调，那批数据永远留在旧主。实测这么丢过 28 行（PG 必现、MySQL 偶发）。
                Boolean reached = captureReachedBoundary(taskId, src, cfg, sourceIsPg, boundary);
                last.put("reachedBoundary", reached);
                last.put("boundary", boundary);
                if (Boolean.TRUE.equals(reached) && Boolean.TRUE.equals(last.get("caughtUp"))) {
                    DrainResult r = new DrainResult(true, "已追平");
                    r.details.putAll(last);
                    r.details.put("fenced", fenced);
                    r.details.put("boundary", boundary);
                    logger.warn("[{}] 链路已追平（界={}），可以安全切换", taskId, boundary);
                    return r;
                }
                Thread.sleep(1000L);
            }

            // 超时：把只读解除，让业务先继续跑——切不过去总比切过去丢数据强
            if (fenced) {
                String err = setReadOnly(src, sourceIsPg, false);
                if (err != null) {
                    logger.error("[{}] 追平超时后解除旧主只读失败，请人工处理: {}", taskId, err);
                }
            }
            String why = (last != null && !Boolean.TRUE.equals(last.get("reachedBoundary")))
                    ? "capture 尚未读到停写位点 " + boundary
                    : "已捕获的变更尚未应用完";
            DrainResult r = new DrainResult(false,
                    "等待追平超时（" + timeoutMs + "ms，" + why + "），已放弃切换并解除旧主只读");
            if (last != null) {
                r.details.putAll(last);
            }
            r.details.put("fenced", false);
            r.details.put("boundary", boundary);
            return r;
        } catch (Exception e) {
            logger.error("[{}] 计划内切换的停写/追平失败", taskId, e);
            return new DrainResult(false, "停写/追平失败: " + e.getMessage());
        }
    }

    /**
     * 当前还差多少没应用。
     *
     * <p>用的就是位点可视化那三段位点：{@code pending_events} = THL 已产出的最新 seqno −
     * 已应用 checkpoint 的 seqno。它为 0 才叫"目标端已经拿到了源端的全部变更"。
     */
    public Map<String, Object> drainState(String taskId) {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> view = checkpointView.getCheckpointVisualization(taskId);
        Object gapsObj = view.get("gaps");
        Long pending = null;
        if (gapsObj instanceof Map) {
            Object p = ((Map<?, ?>) gapsObj).get("pending_events");
            if (p instanceof Number) {
                pending = ((Number) p).longValue();
            }
        }
        out.put("pendingEvents", pending);
        out.put("unappliedThlFiles", unappliedThlFiles(taskId));
        // pending 读不出来（位点文件还没产出）时不能当作"已追平"——那正好是最危险的误判方向
        out.put("caughtUp", pending != null && pending == 0L
                && (Integer) out.get("unappliedThlFiles") == 0);
        return out;
    }

    /**
     * 还有几个 THL 文件没被应用完。
     *
     * <p>{@code .increment_progress} 里每行是 {@code 文件名|已应用到的 seqno}，
     * {@code -1} 表示该文件已整体应用完。有文件不在进度里、或标的不是 -1，就说明还有活没干完。
     * 这一条与 {@code pending_events} 是两把独立的尺子：前者按文件、后者按事件，
     * 任一不为零都不能切。
     */
    private int unappliedThlFiles(String taskId) {
        File thlDir = new File("files/" + taskId + "/thl_output");
        File[] thls = thlDir.listFiles((d, n) -> n.endsWith(".thl"));
        if (thls == null || thls.length == 0) {
            return 0;
        }
        Properties progress = new Properties();
        File progressFile = new File("files/" + taskId + "/checkpoint/.increment_progress");
        Map<String, String> applied = new LinkedHashMap<>();
        if (progressFile.isFile()) {
            try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(progressFile))) {
                String line;
                while ((line = r.readLine()) != null) {
                    String[] parts = line.split("\\|");
                    if (parts.length >= 2) {
                        applied.put(parts[0], parts[1]);
                    }
                }
            } catch (Exception e) {
                logger.warn("[{}] 读增量进度失败，保守判为未追平: {}", taskId, e.getMessage());
                return thls.length;
            }
        }
        int pending = 0;
        for (File f : thls) {
            if (!"-1".equals(applied.get(f.getName()))) {
                pending++;
            }
        }
        // 最新的那个文件永远不会被标 -1（还在写），所以它不算未应用——
        // 它到底追没追平由 pending_events 那把尺子回答
        return Math.max(0, pending - 1);
    }

    // ==================== 源端操作 ====================

    private Connection openSource(Properties cfg, boolean pg) throws Exception {
        String host = cfg.getProperty("source.db.host");
        String port = cfg.getProperty("source.db.port");
        String user = cfg.getProperty("source.db.username");
        // config.properties 里的口令是 AES-GCM 密文（ENC: 前缀），直接拿去连库会 Access denied
        String pass = com.migration.common.crypto.CredentialCipher.decrypt(
                cfg.getProperty("source.db.password"));
        String db = cfg.getProperty("source.db.database", "");
        com.migration.common.ssl.SslMaterial ssl =
                com.migration.common.ssl.SslMaterial.from(cfg, "source");
        String url = pg
                ? "jdbc:postgresql://" + host + ":" + port + "/" + (db.isEmpty() ? "postgres" : db)
                        + "?" + ssl.pgUrlParams()
                : "jdbc:mysql://" + host + ":" + port + "/?" + ssl.mysqlUrlParams()
                        + "&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        return DriverManager.getConnection(url, user, pass);
    }

    /** @return null 表示成功，否则返回失败原因（通常是权限不足） */
    private String setReadOnly(Connection conn, boolean pg, boolean on) {
        try (Statement st = conn.createStatement()) {
            if (!pg) {
                // MySQL 的 SET GLOBAL read_only 会等所有已开启的写事务提交、等元数据锁释放，
                // 默认能等到天荒地老（lock_wait_timeout 默认一年）。这里钉一个短超时，
                // 让它要么很快成功、要么明确报错——切换流程不能挂在一条 SET 上。
                try {
                    st.execute("SET SESSION lock_wait_timeout = 30");
                } catch (Exception ignored) {
                    // 老版本/权限不足拿不到这个会话变量：不影响主流程，只是可能等久一点
                }
            }
            if (pg) {
                st.execute("ALTER SYSTEM SET default_transaction_read_only = " + (on ? "on" : "off"));
                st.execute("SELECT pg_reload_conf()");
            } else {
                st.execute("SET GLOBAL read_only = " + (on ? "ON" : "OFF"));
            }
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * capture 是否已经读过了停写那一刻的源端位点。
     *
     * @return true 已越过；false 还没到；null 判不出来（判不出来一律不算追平——
     *         "读不出来就当作已追平"正是最危险的误判方向）
     */
    private Boolean captureReachedBoundary(String taskId, Connection src, Properties cfg,
                                           boolean pg, String boundary) {
        if (boundary == null || boundary.isEmpty() || "-".equals(boundary)) {
            return null;
        }
        if (pg) {
            Long want = parseLsn(boundary);
            if (want == null) {
                return null;
            }
            // 首选复制槽的 confirmed_flush_lsn：它由解码端的反馈推动，PG 的 keepalive 也会推着它走，
            // 所以停写之后它照样能追到当前 WAL 位点。capture 自己那份位点文件是按事件数落盘的，
            // 停写之后没有新事件就不再刷新，拿它当唯一依据会让切换永远等不到。
            String slot = cfg.getProperty("capture.wal.slot.name",
                    "migration_slot_" + taskId.replaceAll("[^a-z0-9_]", "_"));
            try (java.sql.PreparedStatement ps = src.prepareStatement(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, slot);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Long cur = parseLsn(rs.getString(1));
                        if (cur != null) {
                            return cur >= want;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("[{}] 读复制槽 {} 的 confirmed_flush_lsn 失败: {}", taskId, slot, e.getMessage());
            }
            Long cur = parseLsn(captureProperty(taskId, "wal.lsn"));
            return cur == null ? null : cur >= want;
        }

        String file = captureProperty(taskId, "binlog.file");
        String pos = captureProperty(taskId, "binlog.position");
        if (file == null || pos == null) {
            return null;
        }
        int sep = boundary.lastIndexOf(':');
        if (sep <= 0) {
            return null;
        }
        try {
            String wantFile = boundary.substring(0, sep).trim();
            long wantPos = Long.parseLong(boundary.substring(sep + 1).trim());
            int cmp = file.trim().compareTo(wantFile);
            if (cmp != 0) {
                // binlog 文件名带定长序号（mysql-bin.000021），字典序即时间序
                return cmp > 0;
            }
            return Long.parseLong(pos.trim()) >= wantPos;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 读 capture 进程自己持久化的位点属性。 */
    private String captureProperty(String taskId, String key) {
        File f = new File("files/" + taskId + "/binlog_output/capture_position.properties");
        if (!f.isFile()) {
            return null;
        }
        Properties p = new Properties();
        try (FileInputStream in = new FileInputStream(f)) {
            p.load(in);
            String v = p.getProperty(key);
            return (v == null || v.trim().isEmpty()) ? null : v.trim();
        } catch (Exception e) {
            return null;
        }
    }

    /** PG LSN 文本 "1/AD34E928" → 可比较的数值（高 32 位/低 32 位拼接）。 */
    static Long parseLsn(String lsn) {
        if (lsn == null || lsn.trim().isEmpty()) {
            return null;
        }
        String s = lsn.trim();
        int slash = s.indexOf('/');
        try {
            if (slash < 0) {
                return Long.parseLong(s);
            }
            return (Long.parseUnsignedLong(s.substring(0, slash), 16) << 32)
                    + Long.parseUnsignedLong(s.substring(slash + 1), 16);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String currentPosition(Connection conn, boolean pg) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(pg ? "SELECT pg_current_wal_lsn()::text" : "SHOW MASTER STATUS")) {
            if (rs.next()) {
                return pg ? rs.getString(1) : rs.getString(1) + ":" + rs.getString(2);
            }
        } catch (Exception e) {
            logger.warn("读取源端当前位点失败（不影响追平判定）: {}", e.getMessage());
        }
        return "-";
    }

    private Properties loadTaskConfig(String taskId) {
        File f = new File("files/" + taskId + "/config.properties");
        if (!f.isFile()) {
            return null;
        }
        Properties p = new Properties();
        try (FileInputStream in = new FileInputStream(f)) {
            p.load(in);
            return p;
        } catch (Exception e) {
            logger.warn("读取任务配置失败: {}", e.getMessage());
            return null;
        }
    }
}
