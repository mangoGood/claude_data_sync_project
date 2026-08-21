package com.synctask.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synctask.entity.TrafficRecording;
import com.synctask.entity.TrafficTaskConfig;
import com.synctask.entity.Workflow;
import com.synctask.repository.TrafficRecordingRepository;
import com.synctask.repository.TrafficTaskConfigRepository;
import com.synctask.util.JdbcSslOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 流量复制/回放的启动前预检。
 *
 * <p>与 {@code schemaPrecheck} 是两套东西：那一套查表结构、主键、字符集，对
 * "录一串语句再放一遍" 完全不适用——跑出来的 FAIL 全是假阳性。这里查的是真正会让
 * 流量任务出事的三类问题：<b>权限不够开不了日志</b>、<b>源库扛不住这份开销</b>、
 * <b>回放会打到不该打的实例上</b>。
 */
@Service
public class TrafficPrecheckService {

    private static final Logger logger = LoggerFactory.getLogger(TrafficPrecheckService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 源库 QPS 超过它就提示开销风险。general_log 给每条语句加一次 CSV 写 + 一把锁。 */
    private static final long QPS_WARN_THRESHOLD = 2000;

    /**
     * 直接注入仓储而不是 {@code TrafficTaskService}。
     *
     * <p>后者也要注入本类（启动门禁要调预检），两两相依会让 Spring 在启动时
     * 直接判循环依赖失败——整个后端起不来。预检本来也只需要读配置，不需要服务层逻辑。
     */
    @Autowired
    private TrafficTaskConfigRepository configRepository;

    @Autowired
    private TrafficRecordingRepository recordingRepository;

    /** 单项结果。 */
    public static final class Item {
        final String checkName;
        final String status;   // PASS / FAIL / WARN
        final String message;
        final String detail;

        Item(String checkName, String status, String message, String detail) {
            this.checkName = checkName;
            this.status = status;
            this.message = message;
            this.detail = detail;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new HashMap<>();
            m.put("checkName", checkName);
            m.put("status", status);
            m.put("message", message);
            m.put("detail", detail);
            return m;
        }
    }

    public Map<String, Object> precheck(Workflow workflow, Long userId) {
        List<Item> items = new ArrayList<>();
        boolean isReplay = TrafficTaskService.TYPE_REPLAY.equals(workflow.getTaskType());
        String engine = isReplay ? workflow.getTargetType() : workflow.getSourceType();

        if (engine != null && !"mysql".equalsIgnoreCase(engine)) {
            items.add(new Item("库类型", "FAIL", "流量复制与回放目前仅支持 MySQL", "当前: " + engine));
            return summarize(items);
        }

        if (isReplay) {
            checkReplay(workflow, items);
        } else {
            checkCapture(workflow, items);
        }
        return summarize(items);
    }

    // ==================== 复制 ====================

    private void checkCapture(Workflow workflow, List<Item> items) {
        String conn = workflow.getSourceConnection();
        if (conn == null || conn.isEmpty()) {
            items.add(new Item("源库连接", "FAIL", "未配置源库连接", null));
            return;
        }
        try (Connection c = open(conn, workflow.getSourceSslMode())) {
            items.add(new Item("源库连接", "PASS", "源库连接成功", null));

            try (Statement st = c.createStatement()) {
                // 采集连接自身不该进语句日志
                trySilent(st);

                String grants = grantsOf(st);
                boolean canSetGlobal = grants.contains("SUPER")
                        || grants.contains("SYSTEM_VARIABLES_ADMIN")
                        || grants.contains("ALL PRIVILEGES");
                items.add(new Item("改全局变量权限", canSetGlobal ? "PASS" : "FAIL",
                        canSetGlobal ? "具备开启 general_log 所需的权限"
                                : "账号缺少 SUPER / SYSTEM_VARIABLES_ADMIN，无法开启源库语句日志（E3120）",
                        canSetGlobal ? null : "binlog 里没有 SELECT，流量复制只能靠 general_log"));

                boolean canRotate = grants.contains("ALL PRIVILEGES")
                        || grants.contains("`mysql`")
                        || grants.contains("ON *.*") && (grants.contains("CREATE") && grants.contains("DROP"));
                items.add(new Item("日志表轮转权限", canRotate ? "PASS" : "WARN",
                        canRotate ? "具备 RENAME mysql.general_log 所需的权限"
                                : "可能缺少 mysql 库上的 CREATE/DROP/ALTER，轮转会失败（E3121）",
                        canRotate ? null : "日志表只能靠 RENAME 换表轮转，轮转不了会把源库磁盘写满"));

                String readOnly = scalar(st, "SELECT @@GLOBAL.read_only");
                boolean ro = "1".equals(readOnly) || "ON".equalsIgnoreCase(readOnly);
                items.add(new Item("源库可写", ro ? "FAIL" : "PASS",
                        ro ? "源库处于 read_only（多半是只读副本），改不了 general_log" : "源库非只读",
                        ro ? "请换主库作为流量复制的源" : null));

                String glNow = scalar(st, "SELECT @@GLOBAL.general_log");
                String loNow = scalar(st, "SELECT @@GLOBAL.log_output");
                boolean alreadyOn = "1".equals(glNow) || "ON".equalsIgnoreCase(glNow);
                items.add(new Item("语句日志当前状态",
                        alreadyOn ? "WARN" : "PASS",
                        alreadyOn ? "源库的 general_log 已经是开着的（log_output=" + loNow + "）"
                                : "任务会临时开启 general_log 并在结束时还原（当前 " + glNow + "/" + loNow + "）",
                        alreadyOn ? "可能有别的系统也在读这张表，本任务的轮转会把它读不到的数据一起拿走" : null));

                long qps = estimateQps(st);
                items.add(new Item("源库负载", qps > QPS_WARN_THRESHOLD ? "WARN" : "PASS",
                        "源库当前约 " + qps + " QPS",
                        qps > QPS_WARN_THRESHOLD
                                ? "general_log 会给每条语句加一次日志表写入与一把互斥锁，"
                                  + "这个量级下开销可观。建议降低采样率、缩小库白名单，或改在低峰期录制"
                                : null));
            }
        } catch (SQLException e) {
            items.add(new Item("源库连接", "FAIL", "源库连接失败: " + e.getMessage(), null));
        }
    }

    // ==================== 回放 ====================

    private void checkReplay(Workflow workflow, List<Item> items) {
        TrafficTaskConfig cfg = configRepository.findById(workflow.getId()).orElse(null);
        TrafficRecording rec = null;
        if (cfg != null && cfg.getReplayRecordingId() != null) {
            rec = recordingRepository.findByIdAndIsDeletedFalse(cfg.getReplayRecordingId()).orElse(null);
        }
        if (rec == null) {
            items.add(new Item("录制文件", "FAIL", "未选择录制文件，或该录制已被删除（E3123）", null));
            return;
        }
        if (!Boolean.TRUE.equals(rec.getSealed())) {
            items.add(new Item("录制文件", "FAIL",
                    "录制尚未封口，分段清单与统计都不完整，不能回放（E3123）",
                    "对应的流量复制任务还在跑，或异常终止过。停止它并重新同步录制元数据"));
            return;
        }
        items.add(new Item("录制文件", "PASS",
                "录制可用：" + rec.getRecordCount() + " 条 / 时长 " + (rec.getDurationMs() / 1000) + " 秒", null));

        if (rec.getGapCount() != null && rec.getGapCount() > 0) {
            items.add(new Item("时间轴空洞", "WARN",
                    "录制里有 " + rec.getGapCount() + " 处时间轴空洞",
                    "捕获中断过，那几段时间源库执行的语句已永久丢失。"
                            + "PRESERVE 档会按原时长静默等待，COMPRESS 档会把空洞压成 0"));
        }

        Map<String, Object> src = parseFingerprint(rec.getSourceFingerprint());

        String conn = workflow.getTargetConnection();
        if (conn == null || conn.isEmpty()) {
            items.add(new Item("目标库连接", "FAIL", "未配置目标库连接", null));
            return;
        }
        try (Connection c = open(conn, workflow.getTargetSslMode())) {
            items.add(new Item("目标库连接", "PASS", "目标库连接成功", null));
            try (Statement st = c.createStatement()) {
                String tgtUuid = scalar(st, "SELECT @@server_uuid");
                String srcUuid = str(src.get("serverUuid"));
                boolean allowSame = cfg != null && Boolean.TRUE.equals(cfg.getReplayAllowSameInstance());
                if (srcUuid != null && srcUuid.equals(tgtUuid)) {
                    items.add(new Item("目标实例隔离", allowSame ? "WARN" : "FAIL",
                            "回放目标与录制源是同一个 MySQL 实例（E3124）",
                            "回放会把源库上已经发生过的操作再做一遍：自增累加翻倍、重复插入、DROP 是真的删。"
                                    + (allowSame ? "已显式放行，风险自负" : "请换一个独立实例")));
                } else {
                    items.add(new Item("目标实例隔离", "PASS", "目标库与录制源是不同实例", null));
                }

                // 大小写敏感性不同 = DDL 会以不同的名字落地，之后所有语句都对不上。
                // 必须<b>按数值</b>比：指纹是 JSON，Jackson 把数字读成 Double，
                // 直接比字符串会得到 "0.0" != "0"，把每一次合法回放都误拦下来。
                String tgtLctn = scalar(st, "SELECT @@GLOBAL.lower_case_table_names");
                Long srcLctn = numOf(src.get("lowerCaseTableNames"));
                if (srcLctn != null && srcLctn != longOf(tgtLctn)) {
                    items.add(new Item("大小写敏感性", "FAIL",
                            "两端 lower_case_table_names 不一致（源 " + srcLctn + " / 目标 " + tgtLctn + "）",
                            "同一条 DDL 会以不同的表名落地，之后引用它的语句会全部失败"));
                } else {
                    items.add(new Item("大小写敏感性", "PASS", "两端 lower_case_table_names 一致", null));
                }

                String tgtMode = scalar(st, "SELECT @@GLOBAL.sql_mode");
                String srcMode = str(src.get("sqlMode"));
                if (srcMode != null && !srcMode.equals(tgtMode)) {
                    items.add(new Item("SQL 模式", "WARN", "两端 sql_mode 不同",
                            "回放会按录制里的 sql_mode 覆盖会话设置以对齐语义；"
                                    + "但目标库的全局默认仍与源库不同，其它连接看到的行为会不一样"));
                }

                String tgtVer = scalar(st, "SELECT VERSION()");
                String srcVer = str(src.get("version"));
                if (srcVer != null && tgtVer != null && !major(srcVer).equals(major(tgtVer))) {
                    items.add(new Item("版本差异", "WARN",
                            "源库 " + srcVer + " → 目标库 " + tgtVer,
                            "跨大版本回放时，部分语法与默认行为可能不同"));
                }

                // 比的是"这份录制<b>实际</b>要用多少条连接"，不是配置里的上限。
                // 上限只是个封顶值：录制里峰值并发才 6 个会话时，200 的上限根本用不到，
                // 拿它跟 max_connections(默认 151) 比，会把每一次正常回放都拦下来。
                int cap = cfg != null && cfg.getReplayMaxSessions() != null ? cfg.getReplayMaxSessions() : 200;
                int peak = rec.getSessionCount() == null ? 0 : rec.getSessionCount();
                int need = peak > 0 ? Math.min(cap, peak) : Math.min(cap, 1);
                long maxConn = longOf(scalar(st, "SELECT @@GLOBAL.max_connections"));
                if (maxConn > 0 && maxConn < need) {
                    items.add(new Item("连接数上限", "FAIL",
                            "目标库 max_connections=" + maxConn + "，而这份录制回放时需要 " + need + " 条连接",
                            "回放会为每个源会话建一条目标连接，连接建不出来会大面积报错"));
                } else if (maxConn > 0 && cap > maxConn) {
                    items.add(new Item("连接数上限", "WARN",
                            "并发会话上限设为 " + cap + "，超过目标库 max_connections=" + maxConn,
                            "这份录制峰值并发 " + peak + " 个会话，当前够用；但上限设得比目标库还高，"
                                    + "换一份并发更高的录制时会连不出来"));
                } else {
                    items.add(new Item("连接数上限", "PASS",
                            "目标库 max_connections=" + maxConn + "，录制峰值并发 " + peak + " 个会话", null));
                }

                boolean allowDangerous = cfg != null && Boolean.TRUE.equals(cfg.getReplayAllowDangerous());
                if (allowDangerous) {
                    items.add(new Item("破坏性语句", "WARN", "已放行 DROP DATABASE / SET GLOBAL 等破坏性语句",
                            "录制里若含这类语句，会在目标库上真的执行"));
                }
            }
        } catch (SQLException e) {
            items.add(new Item("目标库连接", "FAIL", "目标库连接失败: " + e.getMessage(), null));
        }
    }

    // ==================== 内部 ====================

    /** 连接串形如 {@code mysql://user:pass@host:port}。 */
    private Connection open(String connStr, String sslMode) throws SQLException {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("^\\w+://([^:]+):([^@]*)@([^:/]+):(\\d+)").matcher(connStr);
        if (!m.find()) {
            throw new SQLException("连接串格式不正确: " + connStr.replaceAll(":[^:@]*@", ":***@"));
        }
        String user = m.group(1);
        String pass = m.group(2);
        String host = m.group(3);
        String port = m.group(4);
        String url = String.format("jdbc:mysql://%s:%s/?%s&serverTimezone=UTC&characterEncoding=utf8"
                        + "&allowPublicKeyRetrieval=true&connectTimeout=8000&socketTimeout=15000",
                host, port, JdbcSslOptions.mysql(sslMode, null));
        return DriverManager.getConnection(url, user, pass);
    }

    /** 预检自身的查询不该进语句日志——它跑在可能已经开着 general_log 的源库上。 */
    private static void trySilent(Statement st) {
        try {
            st.execute("SET SESSION sql_log_off = 1");
        } catch (SQLException ignored) {
            // 权限不够就算了，只是多几行噪声
        }
    }

    private static String grantsOf(Statement st) {
        StringBuilder sb = new StringBuilder();
        try (ResultSet rs = st.executeQuery("SHOW GRANTS FOR CURRENT_USER()")) {
            while (rs.next()) {
                sb.append(rs.getString(1)).append('\n');
            }
        } catch (SQLException e) {
            logger.debug("读取权限失败: {}", e.getMessage());
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }

    /** 两次采样 Questions 之差估算 QPS。比 SHOW STATUS 的累计值有意义得多。 */
    private static long estimateQps(Statement st) {
        try {
            long q1 = statusValue(st, "Questions");
            long t1 = System.currentTimeMillis();
            Thread.sleep(1000);
            long q2 = statusValue(st, "Questions");
            long dt = Math.max(1, System.currentTimeMillis() - t1);
            return Math.max(0, (q2 - q1) * 1000 / dt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (SQLException e) {
            return 0;
        }
    }

    private static long statusValue(Statement st, String name) throws SQLException {
        try (ResultSet rs = st.executeQuery("SHOW GLOBAL STATUS LIKE '" + name + "'")) {
            return rs.next() ? longOf(rs.getString(2)) : 0;
        }
    }

    private static String scalar(Statement st, String sql) {
        try (ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseFingerprint(String json) {
        if (json == null || json.isEmpty()) return new HashMap<>();
        try {
            return MAPPER.readValue(json, Map.class);
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static String major(String version) {
        int i = version.indexOf('.');
        return i > 0 ? version.substring(0, i) : version;
    }

    /** 指纹里的数字字段：Jackson 读成 Double/Integer 都可能，统一按数值取。 */
    private static Long numOf(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.longValue();
        try {
            return (long) Double.parseDouble(String.valueOf(o).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static long longOf(String s) {
        try {
            return s == null ? 0 : Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    Map<String, Object> summarize(List<Item> items) {
        int passed = 0;
        int failed = 0;
        int warnings = 0;
        List<Map<String, Object>> checks = new ArrayList<>();
        for (Item i : items) {
            checks.add(i.toMap());
            switch (i.status) {
                case "FAIL": failed++; break;
                case "WARN": warnings++; break;
                default: passed++;
            }
        }
        Map<String, Object> out = new HashMap<>();
        out.put("overall", failed > 0 ? "FAIL" : (warnings > 0 ? "WARN" : "PASS"));
        out.put("passed", passed);
        out.put("failed", failed);
        out.put("warnings", warnings);
        out.put("checks", checks);
        return out;
    }
}
