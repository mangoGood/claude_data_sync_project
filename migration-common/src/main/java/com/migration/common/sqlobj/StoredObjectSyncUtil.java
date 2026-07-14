package com.migration.common.sqlobj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * MySQL 存储对象（procedure / function / trigger / event）同步工具，库级同步专用。
 *
 * <p>分工（与库级同步的双写规避设计对应）：
 * <ul>
 *   <li>全量阶段：{@link #copyProceduresAndFunctions} —— 表数据迁移完成后复制存储过程/函数；</li>
 *   <li>增量阶段：新建的 procedure/function 由增量 DDL 通道应用（SchemaEvolutionService）；</li>
 *   <li>任务结束：{@link #copyTriggersAndEvents} —— trigger/event 若在运行期同步，目标库会对
 *       同步写入再次触发造成双写，故统一延迟到任务结束时复制。</li>
 * </ul>
 *
 * <p>所有 CREATE 语句先经 {@link #stripDefiner}：binlog/SHOW CREATE 输出带
 * {@code DEFINER=`user`@`host`}，目标库通常无对应账号或执行者无 SUPER 权限，保留必失败。
 */
public final class StoredObjectSyncUtil {

    private static final Logger logger = LoggerFactory.getLogger(StoredObjectSyncUtil.class);

    /** DEFINER=`user`@`host` / DEFINER=user@host（无空格整体一个词） */
    private static final Pattern DEFINER_PATTERN =
            Pattern.compile("(?i)DEFINER\\s*=\\s*\\S+\\s+");

    private StoredObjectSyncUtil() {}

    /** 去掉 CREATE 语句中的 DEFINER 子句。 */
    public static String stripDefiner(String ddl) {
        if (ddl == null) {
            return null;
        }
        return DEFINER_PATTERN.matcher(ddl).replaceFirst("");
    }

    /**
     * 把源库 database 下的全部存储过程与函数复制到目标库（先 DROP IF EXISTS 再建，幂等）。
     * 单个对象失败只记日志继续，返回成功复制的数量。
     */
    public static int copyProceduresAndFunctions(Connection source, Connection target, String database) {
        int copied = 0;
        copied += copyRoutines(source, target, database, "PROCEDURE");
        copied += copyRoutines(source, target, database, "FUNCTION");
        return copied;
    }

    private static int copyRoutines(Connection source, Connection target, String database, String routineType) {
        int copied = 0;
        List<String> names = new ArrayList<>();
        String listSql = "SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = ?";
        try (java.sql.PreparedStatement ps = source.prepareStatement(listSql)) {
            ps.setString(1, database);
            ps.setString(2, routineType);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            logger.warn("[{}] 枚举 {} 失败: {}", database, routineType, e.getMessage());
            return 0;
        }

        String createColumn = "PROCEDURE".equals(routineType) ? "Create Procedure" : "Create Function";
        for (String name : names) {
            try {
                String createSql = readShowCreate(source,
                        "SHOW CREATE " + routineType + " `" + database + "`.`" + name + "`", createColumn);
                if (createSql == null || createSql.isEmpty()) {
                    logger.warn("[{}] SHOW CREATE {} {} 返回空（可能缺少权限），跳过", database, routineType, name);
                    continue;
                }
                applyInDatabase(target, database,
                        "DROP " + routineType + " IF EXISTS `" + name + "`", stripDefiner(createSql));
                copied++;
                logger.info("[{}] {} {} 已同步到目标库", database, routineType, name);
            } catch (SQLException e) {
                logger.warn("[{}] 同步 {} {} 失败: {}", database, routineType, name, e.getMessage());
            }
        }
        return copied;
    }

    /** trigger/event 同步明细：成功与失败对象清单（失败项含原因），供上层透出到任务日志。 */
    public static class SyncReport {
        public final List<String> succeeded = new ArrayList<>();
        /** 元素格式："TRIGGER trg1: 失败原因" */
        public final List<String> failed = new ArrayList<>();
    }

    /**
     * 把源库 database 下的全部触发器与事件复制到目标库（任务结束时调用），返回逐对象明细。
     * 复制出的 event 保留源库定义原样（含 ENABLE 状态）；单个失败记入报告继续。
     */
    public static SyncReport copyTriggersAndEventsDetailed(Connection source, Connection target, String database) {
        SyncReport report = new SyncReport();

        List<String> triggers = new ArrayList<>();
        try (java.sql.PreparedStatement ps = source.prepareStatement(
                "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    triggers.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            logger.warn("[{}] 枚举 TRIGGER 失败: {}", database, e.getMessage());
            report.failed.add("枚举 " + database + " 的 TRIGGER 失败: " + e.getMessage());
        }
        for (String name : triggers) {
            try {
                String createSql = readShowCreate(source,
                        "SHOW CREATE TRIGGER `" + database + "`.`" + name + "`", "SQL Original Statement");
                if (createSql == null || createSql.isEmpty()) {
                    report.failed.add("TRIGGER " + database + "." + name + ": SHOW CREATE 返回空（可能缺少权限）");
                    continue;
                }
                applyInDatabase(target, database,
                        "DROP TRIGGER IF EXISTS `" + name + "`", stripDefiner(createSql));
                report.succeeded.add("TRIGGER " + database + "." + name);
                logger.info("[{}] TRIGGER {} 已同步到目标库", database, name);
            } catch (SQLException e) {
                logger.warn("[{}] 同步 TRIGGER {} 失败: {}", database, name, e.getMessage());
                report.failed.add("TRIGGER " + database + "." + name + ": " + e.getMessage());
            }
        }

        List<String> events = new ArrayList<>();
        try (java.sql.PreparedStatement ps = source.prepareStatement(
                "SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    events.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            logger.warn("[{}] 枚举 EVENT 失败: {}", database, e.getMessage());
            report.failed.add("枚举 " + database + " 的 EVENT 失败: " + e.getMessage());
        }
        for (String name : events) {
            try {
                String createSql = readShowCreate(source,
                        "SHOW CREATE EVENT `" + database + "`.`" + name + "`", "Create Event");
                if (createSql == null || createSql.isEmpty()) {
                    report.failed.add("EVENT " + database + "." + name + ": SHOW CREATE 返回空（可能缺少权限）");
                    continue;
                }
                applyInDatabase(target, database,
                        "DROP EVENT IF EXISTS `" + name + "`", stripDefiner(createSql));
                report.succeeded.add("EVENT " + database + "." + name);
                logger.info("[{}] EVENT {} 已同步到目标库", database, name);
            } catch (SQLException e) {
                logger.warn("[{}] 同步 EVENT {} 失败: {}", database, name, e.getMessage());
                report.failed.add("EVENT " + database + "." + name + ": " + e.getMessage());
            }
        }
        return report;
    }

    /** 执行 SHOW CREATE 并按列名取定义（列缺失/无权限时返回 null）。 */
    private static String readShowCreate(Connection conn, String showSql, String columnLabel) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(showSql)) {
            if (rs.next()) {
                try {
                    return rs.getString(columnLabel);
                } catch (SQLException e) {
                    return null;
                }
            }
        }
        return null;
    }

    /** 在目标库指定 database 上下文中依次执行语句（SHOW CREATE 输出不带库限定符，需先 USE）。 */
    private static void applyInDatabase(Connection target, String database, String... sqls) throws SQLException {
        try (Statement st = target.createStatement()) {
            st.execute("USE `" + database + "`");
            for (String sql : sqls) {
                if (sql != null && !sql.trim().isEmpty()) {
                    st.execute(sql);
                }
            }
        }
    }
}
