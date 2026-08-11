package com.migration.extract.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * 语法覆盖度自检的独立入口：{@code java -cp migration-extract.jar
 * com.migration.extract.schema.SchemaSelfCheckMain --config files/<taskId>/config.properties}
 *
 * <p>做成独立进程而不是后端的一个预检项，是被工程结构逼的：{@code java-backend} 是与引擎
 * <b>没有编译依赖</b>的独立 Maven 工程（见 build.sh），拿不到这里的 parser；
 * {@code migration-agent} 也只依赖 {@code migration-common}。要让预检用上同一个 parser，
 * 只能像 agent 启动别的引擎进程那样把它当子进程跑——把 parser 复制一份到后端才是真正的坏主意，
 * 两份语法各自漂移之后，自检通过与运行期正确就没有关系了。
 *
 * <p>退出码：0 = 全部通过；1 = 有表对不上（详情在 stdout）；2 = 连不上库或配置不全。
 */
public final class SchemaSelfCheckMain {

    private static final Logger logger = LoggerFactory.getLogger(SchemaSelfCheckMain.class);

    private SchemaSelfCheckMain() {
    }

    public static void main(String[] args) {
        String configPath = null;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i]) || "-c".equals(args[i])) {
                configPath = args[i + 1];
            }
        }
        if (configPath == null) {
            System.err.println("用法: SchemaSelfCheckMain --config <config.properties>");
            System.exit(2);
        }

        Properties props = new Properties();
        try (InputStream in = new FileInputStream(configPath)) {
            props.load(in);
        } catch (Exception e) {
            System.err.println("读不了配置 " + configPath + ": " + e.getMessage());
            System.exit(2);
        }
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        String url = "jdbc:mysql://" + props.getProperty("source.db.host", "localhost") + ":"
                + props.getProperty("source.db.port", "3306")
                + "/?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";

        try (Connection conn = DriverManager.getConnection(url,
                props.getProperty("source.db.username", "root"),
                props.getProperty("source.db.password", ""))) {

            List<String> tables = resolveTables(conn, props);
            if (tables.isEmpty()) {
                System.out.println("同步范围里没有表，自检跳过");
                System.exit(0);
            }

            SchemaSelfCheck.Result result = new SchemaSelfCheck().run(conn, tables);
            System.out.println(result.report());
            System.exit(result.allPassed() ? 0 : 1);

        } catch (Exception e) {
            logger.error("自检执行失败", e);
            System.err.println("自检执行失败: " + e.getMessage());
            System.exit(2);
        }
    }

    /**
     * 解析同步范围内的表清单。表级同步直接取 {@code migration.included.tables}；
     * 库级同步要去库里枚举——注意只取 BASE TABLE，视图没有 binlog 行事件，
     * 把它们算进来只会得到一堆"SHOW CREATE TABLE 返回的是视图定义"的假失败。
     */
    public static List<String> resolveTables(Connection conn, Properties props) throws Exception {
        Set<String> out = new LinkedHashSet<>();

        for (String t : props.getProperty("migration.included.tables", "").split(",")) {
            if (!t.trim().isEmpty() && t.contains(".")) {
                out.add(t.trim());
            }
        }
        if (!out.isEmpty()) {
            return new ArrayList<>(out);
        }

        Set<String> databases = new LinkedHashSet<>();
        for (String key : new String[]{"sync.db.level.databases", "migration.included.databases"}) {
            for (String db : props.getProperty(key, "").split(",")) {
                if (!db.trim().isEmpty()) {
                    databases.add(db.trim());
                }
            }
        }
        for (String db : databases) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                            + "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")) {
                ps.setString(1, db);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(db + "." + rs.getString(1));
                    }
                }
            }
        }
        return new ArrayList<>(out);
    }

    /** 表名统一小写做集合去重时用（MySQL 的 lower_case_table_names 影响大小写敏感性）。 */
    static String normalize(String qualified) {
        return qualified == null ? null : qualified.trim().toLowerCase(Locale.ROOT);
    }
}
