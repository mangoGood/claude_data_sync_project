package com.synctask.service;

import com.synctask.dto.DatabaseInfo;
import com.synctask.dto.TableInfo;
import com.synctask.dto.ValidationResult;
import com.synctask.util.DataSourcePoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class MetadataService {

    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    private static final Pattern CONNECTION_PATTERN = Pattern.compile(
        "(?:mysql|postgresql|oracle|mongodb|elastic)://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.*))?"
    );

    public static class ParsedConnection {
        public String username;
        public String password;
        public String host;
        public int port;
        public String database;
        public String type;

        public ParsedConnection(String username, String password, String host, int port, String database, String type) {
            this.username = username;
            this.password = password;
            this.host = host;
            this.port = port;
            this.database = database;
            this.type = type;
        }
        
        public boolean isPostgresql() {
            return "postgresql".equals(type);
        }

        public boolean isOracle() {
            return "oracle".equals(type);
        }

        public boolean isMongo() {
            return "mongodb".equals(type);
        }

        public boolean isElastic() {
            return "elasticsearch".equals(type);
        }
    }

    public ParsedConnection parseConnection(String connectionStr) {
        if (connectionStr == null || connectionStr.isEmpty()) {
            throw new IllegalArgumentException("连接串不能为空");
        }

        String dbType;
        if (connectionStr.startsWith("postgresql://")) {
            dbType = "postgresql";
        } else if (connectionStr.startsWith("oracle://")) {
            dbType = "oracle";
        } else if (connectionStr.startsWith("mongodb://")) {
            dbType = "mongodb";
        } else if (connectionStr.startsWith("elastic://")) {
            dbType = "elasticsearch";
        } else {
            dbType = "mysql";
        }

        Matcher matcher = CONNECTION_PATTERN.matcher(connectionStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("连接串格式不正确，正确格式: mysql://user:pass@host:port 或 postgresql://user:pass@host:port 或 oracle://user:pass@host:port/service");
        }

        String username = matcher.group(1);
        String password = matcher.group(2);
        String host = matcher.group(3);
        int port = Integer.parseInt(matcher.group(4));
        String database = matcher.group(5);

        return new ParsedConnection(username, password, host, port, database, dbType);
    }

    public static class ConnectionTestResult {
        public boolean connected;
        public String errorType;
        public String errorMessage;
        public String suggestion;

        public ConnectionTestResult(boolean connected, String errorType, String errorMessage, String suggestion) {
            this.connected = connected;
            this.errorType = errorType;
            this.errorMessage = errorMessage;
            this.suggestion = suggestion;
        }
    }

    public ConnectionTestResult testConnectionDetailed(String connectionStr, String expectedType) {
        ParsedConnection conn = parseConnection(connectionStr);
        boolean isPg = conn.isPostgresql();
        boolean isOracle = conn.isOracle();
        boolean isMongo = conn.isMongo();
        boolean expectPg = "postgresql".equalsIgnoreCase(expectedType);
        boolean expectOracle = "oracle".equalsIgnoreCase(expectedType);
        boolean expectMongo = "mongodb".equalsIgnoreCase(expectedType);

        // MongoDB 不走 JDBC，类型互斥后单独处理
        if (expectMongo && !isMongo) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望MongoDB数据库，但连接串格式不匹配", "连接串应以 mongodb:// 开头");
        }
        if (!expectMongo && isMongo) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "连接串为MongoDB格式，但所选数据库类型不是MongoDB", "请检查数据库类型是否正确");
        }
        if (isMongo) {
            return testMongoConnectionDetailed(conn);
        }

        // Elasticsearch 走 HTTP REST，类型互斥后单独处理
        boolean isElastic = conn.isElastic();
        boolean expectElastic = "elasticsearch".equalsIgnoreCase(expectedType);
        if (expectElastic && !isElastic) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望Elasticsearch，但连接串格式不匹配", "连接串应以 elastic:// 开头");
        }
        if (!expectElastic && isElastic) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "连接串为Elasticsearch格式，但所选数据库类型不是Elasticsearch", "请检查数据库类型是否正确");
        }
        if (isElastic) {
            return testElasticConnectionDetailed(conn);
        }

        try {
            if (isPg) {
                Class.forName("org.postgresql.Driver");
            } else if (isOracle) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
        } catch (ClassNotFoundException e) {
            return new ConnectionTestResult(false, "DRIVER_NOT_FOUND", 
                "数据库驱动未找到: " + e.getMessage(), "请确保依赖中包含对应的数据库驱动");
        }

        if (expectPg && !isPg) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望PostgreSQL数据库，但连接串格式不匹配", "请检查数据库类型是否正确");
        }
        if (expectOracle && !isOracle) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望Oracle数据库，但连接串格式不匹配", "请检查数据库类型是否正确");
        }
        if (!expectPg && isPg) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望MySQL/Oracle数据库，但连接串格式为PostgreSQL", "请检查数据库类型是否正确");
        }
        if (!expectOracle && isOracle) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望MySQL/PostgreSQL数据库，但连接串格式为Oracle", "请检查数据库类型是否正确");
        }

        String jdbcUrl;
        if (isPg) {
            jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?connectTimeout=15&socketTimeout=15&stringtype=unspecified",
                conn.host, conn.port, (conn.database != null && !conn.database.isEmpty()) ? conn.database : "postgres");
        } else if (isOracle) {
            String service = (conn.database != null && !conn.database.isEmpty()) ? conn.database : "ORCL";
            jdbcUrl = String.format("jdbc:oracle:thin:@%s:%d/%s", conn.host, conn.port, service);
        } else {
            jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&connectTimeout=15000&socketTimeout=15000&allowPublicKeyRetrieval=true",
                conn.host, conn.port, (conn.database != null && !conn.database.isEmpty()) ? conn.database : "");
        }

        // 测试连接必须用一次性直连（DriverManager），不能走连接池：
        // 测试语义是"验证当前输入的凭证"，复用池中既有连接会掩盖凭证错误，
        // 且一次性测试不应催生常驻池。
        try (Connection connection = java.sql.DriverManager.getConnection(jdbcUrl, conn.username, conn.password)) {
            if (connection.isValid(5)) {
                String dbTypeName = isPg ? "PostgreSQL" : (isOracle ? "Oracle" : "MySQL");
                return new ConnectionTestResult(true, null, dbTypeName + "连接成功", null);
            } else {
                return new ConnectionTestResult(false, "CONNECTION_FAILED", "连接验证失败", "请检查数据库服务器状态");
            }
        } catch (java.sql.SQLInvalidAuthorizationSpecException e) {
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
        } catch (java.sql.SQLNonTransientConnectionException e) {
            String msg = e.getMessage();
            if (msg != null && (msg.contains("Access denied") || msg.contains("authentication"))) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查数据库服务器地址和端口是否正确，以及网络是否可达");
        } catch (com.mysql.cj.exceptions.WrongArgumentException e) {
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
        } catch (java.sql.SQLException e) {
            String msg = e.getMessage();
            if (msg != null && (msg.contains("Access denied") || msg.contains("authentication") || msg.contains("password") || msg.contains("ORA-01017"))) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            // "Communications link failure" 是 MySQL 驱动在 TCP 根本连不上时的顶层报文
            // （底层往往是 Connection refused，但顶层不含该词），最常见就是地址/端口填错——
            // 归到 NETWORK_ERROR 并给出"检查地址和端口"的明确指引，别让用户对着裸驱动报文猜。
            if (msg != null && (msg.contains("Connection refused") || msg.contains("timed out")
                    || msg.contains("timeout") || msg.contains("ORA-12541")
                    || msg.contains("Communications link failure") || msg.contains("No route to host")
                    || msg.contains("Connection reset"))) {
                return new ConnectionTestResult(false, "NETWORK_ERROR",
                    "网络不可达：无法建立到数据库的连接", "请检查主机地址和端口是否正确、数据库服务是否已启动且可从本机访问");
            }
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        } catch (Exception e) {
            if (e instanceof java.util.concurrent.TimeoutException || 
                (e.getCause() != null && e.getCause() instanceof java.util.concurrent.TimeoutException)) {
                return new ConnectionTestResult(false, "TIMEOUT",
                    "连接超时：20秒内未连接到数据库服务器", "请检查数据库服务器是否可达，以及防火墙设置");
            }
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    /**
     * 构建一次性 MongoClient。使用 directConnection 只连指定节点，避免驱动按副本集
     * 配置里的内部主机名（容器 hostname 等）重路由导致不可达；副本集属性仍可通过
     * hello 命令读取。调用方负责 close。
     */
    private com.mongodb.client.MongoClient buildMongoClient(ParsedConnection conn) {
        String userInfo = "";
        if (conn.username != null && !conn.username.isEmpty()) {
            userInfo = urlEncode(conn.username) + ":" + urlEncode(conn.password) + "@";
        }
        String uri = String.format(
            "mongodb://%s%s:%d/?authSource=admin&directConnection=true"
                + "&connectTimeoutMS=15000&socketTimeoutMS=15000&serverSelectionTimeoutMS=15000",
            userInfo, conn.host, conn.port);
        return com.mongodb.client.MongoClients.create(uri);
    }

    private static String urlEncode(String s) {
        return java.net.URLEncoder.encode(s == null ? "" : s, java.nio.charset.StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    /** 对 Mongo 节点执行 hello 命令（4.x 之前叫 isMaster，hello 自 4.4.2 起可用并向后兼容路由）。 */
    private org.bson.Document mongoHello(com.mongodb.client.MongoClient client) {
        try {
            return client.getDatabase("admin").runCommand(new org.bson.Document("hello", 1));
        } catch (com.mongodb.MongoCommandException e) {
            return client.getDatabase("admin").runCommand(new org.bson.Document("isMaster", 1));
        }
    }

    /** mongos 的 hello 响应带 msg=isdbgrid（无 setName）——用于识别分片集群路由节点。 */
    private static boolean isMongos(org.bson.Document hello) {
        return "isdbgrid".equals(hello.getString("msg"));
    }

    private ConnectionTestResult testMongoConnectionDetailed(ParsedConnection conn) {
        try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
            org.bson.Document hello = mongoHello(client);
            String setName = hello.getString("setName");
            String suffix = isMongos(hello)
                    ? "（分片集群 mongos）"
                    : (setName != null ? "（副本集: " + setName + "）" : "（独立节点，非副本集）");
            return new ConnectionTestResult(true, null, "MongoDB连接成功" + suffix, null);
        } catch (com.mongodb.MongoSecurityException e) {
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
        } catch (com.mongodb.MongoTimeoutException e) {
            // 认证失败在驱动内也可能表现为服务器选择超时，从 cause 链里甄别
            if (hasMongoAuthFailureCause(e)) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查MongoDB地址和端口是否正确，以及网络是否可达");
        } catch (Exception e) {
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    private boolean hasMongoAuthFailureCause(Throwable e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("Authentication failed")
                || msg.contains("MongoSecurityException")
                || msg.contains("SCRAM"));
    }

    // ==================== Elasticsearch（HTTP REST，不走 JDBC） ====================

    /** GET {path}，返回 [statusCode, body]；网络级失败抛异常。 */
    private String[] esHttpGet(ParsedConnection conn, String path) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(15))
                .build();
        java.net.http.HttpRequest.Builder b = java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create("http://" + conn.host + ":" + conn.port + path))
                .timeout(java.time.Duration.ofSeconds(15));
        if (conn.username != null && !conn.username.isEmpty()) {
            String basic = java.util.Base64.getEncoder().encodeToString(
                    (conn.username + ":" + (conn.password == null ? "" : conn.password))
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            b.header("Authorization", "Basic " + basic);
        }
        java.net.http.HttpResponse<String> resp = client.send(b.GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        return new String[]{String.valueOf(resp.statusCode()), resp.body()};
    }

    private ConnectionTestResult testElasticConnectionDetailed(ParsedConnection conn) {
        try {
            String[] resp = esHttpGet(conn, "/");
            int status = Integer.parseInt(resp[0]);
            if (status == 401 || status == 403) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            if (status >= 300) {
                return new ConnectionTestResult(false, "CONNECTION_FAILED",
                    "连接失败：HTTP " + status, "请检查Elasticsearch服务状态");
            }
            String version = "";
            try {
                com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(resp[1]).getAsJsonObject();
                version = json.getAsJsonObject("version").get("number").getAsString();
            } catch (Exception ignore) {
                // 版本解析失败不影响连通性结论
            }
            return new ConnectionTestResult(true, null,
                "Elasticsearch连接成功" + (version.isEmpty() ? "" : "（版本 " + version + "）"), null);
        } catch (java.net.ConnectException | java.net.http.HttpConnectTimeoutException e) {
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查Elasticsearch地址和端口是否正确，以及网络是否可达");
        } catch (Exception e) {
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    public List<String> listSchemas(String connectionStr, String database) {
        ParsedConnection conn = parseConnection(connectionStr);
        if (!conn.isPostgresql() && !conn.isOracle()) {
            throw new IllegalArgumentException("listSchemas 仅支持PostgreSQL和Oracle数据库");
        }

        List<String> schemas = new ArrayList<>();
        try {
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else {
                Class.forName("oracle.jdbc.OracleDriver");
            }
            String jdbcUrl = buildJdbcUrl(conn, database);
            try (Connection connection = DataSourcePoolManager.getConnection(jdbcUrl, conn.username, conn.password)) {
                String sql;
                if (conn.isPostgresql()) {
                    sql = "SELECT schema_name FROM information_schema.schemata " +
                          "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') " +
                          "ORDER BY schema_name";
                } else {
                    // Oracle: 列出用户可访问的 schema（排除系统 schema）
                    sql = "SELECT username FROM all_users " +
                          "WHERE username NOT IN ('SYS','SYSTEM','OUTLN','DBSNMP','APPQOSSYS','WMSYS','XDB','ORDSYS','MDSYS','CTXSYS','EXFSYS','OLAPSYS','ORDDATA','LBACSYS','ANONYMOUS','APEX_030200','FLOWS_FILES','APEX_PUBLIC_USER','SYSMAN','OWBSYS','SPATIAL_WFS_ADMIN_USR','SPATIAL_CSW_ADMIN_USR') " +
                          "ORDER BY username";
                }
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        schemas.add(rs.getString(1));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("查询schema列表失败: {}", e.getMessage());
            throw new RuntimeException("查询schema列表失败: " + e.getMessage());
        }
        return schemas;
    }

    public List<TableInfo> listTables(String connectionStr, String database, String schema) {
        ParsedConnection conn = parseConnection(connectionStr);
        if (conn.isPostgresql()) {
            List<TableInfo> tables = new ArrayList<>();
            try {
                Class.forName("org.postgresql.Driver");
                String jdbcUrl = buildJdbcUrl(conn, database);
                try (Connection connection = DataSourcePoolManager.getConnection(jdbcUrl, conn.username, conn.password)) {
                    String effectiveSchema = (schema != null && !schema.isEmpty()) ? schema : "public";
                    try (PreparedStatement stmt = connection.prepareStatement(
                             "SELECT tablename FROM pg_tables WHERE schemaname = ?")) {
                        stmt.setString(1, effectiveSchema);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String tableName = rs.getString(1);
                                long rows = getPgRowCount(connection, tableName);
                                tables.add(new TableInfo(tableName, rows, "", "TABLE"));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("查询表列表失败: {}", e.getMessage());
                throw new RuntimeException("查询表列表失败: " + e.getMessage());
            }
            return tables;
        }
        if (conn.isOracle()) {
            List<TableInfo> tables = new ArrayList<>();
            try {
                Class.forName("oracle.jdbc.OracleDriver");
                String jdbcUrl = buildJdbcUrl(conn, database);
                try (Connection connection = DataSourcePoolManager.getConnection(jdbcUrl, conn.username, conn.password)) {
                    String owner = (schema != null && !schema.isEmpty()) ? schema.toUpperCase() : conn.username.toUpperCase();
                    try (PreparedStatement stmt = connection.prepareStatement(
                             "SELECT table_name FROM all_tables WHERE owner = ? ORDER BY table_name")) {
                        stmt.setString(1, owner);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String tableName = rs.getString(1);
                                tables.add(new TableInfo(tableName, 0, "", "TABLE"));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("查询Oracle表列表失败: {}", e.getMessage());
                throw new RuntimeException("查询Oracle表列表失败: " + e.getMessage());
            }
            return tables;
        }
        return listTables(connectionStr, database);
    }

    private String buildJdbcUrl(ParsedConnection conn, String database) {
        if (conn.isPostgresql()) {
            if (database != null && !database.isEmpty()) {
                return String.format("jdbc:postgresql://%s:%d/%s?currentSchema=public&stringtype=unspecified", conn.host, conn.port, database);
            }
            return String.format("jdbc:postgresql://%s:%d/?currentSchema=public&stringtype=unspecified", conn.host, conn.port);
        }
        if (conn.isOracle()) {
            String service = (database != null && !database.isEmpty()) ? database : (conn.database != null && !conn.database.isEmpty() ? conn.database : "ORCL");
            return String.format("jdbc:oracle:thin:@%s:%d/%s", conn.host, conn.port, service);
        }
        if (database != null && !database.isEmpty()) {
            return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true", conn.host, conn.port, database);
        }
        return String.format("jdbc:mysql://%s:%d/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true", conn.host, conn.port);
    }
    
    private String buildJdbcUrl(ParsedConnection conn) {
        return buildJdbcUrl(conn, conn.database);
    }

    public boolean testConnection(String connectionStr) {
        ParsedConnection conn = parseConnection(connectionStr);

        if (conn.isMongo()) {
            try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
                client.getDatabase("admin").runCommand(new org.bson.Document("ping", 1));
                return true;
            } catch (Exception e) {
                logger.error("测试MongoDB连接失败: {}", e.getMessage());
                return false;
            }
        }

        try {
            Connection connection;
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else if (conn.isOracle()) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
            connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, null),
                    conn.username, conn.password);
            
            boolean valid = connection.isValid(5);
            connection.close();
            return valid;
        } catch (SQLException e) {
            logger.error("测试连接失败: {}", e.getMessage());
            return false;
        } catch (ClassNotFoundException e) {
            logger.error("数据库驱动未找到: {}", e.getMessage());
            return false;
        }
    }

    public List<String> listDatabases(String connectionStr) {
        ParsedConnection conn = parseConnection(connectionStr);

        List<String> databases = new ArrayList<>();

        if (conn.isMongo()) {
            try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
                for (String dbName : client.listDatabaseNames()) {
                    if (!isMongoSystemDatabase(dbName)) {
                        databases.add(dbName);
                    }
                }
                logger.info("查询到 {} 个MongoDB数据库", databases.size());
                return databases;
            } catch (Exception e) {
                logger.error("查询MongoDB数据库列表失败: {}", e.getMessage());
                throw new RuntimeException("查询数据库列表失败: " + e.getMessage());
            }
        }

        try {
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else if (conn.isOracle()) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
            
            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, null),
                    conn.username, conn.password)) {
                
                if (conn.isPostgresql()) {
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT datname FROM pg_database WHERE datistemplate = false")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            if (!isPgSystemDatabase(dbName)) {
                                databases.add(dbName);
                            }
                        }
                    }
                } else if (conn.isOracle()) {
                    // Oracle 没有"数据库"概念，使用服务名/SID 作为标识，直接返回连接串中指定的服务名
                    String service = (conn.database != null && !conn.database.isEmpty()) ? conn.database : "ORCL";
                    databases.add(service);
                } else {
                    DatabaseMetaData metaData = connection.getMetaData();
                    ResultSet rs = metaData.getCatalogs();
                    
                    while (rs.next()) {
                        String dbName = rs.getString("TABLE_CAT");
                        if (!isSystemDatabase(dbName)) {
                            databases.add(dbName);
                        }
                    }
                }
                
                logger.info("查询到 {} 个数据库", databases.size());
            }
        } catch (SQLException e) {
            logger.error("查询数据库列表失败: {}", e.getMessage());
            throw new RuntimeException("查询数据库列表失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("数据库驱动未找到: {}", e.getMessage());
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        
        return databases;
    }
    
    private boolean isPgSystemDatabase(String dbName) {
        return "postgres".equalsIgnoreCase(dbName) ||
               "template0".equalsIgnoreCase(dbName) ||
               "template1".equalsIgnoreCase(dbName);
    }

    private boolean isSystemDatabase(String dbName) {
        return "information_schema".equalsIgnoreCase(dbName) ||
               "mysql".equalsIgnoreCase(dbName) ||
               "performance_schema".equalsIgnoreCase(dbName) ||
               "sys".equalsIgnoreCase(dbName);
    }

    private boolean isMongoSystemDatabase(String dbName) {
        return "admin".equalsIgnoreCase(dbName) ||
               "local".equalsIgnoreCase(dbName) ||
               "config".equalsIgnoreCase(dbName);
    }

    /** 列过滤支持的类型（整数/bit/浮点定点/日期时间），供前端“列名过滤”页签按类型放行 */
    private static final Set<String> FILTERABLE_COLUMN_TYPES = new HashSet<>(Arrays.asList(
        "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "bit",
        "decimal", "numeric", "float", "double",
        "date", "datetime", "timestamp", "time", "year"
    ));

    /**
     * 查询表的列信息（列处理页面用，目前仅支持 MySQL 源）。
     * 返回每列的 name/dataType/columnType/primaryKey/filterable。
     */
    public List<java.util.Map<String, Object>> listColumns(String connectionStr, String database, String table) {
        ParsedConnection conn = parseConnection(connectionStr);
        if (conn.isMongo() || conn.isPostgresql() || conn.isOracle() || conn.isElastic()) {
            throw new RuntimeException("列处理目前仅支持 MySQL 数据源");
        }
        List<java.util.Map<String, Object>> columns = new ArrayList<>();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, database), conn.username, conn.password);
                 PreparedStatement stmt = connection.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY FROM information_schema.COLUMNS " +
                     "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
                        String dataType = rs.getString("DATA_TYPE");
                        col.put("name", rs.getString("COLUMN_NAME"));
                        col.put("dataType", dataType);
                        col.put("columnType", rs.getString("COLUMN_TYPE"));
                        col.put("primaryKey", "PRI".equalsIgnoreCase(rs.getString("COLUMN_KEY")));
                        col.put("filterable", dataType != null
                                && FILTERABLE_COLUMN_TYPES.contains(dataType.toLowerCase()));
                        columns.add(col);
                    }
                }
            }
            logger.info("表 {}.{} 查询到 {} 个列", database, table, columns.size());
        } catch (SQLException e) {
            logger.error("查询列信息失败: {}", e.getMessage());
            throw new RuntimeException("查询列信息失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        return columns;
    }

    public List<TableInfo> listTables(String connectionStr, String database) {
        ParsedConnection conn = parseConnection(connectionStr);

        List<TableInfo> tables = new ArrayList<>();

        if (conn.isMongo()) {
            try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
                com.mongodb.client.MongoDatabase db = client.getDatabase(database);
                for (String name : db.listCollectionNames()) {
                    if (name.startsWith("system.")) {
                        continue;
                    }
                    long docs = db.getCollection(name).estimatedDocumentCount();
                    tables.add(new TableInfo(name, docs, "", "COLLECTION"));
                }
                logger.info("MongoDB数据库 {} 查询到 {} 个集合", database, tables.size());
                return tables;
            } catch (Exception e) {
                logger.error("查询MongoDB集合列表失败: {}", e.getMessage());
                throw new RuntimeException("查询表列表失败: " + e.getMessage());
            }
        }

        try {
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else if (conn.isOracle()) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
            
            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, database),
                    conn.username, conn.password)) {
                
                if (conn.isPostgresql()) {
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery(
                             "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")) {
                        while (rs.next()) {
                            String tableName = rs.getString(1);
                            long rows = getPgRowCount(connection, tableName);
                            tables.add(new TableInfo(tableName, rows, "", "TABLE"));
                        }
                    }
                } else if (conn.isOracle()) {
                    String owner = (database != null && !database.isEmpty()) ? database.toUpperCase() : conn.username.toUpperCase();
                    try (PreparedStatement stmt = connection.prepareStatement(
                             "SELECT table_name FROM all_tables WHERE owner = ? ORDER BY table_name")) {
                        stmt.setString(1, owner);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String tableName = rs.getString(1);
                                tables.add(new TableInfo(tableName, 0, "", "TABLE"));
                            }
                        }
                    }
                } else {
                    DatabaseMetaData metaData = connection.getMetaData();
                    ResultSet rs = metaData.getTables(database, null, "%", new String[]{"TABLE"});
                    
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        long rows = getRowCount(connection, database, tableName);
                        String size = getTableSize(connection, database, tableName);
                        String engine = getTableEngine(metaData, database, tableName);
                        
                        tables.add(new TableInfo(tableName, rows, size, engine));
                    }
                }
                
                logger.info("数据库 {} 查询到 {} 个表", database, tables.size());
            }
        } catch (SQLException e) {
            logger.error("查询表列表失败: {}", e.getMessage());
            throw new RuntimeException("查询表列表失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("数据库驱动未找到: {}", e.getMessage());
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        
        return tables;
    }
    
    private long getPgRowCount(Connection connection, String tableName) {
        // 表名是标识符无法参数化：对引号转义（表名来自服务端元数据枚举，转义防御异常命名）
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM \"" + tableName.replace("\"", "\"\"") + "\"")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.warn("获取PG表 {} 行数失败: {}", tableName, e.getMessage());
        }
        return 0;
    }

    private long getRowCount(Connection connection, String database, String tableName) {
        // 表名是标识符无法参数化：对反引号转义（表名来自服务端元数据枚举，转义防御异常命名）
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM `" + tableName.replace("`", "``") + "`")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.warn("获取表 {} 行数失败: {}", tableName, e.getMessage());
        }
        return 0;
    }

    private String getTableSize(Connection connection, String database, String tableName) {
        try (PreparedStatement stmt = connection.prepareStatement(
                 "SELECT ROUND(data_length + index_length) as size_bytes " +
                 "FROM information_schema.tables " +
                 "WHERE table_schema = ? AND table_name = ?")) {
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long bytes = rs.getLong("size_bytes");
                    return formatSize(bytes);
                }
            }
        } catch (SQLException e) {
            logger.warn("获取表 {} 大小失败: {}", tableName, e.getMessage());
        }
        return "0 B";
    }

    private String getTableEngine(DatabaseMetaData metaData, String database, String tableName) {
        try (ResultSet rs = metaData.getTables(database, null, tableName, new String[]{"TABLE"})) {
            if (rs.next()) {
                return rs.getString("TABLE_TYPE");
            }
        } catch (SQLException e) {
            logger.warn("获取表 {} 引擎失败: {}", tableName, e.getMessage());
        }
        return "UNKNOWN";
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    public DatabaseInfo getDatabaseWithTables(String connectionStr, String database) {
        DatabaseInfo dbInfo = new DatabaseInfo(database);
        
        try {
            List<TableInfo> tables = listTables(connectionStr, database);
            dbInfo.setTables(tables);
            dbInfo.setAccessible(true);
        } catch (Exception e) {
            dbInfo.setAccessible(false);
            dbInfo.setErrorMessage(e.getMessage());
            logger.error("获取数据库 {} 信息失败: {}", database, e.getMessage());
        }
        
        return dbInfo;
    }

    public ValidationResult validateForSubscribe(String sourceConnection, String sourceType) {
        ValidationResult result = new ValidationResult();
        boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceType);
        boolean sourceIsOracle = "oracle".equalsIgnoreCase(sourceType);

        ParsedConnection sourceConn = parseConnection(sourceConnection);

        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn, sourceIsPg),
                sourceConn.username, sourceConn.password)) {

            String dbTypeName = sourceIsPg ? "PostgreSQL" : (sourceIsOracle ? "Oracle" : "MySQL");
            result.addItem("源库连接", "源数据库连接检查", true,
                    dbTypeName + "源库连接成功", "info");

            if (sourceIsPg) {
                checkPgWalConfig(sourceDb, result);
                checkPgReplicationSlot(sourceDb, result);
            } else if (sourceIsOracle) {
                checkOracleLogMinerConfig(sourceDb, result);
                checkOracleArchiveLog(sourceDb, result);
            } else {
                checkBinlogEnabled(sourceDb, result);
                checkBinlogFormat(sourceDb, result);
                checkBinlogRowImage(sourceDb, result);
                String sourceVersion = getMySQLVersion(sourceDb);
                checkServerId(sourceDb, sourceVersion, result);
            }

            checkSubscribePermissions(sourceDb, sourceIsPg, sourceIsOracle, result);

        } catch (SQLException e) {
            logger.error("订阅校验失败: {}", e.getMessage());
            result.addItem("源库连接", "源数据库连接检查", false,
                    "连接失败: " + e.getMessage(), "error");
        }

        boolean allPassed = result.getCheckItems().stream().allMatch(ValidationResult.CheckItem::isPassed);
        result.setAllPassed(allPassed);

        return result;
    }

    private void checkOracleLogMinerConfig(Connection conn, ValidationResult result) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT log_mode FROM v$database")) {
                if (rs.next()) {
                    String logMode = rs.getString(1);
                    if ("ARCHIVELOG".equalsIgnoreCase(logMode)) {
                        result.addItem("归档模式", "Oracle归档模式检查", true,
                                "数据库处于ARCHIVELOG模式，支持LogMiner", "info");
                    } else {
                        result.addItem("归档模式", "Oracle归档模式检查", false,
                                "数据库处于" + logMode + "模式，需切换到ARCHIVELOG模式才能使用LogMiner", "error");
                    }
                }
            }
        } catch (SQLException e) {
            result.addItem("归档模式", "Oracle归档模式检查", true,
                    "无法检查归档模式（可能权限不足）: " + e.getMessage(), "warning");
        }
    }

    private void checkOracleArchiveLog(Connection conn, ValidationResult result) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM v$archived_log WHERE archived = 'YES'")) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    result.addItem("归档日志", "Oracle归档日志可用性检查", true,
                            "检测到 " + count + " 个归档日志", "info");
                }
            }
        } catch (SQLException e) {
            result.addItem("归档日志", "Oracle归档日志可用性检查", true,
                    "无法检查归档日志（可能权限不足）: " + e.getMessage(), "warning");
        }
    }

    private void checkPgReplicationSlot(Connection conn, ValidationResult result) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT slot_name, plugin, slot_type FROM pg_replication_slots WHERE slot_type = 'logical' LIMIT 5")) {
                java.util.List<String> slots = new java.util.ArrayList<>();
                while (rs.next()) {
                    slots.add(rs.getString("slot_name") + "(" + rs.getString("plugin") + ")");
                }
                if (!slots.isEmpty()) {
                    result.addItem("复制槽", "PostgreSQL逻辑复制槽检查", true,
                            "检测到逻辑复制槽: " + String.join(", ", slots), "info");
                } else {
                    result.addItem("复制槽", "PostgreSQL逻辑复制槽检查", true,
                            "暂无逻辑复制槽，订阅任务将自动创建", "info");
                }
            }
        } catch (SQLException e) {
            result.addItem("复制槽", "PostgreSQL逻辑复制槽检查", true,
                    "无法检查复制槽（可能权限不足）: " + e.getMessage(), "warning");
        }
    }

    private void checkSubscribePermissions(Connection conn, boolean isPg, boolean isOracle, ValidationResult result) {
        try {
            if (isPg) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT has_database_privilege(current_database(), 'connect') AND " +
                         "has_database_privilege(current_database(), 'usage')")) {
                    if (rs.next() && rs.getBoolean(1)) {
                        result.addItem("源库权限", "订阅所需源库权限检查", true,
                                "当前用户具有源库连接和使用权限", "info");
                    } else {
                        result.addItem("源库权限", "订阅所需源库权限检查", false,
                                "当前用户缺少源库连接或使用权限", "error");
                    }
                }
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT pg_is_in_recovery()")) {
                    if (rs.next() && rs.getBoolean(1)) {
                        result.addItem("主从角色", "源库主从角色检查", false,
                                "当前连接的是PostgreSQL从库，无法进行逻辑复制", "error");
                    }
                }
            } else if (isOracle) {
                // Oracle: 检查执行 DBMS_LOGMNR 所需的权限
                boolean hasExecuteCatalogRole = false;
                boolean hasExecuteAnyProcedure = false;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT granted_role FROM user_role_privs WHERE granted_role IN ('EXECUTE_CATALOG_ROLE','DBA')")) {
                    while (rs.next()) {
                        String role = rs.getString(1);
                        if ("EXECUTE_CATALOG_ROLE".equalsIgnoreCase(role) || "DBA".equalsIgnoreCase(role)) {
                            hasExecuteCatalogRole = true;
                        }
                    }
                }
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT privilege FROM user_sys_privs WHERE privilege = 'EXECUTE ANY PROCEDURE'")) {
                    if (rs.next()) {
                        hasExecuteAnyProcedure = true;
                    }
                } catch (SQLException ignored) {}

                if (hasExecuteCatalogRole || hasExecuteAnyProcedure) {
                    result.addItem("源库权限", "LogMiner订阅所需权限检查", true,
                            "当前用户具有执行DBMS_LOGMNR所需权限", "info");
                } else {
                    result.addItem("源库权限", "LogMiner订阅所需权限检查", false,
                            "当前用户缺少EXECUTE CATALOG ROLE或EXECUTE ANY PROCEDURE权限，无法执行LogMiner", "error");
                }

                // 检查是否为主库（非备库）
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT database_role FROM v$database")) {
                    if (rs.next()) {
                        String role = rs.getString(1);
                        if (!"PRIMARY".equalsIgnoreCase(role)) {
                            result.addItem("主从角色", "源库主从角色检查", false,
                                    "当前连接的是Oracle " + role + "，建议连接到PRIMARY数据库进行LogMiner", "warning");
                        }
                    }
                } catch (SQLException ignored) {}
            } else {
                // MySQL 8.x 使用 Super_priv/Repl_slave_priv/Reload_priv，5.x 使用 SUPER/REPLICATION_SLAVE/RELOAD
                boolean hasRepl = false;
                boolean hasReload = false;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT Super_priv, Repl_slave_priv, Reload_priv FROM mysql.user WHERE user = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)")) {
                    if (rs.next()) {
                        hasRepl = "Y".equalsIgnoreCase(rs.getString("Repl_slave_priv"));
                        hasReload = "Y".equalsIgnoreCase(rs.getString("Reload_priv"));
                    }
                } catch (SQLException e8) {
                    // fallback for MySQL 5.x
                    try (Statement stmt2 = conn.createStatement();
                         ResultSet rs2 = stmt2.executeQuery(
                             "SELECT SUPER, REPLICATION_SLAVE, RELOAD FROM mysql.user WHERE user = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)")) {
                        if (rs2.next()) {
                            hasRepl = rs2.getBoolean("REPLICATION_SLAVE");
                            hasReload = rs2.getBoolean("RELOAD");
                        }
                    }
                }
                if (hasRepl) {
                    result.addItem("源库权限", "Binlog订阅所需权限检查", true,
                            "当前用户具有REPLICATION SLAVE权限", "info");
                } else {
                    result.addItem("源库权限", "Binlog订阅所需权限检查", false,
                            "当前用户缺少REPLICATION SLAVE权限，无法读取Binlog", "error");
                }
                if (!hasReload) {
                    result.addItem("RELOAD权限", "Binlog订阅建议具有RELOAD权限", false,
                            "当前用户缺少RELOAD权限，可能无法刷新Binlog状态", "warning");
                }
            }
        } catch (SQLException e) {
            result.addItem("源库权限", "订阅所需源库权限检查", true,
                    "无法检查权限（非致命）: " + e.getMessage(), "warning");
        }
    }

    public ValidationResult validateForMigration(String sourceConnection, String targetConnection,
                                                  String migrationMode, String sourceType, String targetType) {
        return validateForMigration(sourceConnection, targetConnection, migrationMode, sourceType, targetType, null);
    }

    public ValidationResult validateForMigration(String sourceConnection, String targetConnection,
                                                  String migrationMode, String sourceType, String targetType,
                                                  String drMode) {
        ValidationResult result = new ValidationResult();

        boolean sourceIsMongo = "mongodb".equalsIgnoreCase(sourceType);
        boolean targetIsMongo = "mongodb".equalsIgnoreCase(targetType);
        if (sourceIsMongo || targetIsMongo) {
            return validateForMongoMigration(sourceConnection, targetConnection, migrationMode,
                    sourceIsMongo, targetIsMongo, result);
        }

        boolean sourceIsEs = "elasticsearch".equalsIgnoreCase(sourceType);
        boolean targetIsEs = "elasticsearch".equalsIgnoreCase(targetType);
        if (sourceIsEs || targetIsEs) {
            return validateForElasticMigration(sourceConnection, targetConnection, migrationMode,
                    sourceIsEs, sourceType, result);
        }

        boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceType);
        boolean sourceIsOracle = "oracle".equalsIgnoreCase(sourceType);
        boolean targetIsPg = "postgresql".equalsIgnoreCase(targetType);
        boolean bothMysql = !sourceIsPg && !sourceIsOracle && !targetIsPg;
        boolean mysqlToPg = !sourceIsPg && !sourceIsOracle && targetIsPg;
        boolean pgToMysql = sourceIsPg && !targetIsPg;
        boolean bothPg = sourceIsPg && targetIsPg;
        boolean oracleToPg = sourceIsOracle && targetIsPg;
        boolean bidirectional = "BIDIRECTIONAL".equalsIgnoreCase(drMode);

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);

        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn),
                sourceConn.username, sourceConn.password);
             Connection targetDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(targetConn),
                targetConn.username, targetConn.password)) {

            String sourceLabel = sourceIsPg ? "PostgreSQL" : (sourceIsOracle ? "Oracle" : "MySQL");
            result.addItem("源库连接", "源数据库连接检查", true,
                sourceLabel + "源库连接成功", "info");
            result.addItem("目标库连接", "目标数据库连接检查", true,
                targetIsPg ? "PostgreSQL目标库连接成功" : "MySQL目标库连接成功", "info");

            if (bothMysql) {
                String sourceVersion = getMySQLVersion(sourceDb);
                String targetVersion = getMySQLVersion(targetDb);

                if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                    checkBinlogEnabled(sourceDb, result);
                    checkBinlogFormat(sourceDb, result);
                    checkBinlogRowImage(sourceDb, result);
                    checkServerId(sourceDb, sourceVersion, result);
                }

                checkVersionCompatibility(sourceVersion, targetVersion, result);
                checkSqlModeCompatibility(sourceDb, targetDb, result);
                checkSourcePermissions(sourceDb, migrationMode, result);
                checkTargetPermissions(targetDb, targetVersion, result);
            }

            if (mysqlToPg) {
                if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                    checkBinlogEnabled(sourceDb, result);
                    checkBinlogFormat(sourceDb, result);
                    checkBinlogRowImage(sourceDb, result);
                }
            }

            if (pgToMysql) {
                // PG→MySQL: 检查源端PG的WAL配置
                checkPgWalConfig(sourceDb, result);
            }

            if (bothPg) {
                // PG→PG（含单向/双向灾备）：增量捕获需要源端 WAL 为 logical。
                if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                    checkPgWalConfig(sourceDb, result);
                    checkPgReplicationSlot(sourceDb, result);
                    // 双向灾备：反向通道（B→A）的 capture 跑在目标库 B 上，故 B 也需 logical WAL。
                    if (bidirectional) {
                        checkPgWalConfigLabeled(targetDb, result, "目标库");
                    }
                }
            }

            if (oracleToPg) {
                // Oracle→PG: 检查源端Oracle的LogMiner/归档日志配置
                checkOracleLogMinerConfig(sourceDb, result, migrationMode);
            }

        } catch (SQLException e) {
            logger.error("数据库校验失败: {}", e.getMessage());
            if (!result.getCheckItems().stream().anyMatch(item -> !item.isPassed())) {
                result.addItem("连接检查", "数据库连接检查", false, "连接失败: " + e.getMessage(), "error");
            }
        }

        boolean allPassed = result.getCheckItems().stream().allMatch(ValidationResult.CheckItem::isPassed);
        result.setAllPassed(allPassed);

        return result;
    }

    /**
     * MySQL → Elasticsearch 同步预校验。
     * 检查项：类型组合（ES 仅可作目标、源必须 MySQL）、源库连接、增量所需 binlog 配置
     * （复用 mysql→mysql 的三项检查）、目标 ES 连接/集群健康/版本。
     */
    private ValidationResult validateForElasticMigration(String sourceConnection, String targetConnection,
                                                         String migrationMode, boolean sourceIsEs,
                                                         String sourceType, ValidationResult result) {
        if (sourceIsEs) {
            result.addItem("类型组合", "Elasticsearch 只能作为同步目标", false,
                    "Elasticsearch 不支持作为同步源，仅支持 MySQL 到 Elasticsearch", "error");
            result.setAllPassed(false);
            return result;
        }
        if (!"mysql".equalsIgnoreCase(sourceType)) {
            result.addItem("类型组合", "Elasticsearch 目标仅支持 MySQL 源", false,
                    "到 Elasticsearch 的同步目前仅支持 MySQL 源（binlog 增量捕获）", "error");
            result.setAllPassed(false);
            return result;
        }

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        if (!targetConn.isElastic()) {
            result.addItem("连接串格式", "Elasticsearch 连接串格式检查", false,
                    "目标连接串应以 elastic:// 开头", "error");
            result.setAllPassed(false);
            return result;
        }

        // 源库：连接 + 增量所需 binlog 配置（与 mysql→mysql 相同的三项检查）
        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn), sourceConn.username, sourceConn.password)) {
            result.addItem("源库连接", "源数据库连接检查", true, "MySQL源库连接成功", "info");
            if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                checkBinlogEnabled(sourceDb, result);
                checkBinlogFormat(sourceDb, result);
                checkBinlogRowImage(sourceDb, result);
            }
        } catch (SQLException e) {
            logger.error("MySQL 源库校验失败: {}", e.getMessage());
            result.addItem("源库连接", "源数据库连接检查", false, "连接失败: " + e.getMessage(), "error");
        }

        // 目标 ES：连接 + 集群健康 + 版本
        try {
            String[] info = esHttpGet(targetConn, "/");
            int status = Integer.parseInt(info[0]);
            if (status == 401 || status == 403) {
                result.addItem("目标库连接", "目标Elasticsearch连接检查", false,
                        "认证失败：用户名或密码错误", "error");
            } else if (status >= 300) {
                result.addItem("目标库连接", "目标Elasticsearch连接检查", false,
                        "连接失败: HTTP " + status, "error");
            } else {
                result.addItem("目标库连接", "目标Elasticsearch连接检查", true,
                        "Elasticsearch目标连接成功", "info");

                String version = null;
                try {
                    version = com.google.gson.JsonParser.parseString(info[1]).getAsJsonObject()
                            .getAsJsonObject("version").get("number").getAsString();
                } catch (Exception ignore) {
                    // 版本信息缺失不阻塞
                }
                result.addItem("目标库版本", "目标Elasticsearch版本检查", true,
                        version != null ? "Elasticsearch " + version : "无法获取版本信息（非致命）",
                        version != null ? "info" : "warning");

                try {
                    String[] health = esHttpGet(targetConn, "/_cluster/health");
                    String clusterStatus = com.google.gson.JsonParser.parseString(health[1])
                            .getAsJsonObject().get("status").getAsString();
                    boolean healthy = !"red".equalsIgnoreCase(clusterStatus);
                    result.addItem("目标集群健康", "目标Elasticsearch集群健康检查", healthy,
                            "集群状态: " + clusterStatus + (healthy ? "" : "，red 状态无法可靠写入"),
                            healthy ? "info" : "error");
                } catch (Exception e) {
                    result.addItem("目标集群健康", "目标Elasticsearch集群健康检查", true,
                            "无法获取集群健康（非致命）: " + e.getMessage(), "warning");
                }
            }
        } catch (Exception e) {
            logger.error("Elasticsearch 目标校验失败: {}", e.getMessage());
            result.addItem("目标库连接", "目标Elasticsearch连接检查", false,
                    "连接失败: " + e.getMessage(), "error");
        }

        boolean allPassed = result.getCheckItems().stream().allMatch(ValidationResult.CheckItem::isPassed);
        result.setAllPassed(allPassed);
        return result;
    }

    /**
     * MongoDB 同步预校验：仅支持 mongodb→mongodb（副本集到副本集）。
     * 检查项：类型组合互斥、源/目标连接、源/目标副本集（Change Streams 依赖副本集 oplog）、
     * 目标节点可写（Primary）、服务器版本。
     */
    private ValidationResult validateForMongoMigration(String sourceConnection, String targetConnection,
                                                       String migrationMode,
                                                       boolean sourceIsMongo, boolean targetIsMongo,
                                                       ValidationResult result) {
        if (!sourceIsMongo || !targetIsMongo) {
            result.addItem("类型组合", "MongoDB 仅支持 MongoDB 到 MongoDB 的同步", false,
                    "MongoDB 只能与 MongoDB 互相同步，不支持与其它数据库类型组合", "error");
            result.setAllPassed(false);
            return result;
        }

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        if (!sourceConn.isMongo() || !targetConn.isMongo()) {
            result.addItem("连接串格式", "MongoDB 连接串格式检查", false,
                    "连接串应以 mongodb:// 开头", "error");
            result.setAllPassed(false);
            return result;
        }

        checkMongoEndpoint(sourceConn, "源库", true, migrationMode, result);
        checkMongoEndpoint(targetConn, "目标库", false, migrationMode, result);

        boolean allPassed = result.getCheckItems().stream().allMatch(ValidationResult.CheckItem::isPassed);
        result.setAllPassed(allPassed);
        return result;
    }

    private void checkMongoEndpoint(ParsedConnection conn, String side, boolean isSource,
                                    String migrationMode, ValidationResult result) {
        boolean needIncrement = "fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode);

        try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
            org.bson.Document hello = mongoHello(client);
            result.addItem(side + "连接", side + "MongoDB连接检查", true,
                    "MongoDB" + side + "连接成功", "info");

            // 副本集/分片集群检查：源端 Change Streams 依赖副本集 oplog；分片集群经 mongos
            // 打开的 change stream 会聚合全部 shard 的写入（官方集群级 CDC 机制），等同通过。
            // 分片集群请直接填 mongos 地址，勿逐个 shard 直连——那会读到 chunk 迁移的孤儿文档。
            String setName = hello.getString("setName");
            boolean isReplicaSet = setName != null && !setName.isEmpty();
            boolean isSharded = isMongos(hello);
            boolean streamCapable = isReplicaSet || isSharded;
            String topology = isSharded ? "分片集群（mongos）"
                    : (isReplicaSet ? "副本集（" + setName + "）" : "独立节点");
            if (isSource) {
                boolean passed = streamCapable || !needIncrement;
                String message = streamCapable
                        ? "源库为" + topology + "，支持 Change Streams 增量捕获"
                        : "源库为独立节点" + (needIncrement ? "，增量同步（Change Streams）需要副本集或分片集群（mongos）" : "，仅全量可运行，建议使用副本集");
                result.addItem("源库副本集", "增量同步需要源库为副本集或分片集群", passed, message,
                        needIncrement && !streamCapable ? "error" : (streamCapable ? "info" : "warning"));
            } else {
                String message = streamCapable
                        ? "目标库为" + topology
                        : "目标库为独立节点，本功能定义为副本集/分片集群间同步，建议使用副本集或 mongos";
                result.addItem("目标库副本集", "目标库应为副本集或分片集群", streamCapable, message,
                        streamCapable ? "info" : "error");

                // 目标可写：directConnection 连的节点必须是 Primary 才能写入
                Boolean writable = hello.getBoolean("isWritablePrimary");
                if (writable == null) {
                    writable = hello.getBoolean("ismaster");
                }
                boolean isWritable = Boolean.TRUE.equals(writable);
                result.addItem("目标库可写", "目标节点需为 Primary（可写）", isWritable,
                        isWritable ? "目标节点为 Primary，可写入" : "目标节点不可写（非 Primary），请连接副本集 Primary 节点",
                        isWritable ? "info" : "error");
            }

            // 版本检查：Change Streams 需要 4.0+（resumeAfter 恢复语义稳定）
            try {
                org.bson.Document buildInfo = client.getDatabase("admin")
                        .runCommand(new org.bson.Document("buildInfo", 1));
                String version = buildInfo.getString("version");
                boolean versionOk = true;
                if (version != null && isSource && needIncrement) {
                    try {
                        int major = Integer.parseInt(version.split("\\.")[0]);
                        versionOk = major >= 4;
                    } catch (NumberFormatException ignore) {
                        // 无法解析版本号时不阻塞
                    }
                }
                result.addItem(side + "版本", side + "MongoDB版本检查", versionOk,
                        "MongoDB " + version + (versionOk ? "" : "，增量同步（Change Streams）需要 4.0 及以上版本"),
                        versionOk ? "info" : "error");
            } catch (Exception e) {
                result.addItem(side + "版本", side + "MongoDB版本检查", true,
                        "无法获取版本信息（非致命）: " + e.getMessage(), "warning");
            }
        } catch (Exception e) {
            logger.error("MongoDB {} 校验失败: {}", side, e.getMessage());
            result.addItem(side + "连接", side + "MongoDB连接检查", false,
                    "连接失败: " + e.getMessage(), "error");
        }
    }

    /**
     * 检查 Oracle 源库的 LogMiner/归档日志配置（增量同步所需）。
     */
    private void checkOracleLogMinerConfig(Connection conn, ValidationResult result, String migrationMode) {
        boolean needIncrement = "fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode);

        // 1. 检查归档日志模式（增量同步需要 ARCHIVELOG）
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT LOG_MODE FROM V$DATABASE")) {
            if (rs.next()) {
                String logMode = rs.getString(1);
                boolean passed = !needIncrement || "ARCHIVELOG".equalsIgnoreCase(logMode);
                String message = "ARCHIVELOG".equalsIgnoreCase(logMode)
                        ? "数据库处于 ARCHIVELOG 模式，支持 LogMiner 增量捕获"
                        : "数据库处于 " + logMode + " 模式" + (needIncrement ? "，增量同步需要 ARCHIVELOG 模式" : "");
                result.addItem("归档日志模式", "增量同步需要源数据库处于 ARCHIVELOG 模式", passed, message, needIncrement ? "error" : "info");
            }
        } catch (SQLException e) {
            result.addItem("归档日志模式", "增量同步需要源数据库处于 ARCHIVELOG 模式", false,
                "检查失败: " + e.getMessage(), "warning");
        }

        if (!needIncrement) {
            return;
        }

        // 2. 检查补充日志（LogMiner 需要 PRIMARY KEY 补充日志才能识别 PK 列）
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE")) {
            if (rs.next()) {
                String pkLog = rs.getString(1);
                String allLog = rs.getString(2);
                boolean pkEnabled = "YES".equalsIgnoreCase(pkLog);
                boolean allEnabled = "YES".equalsIgnoreCase(allLog);
                boolean passed = pkEnabled || allEnabled;
                String message = passed
                        ? (allEnabled ? "已启用 ALL 补充日志" : "已启用 PRIMARY KEY 补充日志")
                        : "未启用补充日志，增量同步需要至少启用 PRIMARY KEY 补充日志";
                result.addItem("补充日志", "增量同步需要源数据库启用补充日志", passed, message, "error");
            }
        } catch (SQLException e) {
            result.addItem("补充日志", "增量同步需要源数据库启用补充日志", false,
                "检查失败: " + e.getMessage(), "warning");
        }

        // 3. 检查当前用户是否有 LogMiner 相关权限（SELECT ANY TRANSACTION / EXECUTE ON DBMS_LOGMNR）
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT PRIVILEGE FROM USER_SYS_PRIVS WHERE PRIVILEGE IN ('SELECT ANY TRANSACTION','SELECT ANY DICTIONARY')")) {
            boolean hasSelectAnyTxn = false;
            boolean hasSelectAnyDict = false;
            while (rs.next()) {
                String priv = rs.getString(1);
                if ("SELECT ANY TRANSACTION".equalsIgnoreCase(priv)) hasSelectAnyTxn = true;
                if ("SELECT ANY DICTIONARY".equalsIgnoreCase(priv)) hasSelectAnyDict = true;
            }
            boolean passed = hasSelectAnyTxn || hasSelectAnyDict;
            String message = passed
                    ? "用户具备 LogMiner 所需权限(" + (hasSelectAnyTxn ? "SELECT ANY TRANSACTION" : "SELECT ANY DICTIONARY") + ")"
                    : "用户缺少 LogMiner 所需权限（SELECT ANY TRANSACTION 或 SELECT ANY DICTIONARY）";
            result.addItem("LogMiner权限", "增量同步需要 SELECT ANY TRANSACTION 或 SELECT ANY DICTIONARY 权限", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("LogMiner权限", "增量同步需要 LogMiner 相关权限", false,
                "检查失败: " + e.getMessage(), "warning");
        }
    }
    
    private void checkPgWalConfig(Connection conn, ValidationResult result) {
        checkPgWalConfigLabeled(conn, result, "源库");
    }

    /** WAL 级别检查（label 区分源库/目标库；双向灾备时目标库也需 logical WAL 供反向 capture）。 */
    private void checkPgWalConfigLabeled(Connection conn, ValidationResult result, String label) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
                if (rs.next()) {
                    String walLevel = rs.getString(1);
                    boolean passed = "logical".equalsIgnoreCase(walLevel);
                    String message = passed ? label + "WAL级别为logical，支持增量同步" :
                        label + "WAL级别为" + walLevel + "，增量同步需要设置为logical";
                    result.addItem(label + "WAL级别", "增量同步需要" + label + "WAL级别为logical", passed, message, "error");
                }
            }
        } catch (SQLException e) {
            result.addItem(label + "WAL级别", "增量同步需要" + label + "WAL级别为logical", false,
                "检查失败: " + e.getMessage(), "warning");
        }
    }
    
    private String buildJdbcUrl(ParsedConnection conn, boolean isPg) {
        if (isPg) {
            return String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified",
                conn.host, conn.port, conn.database);
        }
        return buildJdbcUrl(conn);
    }

    public ValidationResult validateForMigration(String sourceConnection, String targetConnection, String migrationMode) {
        ValidationResult result = new ValidationResult();
        
        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        
        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn),
                sourceConn.username, sourceConn.password);
             Connection targetDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(targetConn),
                targetConn.username, targetConn.password)) {
            
            String sourceVersion = getMySQLVersion(sourceDb);
            String targetVersion = getMySQLVersion(targetDb);
            
            if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                checkBinlogEnabled(sourceDb, result);
                checkBinlogFormat(sourceDb, result);
                checkBinlogRowImage(sourceDb, result);
                checkServerId(sourceDb, sourceVersion, result);
            }
            
            checkVersionCompatibility(sourceVersion, targetVersion, result);
            checkSqlModeCompatibility(sourceDb, targetDb, result);
            checkSourcePermissions(sourceDb, migrationMode, result);
            checkTargetPermissions(targetDb, targetVersion, result);
            
        } catch (SQLException e) {
            logger.error("数据库校验失败: {}", e.getMessage());
            result.addItem("连接检查", "数据库连接检查", false, "连接失败: " + e.getMessage(), "error");
        }
        
        boolean allPassed = result.getCheckItems().stream().allMatch(ValidationResult.CheckItem::isPassed);
        result.setAllPassed(allPassed);
        
        return result;
    }

    private String getMySQLVersion(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return "unknown";
    }

    private void checkBinlogEnabled(Connection conn, ValidationResult result) {
        try {
            String logBin = getVariable(conn, "log_bin");
            boolean passed = "ON".equalsIgnoreCase(logBin) || "1".equals(logBin);
            String message = passed ? "Binlog已开启" : "Binlog未开启，增量同步需要开启binlog";
            result.addItem("Binlog开启状态", "增量同步需要源数据库开启binlog", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("Binlog开启状态", "增量同步需要源数据库开启binlog", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkBinlogFormat(Connection conn, ValidationResult result) {
        try {
            String format = getVariable(conn, "binlog_format");
            boolean passed = "ROW".equalsIgnoreCase(format);
            String message = passed ? "Binlog格式为ROW" : "当前Binlog格式为" + format + "，需要设置为ROW";
            result.addItem("Binlog格式", "增量同步需要Binlog格式为ROW", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("Binlog格式", "增量同步需要Binlog格式为ROW", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkBinlogRowImage(Connection conn, ValidationResult result) {
        try {
            String rowImage = getVariable(conn, "binlog_row_image");
            if (rowImage == null || rowImage.isEmpty()) {
                result.addItem("Binlog Row Image", "binlog_row_image需设置为FULL", true, "参数不存在(可能是MySQL 5.5及以下版本)，跳过检查", "warning");
                return;
            }
            boolean passed = "FULL".equalsIgnoreCase(rowImage);
            String message = passed ? "binlog_row_image为FULL" : "当前binlog_row_image为" + rowImage + "，需要设置为FULL";
            result.addItem("Binlog Row Image", "binlog_row_image需设置为FULL", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("Binlog Row Image", "binlog_row_image需设置为FULL", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkServerId(Connection conn, String version, ValidationResult result) {
        try {
            String serverIdStr = getVariable(conn, "server_id");
            if (serverIdStr == null || serverIdStr.isEmpty() || "0".equals(serverIdStr)) {
                result.addItem("Server ID", "增量同步需要设置server_id", false, "server_id未设置或为0", "error");
                return;
            }
            
            long serverId = Long.parseLong(serverIdStr);
            boolean passed;
            String message;
            
            if (isVersionAtLeast(version, "5.7.0")) {
                passed = serverId >= 1 && serverId <= 4294967296L;
                message = passed ? "server_id=" + serverId + "，符合要求" : "server_id=" + serverId + "，MySQL 5.7+需要设置在1-4294967296之间";
            } else {
                passed = serverId >= 2 && serverId <= 4294967296L;
                message = passed ? "server_id=" + serverId + "，符合要求" : "server_id=" + serverId + "，MySQL 5.6及以下需要设置在2-4294967296之间";
            }
            
            result.addItem("Server ID", "增量同步需要设置server_id", passed, message, "error");
        } catch (Exception e) {
            result.addItem("Server ID", "增量同步需要设置server_id", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkVersionCompatibility(String sourceVersion, String targetVersion, ValidationResult result) {
        boolean passed = compareVersions(sourceVersion, targetVersion) <= 0;
        String message = passed 
            ? "源数据库版本(" + sourceVersion + ") <= 目标数据库版本(" + targetVersion + ")" 
            : "源数据库版本(" + sourceVersion + ") > 目标数据库版本(" + targetVersion + ")，可能导致兼容性问题";
        result.addItem("版本兼容性", "源数据库版本不能高于目标数据库版本", passed, message, "error");
    }

    private void checkSqlModeCompatibility(Connection sourceDb, Connection targetDb, ValidationResult result) {
        try {
            String sourceSqlMode = getVariable(sourceDb, "sql_mode");
            String targetSqlMode = getVariable(targetDb, "sql_mode");
            
            Set<String> sourceModes = parseSqlMode(sourceSqlMode);
            Set<String> targetModes = parseSqlMode(targetSqlMode);
            
            boolean passed = sourceModes.equals(targetModes);
            String message = passed 
                ? "sql_mode一致" 
                : "源数据库sql_mode(" + sourceSqlMode + ")与目标数据库(" + targetSqlMode + ")不一致";
            result.addItem("SQL Mode一致性", "源和目标数据库sql_mode需要一致", passed, message, "warning");
        } catch (SQLException e) {
            result.addItem("SQL Mode一致性", "源和目标数据库sql_mode需要一致", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkSourcePermissions(Connection conn, String migrationMode, ValidationResult result) {
        try {
            Set<String> grantedPrivileges = getGrantedPrivileges(conn);
            
            Set<String> requiredPrivileges = new HashSet<>(Arrays.asList("SELECT", "SHOW VIEW", "EVENT"));
            
            if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                requiredPrivileges.addAll(Arrays.asList("LOCK TABLES", "REPLICATION SLAVE", "REPLICATION CLIENT"));
            }
            
            Set<String> missingPrivileges = new HashSet<>();
            for (String priv : requiredPrivileges) {
                boolean hasPriv = grantedPrivileges.contains(priv) || 
                                  grantedPrivileges.contains("ALL PRIVILEGES") ||
                                  grantedPrivileges.contains("ALL");
                if (!hasPriv) {
                    missingPrivileges.add(priv);
                }
            }
            
            boolean passed = missingPrivileges.isEmpty();
            String mode = "fullAndIncre".equals(migrationMode) ? "全量+增量" : "full".equals(migrationMode) ? "全量" : "增量";
            String message = passed 
                ? mode + "同步所需权限已具备" 
                : mode + "同步缺少权限: " + String.join(", ", missingPrivileges);
            result.addItem("源数据库权限", mode + "同步需要SELECT、SHOW VIEW、EVENT等权限", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("源数据库权限", "检查源数据库账号权限", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkTargetPermissions(Connection conn, String targetVersion, ValidationResult result) {
        try {
            Set<String> grantedPrivileges = getGrantedPrivileges(conn);
            
            Set<String> requiredPrivileges = new HashSet<>(Arrays.asList(
                "SELECT", "CREATE", "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", 
                "CREATE VIEW", "CREATE ROUTINE", "REFERENCES"
            ));
            
            if (isVersionInRange(targetVersion, "8.0.14", "8.0.18")) {
                requiredPrivileges.add("SESSION_VARIABLES_ADMIN");
            }
            
            Set<String> missingPrivileges = new HashSet<>();
            for (String priv : requiredPrivileges) {
                boolean hasPriv = grantedPrivileges.contains(priv) || 
                                  grantedPrivileges.contains("ALL PRIVILEGES") ||
                                  grantedPrivileges.contains("ALL");
                if (!hasPriv) {
                    missingPrivileges.add(priv);
                }
            }
            
            boolean passed = missingPrivileges.isEmpty();
            String message = passed 
                ? "目标数据库所需权限已具备" 
                : "目标数据库缺少权限: " + String.join(", ", missingPrivileges);
            result.addItem("目标数据库权限", "目标数据库需要SELECT、CREATE、DROP、INSERT、UPDATE、ALTER等权限", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("目标数据库权限", "检查目标数据库账号权限", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private String getVariable(Connection conn, String variableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE '" + variableName + "'")) {
            if (rs.next()) {
                return rs.getString("Value");
            }
        }
        return null;
    }

    private Set<String> getGrantedPrivileges(Connection conn) throws SQLException {
        Set<String> privileges = new HashSet<>();
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW GRANTS")) {
            while (rs.next()) {
                String grant = rs.getString(1);
                Set<String> extracted = parseGrants(grant);
                privileges.addAll(extracted);
            }
        }
        
        return privileges;
    }

    private Set<String> parseGrants(String grant) {
        Set<String> privileges = new HashSet<>();
        
        int start = grant.indexOf("GRANT ");
        int end = grant.indexOf(" ON ");
        if (start >= 0 && end > start) {
            String privStr = grant.substring(start + 6, end).trim();
            String[] privs = privStr.split(",");
            for (String priv : privs) {
                privileges.add(priv.trim().toUpperCase());
            }
        }
        
        return privileges;
    }

    private Set<String> parseSqlMode(String sqlMode) {
        Set<String> modes = new HashSet<>();
        if (sqlMode != null && !sqlMode.isEmpty()) {
            String[] parts = sqlMode.split(",");
            for (String part : parts) {
                modes.add(part.trim().toUpperCase());
            }
        }
        return modes;
    }

    private int compareVersions(String v1, String v2) {
        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");
        
        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            int num1 = i < parts1.length ? parseVersionPart(parts1[i]) : 0;
            int num2 = i < parts2.length ? parseVersionPart(parts2[i]) : 0;
            if (num1 != num2) {
                return num1 - num2;
            }
        }
        return 0;
    }

    private int parseVersionPart(String part) {
        try {
            return Integer.parseInt(part.replaceAll("[^0-9].*", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private boolean isVersionAtLeast(String version, String minVersion) {
        return compareVersions(version, minVersion) >= 0;
    }

    private boolean isVersionInRange(String version, String minVersion, String maxVersion) {
        return compareVersions(version, minVersion) >= 0 && compareVersions(version, maxVersion) <= 0;
    }
}
