package com.migration.agent.util;

import com.migration.dialect.SqlDialect;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectionStringParser {
    private static final Pattern MYSQL_PATTERN = 
        Pattern.compile("mysql://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.+))?");
    
    private static final Pattern PG_PATTERN = 
        Pattern.compile("postgresql://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.+))?");
    
    private static final Pattern ORACLE_PATTERN = 
        Pattern.compile("oracle://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.+))?");
    
    private static final Pattern JDBC_MYSQL_PATTERN =
        Pattern.compile("jdbc:mysql://([^:]+):(\\d+)/([^?]+)(?:\\?.*)?");
    
    private static final Pattern JDBC_PG_PATTERN =
        Pattern.compile("jdbc:postgresql://([^:]+):(\\d+)/([^?]+)(?:\\?.*)?");

    /**
     * Oracle JDBC Thin 连接串格式：
     *   jdbc:oracle:thin:@host:port:SID
     *   jdbc:oracle:thin:@host:port/SERVICE
     *   jdbc:oracle:thin:@//host:port/SERVICE
     */
    private static final Pattern JDBC_ORACLE_SID_PATTERN =
        Pattern.compile("jdbc:oracle:thin:@([^:]+):(\\d+):([^?]+)");

    private static final Pattern JDBC_ORACLE_SERVICE_PATTERN =
        Pattern.compile("jdbc:oracle:thin:@(?:/{0,2})?([^:/]+):(\\d+)/([^?]+)");

    public static class ConnectionInfo {
        private String host;
        private int port;
        private String database;
        private String username;
        private String password;
        private String type = "mysql";
        
        public String getHost() {
            return host;
        }
        
        public void setHost(String host) {
            this.host = host;
        }
        
        public int getPort() {
            return port;
        }
        
        public void setPort(int port) {
            this.port = port;
        }
        
        public String getDatabase() {
            return database;
        }
        
        public void setDatabase(String database) {
            this.database = database;
        }
        
        public String getUsername() {
            return username;
        }
        
        public void setUsername(String username) {
            this.username = username;
        }
        
        public String getPassword() {
            return password;
        }
        
        public void setPassword(String password) {
            this.password = password;
        }
        
        public String getType() {
            return type;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        
        @Override
        public String toString() {
            // 协议名（mysql/postgresql/oracle）由方言归一，未知类型回退 mysql
            String protocol = SqlDialect.forType(type).getType();
            return String.format("%s://%s:***@%s:%d/%s", protocol, username, host, port, database);
        }
    }
    
    public static ConnectionInfo parse(String connectionString) {
        if (connectionString == null || connectionString.trim().isEmpty()) {
            return null;
        }
        
        String trimmed = connectionString.trim();
        
        if (trimmed.startsWith("jdbc:postgresql://")) {
            return parseJdbcUrl(trimmed, JDBC_PG_PATTERN, "postgresql");
        }
        if (trimmed.startsWith("jdbc:mysql://")) {
            return parseJdbcUrl(trimmed, MYSQL_PATTERN, "mysql");
        }
        if (trimmed.startsWith("jdbc:oracle:thin:@")) {
            return parseOracleJdbcUrl(trimmed);
        }
        
        Pattern pattern;
        if (trimmed.startsWith("postgresql://")) {
            pattern = PG_PATTERN;
        } else if (trimmed.startsWith("oracle://")) {
            pattern = ORACLE_PATTERN;
        } else {
            pattern = MYSQL_PATTERN;
        }
        
        Matcher matcher = pattern.matcher(trimmed);
        
        if (!matcher.matches()) {
            String expectedFormat;
            if (pattern == PG_PATTERN) {
                expectedFormat = "postgresql://user:pass@host:port/db";
            } else if (pattern == ORACLE_PATTERN) {
                expectedFormat = "oracle://user:pass@host:port/service";
            } else {
                expectedFormat = "mysql://user:pass@host:port/db";
            }
            throw new IllegalArgumentException("Invalid connection string format. Expected: " + expectedFormat);
        }
        
        ConnectionInfo info = new ConnectionInfo();
        info.setUsername(matcher.group(1));
        info.setPassword(matcher.group(2));
        info.setHost(matcher.group(3));
        info.setPort(Integer.parseInt(matcher.group(4)));
        info.setDatabase(matcher.group(5) != null ? matcher.group(5) : "");
        if (trimmed.startsWith("postgresql://")) {
            info.setType("postgresql");
        } else if (trimmed.startsWith("oracle://")) {
            info.setType("oracle");
        } else {
            info.setType("mysql");
        }
        
        return info;
    }

    /**
     * 解析 Oracle JDBC Thin 连接串。
     * 支持以下格式：
     *   jdbc:oracle:thin:@host:port:SID
     *   jdbc:oracle:thin:@host:port/SERVICE
     *   jdbc:oracle:thin:@//host:port/SERVICE
     */
    private static ConnectionInfo parseOracleJdbcUrl(String url) {
        ConnectionInfo info = new ConnectionInfo();
        info.setType("oracle");

        // 尝试 SID 格式：jdbc:oracle:thin:@host:port:SID
        Matcher sidMatcher = JDBC_ORACLE_SID_PATTERN.matcher(url);
        // 尝试 SERVICE 格式：jdbc:oracle:thin:@host:port/SERVICE 或 jdbc:oracle:thin:@//host:port/SERVICE
        Matcher serviceMatcher = JDBC_ORACLE_SERVICE_PATTERN.matcher(url);

        if (serviceMatcher.matches()) {
            info.setHost(serviceMatcher.group(1));
            info.setPort(Integer.parseInt(serviceMatcher.group(2)));
            info.setDatabase(serviceMatcher.group(3));
        } else if (sidMatcher.matches()) {
            info.setHost(sidMatcher.group(1));
            info.setPort(Integer.parseInt(sidMatcher.group(2)));
            info.setDatabase(sidMatcher.group(3));
        } else {
            throw new IllegalArgumentException("Invalid Oracle JDBC connection string format: " + url);
        }

        // 解析查询参数中的 user/password
        String queryPart = "";
        int queryIdx = url.indexOf('?');
        if (queryIdx >= 0) {
            queryPart = url.substring(queryIdx + 1);
        }
        
        for (String param : queryPart.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                String key = kv[0].trim();
                String value = kv[1].trim();
                if ("user".equals(key) || "username".equals(key)) {
                    info.setUsername(value);
                } else if ("password".equals(key)) {
                    info.setPassword(value);
                }
            }
        }
        
        return info;
    }

    private static ConnectionInfo parseJdbcUrl(String url, Pattern pattern, String dbType) {
        Pattern jdbcPattern = "postgresql".equals(dbType) ? JDBC_PG_PATTERN : JDBC_MYSQL_PATTERN;
        Matcher matcher = jdbcPattern.matcher(url);
        
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid JDBC connection string format: " + url);
        }
        
        ConnectionInfo info = new ConnectionInfo();
        info.setHost(matcher.group(1));
        info.setPort(Integer.parseInt(matcher.group(2)));
        info.setDatabase(matcher.group(3));
        info.setType(dbType);

        String queryPart = "";
        int queryIdx = url.indexOf('?');
        if (queryIdx >= 0) {
            queryPart = url.substring(queryIdx + 1);
        }
        
        for (String param : queryPart.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                String key = kv[0].trim();
                String value = kv[1].trim();
                if ("user".equals(key) || "username".equals(key)) {
                    info.setUsername(value);
                } else if ("password".equals(key)) {
                    info.setPassword(value);
                }
            }
        }
        
        return info;
    }
}
