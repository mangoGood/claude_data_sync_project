package com.migration.config;

import java.util.Properties;

public class DatabaseConfig {
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String dbType = "mysql";
    private String schema;

    public DatabaseConfig(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public DatabaseConfig(String host, int port, String database, String username, String password, String dbType) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.dbType = dbType != null ? dbType : "mysql";
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType != null ? dbType : "mysql";
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getJdbcUrl() {
        if ("postgresql".equals(dbType)) {
            String currentSchema = (schema != null && !schema.isEmpty()) ? schema : "public";
            return String.format("jdbc:postgresql://%s:%d/%s?currentSchema=%s&stringtype=unspecified",
                               host, port, database, currentSchema);
        }
        if ("oracle".equals(dbType)) {
            // Oracle 使用 service name 方式连接: jdbc:oracle:thin:@host:port/service
            // database 字段存储服务名/SID
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            return String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        }
        return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&autoReconnect=true&connectTimeout=30000&socketTimeout=0",
                           host, port, database);
    }

    public String getJdbcDriverClass() {
        if ("postgresql".equals(dbType)) {
            return "org.postgresql.Driver";
        }
        if ("oracle".equals(dbType)) {
            return "oracle.jdbc.OracleDriver";
        }
        return "com.mysql.cj.jdbc.Driver";
    }

    public String getCreateDatabaseSql() {
        if ("postgresql".equals(dbType)) {
            return "CREATE DATABASE \"" + database + "\" WITH ENCODING 'UTF8'";
        }
        // Oracle 不支持通过 JDBC 创建"数据库"（即 PDB），此处返回空语句
        if ("oracle".equals(dbType)) {
            return "SELECT 1 FROM dual";
        }
        return "CREATE DATABASE IF NOT EXISTS `" + database + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
    }

    public String getRootJdbcUrl() {
        if ("postgresql".equals(dbType)) {
            return String.format("jdbc:postgresql://%s:%d/postgres?stringtype=unspecified", host, port);
        }
        if ("oracle".equals(dbType)) {
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            return String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        }
        return String.format("jdbc:mysql://%s:%d/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8",
                           host, port);
    }

    public boolean isOracle() {
        return "oracle".equalsIgnoreCase(dbType);
    }

    @Override
    public String toString() {
        return String.format("DatabaseConfig{host='%s', port=%d, database='%s', username='%s', dbType='%s'}",
                           host, port, database, username, dbType);
    }
}
