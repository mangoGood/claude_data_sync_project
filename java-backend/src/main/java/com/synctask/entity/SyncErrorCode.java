package com.synctask.entity;

public enum SyncErrorCode {

    SOURCE_DB_CONNECTION_FAILED("E1001", "源数据库连接失败", "请检查源数据库地址、端口、用户名和密码是否正确，确认数据库服务已启动且网络可达"),
    TARGET_DB_CONNECTION_FAILED("E1002", "目标数据库连接失败", "请检查目标数据库地址、端口、用户名和密码是否正确，确认数据库服务已启动且网络可达"),
    SOURCE_DB_AUTH_FAILED("E1003", "源数据库认证失败", "请检查源数据库用户名和密码是否正确，确认用户有远程登录权限"),
    TARGET_DB_AUTH_FAILED("E1004", "目标数据库认证失败", "请检查目标数据库用户名和密码是否正确，确认用户有远程登录权限"),
    SOURCE_DB_UNREACHABLE("E1005", "源数据库网络不可达", "请检查源数据库主机地址是否正确，确认网络连通性和防火墙规则"),
    TARGET_DB_UNREACHABLE("E1006", "目标数据库网络不可达", "请检查目标数据库主机地址是否正确，确认网络连通性和防火墙规则"),

    BINLOG_NOT_ENABLED("E2001", "源数据库Binlog未开启", "请在MySQL配置文件中设置 log_bin=ON 并重启数据库服务"),
    BINLOG_FORMAT_ERROR("E2002", "Binlog格式不是ROW", "请在MySQL配置文件中设置 binlog_format=ROW 并重启数据库服务"),
    BINLOG_ROW_IMAGE_ERROR("E2003", "Binlog Row Image不是FULL", "请在MySQL配置文件中设置 binlog_row_image=FULL 并重启数据库服务"),
    SERVER_ID_NOT_SET("E2004", "源数据库server_id未设置", "请在MySQL配置文件中设置 server_id 为非0值并重启数据库服务"),
    CHECKPOINT_INIT_FAILED("E2005", "Checkpoint初始化失败", "请检查源数据库连接是否正常，确认用户有REPLICATION权限"),
    WAL_LSN_GET_FAILED("E2006", "PostgreSQL WAL LSN获取失败", "请检查PostgreSQL连接是否正常，确认用户有replication权限"),

    CAPTURE_PROCESS_START_FAILED("E3001", "Capture进程启动失败", "请检查Agent日志，确认capture模块JAR包存在且配置正确"),
    CAPTURE_PROCESS_CRASHED("E3002", "Capture进程异常退出", "请检查Agent日志，确认源数据库连接正常且binlog/WAL可访问"),
    EXTRACT_PROCESS_START_FAILED("E3003", "Extract进程启动失败", "请检查Agent日志，确认extract模块JAR包存在且配置正确"),
    INCREMENT_PROCESS_START_FAILED("E3004", "增量同步进程启动失败", "请检查Agent日志，确认increment模块JAR包存在且配置正确"),
    INCREMENT_PROCESS_CRASHED("E3005", "增量同步进程异常退出", "请检查Agent日志，可能是目标数据库连接中断或SQL执行异常"),

    FULL_MIGRATION_FAILED("E4001", "全量同步失败", "请检查Agent日志，确认源库和目标库连接正常，表结构和数据无异常"),
    FULL_MIGRATION_TIMEOUT("E4002", "全量同步超时", "请检查数据量是否过大，考虑分批同步或优化网络带宽"),
    TARGET_DB_WRITE_FAILED("E4003", "目标数据库写入失败", "请检查目标数据库磁盘空间、表结构是否与源库一致、是否有写入权限"),
    DUPLICATE_KEY_ERROR("E4004", "主键冲突写入失败", "请检查目标库是否已存在相同主键的数据，可通过恢复任务自动跳过重复数据"),

    SOURCE_DB_CONFIG_EMPTY("E5001", "源数据库配置为空", "请检查任务创建时源数据库连接信息是否填写完整"),
    TARGET_DB_CONFIG_EMPTY("E5002", "目标数据库配置为空", "请检查任务创建时目标数据库连接信息是否填写完整"),
    CONNECTION_STRING_PARSE_FAILED("E5003", "连接串解析失败", "请检查连接串格式是否正确，正确格式: mysql://user:pass@host:port 或 postgresql://user:pass@host:port"),

    UNKNOWN_ERROR("E9999", "未知错误", "请查看Agent日志获取详细错误信息，或联系技术支持");

    private final String code;
    private final String description;
    private final String solution;

    SyncErrorCode(String code, String description, String solution) {
        this.code = code;
        this.description = description;
        this.solution = solution;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public String getSolution() {
        return solution;
    }

    public static SyncErrorCode fromCode(String code) {
        if (code == null || code.isEmpty()) {
            return null;
        }
        for (SyncErrorCode errorCode : values()) {
            if (errorCode.code.equals(code)) {
                return errorCode;
            }
        }
        return UNKNOWN_ERROR;
    }

    public static SyncErrorCode fromException(Exception e) {
        if (e == null || e.getMessage() == null) {
            return UNKNOWN_ERROR;
        }
        String msg = e.getMessage().toLowerCase();

        if (msg.contains("access denied") || msg.contains("authentication failed") || msg.contains("28000")) {
            if (msg.contains("source") || msg.contains("源")) {
                return SOURCE_DB_AUTH_FAILED;
            }
            return TARGET_DB_AUTH_FAILED;
        }
        if (msg.contains("connection refused") || msg.contains("connect timed out") || msg.contains("no route to host")) {
            if (msg.contains("source") || msg.contains("源")) {
                return SOURCE_DB_UNREACHABLE;
            }
            return TARGET_DB_UNREACHABLE;
        }
        if (msg.contains("communications link failure") || msg.contains("unable to acquire jdbc connection")) {
            return SOURCE_DB_CONNECTION_FAILED;
        }
        if (msg.contains("duplicate entry") || msg.contains("1062") || msg.contains("duplicate key")) {
            return DUPLICATE_KEY_ERROR;
        }
        if (msg.contains("binlog") && msg.contains("not enabled")) {
            return BINLOG_NOT_ENABLED;
        }
        if (msg.contains("binlog_format") || msg.contains("binlog format")) {
            return BINLOG_FORMAT_ERROR;
        }

        return UNKNOWN_ERROR;
    }
}
