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
    ORACLE_SCN_GET_FAILED("E2007", "Oracle SCN获取失败", "请检查Oracle连接是否正常，确认用户有SELECT ANY DICTIONARY权限且数据库处于ARCHIVELOG模式"),
    ORACLE_LOGMINER_START_FAILED("E2008", "Oracle LogMiner会话启动失败", "请确认数据库处于ARCHIVELOG模式，用户具有EXECUTE_CATALOG_ROLE权限，且redo日志可访问"),

    CAPTURE_PROCESS_START_FAILED("E3001", "Capture进程启动失败", "请检查Agent日志，确认capture模块JAR包存在且配置正确"),
    CAPTURE_PROCESS_CRASHED("E3002", "Capture进程异常退出", "请检查Agent日志，确认源数据库连接正常且binlog/WAL可访问"),
    EXTRACT_PROCESS_START_FAILED("E3003", "Extract进程启动失败", "请检查Agent日志，确认extract模块JAR包存在且配置正确"),
    INCREMENT_PROCESS_START_FAILED("E3004", "增量同步进程启动失败", "请检查Agent日志，确认increment模块JAR包存在且配置正确"),
    INCREMENT_PROCESS_CRASHED("E3005", "增量同步进程异常退出", "请检查Agent日志，可能是目标数据库连接中断或SQL执行异常"),
    CAPTURE_POSITION_UNAVAILABLE("E3006", "源端日志已被清理，续传位点不可用", "源库的binlog/WAL/redo已过保留期，增量位点无法恢复。请延长源库日志保留期后重新初始化全量同步"),
    PROCESS_CRASH_LOOP("E3007", "子进程反复崩溃重启", "进程能起来但很快再次退出（crash-loop），任务表面在跑实则不断丢进度。请查看该子进程日志定位崩溃原因"),
    TASK_DISK_QUOTA_EXCEEDED("E3008", "任务磁盘用量超限", "任务目录 files/<taskId> 超过 task.disk.quota.mb 配额。请清理历史任务目录、下调日志级别或调大配额"),
    INCREMENT_CONVERT_FAILED("E3009", "增量事件转换失败", "该事件无法转换成目标端SQL（未知类型/结构不匹配/数据异常）。可在死信页面裁决跳过该事件，或将 increment.convert.error.policy 设为 DEAD_LETTER 让此类事件自动记死信并跳过"),
    THL_FILE_UNREADABLE("E3010", "THL文件读取中断", "THL文件损坏或读取过程异常，已在断点处停止且未跳过剩余事件。请检查磁盘与 thl_output 目录，必要时重新初始化增量"),
    BIDI_WRITE_CONFLICT("E3011", "双向同步写写冲突", "两端同时修改了同一行，且冲突策略配置为 ERROR（不自动丢写）。请人工确认应保留哪一端的值，或改用 LWW_SOURCE_TS/NODE_PRIORITY 策略自动裁决"),
    // E3012 未分配（历史空位，新增错误码请顺延，不要复用）
    ROUTE_TYPED_PIPELINE_UNAVAILABLE("E3013", "汇聚/拆分事件缺少类型化值", "命中路由规则的表其事件没有类型化值（rows_typed），无法生成带来源标识列的 DML——文本路径的 UPDATE/DELETE 只按源主键定位，会改到同一张汇聚表里其它来源的同主键行，因此已停止应用。请检查该表的路由规则是否配错（不该汇聚的表被规则命中）、源端 binlog_row_image 是否为 FULL，以及该源→目标引擎对是否支持类型化管道（increment.typed.pipeline.enabled 是否被关掉）"),
    CHECKPOINT_HYDRATE_FAILED("E3014", "位点回灌失败", "本地没有位点、又读不到中心库里的位点，无法判断这是首次启动还是跨机接管。此时若按首次启动去取源库当前位点，会静默跳过崩溃到接管之间的全部变更，因此任务停在这里等人处置。请检查 agent 到元数据库的连通性（agent.properties 的 mysql.db.*）后重启任务；确认这确实是一个全新任务时，可临时将 checkpoint.hydrate.fail.stop 设为 false"),

    ELASTIC_PROCESS_START_FAILED("E3101", "Elastic同步进程启动失败", "请检查Agent日志，确认elastic模块JAR包存在且配置正确"),
    ELASTIC_SYNC_FAILED("E3102", "Elastic同步失败", "请检查Agent日志，确认Elasticsearch连接正常、索引可写且源库binlog可访问"),

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
