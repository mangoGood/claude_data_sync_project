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
    LOB_TYPED_PIPELINE_UNAVAILABLE("E3012", "大字段事件缺少类型化值", "事件里带的是大字段引用（内容在磁盘上），但没有类型化值（rows_typed），只能走文本路径——文本路径把参数拼成 SQL 字面量，会把 \"@lob:...\" 这串引用本身当成内容写进目标 BLOB/TEXT 列，长度和语法都看不出问题，属于静默数据损坏，因此已停止应用。请确认源→目标是 mysql→mysql、increment.typed.pipeline.enabled 未被关掉、且源端 binlog_row_image 为 FULL/NOBLOB"),
    ROUTE_TYPED_PIPELINE_UNAVAILABLE("E3013", "汇聚/拆分事件缺少类型化值", "命中路由规则的表其事件没有类型化值（rows_typed），无法生成带来源标识列的 DML——文本路径的 UPDATE/DELETE 只按源主键定位，会改到同一张汇聚表里其它来源的同主键行，因此已停止应用。请检查该表的路由规则是否配错（不该汇聚的表被规则命中）、源端 binlog_row_image 是否为 FULL，以及该源→目标引擎对是否支持类型化管道（increment.typed.pipeline.enabled 是否被关掉）"),
    BINLOG_EVENT_DESERIALIZE_FAILED("E3019", "binlog事件反序列化失败", "binlog 连接器解析不了这个事件，而它的默认行为是跳过——跳过一个行事件就是永久少同步几行数据，且不会有任何报错，因此已停止捕获。最常见的原因是源库开了 binlog_row_value_options=PARTIAL_JSON（JSON 差量更新，连接器不支持），其次是源库版本引入了新的事件编码。请把该参数置空后重启任务；确认这些事件确实可以丢弃时，可将 capture.deserialization.failure.policy 设为 SKIP"),
    BINLOG_COLUMN_LAYOUT_MISMATCH("E3021", "列布局与源库当前定义不一致", "行事件的列数与源库当前的表定义对不上，说明表结构在抽取过程中变更过（链路有延迟时做了 ALTER TABLE 加列/删列）。列清单是按当前定义查的，硬解会让整行的值与列错位——写进目标库的是合法值、看不出异常，属于静默数据损坏，因此已停止抽取。请把源库 binlog_row_metadata 设为 FULL（列名随 binlog 事件一起下发，不再依赖当前表定义），或等积压追平后再做 DDL"),
    BINLOG_EVENT_TYPE_UNSUPPORTED("E3020", "不支持的binlog事件类型", "抽取时遇到了不在已知可忽略清单里的事件类型，继续跑等于把它携带的数据静默丢掉，因此已停止。常见来源：源库开了 binlog_transaction_compression（整个事务被打包成 TRANSACTION_PAYLOAD）、binlog_row_value_options=PARTIAL_JSON（PARTIAL_UPDATE_ROWS_EVENT），或源库版本新增了事件类型。请核对错误信息里的类型名与源库参数；确认该类型不带数据时，可将 extract.unknown.event.policy 设为 SKIP 放行"),
    XA_BUFFER_QUOTA_EXCEEDED("E3018", "XA事务缓冲超限", "源库的 XA 事务在 binlog 里是分两段写的：行事件在 XA PREPARE 时刻就落盘，提交/回滚决议要等到 XA COMMIT/ROLLBACK。为了保证\"源库提交时目标库才提交\"（否则源库回滚的 XA 会在目标库留下永久幻影行），extract 会把未决分支整段缓冲到磁盘。现在缓冲量突破了配额，说明源库存在长期未提交的 XA 分支。请在源库执行 XA RECOVER 排查并提交/回滚这些分支；确认需要更大缓冲时，调大 sync.xa.branch.max.bytes / sync.xa.pending.max.bytes / sync.xa.pending.max.branches"),
    UNIQUE_KEY_CONFLICT("E3017", "唯一键冲突（非主键）", "目标端存在源端没有的唯一索引/约束，把这一行挡住了。主键冲突属于幂等重放可以忽略，但唯一键冲突忽略掉就是永久丢一行，因此默认停下等人处置。请核对两端的唯一索引差异；确认可以丢弃这类行时，将 increment.unique.conflict.policy 设为 IGNORE"),

    SCHEMA_TIMELINE_VERSION_MISSING("E3022", "表结构时序库缺少该位点的版本", "表结构时序库里没有这条事件所在位点的表结构版本，无法按\"事件当时\"的结构解析。常见原因：任务是在时序库启用之前建的（缺基线）、该表的基线因 CREATE TABLE ... AS SELECT 之类推不出结构而被标记不可用、或跨机接管时时序库没有随位点一起回灌。降级回查源库当前定义会退回到\"用现在的结构解释过去的事件\"，因此在 extract.schema.timeline.fallback=FAIL_STOP 下停止抽取。请确认时序库文件存在且已回灌；允许降级时将该参数设为 RESNAPSHOT"),
    SCHEMA_TIMELINE_DDL_PARSE_FAILED("E3023", "DDL 解析失败（表结构时序库）", "时序库解析不了这条 DDL，无法把它施加到表结构模型上，该表之后的版本都会失准。这通常意味着遇到了语法覆盖之外的 DDL 形态。错误信息里带有原始语句，请据此补语法；在补齐之前，该表会按 extract.schema.timeline.fallback 的设置降级回查源库当前定义（RESNAPSHOT）或停止抽取（FAIL_STOP）"),
    SCHEMA_TIMELINE_COLUMN_MISMATCH("E3024", "表结构版本与事件列名不一致", "时序库算出的列布局与 binlog 事件自带的列名（binlog_row_metadata=FULL）对不上，说明时序库跟丢了源库的真实结构——多半是某条 DDL 被漏施加或施加错了。事件列名是与行值同一时刻的权威信息，两者矛盾时硬解就是整行错位的静默数据损坏，因此已停止抽取。请把错误信息里的两份列清单与该表的 DDL 历史对照，并把漏掉的 DDL 形态补进语法"),

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
