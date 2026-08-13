package com.migration.extract;

import com.migration.common.AbstractExtractor;
import com.migration.common.txn.TxnMetadata;
import com.migration.db.ConnectionPoolManager;
import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.Pipeline;
import com.migration.thl.pipeline.PipelineConfig;
import com.migration.thl.pipeline.PipelineContext;
import com.migration.thl.pipeline.PipelineContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class MySQLBinlogExtractor extends AbstractExtractor<byte[], THLEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MySQLBinlogExtractor.class);

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';

    private String inputDir;
    private String outputDir;
    // 下列成员对子类开放（TiCDCExtractor 复用同一套 seqno/管线/列元数据缓存，
    // 只替换“把一条 capture 记录解析成 THLEvent”这一步）
    protected long seqno = 1;
    private String seqnoFile;

    private String sourceHost;
    private int sourcePort;
    private String sourceUser;
    private String sourcePassword;
    protected Connection sourceConnection;

    private Map<Long, Map<String, String>> tableMapCache = new HashMap<>();
    private Map<String, List<String>> tableSchemaCache = new HashMap<>();
    private Map<String, List<String>> tableColumnTypeCache = new HashMap<>();
    protected Map<String, List<String>> tableColumnFullTypeCache = new HashMap<>();
    protected Map<String, Map<String, List<String>>> enumSetValuesCache = new HashMap<>();
    private Map<String, List<String>> primaryKeyCache = new HashMap<>();
    /** 每张表的生成列（STORED/VIRTUAL）列名：随列类型一起查出来，与列类型缓存同生共死。 */
    protected Map<String, List<String>> generatedColumnCache = new HashMap<>();

    protected Pipeline pipeline;

    protected String checkpointBinlogFile;
    protected long checkpointBinlogPosition;
    protected boolean skipBeforeCheckpoint = false;

    /**
     * 源库 XA 事务缓冲：{@code XA START … XA PREPARE} 之间的事件先扣下，等到 {@code XA COMMIT}
     * 才按普通事务重放下发（{@code XA ROLLBACK} 则整段丢弃）。见 {@link XaTransactionBuffer}。
     */
    private XaTransactionBuffer xaBuffer;

    /** 重放 XA 分支时置位：跳过 XA 拦截与位点跳过判定，并把 tx_id 钉在分支上。 */
    private String replayTxId;

    @Override
    protected void doInitialize() throws Exception {
        inputDir = props.getProperty("extract.input.dir", "binlog_output");
        outputDir = props.getProperty("extract.output.dir", "thl_output");
        seqnoFile = outputDir + "/.extractor_seqno";

        sourceHost = props.getProperty("source.db.host", "localhost");
        sourcePort = Integer.parseInt(props.getProperty("source.db.port", "3306"));
        sourceUser = props.getProperty("source.db.username", "root");
        sourcePassword = props.getProperty("source.db.password", "");

        checkpointBinlogFile = props.getProperty("checkpoint.binlog.file", "");
        checkpointBinlogPosition = Long.parseLong(props.getProperty("checkpoint.binlog.position", "0"));
        skipBeforeCheckpoint = Boolean.parseBoolean(props.getProperty("extract.skip.before.checkpoint", "false"));

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        loadSeqno();
        connectToSourceDatabase();

        xaBuffer = new XaTransactionBuffer(props, outputDir);
        xaBuffer.recover();

        unknownEventSkip = "SKIP".equalsIgnoreCase(
                props.getProperty("extract.unknown.event.policy", "FAIL_STOP").trim());

        // 表结构时序库：本阶段只写不读——基线与 DDL 攒成按位点索引的版本链并落盘，
        // 行事件的解析仍然走下面那几个 information_schema 查询。切换在阶段 4。
        com.migration.extract.schema.SchemaTimelineConfig timelineConfig =
                com.migration.extract.schema.SchemaTimelineConfig.load(props,
                        props.getProperty("task.id", System.getProperty("task.id", "unknown")));
        if (timelineConfig.isEnabled()) {
            schemaTracker = new com.migration.extract.schema.SchemaTracker(props,
                    props.getProperty("task.id", System.getProperty("task.id", "unknown")));
            typeRenderMode = com.migration.extract.schema.TypeRenderMode
                    .forServerVersion(readSourceVersion());
            logger.info("表结构时序库 COLUMN_TYPE 渲染口径: {}（源库版本决定）", typeRenderMode);
            runSchemaSelfCheck();
        }

        PipelineContext pipelineContext = new PipelineContextImpl(props);
        ((PipelineContextImpl) pipelineContext).setSourceConnection(sourceConnection);
        pipeline = PipelineConfig.loadFromProperties(props, pipelineContext);
        if (pipeline != null) {
            pipeline.prepare();
            logger.info("Pipeline initialized with {} filters", pipeline.getFilters().size());
        }

        logger.info("MySQL Binlog Extractor initialized - input: {}, output: {}, seqno: {}, skipBeforeCheckpoint: {}",
                inputDir, outputDir, seqno, skipBeforeCheckpoint);
    }

    /**
     * 启动时的语法覆盖度自检：对同步范围内每张表 {@code SHOW CREATE TABLE} → 解析 →
     * 与 {@code information_schema} 逐列比对。
     *
     * <p>放在这里是因为它必须跑在<b>消费任何事件之前</b>：语法覆盖不了某张表时，
     * 那张表的每个版本都会是错的，早一步知道就少一段错误解析。
     *
     * <p>默认<b>只报告不阻断</b>——自检失败说明语法有缺口，但运行期已经有分级降级
     * （缺版本走 fallback、算错了被交叉校验 E3024 拦住）兜着，为它停机会把
     * "有一张冷门表解析不了"升级成"整个任务起不来"。要严格把关就把
     * {@code extract.schema.selfcheck.fail.stop} 置 true。
     */
    private void runSchemaSelfCheck() {
        if (!Boolean.parseBoolean(props.getProperty("extract.schema.selfcheck.enabled", "true"))) {
            return;
        }
        try {
            java.util.List<String> tables =
                    com.migration.extract.schema.SchemaSelfCheckMain.resolveTables(sourceConnection, props);
            if (tables.isEmpty()) {
                return;
            }
            com.migration.extract.schema.SchemaSelfCheck.Result result =
                    new com.migration.extract.schema.SchemaSelfCheck().run(sourceConnection, tables);
            if (result.allPassed()) {
                logger.info("表结构语法自检通过：{} 张表", result.passed());
                return;
            }
            logger.warn("表结构语法自检有 {} 张表对不上，这些表的时序库版本会失准：\n{}",
                    result.failed(), result.report());
            if (Boolean.parseBoolean(props.getProperty("extract.schema.selfcheck.fail.stop", "false"))) {
                throw new com.migration.extract.schema.DdlParseException(
                        "表结构语法自检未通过（extract.schema.selfcheck.fail.stop=true）: "
                                + result.report(), "");
            }
        } catch (com.migration.extract.schema.DdlParseException e) {
            throw e;
        } catch (Exception e) {
            logger.warn("表结构语法自检执行失败（不阻断启动）: {}", e.getMessage());
        }
    }

    /**
     * 源库版本号，决定 {@code COLUMN_TYPE} 里整数类型带不带显示宽度
     * （8.0.19 起不再回显）。读不到时按新库口径，见 {@code TypeRenderMode}。
     */
    private String readSourceVersion() {
        try (Statement st = sourceConnection.createStatement();
             ResultSet rs = st.executeQuery("SELECT VERSION()")) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            logger.warn("读不到源库版本，COLUMN_TYPE 按 8.0.19+ 口径渲染: {}", e.getMessage());
            return null;
        }
    }

    private void connectToSourceDatabase() throws SQLException {
        String url = "jdbc:mysql://" + sourceHost + ":" + sourcePort + "/?"
                + com.migration.common.ssl.SslMaterial.from(props, "source").mysqlUrlParams()
                + "&serverTimezone=UTC&characterEncoding=UTF-8";
        sourceConnection = ConnectionPoolManager.getConnection(url, sourceUser, sourcePassword);
        logger.info("Connected to source database: {}:{}", sourceHost, sourcePort);
    }

    @Override
    protected THLEvent doExtract(byte[] input) throws Exception {
        String eventStr = new String(input, "UTF-8");
        if (eventStr.trim().isEmpty()) {
            return null;
        }

        String[] fields = eventStr.split(String.valueOf(FIELD_SEP));
        if (fields.length < 5) {
            logger.warn("Invalid event format, skipping: {}", eventStr.substring(0, Math.min(100, eventStr.length())));
            return null;
        }

        String eventType = fields[0].trim();
        String binlogFile = fields[1].trim();
        long binlogPosition = 0;
        try {
            binlogPosition = Long.parseLong(fields[2].trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid binlog position in event: {}", fields[2]);
            return null;
        }
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(fields[3].trim());
        } catch (NumberFormatException e) {
            timestamp = System.currentTimeMillis();
        }
        long serverId = 0;
        try {
            serverId = Long.parseLong(fields[4].trim());
        } catch (NumberFormatException e) {
            // ignore
        }

        // 重放 XA 分支时不走位点跳过：分支的行事件位点停在 prepare 时刻，天然小于续传位点，
        // 按位点跳会把一个源库<b>已提交</b>的事务整段丢掉。它该不该应用，由 XA COMMIT 的位点说了算。
        if (skipBeforeCheckpoint && replayTxId == null
                && checkpointBinlogFile != null && !checkpointBinlogFile.isEmpty()) {
            if (shouldSkipEvent(binlogFile, binlogPosition)) {
                return null;
            }
        }

        // 表结构基线：capture 在启动时为同步范围内每张表写的一条 SHOW CREATE TABLE 原文。
        // 它是给表结构时序库播种用的<b>带内元数据</b>，不是 binlog 事件——必须在分配 seqno
        // 之前消费掉并返回 null，否则 THL 里会多出下游不认识的事件；而如果先 seqno++ 再返回
        // null，又会在 THL 里留下永久空洞（增量端按 seqno 连续性推进）。
        if ("SCHEMA_BASELINE".equals(eventType)) {
            if (schemaTracker != null) {
                schemaTracker.onBaseline(fields, binlogFile, binlogPosition);
            }
            return null;
        }

        String eventData = fields.length > 5 ? fields[5] : "";

        // XA 事务：START…PREPARE 之间的事件整段扣下落盘，等 XA COMMIT 才重放成普通事务。
        // 必须在分配 seqno <b>之前</b>判定——被扣下的行不能消耗 seqno，否则 THL 里留下永久空洞，
        // 而增量端的 readEventAfter 是按 seqno 连续性推进的。
        if (xaBuffer != null && replayTxId == null
                && xaBuffer.inspect(eventType, eventData, eventStr, binlogFile, binlogPosition, timestamp)
                        != XaTransactionBuffer.Verdict.PASS) {
            return null;
        }

        THLEvent thlEvent = new THLEvent();
        thlEvent.setSeqno(seqno++);
        thlEvent.setEventId(binlogFile + ":" + binlogPosition);
        thlEvent.setSourceId("mysql");
        thlEvent.setSourceTstamp(new Timestamp(timestamp));
        thlEvent.addMetadata("event_type", eventType);
        thlEvent.addMetadata("binlog_file", binlogFile);
        thlEvent.addMetadata("binlog_position", binlogPosition);
        thlEvent.addMetadata("server_id", serverId);

        if ("SYNC_HEARTBEAT".equals(eventType)) {
            thlEvent.setType(THLEvent.HEARTBEAT_EVENT);
            thlEvent.addMetadata("operation", "HEARTBEAT");
            thlEvent.addMetadata("source_db_timestamp", timestamp);
            return thlEvent;
        } else if ("TABLE_MAP".equals(eventType)) {
            parseTableMapEvent(thlEvent, eventData);
        } else if (isWriteRowsEvent(eventType)) {
            parseRowEvent(thlEvent, eventData, "INSERT");
        } else if (isUpdateRowsEvent(eventType)) {
            parseRowEvent(thlEvent, eventData, "UPDATE");
        } else if (isDeleteRowsEvent(eventType)) {
            parseRowEvent(thlEvent, eventData, "DELETE");
        } else if ("QUERY".equals(eventType)) {
            parseQueryEvent(thlEvent, eventData);
        } else if ("XID".equals(eventType)) {
            thlEvent.addMetadata("operation", "COMMIT");
        } else if ("ROTATE".equals(eventType)) {
            thlEvent.addMetadata("operation", "ROTATE");
        } else {
            checkIgnorableEventType(eventType, binlogFile, binlogPosition);
        }

        stampTransaction(thlEvent, eventType, binlogFile, binlogPosition, eventData);

        if (pipeline != null) {
            thlEvent = pipeline.process(thlEvent);
        }

        if (thlEvent != null) {
            Boolean multiRow = (Boolean) thlEvent.getMetadata().get("multi_row");
            if (multiRow != null && multiRow) {
                @SuppressWarnings("unchecked")
                java.util.List<String> rowsData = (java.util.List<String>) thlEvent.getMetadata().get("rows_data");
                if (rowsData != null && rowsData.size() > 1) {
                    long reservedSeqno = seqno - 1 + rowsData.size() - 1;
                    seqno = reservedSeqno + 1;
                    logger.debug("Reserved seqno range for multi-row event: {} to {} ({} rows)", 
                            thlEvent.getSeqno(), reservedSeqno, rowsData.size());
                }
            }
        }

        return thlEvent;
    }

    /**
     * 不带任何数据变更、丢掉也没有后果的 binlog 事件类型（<b>白名单</b>）。
     *
     * <p>剩下的一律当成"可能带数据"的未知类型处理。之所以是白名单而不是黑名单：
     * MySQL 每个大版本都在加新事件类型，黑名单漏一个就是一次静默丢数据，而且不报错、
     * 位点照常前进，只有对账时才看得出来。实测踩过两次——
     * {@code TRANSACTION_PAYLOAD}（压缩 binlog，整个事务被打包成一个事件）和
     * {@code PARTIAL_UPDATE_ROWS_EVENT}（binlog_row_value_options=PARTIAL_JSON 下的 JSON 差量
     * 更新），两者都不在原来的分发链里，UPDATE/整事务直接消失且 {@code error_status} 是空的。
     */
    private static final java.util.Set<String> IGNORABLE_EVENT_TYPES =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "UNKNOWN", "START_V3", "STOP", "INTVAR", "SLAVE", "RAND", "USER_VAR",
                    "FORMAT_DESCRIPTION", "INCIDENT", "HEARTBEAT", "IGNORABLE",
                    // ROWS_QUERY 带的是原始 SQL 文本（binlog_rows_query_log_events=ON），不是数据
                    "ROWS_QUERY", "GTID", "ANONYMOUS_GTID", "PREVIOUS_GTIDS",
                    "TRANSACTION_CONTEXT", "VIEW_CHANGE",
                    // MariaDB 专有：注解与检查点，同样不带数据
                    "ANNOTATE_ROWS", "BINLOG_CHECKPOINT", "MARIADB_GTID", "MARIADB_GTID_LIST"));

    /** 未知事件类型是否只告警不停机（{@code extract.unknown.event.policy=SKIP}）。 */
    private boolean unknownEventSkip;

    /** 表结构时序库；{@code extract.schema.timeline.mode=OFF}（默认）时为 null。 */
    private com.migration.extract.schema.SchemaTracker schemaTracker;

    public com.migration.extract.schema.SchemaTracker getSchemaTracker() {
        return schemaTracker;
    }

    /** 遇到不认识、且可能带数据的 binlog 事件类型：默认停下来上报，不静默丢。 */
    private void checkIgnorableEventType(String eventType, String binlogFile, long binlogPosition) {
        if (IGNORABLE_EVENT_TYPES.contains(eventType)) {
            return;
        }
        String detail = "不支持的 binlog 事件类型 " + eventType + " @ " + binlogFile + ":" + binlogPosition
                + "。该类型不在已知的可忽略清单里，继续跑等于把它携带的数据静默丢掉";
        if (unknownEventSkip) {
            logger.error("{}（按 extract.unknown.event.policy=SKIP 放过）", detail);
            return;
        }
        throw new UnsupportedBinlogEventException(detail);
    }

    /** 抽取遇到不认识的事件类型：由 ContinuousExtractMain 收口成 error_status + 停止抽取。 */
    public static final class UnsupportedBinlogEventException extends RuntimeException {
        public UnsupportedBinlogEventException(String message) {
            super(message);
        }
    }

    /**
     * 当前源事务标识：取 BEGIN 事件的 binlog 位点。
     * 不用 XID 的值是因为 XID 只在事务<b>末尾</b>才出现，而行事件在它之前就得打上标识；
     * BEGIN 的位点同样唯一（binlog 内单调递增），且天然在事务首位可用。不在事务中为 null。
     */
    private String currentTxId;

    /**
     * 给事件打上源事务边界（{@code tx_id} / {@code tx_last}）。
     *
     * <p>MySQL 行格式 binlog 里每个事务都是 {@code BEGIN … 行事件 … XID}（单语句自动提交也一样），
     * 因此 BEGIN 开启、XID 收尾。DDL 的 QUERY 事件是隐式提交的独立事务，不并入当前事务。
     */
    private void stampTransaction(THLEvent thlEvent, String eventType,
                                  String binlogFile, long binlogPosition, String eventData) {
        if (replayTxId != null) {
            // XA 分支重放：整段共用分支的 tx_id，收尾的 tx_last 由合成的 COMMIT 事件承担
            // （不能打在最后一个行事件上——多行事件会在 extract 主循环里被拆成 N 条，
            // tx_last 会被复制到每一行，增量端看到第一行就提交，事务照样被切开）
            thlEvent.addMetadata(TxnMetadata.TX_ID, replayTxId);
            return;
        }
        if ("QUERY".equals(eventType)) {
            Object sqlMeta = thlEvent.getMetadata().get("sql");
            String sql = sqlMeta != null ? sqlMeta.toString().trim() : "";
            if ("BEGIN".equalsIgnoreCase(sql)) {
                currentTxId = binlogFile + ":" + binlogPosition;
                thlEvent.addMetadata(TxnMetadata.TX_ID, currentTxId);
                return;
            }
            if ("COMMIT".equalsIgnoreCase(sql)) {
                // 非事务引擎（MyISAM 等）用 QUERY 'COMMIT' 而不是 XID 收尾
                closeTransaction(thlEvent, binlogFile, binlogPosition);
                return;
            }
            if (SAVEPOINT_STMT.matcher(sql).find()) {
                // SAVEPOINT 是<b>事务内</b>的语句，不是隐式提交的 DDL——ROW 格式下它照样进 binlog
                // （实测：BEGIN → 行事件 → `SAVEPOINT `sp2`` → 行事件 → XID）。
                // 落进下面那条 DDL 分支会把 currentTxId 清掉，于是 savepoint 之后的行事件全部丢失
                // tx_id，事务一致模式下一个源事务被切成两个目标事务——正是该模式要防的"半个事务"。
                // Spring 的 PROPAGATION_NESTED、各类 ORM 的嵌套事务都会产生 savepoint，很常见。
                if (currentTxId != null) {
                    thlEvent.addMetadata(TxnMetadata.TX_ID, currentTxId);
                }
                return;
            }
            currentTxId = null;   // DDL：隐式提交，自成一个事务
            return;
        }
        if ("XID".equals(eventType)) {
            closeTransaction(thlEvent, binlogFile, binlogPosition);
            java.util.regex.Matcher m = XID_VALUE.matcher(eventData == null ? "" : eventData);
            if (m.find()) {
                thlEvent.addMetadata(TxnMetadata.TX_SOURCE_ID, m.group(1));
            }
            return;
        }
        if (currentTxId != null) {
            thlEvent.addMetadata(TxnMetadata.TX_ID, currentTxId);
        }
    }

    /** 收尾当前事务：没见过 BEGIN（如断点续传恰好落在事务中间）时退化为该事件自成一事务。 */
    private void closeTransaction(THLEvent thlEvent, String binlogFile, long binlogPosition) {
        thlEvent.addMetadata(TxnMetadata.TX_ID,
                currentTxId != null ? currentTxId : binlogFile + ":" + binlogPosition);
        thlEvent.addMetadata(TxnMetadata.TX_LAST, Boolean.TRUE);
        currentTxId = null;
    }

    private static final java.util.regex.Pattern XID_VALUE =
            java.util.regex.Pattern.compile("xid=(\\d+)");

    /** 事务内的 SAVEPOINT 语句：{@code SAVEPOINT `sp1`} / {@code ROLLBACK TO SAVEPOINT sp1} / {@code RELEASE SAVEPOINT sp1}。 */
    private static final java.util.regex.Pattern SAVEPOINT_STMT = java.util.regex.Pattern.compile(
            "^\\s*(SAVEPOINT\\s|ROLLBACK\\s+TO\\b|RELEASE\\s+SAVEPOINT\\b)",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    /** 重放 XA 分支时逐条接收事件的下游（由 extract 主循环负责落 THL、按大小轮转文件）。 */
    public interface XaReplaySink {
        void accept(THLEvent event) throws Exception;
    }

    /** 是否正在收集某个 XA 分支——extract 主循环据此压住 {@code .cap} 读取进度的落盘。 */
    public boolean isXaBranchActive() {
        return xaBuffer != null && xaBuffer.isBranchActive();
    }

    /** 已 prepare、等源库决议的 XA 分支数（落成指标供页面观测）。 */
    public int xaPendingBranchCount() {
        return xaBuffer == null ? 0 : xaBuffer.pendingBranchCount();
    }

    /** 最老的未决 XA 分支已经等了多久（毫秒）。 */
    public long xaOldestPendingAgeMs() {
        return xaBuffer == null ? 0 : xaBuffer.oldestPendingAgeMs();
    }

    /**
     * 把已决议（源库 {@code XA COMMIT}）的 XA 分支重放成<b>一个普通事务</b>下发。
     *
     * <p>每次 {@code extract()} 之后调用一次：绝大多数时候没有分支可放，直接返回。
     * 重放是流式的（边读落盘文件边下发），一个 1GB 的 XA 事务不会整段进堆。
     *
     * <p>重放出来的事件有三处被改写，都是为了让下游"看起来就是一个发生在提交点的普通事务"：
     * <ul>
     *   <li>{@code tx_id} 统一钉在分支上，末尾补一个 {@code XID} 事件带 {@code tx_last}
     *       —— 增量端据此把整个 XA 事务原子提交；</li>
     *   <li>{@code binlog_file/binlog_position} 与 {@code eventId} 改写成 <b>XA COMMIT 的位点</b>
     *       —— 行事件原本的位点停在 prepare 时刻，照原样下发会让应用端位点<b>倒退</b>，
     *       重启后从更早的位点重放一大段；原位点保留在 {@code xa_prepare_position} 里备查；</li>
     *   <li>{@code sourceTstamp} 取提交时刻 —— 数据是在 XA COMMIT 那一刻才在源库可见的，
     *       用 prepare 时间算延迟会把"事务一直没提交"错记成同步延迟。</li>
     * </ul>
     */
    public void drainXaReplay(XaReplaySink sink) throws Exception {
        if (xaBuffer == null) {
            return;
        }
        XaTransactionBuffer.Branch branch;
        while ((branch = xaBuffer.takeReplay()) != null) {
            replayXaBranch(branch, sink);
        }
    }

    private void replayXaBranch(XaTransactionBuffer.Branch branch, XaReplaySink sink) throws Exception {
        String txId = "xa:" + branch.getKey();
        int emitted = 0;
        replayTxId = txId;
        try (java.io.BufferedReader reader = xaBuffer.openReplayReader(branch)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                THLEvent event = doExtract(line.getBytes("UTF-8"));
                if (event == null) {
                    continue;
                }
                rewriteToCommitPoint(event, branch, emitted++);
                sink.accept(event);
            }
        } finally {
            replayTxId = null;
        }

        if (emitted > 0) {
            sink.accept(xaCommitEvent(branch, txId));
        }
        xaBuffer.finishReplay(branch);
        logger.info("XA 分支 {} 按源库提交点整体下发: {} 个事件 @ {}:{}",
                branch.getXid(), emitted, branch.getCommitFile(), branch.getCommitPos());
    }

    private void rewriteToCommitPoint(THLEvent event, XaTransactionBuffer.Branch branch, int index) {
        Object prepareFile = event.getMetadata().get("binlog_file");
        Object preparePos = event.getMetadata().get("binlog_position");
        event.addMetadata("xa_xid", branch.getXid());
        event.addMetadata("xa_prepare_position", prepareFile + ":" + preparePos);
        event.addMetadata("binlog_file", branch.getCommitFile());
        event.addMetadata("binlog_position", branch.getCommitPos());
        event.setEventId(branch.getCommitFile() + ":" + branch.getCommitPos() + "#" + index);
        if (branch.getCommitTimestamp() > 0) {
            if (event.getSourceTstamp() != null) {
                event.addMetadata("xa_prepare_timestamp", event.getSourceTstamp().getTime());
            }
            event.setSourceTstamp(new Timestamp(branch.getCommitTimestamp()));
        }
    }

    /** 合成的事务收尾事件：形态与普通事务的 XID 完全一致，下游无需认识 XA。 */
    private THLEvent xaCommitEvent(XaTransactionBuffer.Branch branch, String txId) {
        THLEvent event = new THLEvent();
        event.setSeqno(seqno++);
        event.setEventId(branch.getCommitFile() + ":" + branch.getCommitPos() + "#c");
        event.setSourceId("mysql");
        event.setSourceTstamp(new Timestamp(branch.getCommitTimestamp() > 0
                ? branch.getCommitTimestamp() : System.currentTimeMillis()));
        event.addMetadata("event_type", "XID");
        event.addMetadata("operation", "COMMIT");
        event.addMetadata("binlog_file", branch.getCommitFile());
        event.addMetadata("binlog_position", branch.getCommitPos());
        event.addMetadata("xa_xid", branch.getXid());
        event.addMetadata(TxnMetadata.TX_ID, txId);
        event.addMetadata(TxnMetadata.TX_LAST, Boolean.TRUE);
        return event;
    }

    private boolean shouldSkipEvent(String binlogFile, long binlogPosition) {
        if (checkpointBinlogFile == null || checkpointBinlogFile.isEmpty()) {
            return false;
        }

        int cmp = binlogFile.compareTo(checkpointBinlogFile);
        if (cmp < 0) {
            return true;
        } else if (cmp == 0) {
            return binlogPosition < checkpointBinlogPosition;
        }
        return false;
    }

    private boolean isWriteRowsEvent(String eventType) {
        return "WRITE_ROWS".equals(eventType) || "EXT_WRITE_ROWS".equals(eventType);
    }

    private boolean isUpdateRowsEvent(String eventType) {
        return "UPDATE_ROWS".equals(eventType) || "EXT_UPDATE_ROWS".equals(eventType);
    }

    private boolean isDeleteRowsEvent(String eventType) {
        return "DELETE_ROWS".equals(eventType) || "EXT_DELETE_ROWS".equals(eventType);
    }

    private void parseTableMapEvent(THLEvent thlEvent, String eventData) {
        Long tableId = null;
        String database = null;
        String table = null;

        java.util.regex.Matcher tableIdMatcher = java.util.regex.Pattern.compile("tableId=(\\d+)").matcher(eventData);
        if (tableIdMatcher.find()) {
            try {
                tableId = Long.parseLong(tableIdMatcher.group(1));
            } catch (NumberFormatException e) { /* ignore */ }
        }

        java.util.regex.Matcher databaseMatcher = java.util.regex.Pattern.compile("database='([^']+)'").matcher(eventData);
        if (databaseMatcher.find()) {
            database = databaseMatcher.group(1);
        }

        java.util.regex.Matcher tableMatcher = java.util.regex.Pattern.compile("table='([^']+)'").matcher(eventData);
        if (tableMatcher.find()) {
            table = tableMatcher.group(1);
        }

        if (tableId != null && database != null && table != null) {
            Map<String, String> tableInfo = new HashMap<>();
            tableInfo.put("database", database);
            tableInfo.put("table", table);
            tableMapCache.put(tableId, tableInfo);

            String binlogFile = String.valueOf(thlEvent.getMetadata().getOrDefault("binlog_file", ""));
            long binlogPos = thlEvent.getMetadata().get("binlog_position") instanceof Number
                    ? ((Number) thlEvent.getMetadata().get("binlog_position")).longValue() : 0L;

            ResolvedSchema resolved = resolveSchema(database, table, eventData, binlogFile, binlogPos);

            tableInfo.put("columns", String.join(",", resolved.columns));
            tableInfo.put("column_types", String.join(",", resolved.dataTypes));
            if (resolved.fullTypes != null) {
                tableInfo.put("column_full_types", String.join(",", resolved.fullTypes));
            }
            tableInfo.put("primary_keys", String.join(",", resolved.primaryKeys));
            if (resolved.generatedColumns != null && !resolved.generatedColumns.isEmpty()) {
                tableInfo.put("generated_columns", String.join(",", resolved.generatedColumns));
            }
            if (resolved.enumSetValues != null && !resolved.enumSetValues.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, List<String>> entry : resolved.enumSetValues.entrySet()) {
                    if (sb.length() > 0) sb.append(";");
                    sb.append(entry.getKey()).append("=").append(String.join(",", entry.getValue()));
                }
                tableInfo.put("enum_set_values", sb.toString());
            }

            thlEvent.addMetadata("database_name", database);
            thlEvent.addMetadata("table_name", table);
            thlEvent.addMetadata("table_id", tableId);
        }
    }

    /**
     * 下游要的六项元数据。两条产出路径共用这一个形状：
     * 表结构时序库（按事件位点取"当时"的结构）与 {@code information_schema}（源库"此刻"的定义）。
     */
    private static final class ResolvedSchema {
        List<String> columns;
        List<String> dataTypes;
        List<String> fullTypes;
        List<String> primaryKeys;
        List<String> generatedColumns;
        Map<String, List<String>> enumSetValues;
        /** 产出方，只用于日志与指标 */
        String source;
    }

    /**
     * 决定这张表的结构从哪儿来。
     *
     * <p>这是整个时序库方案的切换点。三档行为：
     * <ul>
     *   <li>{@code OFF}：完全走 {@code information_schema}，与改造前逐字一致；</li>
     *   <li>{@code SHADOW}：两条路都算，<b>产出仍用旧路径</b>，只把差异记下来。灰度期靠
     *       "差异数归零"来证明语法覆盖够用，这是单测覆盖率证明不了的；</li>
     *   <li>{@code ON}：时序库说了算，旧路径退为降级兜底。</li>
     * </ul>
     *
     * <p>无论哪一档，只要时序库给得出版本就做一次<b>交叉校验</b>：事件自带的列名
     * （{@code binlog_row_metadata=FULL}）是与行值同一时刻的权威信息，与算出来的版本矛盾
     * 就说明时序库跟丢了——这种情况硬解就是整行错位的静默数据损坏，一律 fail-stop（E3024）。
     * 没有这道校验，一个语法 bug 就能悄悄把整张表写坏，比不上时序库还糟。
     */
    private ResolvedSchema resolveSchema(String database, String table, String eventData,
                                         String binlogFile, long binlogPos) {
        com.migration.extract.schema.TableSchema versioned = null;
        if (schemaTracker != null) {
            versioned = schemaTracker.at(database, table, binlogFile, binlogPos);
            if (versioned != null && versioned.isUnusable()) {
                versioned = null;   // 基线取不到 / 解析失败的表，按"没有版本"处置
            }
            if (versioned != null) {
                crossCheckAgainstEvent(versioned, eventData, database, table, binlogFile, binlogPos);
            }
        }

        boolean authoritative = schemaTracker != null
                && schemaTracker.getConfig().isAuthoritative();

        if (authoritative) {
            if (versioned != null) {
                timelineHit.increment();
                return fromTimeline(versioned);
            }
            timelineMiss.increment();
            String detail = String.format(
                    "表结构时序库没有 %s.%s 在 %s:%d 处的版本（任务可能建于时序库启用之前、"
                            + "该表基线不可用、或跨机接管时没有回灌）",
                    database, table, binlogFile, binlogPos);
            if (schemaTracker.getConfig().getFallback()
                    == com.migration.extract.schema.SchemaTimelineConfig.Fallback.FAIL_STOP) {
                throw new SchemaTimelineMissingException(detail);
            }
            logger.warn("{}——按 fallback=RESNAPSHOT 退回查源库当前定义（这会退回到"
                    + "\"用现在的结构解释过去的事件\"）", detail);
        }

        ResolvedSchema legacy = fromInformationSchema(database, table, eventData);

        // SHADOW：产出用旧路径，只记差异。差异率归零才是切 ON 的依据
        if (versioned != null && schemaTracker != null && !authoritative) {
            compareShadow(versioned, legacy, database, table, binlogFile, binlogPos);
        }
        return legacy;
    }

    /** 时序库版本 → 六项元数据。 */
    private ResolvedSchema fromTimeline(com.migration.extract.schema.TableSchema versioned) {
        ResolvedSchema r = new ResolvedSchema();
        r.columns = versioned.columnNames();
        r.dataTypes = versioned.dataTypes();
        r.fullTypes = versioned.columnTypes(typeRenderMode);
        r.primaryKeys = versioned.getPrimaryKey();
        r.generatedColumns = versioned.generatedColumns();
        r.enumSetValues = versioned.enumSetValues();
        r.source = "timeline";
        return r;
    }

    /** {@code information_schema} 的当前定义 → 六项元数据（改造前的原有逻辑，逐字保留）。 */
    private ResolvedSchema fromInformationSchema(String database, String table, String eventData) {
        ResolvedSchema r = new ResolvedSchema();
        r.columns = resolveColumns(database, table, eventData);

        List<String> columnTypes = getTableColumnTypes(database, table);
        List<String> columnFullTypes = tableColumnFullTypeCache.get(database + "." + table);

        // 用的是事件自带的列布局（与当前表定义不一致）时，类型必须按<b>列名</b>重新对齐，
        // 否则列名对了、类型还错位，等于换了一种写坏方式
        List<String> schemaColumns = getTableColumns(database, table);
        if (!r.columns.equals(schemaColumns)) {
            columnTypes = columnMetaByName(r.columns, schemaColumns, columnTypes);
            if (columnFullTypes != null) {
                columnFullTypes = columnMetaByName(r.columns, schemaColumns, columnFullTypes);
            }
        }
        r.dataTypes = columnTypes;
        r.fullTypes = columnFullTypes;
        r.primaryKeys = getTablePrimaryKeys(database, table);
        r.generatedColumns = generatedColumnCache.get(database + "." + table);
        r.enumSetValues = enumSetValuesCache.get(database + "." + table);
        r.source = "information_schema";
        return r;
    }

    /**
     * 用事件自带的列名校验时序库算出的版本。
     *
     * <p>事件列名只有 {@code binlog_row_metadata=FULL} 才有；没有时这道校验自动跳过
     * （MINIMAL 下时序库仍然是对的，只是少了这层证据）。
     */
    private void crossCheckAgainstEvent(com.migration.extract.schema.TableSchema versioned,
                                        String eventData, String database, String table,
                                        String binlogFile, long binlogPos) {
        List<String> eventColumns = parseEventColumnNames(eventData);
        if (eventColumns.isEmpty()) {
            return;
        }
        List<String> versionColumns = versioned.columnNames();
        if (eventColumns.equals(versionColumns)) {
            return;
        }
        timelineCrossCheckFailed.increment();
        throw new SchemaVersionMismatchException(String.format(
                "表 %s.%s 在 %s:%d：时序库算出的列布局 %s 与事件自带的列名 %s 不一致。"
                        + "事件列名是与行值同一时刻的权威信息，两者矛盾说明时序库跟丢了源库的真实结构"
                        + "（多半是某条 DDL 被漏施加或施加错了）。硬解就是整行错位的静默数据损坏",
                database, table, binlogFile, binlogPos, versionColumns, eventColumns));
    }

    /** SHADOW 档的对算：只记差异，不改变产出。 */
    private void compareShadow(com.migration.extract.schema.TableSchema versioned, ResolvedSchema legacy,
                               String database, String table, String binlogFile, long binlogPos) {
        List<String> diffs = new ArrayList<>();
        if (!versioned.columnNames().equals(legacy.columns)) {
            diffs.add("列名: 时序库=" + versioned.columnNames() + " 旧路径=" + legacy.columns);
        }
        if (!versioned.dataTypes().equals(legacy.dataTypes)) {
            diffs.add("DATA_TYPE: 时序库=" + versioned.dataTypes() + " 旧路径=" + legacy.dataTypes);
        }
        List<String> versionedFull = versioned.columnTypes(typeRenderMode);
        if (legacy.fullTypes != null && !versionedFull.equals(legacy.fullTypes)) {
            diffs.add("COLUMN_TYPE: 时序库=" + versionedFull + " 旧路径=" + legacy.fullTypes);
        }
        if (!versioned.getPrimaryKey().equals(legacy.primaryKeys)) {
            diffs.add("主键: 时序库=" + versioned.getPrimaryKey() + " 旧路径=" + legacy.primaryKeys);
        }
        if (diffs.isEmpty()) {
            timelineHit.increment();
            return;
        }
        timelineShadowDiff.increment();
        // 影子期的差异<b>未必</b>是时序库错了——积压期做过 DDL 时，"旧路径不同"恰恰说明
        // 时序库在干正事。所以这里只记录，由人看着判断，不自动升级成告警风暴（每表一次）
        if (shadowDiffWarned.add(database + "." + table)) {
            logger.warn("[SHADOW] 表 {}.{} 在 {}:{} 两条路算出的结构不同（产出仍用旧路径）:\n  {}",
                    database, table, binlogFile, binlogPos, String.join("\n  ", diffs));
        }
    }

    /** 时序库四项指标：命中 / 未命中降级 / 交叉校验失败 / 影子差异。 */
    private final java.util.concurrent.atomic.LongAdder timelineHit = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder timelineMiss = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder timelineCrossCheckFailed =
            new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder timelineShadowDiff =
            new java.util.concurrent.atomic.LongAdder();
    /** 影子差异每表只告警一次，避免刷屏淹掉真正的信号。 */
    private final java.util.Set<String> shadowDiffWarned = new java.util.HashSet<>();

    /** {@code COLUMN_TYPE} 的渲染口径，按源库版本定（8.0.19 起整数不回显显示宽度）。 */
    private com.migration.extract.schema.TypeRenderMode typeRenderMode =
            com.migration.extract.schema.TypeRenderMode.NO_DISPLAY_WIDTH;

    public Map<String, Long> schemaTimelineMetrics() {
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        m.put("timeline_hit", timelineHit.sum());
        m.put("timeline_miss", timelineMiss.sum());
        m.put("timeline_cross_check_failed", timelineCrossCheckFailed.sum());
        m.put("timeline_shadow_diff", timelineShadowDiff.sum());
        if (schemaTracker != null) {
            m.put("timeline_ddl_parse_failed", schemaTracker.getDdlParseFailed());
            m.put("timeline_idempotent_absorbed", schemaTracker.getApplier().getStats().total());
        }
        return m;
    }

    /** 时序库给不出该位点的版本，且 fallback=FAIL_STOP。由 ContinuousExtractMain 收成 E3022。 */
    public static final class SchemaTimelineMissingException extends RuntimeException {
        public SchemaTimelineMissingException(String message) {
            super(message);
        }
    }

    /** 时序库版本与事件自带列名矛盾。由 ContinuousExtractMain 收成 E3024。 */
    public static final class SchemaVersionMismatchException extends RuntimeException {
        public SchemaVersionMismatchException(String message) {
            super(message);
        }
    }

    private void parseRowEvent(THLEvent thlEvent, String eventData, String operation) {
        thlEvent.addMetadata("operation", operation);

        Long tableId = null;

        java.util.regex.Matcher tableIdMatcher = java.util.regex.Pattern.compile("tableId=(\\d+)").matcher(eventData);
        if (tableIdMatcher.find()) {
            try {
                tableId = Long.parseLong(tableIdMatcher.group(1));
            } catch (NumberFormatException e) { /* ignore */ }
        }

        if (tableId != null) {
            Map<String, String> tableInfo = tableMapCache.get(tableId);
            if (tableInfo != null) {
                thlEvent.addMetadata("database_name", tableInfo.get("database"));
                thlEvent.addMetadata("table_name", tableInfo.get("table"));

                String columnsStr = tableInfo.get("columns");
                if (columnsStr != null) {
                    thlEvent.addMetadata("column_names", columnsStr);
                }

                String columnTypesStr = tableInfo.get("column_types");
                if (columnTypesStr != null) {
                    thlEvent.addMetadata("mysql_column_types", columnTypesStr);
                }

                // 带精度/显示宽度的完整类型（COLUMN_TYPE，如 tinyint(1)/bit(8)），
                // 供增量转换区分 boolean(tinyint(1))/bit 等（DATA_TYPE 丢失了宽度信息）
                String columnFullTypesMeta = tableInfo.get("column_full_types");
                if (columnFullTypesMeta != null) {
                    thlEvent.addMetadata("mysql_column_full_types", columnFullTypesMeta);
                }

                String pkStr = tableInfo.get("primary_keys");
                if (pkStr != null) {
                    thlEvent.addMetadata("primary_keys", pkStr);
                }

                String enumSetValuesStr = tableInfo.get("enum_set_values");
                if (enumSetValuesStr != null) {
                    thlEvent.addMetadata("enum_set_values", enumSetValuesStr);
                }

                // 生成列：值照常按全列顺序下发（下标要对齐），由 apply 端在拼 DML 时跳过
                String generatedColumnsStr = tableInfo.get("generated_columns");
                if (generatedColumnsStr != null) {
                    thlEvent.addMetadata("generated_columns", generatedColumnsStr);
                }

                int columnCount = columnsStr != null ? columnsStr.split(",").length : 0;
                String[] columnTypes = columnTypesStr != null ? columnTypesStr.split(",") : new String[0];
                String[] columnNames = columnsStr != null ? columnsStr.split(",") : new String[0];
                Map<String, List<String>> enumValuesMap = parseEnumSetValuesMap(enumSetValuesStr);

                String columnFullTypesStr = tableInfo.get("column_full_types");
                // 括号感知切分：enum('a','b','c')/set(...)/decimal(20,4) 内含逗号，naive split 会导致
                // 其后所有列的完整类型整体错位（如 bigint unsigned 拿到 "'c')" 而丢失 unsigned 重建）
                String[] columnFullTypes = columnFullTypesStr != null
                        ? splitTopLevelCommas(columnFullTypesStr) : new String[0];

                if ("INSERT".equals(operation) || "DELETE".equals(operation)) {
                    List<String> allRowValues = extractAllRowValuesFromWriteDelete(eventData);
                    if (!allRowValues.isEmpty()) {
                        List<String> formattedRows = new ArrayList<>();
                        ArrayList<ArrayList<Object>> typedRows = new ArrayList<>();
                        boolean hasLob = false;
                        for (String rowValues : allRowValues) {
                            List<String> values = parseValueList(rowValues, columnCount);
                            hasLob |= containsLobRef(values);
                            formattedRows.add(formatRowData(values, columnTypes, columnFullTypes, columnNames, enumValuesMap));
                            if (typedRows != null) {
                                ArrayList<Object> typed = typeRowValues(values, columnTypes, columnFullTypes, columnNames, enumValuesMap);
                                if (typed != null) typedRows.add(typed); else typedRows = null; // 任一行无法类型化则整体回退
                            }
                        }
                        thlEvent.addMetadata("rows_data", formattedRows);
                        thlEvent.addMetadata("row_data", formattedRows.get(0));
                        if (hasLob) {
                            // 带大字段的事件只能走类型化（参数绑定）路径。打上标记，
                            // 让增量端在回退到文本路径之前就 fail-stop——文本路径写出来的是
                            // "@lob:..." 这串字面量，目标列会被静默写坏。
                            thlEvent.addMetadata("has_lob", true);
                        }
                        if (typedRows != null) {
                            // 类型化值管道：供增量端 PreparedStatement 参数绑定，消除 SQL 字面量拼接
                            thlEvent.addMetadata("rows_typed", typedRows);
                        }
                        if (formattedRows.size() > 1) {
                            thlEvent.addMetadata("multi_row", true);
                        }
                    }
                } else if ("UPDATE".equals(operation)) {
                    List<String[]> allBeforeAfter = extractAllBeforeAfterValues(eventData);
                    if (!allBeforeAfter.isEmpty()) {
                        List<String> formattedAfterRows = new ArrayList<>();
                        List<String> formattedBeforeRows = new ArrayList<>();
                        ArrayList<ArrayList<Object>> typedAfterRows = new ArrayList<>();
                        ArrayList<ArrayList<Object>> typedBeforeRows = new ArrayList<>();
                        boolean hasLob = false;
                        for (String[] beforeAfter : allBeforeAfter) {
                            if (beforeAfter[1] != null) {
                                List<String> afterValues = parseValueList(beforeAfter[1], columnCount);
                                hasLob |= containsLobRef(afterValues);
                                formattedAfterRows.add(formatRowData(afterValues, columnTypes, columnFullTypes, columnNames, enumValuesMap));
                                if (typedAfterRows != null) {
                                    ArrayList<Object> typed = typeRowValues(afterValues, columnTypes, columnFullTypes, columnNames, enumValuesMap);
                                    if (typed != null) typedAfterRows.add(typed); else typedAfterRows = null;
                                }
                            }
                            if (beforeAfter[0] != null) {
                                List<String> beforeValues = parseValueList(beforeAfter[0], columnCount);
                                formattedBeforeRows.add(formatRowData(beforeValues, columnTypes, columnFullTypes, columnNames, enumValuesMap));
                                if (typedBeforeRows != null) {
                                    ArrayList<Object> typed = typeRowValues(beforeValues, columnTypes, columnFullTypes, columnNames, enumValuesMap);
                                    if (typed != null) typedBeforeRows.add(typed); else typedBeforeRows = null;
                                }
                            }
                        }
                        if (!formattedAfterRows.isEmpty()) {
                            thlEvent.addMetadata("row_data", formattedAfterRows.get(0));
                            thlEvent.addMetadata("rows_data", formattedAfterRows);
                        }
                        if (!formattedBeforeRows.isEmpty()) {
                            thlEvent.addMetadata("row_data_before", formattedBeforeRows.get(0));
                            thlEvent.addMetadata("rows_data_before", formattedBeforeRows);
                        }
                        if (hasLob) {
                            // 见 INSERT 分支的说明：带大字段的事件禁止回退文本路径。
                            // 前镜像里的大字段是"只留身份"的引用，同样不能当字面量拼进 SQL。
                            thlEvent.addMetadata("has_lob", true);
                        }
                        // 类型化值：UPDATE 需 before/after 同时可类型化且行数配对，否则整体回退文本路径
                        if (typedAfterRows != null && typedBeforeRows != null
                                && !typedAfterRows.isEmpty() && typedAfterRows.size() == typedBeforeRows.size()) {
                            thlEvent.addMetadata("rows_typed", typedAfterRows);
                            thlEvent.addMetadata("rows_before_typed", typedBeforeRows);
                        }
                        if (formattedAfterRows.size() > 1) {
                            thlEvent.addMetadata("multi_row", true);
                        }
                    }
                }
            }
        }
    }

    private List<String> extractAllRowValuesFromWriteDelete(String eventData) {
        List<String> allRows = new ArrayList<>();
        
        int rowsIdx = eventData.indexOf("rows=[");
        if (rowsIdx < 0) {
            String singleRow = extractValuesString(eventData, "values");
            if (singleRow != null) {
                allRows.add(singleRow);
            }
            return allRows;
        }

        int startIdx = rowsIdx + "rows=[".length();
        
        while (startIdx < eventData.length()) {
            while (startIdx < eventData.length() && (eventData.charAt(startIdx) == ' ' || eventData.charAt(startIdx) == '\n')) {
                startIdx++;
            }
            
            if (startIdx >= eventData.length() || eventData.charAt(startIdx) != '[') {
                break;
            }
            
            int rowStartIdx = startIdx + 1;
            int bracketCount = 1;
            int rowEndIdx = rowStartIdx;
            
            while (rowEndIdx < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(rowEndIdx);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                }
                rowEndIdx++;
            }
            
            String rowValues = eventData.substring(rowStartIdx, rowEndIdx - 1);
            allRows.add(rowValues);
            
            startIdx = rowEndIdx;
            while (startIdx < eventData.length() && eventData.charAt(startIdx) == ' ') {
                startIdx++;
            }
            if (startIdx < eventData.length() && eventData.charAt(startIdx) == ',') {
                startIdx++;
            }
        }
        
        return allRows;
    }

    private String[] extractBeforeAfterValues(String eventData) {
        String[] result = new String[2];

        int beforeIdx = eventData.indexOf("before=[");
        if (beforeIdx >= 0) {
            int startIdx = beforeIdx + "before=[".length();
            int bracketCount = 1;
            int endIdx = startIdx;

            while (endIdx < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(endIdx);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                }
                endIdx++;
            }

            result[0] = eventData.substring(startIdx, endIdx - 1);
        }

        int afterIdx = eventData.indexOf("after=[");
        if (afterIdx >= 0) {
            int startIdx = afterIdx + "after=[".length();
            int bracketCount = 1;
            int endIdx = startIdx;

            while (endIdx < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(endIdx);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                }
                endIdx++;
            }

            result[1] = eventData.substring(startIdx, endIdx - 1);
        }

        return result;
    }

    private List<String[]> extractAllBeforeAfterValues(String eventData) {
        List<String[]> result = new ArrayList<>();
        int searchFrom = 0;

        while (searchFrom < eventData.length()) {
            int beforeIdx = eventData.indexOf("before=[", searchFrom);
            if (beforeIdx < 0) break;

            int beforeStart = beforeIdx + "before=[".length();
            int bracketCount = 1;
            int beforeEnd = beforeStart;
            while (beforeEnd < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(beforeEnd);
                if (c == '[') bracketCount++;
                else if (c == ']') bracketCount--;
                beforeEnd++;
            }
            String beforeVal = eventData.substring(beforeStart, beforeEnd - 1);

            int afterIdx = eventData.indexOf("after=[", beforeEnd);
            if (afterIdx < 0) break;

            int afterStart = afterIdx + "after=[".length();
            bracketCount = 1;
            int afterEnd = afterStart;
            while (afterEnd < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(afterEnd);
                if (c == '[') bracketCount++;
                else if (c == ']') bracketCount--;
                afterEnd++;
            }
            String afterVal = eventData.substring(afterStart, afterEnd - 1);

            result.add(new String[]{beforeVal, afterVal});
            searchFrom = afterEnd;
        }

        return result;
    }

    private String extractValuesString(String eventData, String keyword) {
        String searchKey = keyword + "=[";
        int startIdx = eventData.indexOf(searchKey);
        if (startIdx < 0) {
            return null;
        }

        startIdx += searchKey.length();
        int bracketCount = 1;
        int endIdx = startIdx;

        while (endIdx < eventData.length() && bracketCount > 0) {
            char c = eventData.charAt(endIdx);
            if (c == '[') {
                bracketCount++;
            } else if (c == ']') {
                bracketCount--;
            }
            endIdx++;
        }

        return eventData.substring(startIdx, endIdx - 1);
    }

    private List<String> parseValueList(String valuesStr, int expectedCount) {
        List<String> values = new ArrayList<>();
        if (valuesStr == null || valuesStr.isEmpty()) {
            return values;
        }

        List<String> parts = splitByComma(valuesStr);
        for (int i = 0; i < parts.size() && values.size() < expectedCount; i++) {
            values.add(parts.get(i).trim());
        }

        while (values.size() < expectedCount) {
            values.add(null);
        }

        return values;
    }

    private List<String> splitByComma(String str) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        int braceDepth = 0;
        boolean inQuote = false;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '\'' && (i == 0 || str.charAt(i - 1) != '\\')) {
                inQuote = !inQuote;
                current.append(c);
            } else if (!inQuote && c == '[') {
                depth++;
                current.append(c);
            } else if (!inQuote && c == ']') {
                depth--;
                current.append(c);
            } else if (!inQuote && c == '{') {
                braceDepth++;
                current.append(c);
            } else if (!inQuote && c == '}') {
                braceDepth--;
                current.append(c);
            } else if (!inQuote && c == ',' && depth == 0 && braceDepth == 0) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts;
    }

    protected Map<String, List<String>> parseEnumSetValuesMap(String enumSetValuesStr) {
        Map<String, List<String>> map = new HashMap<>();
        if (enumSetValuesStr == null || enumSetValuesStr.isEmpty()) {
            return map;
        }

        String[] entries = enumSetValuesStr.split(";");
        for (String entry : entries) {
            int eqIdx = entry.indexOf("=");
            if (eqIdx > 0) {
                String colName = entry.substring(0, eqIdx).trim();
                String valuesStr = entry.substring(eqIdx + 1).trim();
                if (valuesStr.startsWith("[") && valuesStr.endsWith("]")) {
                    valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
                }
                List<String> values = new ArrayList<>();
                // limit=-1：默认的 split(",") 会把<b>末尾</b>的空串整段丢掉，
                // enum('a','') 序列化成 "col=a," 后会被解回 ["a"]，第 2 个取值凭空消失。
                for (String v : valuesStr.split(",", -1)) {
                    values.add(v.trim());
                }
                map.put(colName, values);
            }
        }
        return map;
    }

    /**
     * 文本路径遇到大字段引用时放的毒丸。
     *
     * <p>取值刻意是<b>非法 SQL</b>：真要是漏到执行阶段，得到的是一个明确的语法错误，
     * 而不是"把 @lob:xxx 这串字符当内容写进 BLOB 列"的静默损坏。
     */
    static final String LOB_TEXT_PATH_POISON = "<<LOB_REQUIRES_TYPED_PATH>>";

    /** 该事件是否携带大字段引用（增量端据此禁止文本回退）。 */
    static boolean containsLobRef(List<String> values) {
        for (String v : values) {
            if (v != null && v.startsWith(com.migration.common.lob.LobRef.MARKER)) {
                return true;
            }
        }
        return false;
    }

    private String formatRowData(List<String> values, String[] columnTypes, String[] columnFullTypes, String[] columnNames, Map<String, List<String>> enumValuesMap) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(",");
            String value = values.get(i);
            String type = i < columnTypes.length ? columnTypes[i] : "";
            String fullType = i < columnFullTypes.length ? columnFullTypes[i] : "";
            String colName = i < columnNames.length ? columnNames[i] : "";

            if (value == null || "null".equalsIgnoreCase(value)) {
                sb.append("null");
            } else if (value.startsWith(com.migration.common.lob.LobRef.MARKER)) {
                // 文本路径拼的是 SQL 字面量，而一个 1GB 的值根本没法出现在字面量里。
                // 这里刻意放一个<b>语法上就非法</b>的毒丸而不是引号包起来的字符串：
                // 后者会被当成正常内容写进目标列，是静默的数据损坏；
                // 前者会在增量端被 has_lob 标记提前拦下（真漏到 SQL 也只会是明确的语法错误）。
                sb.append(LOB_TEXT_PATH_POISON);
            } else if (isBinaryType(type) || isBlobType(type)) {
                if (value.matches("\\[B@[0-9a-f]+")) {
                    sb.append("null");
                } else if (value.startsWith("0x") || value.startsWith("0X")) {
                    sb.append(value);
                } else {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (isTextType(type) || isJsonType(type)) {
                if (value.startsWith("0x") || value.startsWith("0X")) {
                    if (isJsonType(type)) {
                        try {
                            byte[] jsonBytes = hexStringToByteArray(value.substring(2));
                            String jsonStr = com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary.parseAsString(jsonBytes);
                            sb.append("'").append(escapeString(jsonStr)).append("'");
                        } catch (Exception e) {
                            logger.warn("Failed to decode JSON binary value, using CAST: {}", e.getMessage());
                            sb.append("CAST(").append(value).append(" AS JSON)");
                        }
                    } else {
                        String decoded = hexToString(value.substring(2));
                        sb.append("'").append(escapeString(decoded)).append("'");
                    }
                } else {
                    sb.append("'").append(escapeString(value)).append("'");
                }
            } else if (isBitType(type)) {
                if (value.startsWith("0x") || value.startsWith("0X")) {
                    sb.append(value);
                } else if (value.matches("\\d+")) {
                    long bitVal = Long.parseLong(value);
                    sb.append("0x").append(Long.toHexString(bitVal));
                } else if (value.startsWith("{") && value.contains(",")) {
                    long bitVal = 0;
                    String[] bits = value.substring(1, value.length() - 1).split(",");
                    for (String bit : bits) {
                        bitVal |= (1L << Integer.parseInt(bit.trim()));
                    }
                    sb.append("0x").append(Long.toHexString(bitVal));
                } else if (value.startsWith("{") && value.endsWith("}")) {
                    if (value.equals("{}")) {
                        sb.append("0x0");
                    } else {
                        long bitVal = 1L << Integer.parseInt(value.substring(1, value.length() - 1).trim());
                        sb.append("0x").append(Long.toHexString(bitVal));
                    }
                } else {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (value.matches("\\[B@[0-9a-f]+")) {
                sb.append("'").append(value.replace("'", "\\'")).append("'");
            } else if (isEnumType(type) && enumValuesMap.containsKey(colName)) {
                List<String> enumValues = enumValuesMap.get(colName);
                try {
                    int ordinal = Integer.parseInt(value.trim());
                    int idx = ordinal - 1;
                    if (ordinal == 0) {
                        // 序号 0 是 MySQL ENUM 的错误值 ''（非严格模式写入非法取值时留下的），
                        // 不是"第 0 个标签"。原样输出会把字符串 '0' 写进目标列。
                        sb.append("''");
                    } else if (idx >= 0 && idx < enumValues.size()) {
                        sb.append("'").append(enumValues.get(idx).replace("'", "\\'")).append("'");
                    } else {
                        sb.append("'").append(value.replace("'", "\\'")).append("'");
                    }
                } catch (NumberFormatException e) {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (isSetType(type) && enumValuesMap.containsKey(colName)) {
                List<String> setValues = enumValuesMap.get(colName);
                try {
                    long bitMask = Long.parseLong(value.trim());
                    StringBuilder setSb = new StringBuilder();
                    for (int j = 0; j < setValues.size(); j++) {
                        if ((bitMask & (1L << j)) != 0) {
                            if (setSb.length() > 0) setSb.append(",");
                            setSb.append(setValues.get(j));
                        }
                    }
                    sb.append("'").append(escapeString(setSb.toString())).append("'");
                } catch (NumberFormatException e) {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (isDatetimeType(type)) {
                String formatted = formatDatetimeValue(value);
                sb.append("'").append(formatted).append("'");
            } else if (isUnsignedType(fullType) && value.matches("-\\d+")) {
                sb.append(convertToUnsigned(value, fullType));
            } else if (isExtractNumericType(type) || value.matches("-?\\d+(\\.\\d+)?")) {
                sb.append(value);
            } else {
                sb.append("'").append(value.replace("'", "\\'")).append("'");
            }
        }
        return sb.toString();
    }

    /**
     * 类型化值管道：把 .cap 文本值按列类型转换为可直接用于 PreparedStatement 参数绑定的 Java 对象，
     * 与 {@link #formatRowData} 逐分支对应（语义一致），但产出类型化值而非 SQL 字面量——
     * 增量端据此以参数绑定执行，机制性消除引号/字面量/日期格式一类的拼接 bug。
     *
     * <p>白名单外或无法可靠解析的值返回 null（调用方整行回退旧文本路径，行为零风险）。
     * 类型选择：bit/binary/blob → byte[]，tinyint(1) → Boolean，enum/set → 标签 String，
     * 无符号负值 → 重建后的十进制 String，其余（数字/文本/时间）→ String
     * （PG 连接串 stringtype=unspecified 下由服务端按列类型推断）。
     */
    private ArrayList<Object> typeRowValues(List<String> values, String[] columnTypes, String[] columnFullTypes,
                                            String[] columnNames, Map<String, List<String>> enumValuesMap) {
        ArrayList<Object> typed = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            String type = i < columnTypes.length ? columnTypes[i] : "";
            String fullType = i < columnFullTypes.length ? columnFullTypes[i] : "";
            String colName = i < columnNames.length ? columnNames[i] : "";
            String lowerFull = fullType == null ? "" : fullType.toLowerCase();

            if (value == null || "null".equalsIgnoreCase(value)) {
                typed.add(null);
            } else if (value.startsWith(com.migration.common.lob.LobRef.MARKER)) {
                // 大字段引用：内容在磁盘上，这里只把引用原样带下去。
                // 绝不能 return null 走文本回退——文本路径没法表达一个 1GB 的值，
                // 回退过去只会把 "@lob:..." 这串字面量当成内容写进目标列（静默损坏数据）。
                com.migration.common.lob.LobRef ref = com.migration.common.lob.LobRef.parse(value);
                if (ref == null) {
                    throw new IllegalStateException("无法解析大字段引用: " + value);
                }
                typed.add(ref);
            } else if (isBinaryType(type) || isBlobType(type)) {
                if (value.matches("\\[B@[0-9a-f]+")) {
                    typed.add(null); // 与文本路径一致：无法还原的 byte[] toString 按 null 处理
                } else if (value.startsWith("0x") || value.startsWith("0X")) {
                    try {
                        typed.add(hexStringToByteArray(value.substring(2)));
                    } catch (Exception e) {
                        return null;
                    }
                } else {
                    typed.add(value);
                }
            } else if (isTextType(type) || isJsonType(type)) {
                if (value.startsWith("0x") || value.startsWith("0X")) {
                    if (isJsonType(type)) {
                        try {
                            byte[] jsonBytes = hexStringToByteArray(value.substring(2));
                            typed.add(com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary.parseAsString(jsonBytes));
                        } catch (Exception e) {
                            return null; // JSON binary 解码失败：回退文本路径的 CAST 处理
                        }
                    } else {
                        typed.add(hexToString(value.substring(2)));
                    }
                } else {
                    typed.add(value);
                }
            } else if (isBitType(type)) {
                Long bitVal = parseBitValue(value);
                if (bitVal == null) {
                    return null;
                }
                typed.add(bitValueToBytes(bitVal, lowerFull));
            } else if (lowerFull.contains("tinyint(1)") && !lowerFull.contains("unsigned")) {
                if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
                    typed.add(Boolean.TRUE);
                } else if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
                    typed.add(Boolean.FALSE);
                } else {
                    return null;
                }
            } else if (isEnumType(type) && enumValuesMap.containsKey(colName)) {
                List<String> enumValues = enumValuesMap.get(colName);
                try {
                    int ordinal = Integer.parseInt(value.trim());
                    int idx = ordinal - 1;
                    // 序号 0 = ENUM 的错误值 ''（见 formatRowData 同分支）
                    if (ordinal == 0) {
                        typed.add("");
                    } else {
                        typed.add(idx >= 0 && idx < enumValues.size() ? enumValues.get(idx) : value);
                    }
                } catch (NumberFormatException e) {
                    typed.add(value);
                }
            } else if (isSetType(type) && enumValuesMap.containsKey(colName)) {
                List<String> setValues = enumValuesMap.get(colName);
                try {
                    long bitMask = Long.parseLong(value.trim());
                    StringBuilder setSb = new StringBuilder();
                    for (int j = 0; j < setValues.size(); j++) {
                        if ((bitMask & (1L << j)) != 0) {
                            if (setSb.length() > 0) setSb.append(",");
                            setSb.append(setValues.get(j));
                        }
                    }
                    typed.add(setSb.toString());
                } catch (NumberFormatException e) {
                    typed.add(value);
                }
            } else if (isDatetimeType(type)) {
                typed.add(formatDatetimeValue(value));
            } else if (isUnsignedType(fullType) && value.matches("-\\d+")) {
                typed.add(convertToUnsigned(value, fullType));
            } else {
                // 数字/普通文本/时间字符串：原样字符串，由目标端按列类型推断
                typed.add(value);
            }
        }
        return typed;
    }

    /** 解析 .cap 中 BIT 值的各种文本形态（十进制 / 0x十六进制 / BitSet toString）为位值。 */
    private Long parseBitValue(String value) {
        try {
            if (value.startsWith("0x") || value.startsWith("0X")) {
                return Long.parseLong(value.substring(2), 16);
            }
            if (value.matches("\\d+")) {
                return Long.parseLong(value);
            }
            if (value.startsWith("{") && value.endsWith("}")) {
                String inner = value.substring(1, value.length() - 1).trim();
                if (inner.isEmpty()) return 0L;
                long bits = 0;
                for (String bit : inner.split(",")) {
                    bits |= (1L << Integer.parseInt(bit.trim()));
                }
                return bits;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    /** 按 bit(N) 宽度把位值转为大端 byte[]（与全量路径 mysql 驱动返回的 BIT byte[] 形态一致）。 */
    private byte[] bitValueToBytes(long bitVal, String lowerFullType) {
        int bits = 1;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("bit\\((\\d+)\\)").matcher(lowerFullType);
        if (m.find()) {
            bits = Integer.parseInt(m.group(1));
        }
        int nbytes = Math.max(1, (bits + 7) / 8);
        byte[] out = new byte[nbytes];
        for (int b = 0; b < nbytes; b++) {
            out[nbytes - 1 - b] = (byte) ((bitVal >> (b * 8)) & 0xFF);
        }
        return out;
    }

    private boolean isUnsignedType(String fullType) {
        if (fullType == null) return false;
        return fullType.toLowerCase().contains("unsigned");
    }

    private String convertToUnsigned(String value, String fullType) {
        try {
            long signedVal = Long.parseLong(value);
            String lower = fullType.toLowerCase();
            if (lower.startsWith("tinyint")) {
                return String.valueOf(signedVal & 0xFF);
            } else if (lower.startsWith("smallint")) {
                return String.valueOf(signedVal & 0xFFFF);
            } else if (lower.startsWith("mediumint")) {
                return String.valueOf(signedVal & 0xFFFFFF);
            } else if (lower.startsWith("int") || lower.startsWith("integer")) {
                return String.valueOf(signedVal & 0xFFFFFFFFL);
            } else if (lower.startsWith("bigint")) {
                if (signedVal < 0) {
                    BigInteger unsigned = BigInteger.valueOf(signedVal).add(BigInteger.ONE.shiftLeft(64));
                    return unsigned.toString();
                }
                return value;
            }
        } catch (NumberFormatException e) {
            return value;
        }
        return value;
    }

    private boolean isEnumType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("enum");
    }

    private boolean isSetType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("set");
    }

    /** 按顶层逗号切分（忽略括号内逗号），用于 COLUMN_TYPE 列表（enum/set/decimal 等类型体内含逗号）。 */
    static String[] splitTopLevelCommas(String s) {
        List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(s.substring(start));
        return parts.toArray(new String[0]);
    }

    protected boolean isBinaryType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("binary") || lower.equals("varbinary");
    }

    protected boolean isBlobType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("tinyblob") || lower.equals("blob") ||
                lower.equals("mediumblob") || lower.equals("longblob");
    }

    protected boolean isBitType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("bit");
    }

    protected boolean isTextType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("tinytext") || lower.equals("text") ||
                lower.equals("mediumtext") || lower.equals("longtext") ||
                lower.equals("char") || lower.equals("varchar");
    }

    protected boolean isJsonType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("json");
    }

    private String hexToString(String hex) {
        if (hex == null || hex.isEmpty()) return "";
        try {
            // 必须按 UTF-8 整体解码：逐字节 (char) 强转是 Latin-1 语义，中文/emoji 等多字节字符会乱码
            return new String(hexStringToByteArray(hex), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return hex;
        }
    }

    private byte[] hexStringToByteArray(String hex) {
        if (hex == null || hex.isEmpty()) return new byte[0];
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    protected String escapeString(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r");
    }

    protected boolean isDatetimeType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("datetime") || lower.equals("timestamp") || lower.equals("date") || lower.equals("time");
    }

    private String formatDatetimeValue(String value) {
        if (value == null || value.isEmpty()) return value;

        if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+")) {
            if (value.length() > 23) {
                return value.substring(0, 23);
            }
            return value;
        }
        if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
            return value;
        }
        if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return value;
        }
        if (value.matches("\\d{2}:\\d{2}:\\d{2}.*")) {
            if (value.length() > 12) {
                return value.substring(0, 12);
            }
            return value;
        }

        try {
            java.text.SimpleDateFormat inputFmt = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", java.util.Locale.ENGLISH);
            java.text.SimpleDateFormat outputFmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = inputFmt.parse(value.trim());
            return outputFmt.format(date);
        } catch (Exception e) {
            return value;
        }
    }

    private boolean isExtractNumericType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("int") || lower.equals("integer") || lower.equals("bigint") ||
                lower.equals("smallint") || lower.equals("tinyint") || lower.equals("mediumint") ||
                lower.equals("float") || lower.equals("double") || lower.equals("decimal") ||
                lower.equals("numeric") || lower.equals("year");
    }

    private void parseQueryEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "QUERY");
        java.util.regex.Matcher sqlMatcher = java.util.regex.Pattern.compile("sql='(.+)'\\}$").matcher(eventData);
        if (sqlMatcher.find()) {
            thlEvent.addMetadata("sql", sqlMatcher.group(1));
        } else {
            thlEvent.addMetadata("sql", eventData);
        }
        java.util.regex.Matcher dbMatcher = java.util.regex.Pattern.compile("database='([^']*)'").matcher(eventData);
        if (dbMatcher.find()) {
            String database = dbMatcher.group(1);
            thlEvent.addMetadata("database_name", database);
            String sql = thlEvent.getMetadata().getOrDefault("sql", "").toString();
            String ddlDatabase = DdlDatabaseExtractor.extractDatabase(sql, database, "");
            if (ddlDatabase != null && !ddlDatabase.isEmpty()) {
                thlEvent.addMetadata("ddl_database", ddlDatabase);
            }
            // ALTER/DROP/RENAME/TRUNCATE/CREATE TABLE 会改变列布局：失效该表的列元数据缓存，
            // 否则后续 WRITE/UPDATE_ROWS 会沿用 ALTER 前的旧列名，静默丢弃新增列的值。
            invalidateColumnCachesForDdl(sql, database);

            // 同一条 DDL 也施加到表结构时序库上，推出该表的下一个版本。
            // 与上面那行失效缓存的区别正是这次改造的要点：失效缓存之后重查的是源库<b>此刻</b>的
            // 定义（可能已经又变过好几次），而施加式算出的是这条 DDL <b>当时</b>的结构。
            feedSchemaTimeline(sql, database, thlEvent);
        }
    }

    /**
     * 把 DDL 喂给时序库。解析失败时按 {@code extract.schema.timeline.fallback} 处置：
     * RESNAPSHOT 只告警（该表退回 information_schema 旧路径），FAIL_STOP 抛出让抽取停下。
     *
     * <p>阶段 3 里两者的实际后果一样——行事件本来就还走旧路径——但计数与告警要从现在起就准，
     * 灰度期正是靠"解析失败数"这个指标判断语法够不够用。
     */
    private void feedSchemaTimeline(String sql, String database, THLEvent thlEvent) {
        if (schemaTracker == null) {
            return;
        }
        String binlogFile = String.valueOf(thlEvent.getMetadata().getOrDefault("binlog_file", ""));
        long binlogPos = 0L;
        Object pos = thlEvent.getMetadata().get("binlog_position");
        if (pos instanceof Number) {
            binlogPos = ((Number) pos).longValue();
        }
        boolean ok = schemaTracker.onDdl(sql, database, binlogFile, binlogPos);
        if (!ok && schemaTracker.getConfig().getFallback()
                == com.migration.extract.schema.SchemaTimelineConfig.Fallback.FAIL_STOP) {
            throw new com.migration.extract.schema.DdlParseException(
                    "表结构时序库施加 DDL 失败，且 extract.schema.timeline.fallback=FAIL_STOP", sql);
        }
    }

    /**
     * 对表级 DDL 影响的每张表，移除全部按 {@code database.table} 键缓存的列元数据，
     * 使下一个数据事件重新查询 information_schema，拿到 DDL 后的最新列布局。
     */
    protected void invalidateColumnCachesForDdl(String sql, String defaultDatabase) {
        for (String cacheKey : DdlDatabaseAnltrExtractor.extractAffectedTables(sql, defaultDatabase)) {
            boolean removed = false;
            removed |= tableSchemaCache.remove(cacheKey) != null;
            removed |= tableColumnTypeCache.remove(cacheKey) != null;
            removed |= tableColumnFullTypeCache.remove(cacheKey) != null;
            removed |= enumSetValuesCache.remove(cacheKey) != null;
            removed |= primaryKeyCache.remove(cacheKey) != null;
            removed |= generatedColumnCache.remove(cacheKey) != null;
            if (removed) {
                logger.info("DDL 变更表 {}, 失效列元数据缓存，下个数据事件将重新读取 information_schema", cacheKey);
            }
        }
    }

    /** {@code TableMapEventData{... columnTypes=3, 8, 17, columnMetadata=...}} 里的列类型串。 */
    private static final java.util.regex.Pattern TABLE_MAP_COLUMN_TYPES =
            java.util.regex.Pattern.compile("columnTypes=([^=]*?), columnMetadata=");

    /** {@code TableMapEventMetadata{... columnNames=id, ts, updated_at, setStrValues=...}} 里的列名串。 */
    private static final java.util.regex.Pattern TABLE_MAP_COLUMN_NAMES =
            java.util.regex.Pattern.compile("columnNames=(.*?), setStrValues=");

    /** 已经就"该表列布局与源库当前定义不一致"告过警的表（每表一次，不刷屏）。 */
    private final java.util.Set<String> columnLayoutWarned = new java.util.HashSet<>();

    /**
     * 解析这条 TABLE_MAP 对应的列清单——<b>优先用事件自带的列名</b>。
     *
     * <p>为什么不能只信 {@code information_schema}：那查的是<b>此刻</b>的表定义，而事件是过去
     * 某一刻的。链路有延迟时源库执行 {@code ALTER TABLE ... ADD COLUMN x AFTER a} 或
     * {@code DROP COLUMN}，DDL 之前那些还没处理完的行事件就会按新布局对齐——整行左移/右移，
     * 写进目标库的是合法值、看不出任何异常。这正是 Debezium 用 schema history、Canal 用表结构
     * 时序（tsdb）在解决的问题。
     *
     * <p>MySQL 8.0.1+ 的 {@code binlog_row_metadata=FULL} 会把列名<b>放进 TABLE_MAP 事件本身</b>，
     * 那是与行值同一时刻的权威信息，直接用它就没有漂移可言。拿不到（MINIMAL / 5.7）时退回
     * information_schema，并用事件里的列<b>数</b>兜底校验：数量不符说明表结构在这中间变过，
     * 此时无法把值正确对上列，只能停下来上报（E3021），而不是像以前那样"多的截断、少的补 null"
     * 静默写坏。
     */
    private List<String> resolveColumns(String database, String table, String eventData) {
        String cacheKey = database + "." + table;
        List<String> current = getTableColumns(database, table);
        List<String> eventColumns = parseEventColumnNames(eventData);

        if (!eventColumns.isEmpty()) {
            // 事件自带列名（binlog_row_metadata=FULL）：这是与行值同一时刻的权威信息，
            // 表结构在这中间怎么变都不影响——值按事件的列名对齐，类型再按<b>列名</b>去查
            // （见 columnMetaByName），加列/删列/改名都能自愈，不用停任务
            if (!current.equals(eventColumns) && columnLayoutWarned.add(cacheKey)) {
                logger.warn("表 {} 的事件列布局 {} 与源库当前定义 {} 不一致（表结构在抽取过程中变更过），"
                        + "以事件自带的列名为准", cacheKey, eventColumns, current);
            }
            return eventColumns;
        }

        int eventCount = parseEventColumnCount(eventData);
        if (eventCount > 0 && !current.isEmpty() && eventCount != current.size()) {
            // 拿不到列名（binlog_row_metadata=MINIMAL）又列数对不上：没有任何办法把值正确对上列，
            // 硬解就是整行错位的静默写坏，只能停下来
            throw new ColumnLayoutMismatchException(String.format(
                    "表 %s 的行事件有 %d 列，源库当前定义是 %d 列——表结构在抽取过程中变更过，"
                            + "按当前定义解析会让整行的值与列错位（写进去的是合法值，看不出异常）。"
                            + "请把源库 binlog_row_metadata 设为 FULL（列名随事件一起下发，"
                            + "这种情况可以自愈），或等这段积压追平后再做 DDL",
                    cacheKey, eventCount, current.size()));
        }
        return current;
    }

    /**
     * 按<b>列名</b>取列元数据，而不是按下标。
     *
     * <p>用事件自带的列布局时，列的顺序和数量都可能与源库当前定义不同，
     * 而类型/精度只能从 {@code information_schema} 拿——按下标取就会错位，必须按名字查。
     * 查不到的列（DDL 之后已经删掉的列）给空串，下游按"无类型信息"处理。
     */
    private List<String> columnMetaByName(List<String> wantedColumns, List<String> schemaColumns,
                                          List<String> schemaValues) {
        Map<String, String> byName = new HashMap<>();
        for (int i = 0; i < schemaColumns.size() && i < schemaValues.size(); i++) {
            byName.put(schemaColumns.get(i), schemaValues.get(i));
        }
        List<String> out = new ArrayList<>(wantedColumns.size());
        for (String column : wantedColumns) {
            String v = byName.get(column);
            out.add(v == null ? "" : v);
        }
        return out;
    }

    /** 抽取时发现事件的列布局与当前表定义对不上：由 ContinuousExtractMain 收口成 error_status。 */
    public static final class ColumnLayoutMismatchException extends RuntimeException {
        public ColumnLayoutMismatchException(String message) {
            super(message);
        }
    }

    private static List<String> parseEventColumnNames(String eventData) {
        List<String> names = new ArrayList<>();
        if (eventData == null) {
            return names;
        }
        java.util.regex.Matcher m = TABLE_MAP_COLUMN_NAMES.matcher(eventData);
        if (!m.find()) {
            return names;
        }
        String raw = m.group(1).trim();
        if (raw.isEmpty() || "null".equals(raw)) {
            return names;
        }
        for (String name : raw.split("\\s*,\\s*")) {
            if (!name.isEmpty()) {
                names.add(name);
            }
        }
        return names;
    }

    private static int parseEventColumnCount(String eventData) {
        if (eventData == null) {
            return 0;
        }
        java.util.regex.Matcher m = TABLE_MAP_COLUMN_TYPES.matcher(eventData);
        if (!m.find()) {
            return 0;
        }
        String raw = m.group(1).trim();
        if (raw.isEmpty() || "null".equals(raw)) {
            return 0;
        }
        return raw.split("\\s*,\\s*").length;
    }

    protected List<String> getTableColumns(String database, String table) {
        String cacheKey = database + "." + table;
        if (tableSchemaCache.containsKey(cacheKey)) {
            return tableSchemaCache.get(cacheKey);
        }

        List<String> columns = new ArrayList<>();
        try {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
            tableSchemaCache.put(cacheKey, columns);
        } catch (SQLException e) {
            logger.error("Error fetching columns for {}.{}: {}", database, table, e.getMessage());
        }
        return columns;
    }

    protected List<String> getTableColumnTypes(String database, String table) {
        String cacheKey = database + "." + table;
        if (tableColumnTypeCache.containsKey(cacheKey)) {
            return tableColumnTypeCache.get(cacheKey);
        }

        List<String> columnTypes = new ArrayList<>();
        List<String> columnFullTypes = new ArrayList<>();
        Map<String, List<String>> enumSetValues = new HashMap<>();
        List<String> generated = new ArrayList<>();
        try {
            String sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, EXTRA FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columnTypes.add(rs.getString("DATA_TYPE"));
                        String columnType = rs.getString("COLUMN_TYPE");
                        columnFullTypes.add(columnType != null ? columnType : "");
                        String columnName = rs.getString("COLUMN_NAME");
                        if (columnType != null && (columnType.startsWith("enum(") || columnType.startsWith("set("))) {
                            List<String> values = parseEnumSetValues(columnType);
                            enumSetValues.put(columnName, values);
                        }
                        // 生成列（STORED / VIRTUAL）：binlog 行事件里<b>带着它们算好的值</b>，
                        // 但目标库不接受显式写入（MySQL 3105）。列清单必须保留它们——行事件的值是
                        // 按全列顺序排的，剔掉列名会让后面所有列错位——只能在生成 DML 时跳过。
                        String extra = rs.getString("EXTRA");
                        if (extra != null && extra.toUpperCase().contains("GENERATED")) {
                            generated.add(columnName);
                        }
                    }
                }
            }
            tableColumnTypeCache.put(cacheKey, columnTypes);
            tableColumnFullTypeCache.put(cacheKey, columnFullTypes);
            generatedColumnCache.put(cacheKey, generated);
            if (!enumSetValues.isEmpty()) {
                enumSetValuesCache.put(cacheKey, enumSetValues);
            }
            if (!generated.isEmpty()) {
                logger.info("表 {} 含生成列 {}，增量应用时会跳过这些列（由目标库按表达式自行计算）",
                        cacheKey, generated);
            }
        } catch (SQLException e) {
            logger.error("Error fetching column types for {}.{}: {}", database, table, e.getMessage());
        }
        return columnTypes;
    }

    /**
     * 从 {@code enum('a','b')} / {@code set('x','y')} 里切出取值表。
     *
     * <p><b>空串取值必须保留占位</b>：MySQL 允许 {@code enum('','a')}，序号按声明顺序从 1 排
     * （1='' 2='a'）。闭合引号处若加 {@code sb.length() > 0} 判断把空串跳过，取值表就塌成
     * {@code ["a"]}——下游按序号还原时 1 会被解成 'a'（本该是 ''）、2 直接越界，整列静默写错。
     *
     * <p>与 {@code SchemaSelfCheck.parseEnumSetValues} 是同一套规则（含 {@code \\'} 转义），
     * 改这里必须同步改那边，否则表结构语法自检会报出一堆假差异。
     */
    private List<String> parseEnumSetValues(String columnType) {
        List<String> values = new ArrayList<>();
        int start = columnType.indexOf('(');
        int end = columnType.lastIndexOf(')');
        if (start < 0 || end < 0) return values;

        String inner = columnType.substring(start + 1, end);
        StringBuilder sb = new StringBuilder();
        boolean inQuote = false;

        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '\'' && (i == 0 || inner.charAt(i - 1) != '\\')) {
                inQuote = !inQuote;
                if (!inQuote) {
                    values.add(sb.toString());
                    sb = new StringBuilder();
                }
            } else if (inQuote) {
                if (c == '\\' && i + 1 < inner.length()) {
                    char next = inner.charAt(i + 1);
                    if (next == '\'' || next == '\\') {
                        sb.append(next);
                        i++;
                    } else {
                        sb.append(c);
                    }
                } else {
                    sb.append(c);
                }
            }
        }
        return values;
    }

    protected List<String> getTablePrimaryKeys(String database, String table) {
        String cacheKey = database + "." + table;
        if (primaryKeyCache.containsKey(cacheKey)) {
            return primaryKeyCache.get(cacheKey);
        }

        List<String> pkColumns = new ArrayList<>();
        try {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' " +
                    "ORDER BY ORDINAL_POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        pkColumns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
            primaryKeyCache.put(cacheKey, pkColumns);
        } catch (SQLException e) {
            logger.error("Error fetching primary key for {}.{}: {}", database, table, e.getMessage());
        }
        return pkColumns;
    }

    private void loadSeqno() {
        File file = new File(seqnoFile);
        if (!file.exists()) {
            logger.info("No seqno file found, starting from 1");
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            if (line != null && !line.trim().isEmpty()) {
                seqno = Long.parseLong(line.trim()) + 1;
                logger.info("Loaded seqno from file, starting from: {}", seqno);
            }
        } catch (Exception e) {
            logger.warn("Error loading seqno file, starting from 1: {}", e.getMessage());
            seqno = 1;
        }
    }

    public void saveSeqno() {
        File file = new File(seqnoFile);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(String.valueOf(seqno - 1));
        } catch (IOException e) {
            logger.error("Error saving seqno file", e);
        }
    }

    public long getCurrentSeqno() {
        return seqno;
    }

    public void incrementSeqnoForHeartbeat() {
        seqno++;
    }

    public void close() {
        saveSeqno();
        if (xaBuffer != null) {
            // 收集中的分支只关句柄不做收口：那份 .part 落盘文件下次启动会被删掉，
            // 由压回分支起点的 .cap 进度重新收集一遍
            xaBuffer.close();
        }
        if (pipeline != null) {
            pipeline.release();
        }
        if (sourceConnection != null) {
            try {
                sourceConnection.close();
            } catch (SQLException e) {
                logger.error("Error closing source connection", e);
            }
        }
    }
}
