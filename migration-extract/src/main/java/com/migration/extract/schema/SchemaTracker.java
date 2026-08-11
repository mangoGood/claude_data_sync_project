package com.migration.extract.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 * 表结构时序库在抽取端的入口：喂基线、喂 DDL、按位点查版本。
 *
 * <p>持有 {@link SchemaTimeline}（内存索引）、{@link SchemaHistoryFile}（落盘）与
 * {@link DdlApplier}（推演），把三者串成一条：
 *
 * <pre>
 *   capture 的 SCHEMA_BASELINE ─┐
 *                              ├─→ DdlApplier ─→ SchemaTimeline ─→ schema_history.jsonl
 *   binlog 里的 DDL ────────────┘                      ↑
 *                                          行事件按位点查这里（阶段 4 接）
 * </pre>
 *
 * <p><b>本阶段是只写不读的</b>：行事件的解析仍然走 {@code information_schema} 旧路径，
 * 时序库只管把版本攒起来、落盘、上传。这样阶段 3 对同步行为零改变，可以单独上线跑一段时间，
 * 用真实流量把语法覆盖度和版本正确性攒出来，再在阶段 4 切换解析来源。
 */
public class SchemaTracker {

    private static final Logger logger = LoggerFactory.getLogger(SchemaTracker.class);

    private final SchemaTimelineConfig config;
    private final SchemaTimeline timeline = new SchemaTimeline();
    private final SchemaHistoryFile history;
    private final CreateTableParser createTableParser = new CreateTableParser();
    private final DdlApplier applier = new DdlApplier();

    /** 基线播种数 / 因已有版本而忽略的基线数 / 施加成功的 DDL 数 / 解析失败的 DDL 数。 */
    private final LongAdder baselineSeeded = new LongAdder();
    private final LongAdder baselineIgnored = new LongAdder();
    private final LongAdder ddlApplied = new LongAdder();
    private final LongAdder ddlParseFailed = new LongAdder();

    public SchemaTracker(Properties props, String taskId) {
        this.config = SchemaTimelineConfig.load(props, taskId);
        this.history = new SchemaHistoryFile(config.getHistoryPath());
        int loaded = history.load(timeline);
        logger.info("表结构时序库启动: mode={} fallback={} 已装载 {} 个版本 / {} 张表 | {}",
                config.getMode(), config.getFallback(), loaded, timeline.tableCount(),
                config.getHistoryPath());
    }

    public boolean isEnabled() {
        return config.isEnabled();
    }

    public SchemaTimelineConfig getConfig() {
        return config;
    }

    public SchemaTimeline getTimeline() {
        return timeline;
    }

    /**
     * 消费一条 capture 写的基线记录。
     *
     * <p>格式：{@code SCHEMA_BASELINE|file|pos|ts|serverId|db|table|error|CREATE TABLE 原文}
     *
     * <p><b>已持久化的时序库优先，基线只是兜底种子</b>：这张表已经有版本了就忽略这条基线。
     * capture 被 ProcessGuard 重启时会重新打一份基线，而那份基线反映的是"现在"的结构、
     * 位点却标在重启位点上——比它该有的样子新。已有版本的表用自己的版本链才是对的。
     */
    public void onBaseline(String[] fields, String binlogFile, long binlogPos) {
        if (!config.isEnabled() || fields.length < 8) {
            return;
        }
        String db = fields[5].trim();
        String table = fields[6].trim();
        String error = fields[7].trim();
        String createSql = fields.length > 8 ? fields[8] : "";

        if (db.isEmpty() || table.isEmpty()) {
            return;
        }
        if (timeline.hasTable(db, table)) {
            baselineIgnored.increment();
            logger.debug("表 {}.{} 已有版本，忽略本次基线（已持久化的时序库优先）", db, table);
            return;
        }

        if (!error.isEmpty() || createSql.isEmpty()) {
            // 取不到结构也要落一个不可用版本：静默跳过的话，抽取端分不清"这张表没有基线"
            // 与"这张表压根不在同步范围"，前者该降级告警、后者不该有任何动静
            TableSchema unusable = new TableSchema(db, table);
            unusable.markUnusable("基线取不到表结构: " + (error.isEmpty() ? "SHOW CREATE TABLE 为空" : error));
            record(SchemaChange.created(unusable), binlogFile, binlogPos);
            logger.warn("表 {}.{} 的结构基线不可用（{}），该表将按 fallback={} 处置",
                    db, table, error, config.getFallback());
            return;
        }

        try {
            TableSchema schema = createTableParser.parse(createSql, db);
            record(SchemaChange.created(schema), binlogFile, binlogPos);
            baselineSeeded.increment();
        } catch (DdlParseException e) {
            ddlParseFailed.increment();
            TableSchema unusable = new TableSchema(db, table);
            unusable.markUnusable("基线解析失败: " + e.getMessage());
            record(SchemaChange.created(unusable), binlogFile, binlogPos);
            logger.warn("表 {}.{} 的结构基线解析失败（语法缺口，E3023），该表将按 fallback={} 处置: {}",
                    db, table, config.getFallback(), e.getMessage());
        }
    }

    /**
     * 消费一条 DDL：施加到模型上，把新版本写进时序库。
     *
     * @param sql             DDL 原文
     * @param defaultDatabase QUERY 事件的 {@code database=}，非限定表名的库上下文
     * @return 解析失败时返回 false（调用方按 {@code fallback} 决定告警还是停机）
     */
    public boolean onDdl(String sql, String defaultDatabase, String binlogFile, long binlogPos) {
        if (!config.isEnabled()) {
            return true;
        }
        try {
            List<SchemaChange> changes = applier.apply(sql, defaultDatabase, timeline.asLookup());
            for (SchemaChange change : changes) {
                record(change, binlogFile, binlogPos);
            }
            if (!changes.isEmpty()) {
                ddlApplied.increment();
            }
            return true;
        } catch (DdlParseException e) {
            ddlParseFailed.increment();
            logger.warn("DDL 施加失败（E3023，语法缺口）@ {}:{} — {}", binlogFile, binlogPos, e.getMessage());
            return false;
        }
    }

    /** 落一个版本：内存索引 + 落盘。同位点重复（重放）时不重复落盘。 */
    private void record(SchemaChange change, String binlogFile, long binlogPos) {
        if (!timeline.append(change, binlogFile, binlogPos)) {
            return;
        }
        history.append(SchemaTimeline.entryOf(binlogFile, binlogPos,
                change.getDatabase(), change.getTable(), change.getKind(), change.getSchema()));
    }

    /**
     * 按事件位点取该表当时的结构。<b>阶段 3 还没有调用方</b>——行事件仍走
     * {@code information_schema}，切换在阶段 4。
     */
    public TableSchema at(String database, String table, String binlogFile, long binlogPos) {
        return timeline.at(database, table, binlogFile, binlogPos);
    }

    /** 裁剪并重写历史文件。由抽取端在推进位点时按低频调用。 */
    public void prune(String binlogFile, long binlogPos) {
        int removed = timeline.prune(SchemaTimeline.monotonicKey(binlogFile, binlogPos));
        if (removed > 0) {
            history.rewrite(timeline);
        }
    }

    public String statsLine() {
        return String.format("表结构时序库: 表=%d 版本=%d 基线播种=%d 基线忽略=%d DDL施加=%d 解析失败=%d %s",
                timeline.tableCount(), timeline.versionCount(),
                baselineSeeded.sum(), baselineIgnored.sum(),
                ddlApplied.sum(), ddlParseFailed.sum(), applier.statsLine());
    }

    public long getDdlParseFailed() {
        return ddlParseFailed.sum();
    }

    public DdlApplier getApplier() {
        return applier;
    }

    int getTimelineTableCountForTest() {
        return timeline.tableCount();
    }

    int getTimelineVersionCountForTest() {
        return timeline.versionCount();
    }
}
