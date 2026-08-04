package com.migration.common.route;

import java.util.Collections;
import java.util.List;

/**
 * 汇聚规则（N:1）：一组源表 → 同一张目标表。
 *
 * <p><b>主键策略与幂等装载是一体的</b>：汇聚全量走 upsert（不再"未完成即清表重搬"，
 * 那会清掉同一目标表里其他源已搬完的数据）。upsert 的冲突目标是目标表主键——
 * 若沿用源主键（{@link PkStrategy#KEEP}），两个分片的同主键行会互相覆盖、静默丢数据。
 * 因此默认 {@link PkStrategy#COMPOSITE_SOURCE}：目标表主键 = 源主键 + 来源标识列，
 * {@code KEEP} 只在用户显式声明源主键全局唯一（雪花 ID / 全局序列）时开放。
 */
public final class MergeRule {

    /** 目标表主键策略 */
    public enum PkStrategy {
        /** 目标主键 = 源主键 + 来源标识列（默认，唯一能让 upsert 在多源下不丢数据的选择） */
        COMPOSITE_SOURCE,
        /** 沿用源主键：要求用户保证跨源全局唯一 */
        KEEP
    }

    /** 同一目标表被 N 个源重复下发 DDL 时的处理 */
    public enum DdlPolicy {
        /** 首个到达的生效，同指纹后续跳过（默认） */
        FIRST_WINS,
        /** 汇聚表不接受任何源端 DDL */
        SKIP,
        /** 只记录，人工应用 */
        MANUAL
    }

    /** 默认来源标识列：实例 / 库 / 表，三级足以在跨实例同名库表下唯一定位来源 */
    public static final List<String> DEFAULT_TAG_COLUMNS =
            Collections.unmodifiableList(java.util.Arrays.asList("_src_node", "_src_db", "_src_table"));

    public static final String TAG_NODE = "_src_node";
    public static final String TAG_DB = "_src_db";
    public static final String TAG_TABLE = "_src_table";

    private final String id;
    private final TablePattern pattern;
    private final String targetDb;
    private final String targetTable;
    private final PkStrategy pkStrategy;
    private final List<String> tagColumns;
    private final DdlPolicy ddlPolicy;

    public MergeRule(String id, TablePattern pattern, String targetDb, String targetTable,
                     PkStrategy pkStrategy, List<String> tagColumns, DdlPolicy ddlPolicy) {
        this.id = id;
        this.pattern = pattern;
        this.targetDb = targetDb;
        this.targetTable = targetTable;
        this.pkStrategy = pkStrategy == null ? PkStrategy.COMPOSITE_SOURCE : pkStrategy;
        // null = 未配置（用默认三列）；空列表 = 用户显式声明不加来源列（只在 KEEP 策略下合法，加载期已校验）
        this.tagColumns = tagColumns == null
                ? DEFAULT_TAG_COLUMNS : Collections.unmodifiableList(new java.util.ArrayList<>(tagColumns));
        this.ddlPolicy = ddlPolicy == null ? DdlPolicy.FIRST_WINS : ddlPolicy;
    }

    public String getId() {
        return id;
    }

    public TablePattern getPattern() {
        return pattern;
    }

    /** 目标库；null/空 = 用任务默认目标库 */
    public String getTargetDb() {
        return targetDb;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public PkStrategy getPkStrategy() {
        return pkStrategy;
    }

    public List<String> getTagColumns() {
        return tagColumns;
    }

    public DdlPolicy getDdlPolicy() {
        return ddlPolicy;
    }

    /**
     * 来源标识列的取值。未知列名返回 null（调用方按"该列不注值"处理，而不是写空串——
     * 空串会和"来源确实是空库名"混淆）。
     *
     * @param nodeId 来源实例标识（单实例汇聚传任务默认实例名或 null）
     */
    public String tagValue(String tagColumn, String nodeId, String sourceDb, String sourceTable) {
        if (tagColumn == null) {
            return null;
        }
        switch (tagColumn) {
            case TAG_NODE:
                return nodeId == null ? "" : nodeId;
            case TAG_DB:
                return sourceDb;
            case TAG_TABLE:
                return sourceTable;
            default:
                return null;
        }
    }

    /** 目标落点（汇聚不看行数据，落点恒定）。 */
    public RouteTarget target(String defaultTargetDb) {
        String db = (targetDb == null || targetDb.isEmpty()) ? defaultTargetDb : targetDb;
        return RouteTarget.merged(null, db, targetTable);
    }

    @Override
    public String toString() {
        return "merge[" + id + "] " + pattern + " -> "
                + (targetDb == null ? "" : targetDb + ".") + targetTable
                + " pk=" + pkStrategy + " ddl=" + ddlPolicy;
    }
}
