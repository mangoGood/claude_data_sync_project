package com.migration.mongo;

import com.migration.config.ColumnProcessingConfig;
import com.migration.config.ColumnProcessingConfig.ExtraColumn;
import com.migration.config.ColumnProcessingConfig.FilterCondition;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * MongoDB 同步的列处理（列过滤 / 列名映射 / 附加列），复用 SQL 侧同一份
 * {@link ColumnProcessingConfig}（键为 {@code 源库.集合}，与 mysql/pg 表级同步一致）。
 *
 * <p>与 SQL 的差异：Mongo 无 DDL/DEFAULT，附加列不能靠建表承载，必须在<b>写入每个文档时注值</b>；
 * 列名映射即改写文档字段名；列过滤在 Java 侧按文档字段值判定（无法下推 SQL WHERE）。
 * 因此本类提供文档级的 {@link #excluded}（是否命中过滤被排除）与 {@link #transform}
 * （改名 + 附加列），供全量复制与增量 Change Stream 复用。
 *
 * <ul>
 *   <li>全量：命中过滤的文档跳过不写；其余改名 + 附加列后 upsert。</li>
 *   <li>增量：after 镜像命中过滤 → 目标端按 _id 删除（与 mysql/pg 的
 *       "UPDATE 后镜像被过滤 → 转 DELETE" 语义一致）；否则改名 + 附加列后 upsert。</li>
 * </ul>
 */
public final class MongoDocumentProcessor {

    private final ColumnProcessingConfig config;

    public MongoDocumentProcessor(ColumnProcessingConfig config) {
        this.config = config;
    }

    public static MongoDocumentProcessor fromProperties(Properties props) {
        return new MongoDocumentProcessor(ColumnProcessingConfig.loadFromProperties(props));
    }

    /** 该集合是否配置了任意列处理（过滤/映射/附加列）；否则调用方可整体短路走原始文档。 */
    public boolean isActive(String db, String coll) {
        return config != null && config.hasProcessing(db, coll);
    }

    /** 整个任务是否毫无列处理配置。 */
    public boolean isEmpty() {
        return config == null || config.isEmpty();
    }

    /**
     * 该文档是否命中过滤条件而应被排除（不同步）。列名大小写不敏感；
     * BSON 专有数值类型（Decimal128）折算为 BigDecimal 后按数值比较，其余类型
     * （Integer/Long/Double/Date/Boolean/String）交给 {@link ColumnProcessingConfig} 判定。
     */
    public boolean excluded(String db, String coll, Document doc) {
        if (doc == null) {
            return false;
        }
        List<FilterCondition> filters = config.getFilters(db, coll);
        if (filters.isEmpty()) {
            return false;
        }
        String[] names = doc.keySet().toArray(new String[0]);
        List<Object> values = new ArrayList<>(names.length);
        for (String name : names) {
            values.add(normalizeForCompare(doc.get(name)));
        }
        return config.rowExcluded(db, coll, names, values);
    }

    /**
     * 改名 + 附加列后的新文档（不做过滤——过滤由 {@link #excluded} 独立判定）。
     * 保序：按原字段顺序改名，{@code _id} 恒不改名；随后追加附加列。
     * 无映射且无附加列时原样返回，避免无谓拷贝。
     */
    public Document transform(String db, String coll, Document doc) {
        if (doc == null) {
            return null;
        }
        Map<String, String> mapping = config.getColumnMapping(db, coll);
        List<ExtraColumn> extras = config.getExtraColumns(db, coll);
        if (mapping.isEmpty() && extras.isEmpty()) {
            return doc;
        }

        Document out = new Document();
        for (Map.Entry<String, Object> e : doc.entrySet()) {
            String key = e.getKey();
            // _id 是主键/删除定位键，绝不改名
            String target = "_id".equals(key) ? "_id" : config.mapColumn(db, coll, key);
            out.put(target, e.getValue());
        }
        for (ExtraColumn ex : extras) {
            out.put(ex.name, extraValue(ex, db, coll));
        }
        return out;
    }

    /** 索引 key 字段名按列名映射改写（源集合 note 上的唯一索引 → 目标端 remark 上）。 */
    public String mapField(String db, String coll, String field) {
        if ("_id".equals(field)) {
            return "_id";
        }
        return config.mapColumn(db, coll, field);
    }

    private Object extraValue(ExtraColumn ex, String db, String coll) {
        switch (ex.kind) {
            case CREATE_TIME:
            case UPDATE_TIME:
                // Mongo 无列级 DEFAULT/ON UPDATE：写入时点即为"首次同步时间"/"最近更新时间"
                return new Date();
            case CUSTOM:
            default:
                return (ex.customValue == null ? "" : ex.customValue) + "@" + db + "@" + coll;
        }
    }

    /** BSON 专有类型折算为 {@link ColumnProcessingConfig#compare} 可比较的 Java 类型。 */
    private static Object normalizeForCompare(Object v) {
        if (v instanceof Decimal128) {
            return ((Decimal128) v).bigDecimalValue();
        }
        return v;
    }
}
