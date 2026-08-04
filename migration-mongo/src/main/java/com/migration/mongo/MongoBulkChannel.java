package com.migration.mongo;

import com.migration.common.bulk.BulkLoadChannel;
import com.migration.common.bulk.BulkLoadOptions;
import com.migration.common.bulk.BulkLoadStats;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MongoDB 全量的 bulkWrite 装载通道。
 *
 * <p>相对原先"固定 1000 文档一批的 ReplaceOne upsert"补了三件事：
 *
 * <ul>
 *   <li><b>首搬走 insertMany 快路径</b>：目标集合为空时不存在冲突，{@link InsertOneModel} 省掉
 *       ReplaceOne 每条都要做的 {@code _id} 匹配；续搬/重跑（目标已有文档）仍用
 *       {@link ReplaceOneModel} upsert 保持幂等。</li>
 *   <li><b>字节阈值</b>：只按条数攒批时，大文档集合一批就能撞上驱动的批大小上限，
 *       且内存里同时压着上千个大文档。</li>
 *   <li><b>按文档计失败</b>：{@code ordered(false)} 下批里个别文档失败不影响其余文档落库，
 *       {@link MongoBulkWriteException} 会带回<b>逐条</b>错误——照着它计数，而不是把整批算失败。
 *       重复键（11000）按"目标端已有该文档"计成功，与 SQL 全量的口径一致。</li>
 * </ul>
 */
final class MongoBulkChannel implements BulkLoadChannel<Document> {
    private static final Logger logger = LoggerFactory.getLogger(MongoBulkChannel.class);

    /** 单批文档数默认值（沿用原实现的 1000，行为不变）。 */
    static final int DEFAULT_BATCH_DOCS = 1000;
    /** 单批字节默认值：远低于驱动 48MB 的批上限，留足 BSON 编码开销。 */
    static final long DEFAULT_BATCH_BYTES = 8L * 1024 * 1024;
    private static final int DUPLICATE_KEY = 11000;

    private final MongoCollection<Document> target;
    private final boolean insertFastPath;
    private final int batchDocs;
    private final long batchBytes;
    private final BulkLoadStats stats = new BulkLoadStats();
    private final List<WriteModel<Document>> buffered = new ArrayList<>();
    private final ReplaceOptions upsert = new ReplaceOptions().upsert(true);
    private final BulkWriteOptions unordered = new BulkWriteOptions().ordered(false);
    private long bufferedBytes;

    /**
     * @param insertFastPath 目标集合为空（首次搬运）时为 true，走 insertMany 快路径；
     *                       续搬/重跑必须为 false，否则已存在的文档会全部报重复键
     */
    MongoBulkChannel(MongoCollection<Document> target, BulkLoadOptions options, boolean insertFastPath) {
        this.target = target;
        this.insertFastPath = insertFastPath;
        this.batchDocs = options.rows(DEFAULT_BATCH_DOCS);
        this.batchBytes = options.bytes(DEFAULT_BATCH_BYTES);
    }

    @Override
    public void add(Document doc) {
        buffered.add(insertFastPath
                ? new InsertOneModel<>(doc)
                : new ReplaceOneModel<>(new Document("_id", doc.get("_id")), doc, upsert));
        bufferedBytes += estimateSize(doc);
    }

    @Override
    public boolean isFull() {
        return buffered.size() >= batchDocs || bufferedBytes >= batchBytes;
    }

    @Override
    public boolean isEmpty() {
        return buffered.isEmpty();
    }

    BulkLoadStats stats() {
        return stats;
    }

    @Override
    public long[] flush() {
        if (buffered.isEmpty()) {
            return new long[]{0, 0};
        }
        int submitted = buffered.size();
        long flushedBytes = bufferedBytes;
        long[] result;
        try {
            target.bulkWrite(buffered, unordered);
            result = new long[]{submitted, 0};
        } catch (MongoBulkWriteException e) {
            long fail = 0;
            long duplicates = 0;
            for (BulkWriteError err : e.getWriteErrors()) {
                if (err.getCode() == DUPLICATE_KEY) {
                    duplicates++;
                } else {
                    fail++;
                    if (fail <= 3) {
                        logger.error("bulkWrite 文档失败: code={}, {}", err.getCode(), err.getMessage());
                    }
                }
            }
            if (duplicates > 0) {
                logger.debug("bulkWrite 有 {} 条重复键（目标端已有该文档），计成功", duplicates);
            }
            stats.recordBatchFailure(fail);
            result = new long[]{submitted - fail, fail};
        }
        stats.recordBatch(result[0], result[1], flushedBytes);
        buffered.clear();
        bufferedBytes = 0;
        return result;
    }

    /** 粗略估算 BSON 体积（只求量级正确，用于字节阈值攒批）。 */
    static long estimateSize(Object value) {
        if (value == null) {
            return 8;
        }
        if (value instanceof Document) {
            long size = 8;
            for (Map.Entry<String, Object> e : ((Document) value).entrySet()) {
                size += e.getKey().length() + 2 + estimateSize(e.getValue());
            }
            return size;
        }
        if (value instanceof List) {
            long size = 8;
            for (Object item : (List<?>) value) {
                size += estimateSize(item);
            }
            return size;
        }
        if (value instanceof String) {
            return ((String) value).length() * 3L + 5;
        }
        if (value instanceof Binary) {
            return ((Binary) value).length() + 5;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).length + 5;
        }
        return 16;
    }

    @Override
    public void close() {
        buffered.clear();
        bufferedBytes = 0;
    }
}
