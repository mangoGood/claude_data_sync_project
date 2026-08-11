package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.UpdateRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;

/**
 * 符号感知 + 大字段落盘的行事件反序列化器族。
 *
 * <p>两件事：
 * <ol>
 *   <li>覆写 {@code deserializeTimeV2}，用 {@link TimeV2Decoder} 正确还原负 TIME /
 *       超 24h TIME（连接器默认实现丢符号）；</li>
 *   <li>覆写 {@code deserializeBlob}：超过阈值的值<b>边读边落盘</b>，行数组里只放
 *       {@link com.migration.common.lob.LobRef}。默认实现是
 *       {@code inputStream.read(blobLength)}——一个 1GB 的 BLOB 在那里直接分配 1GB。</li>
 * </ol>
 *
 * <p><b>前镜像不落盘</b>：UPDATE 的前镜像、DELETE 的行镜像都只用来定位行，而定位靠主键，
 * 那 1GB 内容留着没有任何用处。前镜像照样算 md5，好让冲突检测还能比对身份。
 * 判定前后镜像不需要复制上游任何代码——{@code deserializeRows} 里
 * {@code deserializeRow(前) , deserializeRow(后)} 是同一个表达式里的两个实参，
 * Java 保证从左到右求值，交替标志就够了。
 */
public final class SignAwareRowsDeserializers {

    private SignAwareRowsDeserializers() {
    }

    /** TABLE_MAP 反序列化器：把表结构映射同步进共享 map，供自定义行反序列化器使用。 */
    public static class SharedTableMapDeserializer extends TableMapEventDataDeserializer {
        private final Map<Long, TableMapEventData> tableMap;

        public SharedTableMapDeserializer(Map<Long, TableMapEventData> tableMap) {
            this.tableMap = tableMap;
        }

        @Override
        public TableMapEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
            TableMapEventData data = super.deserialize(inputStream);
            tableMap.put(data.getTableId(), data);
            return data;
        }
    }

    /**
     * 大字段落盘策略。{@code null} 或阈值为负 = 关闭（完全走上游行为，零回归）。
     */
    public static final class SpillPolicy {
        private final LobSpillWriter writer;
        private final int thresholdBytes;

        public SpillPolicy(LobSpillWriter writer, int thresholdBytes) {
            this.writer = writer;
            this.thresholdBytes = thresholdBytes;
        }

        boolean appliesTo(int length) {
            return writer != null && thresholdBytes >= 0 && length > thresholdBytes;
        }

        LobSpillWriter writer() {
            return writer;
        }
    }

    /** 大字段落盘的公共实现：三种行事件共用。 */
    private static Serializable deserializeBlobWithSpill(int meta, ByteArrayInputStream in,
                                                         SpillPolicy policy, boolean discard,
                                                         BlobReader fallback) throws IOException {
        int blobLength = in.readInteger(meta);
        if (blobLength < 0) {
            // readInteger 返回 int：>2GB 的值会溢出成负数。这种值 MySQL 也搬不动
            // （max_allowed_packet 上限 1GB），在这里显式炸掉比继续跑出乱码强。
            throw new IOException("LOB_TOO_LARGE: 单值长度溢出（> 2GB），无法处理");
        }
        if (policy == null || !policy.appliesTo(blobLength)) {
            return fallback.read(blobLength, in);
        }
        return policy.writer().consume(in, blobLength, discard);
    }

    private interface BlobReader {
        Serializable read(int length, ByteArrayInputStream in) throws IOException;
    }

    private static final BlobReader INLINE = (length, in) -> in.read(length);

    public static class Write extends WriteRowsEventDataDeserializer {
        private final SpillPolicy spillPolicy;

        public Write(Map<Long, TableMapEventData> tableMap) {
            this(tableMap, null);
        }

        public Write(Map<Long, TableMapEventData> tableMap, SpillPolicy spillPolicy) {
            super(tableMap);
            this.spillPolicy = spillPolicy;
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return TimeV2Decoder.readSignedTime(meta, inputStream);
        }

        @Override
        protected Serializable deserializeBlob(int meta, ByteArrayInputStream inputStream) throws IOException {
            // INSERT 只有后镜像，必须留内容
            return deserializeBlobWithSpill(meta, inputStream, spillPolicy, false, INLINE);
        }
    }

    public static class Update extends UpdateRowsEventDataDeserializer {
        private final SpillPolicy spillPolicy;
        /** 下一次 deserializeRow 读的是不是前镜像。每个事件开头重置，避免上个事件出错后错位。 */
        private boolean beforeImage = true;

        public Update(Map<Long, TableMapEventData> tableMap) {
            this(tableMap, null);
        }

        public Update(Map<Long, TableMapEventData> tableMap, SpillPolicy spillPolicy) {
            super(tableMap);
            this.spillPolicy = spillPolicy;
        }

        @Override
        public UpdateRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
            beforeImage = true;
            return super.deserialize(inputStream);
        }

        @Override
        protected Serializable[] deserializeRow(long tableId, BitSet includedColumns,
                                                ByteArrayInputStream inputStream) throws IOException {
            boolean discard = beforeImage;
            beforeImage = !beforeImage;      // 上游按 (前, 后) 交替调用本方法
            currentDiscard = discard;
            try {
                return super.deserializeRow(tableId, includedColumns, inputStream);
            } finally {
                currentDiscard = false;
            }
        }

        /** 当前正在读的这一行是否是前镜像（deserializeBlob 无法自己知道）。 */
        private boolean currentDiscard;

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return TimeV2Decoder.readSignedTime(meta, inputStream);
        }

        @Override
        protected Serializable deserializeBlob(int meta, ByteArrayInputStream inputStream) throws IOException {
            return deserializeBlobWithSpill(meta, inputStream, spillPolicy, currentDiscard, INLINE);
        }
    }

    public static class Delete extends DeleteRowsEventDataDeserializer {
        private final SpillPolicy spillPolicy;

        public Delete(Map<Long, TableMapEventData> tableMap) {
            this(tableMap, null);
        }

        public Delete(Map<Long, TableMapEventData> tableMap, SpillPolicy spillPolicy) {
            super(tableMap);
            this.spillPolicy = spillPolicy;
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return TimeV2Decoder.readSignedTime(meta, inputStream);
        }

        @Override
        protected Serializable deserializeBlob(int meta, ByteArrayInputStream inputStream) throws IOException {
            // DELETE 只带行镜像，用途只有定位行 ⇒ 一律不落盘
            return deserializeBlobWithSpill(meta, inputStream, spillPolicy, true, INLINE);
        }
    }
}
