package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.UpdateRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * 符号感知的行事件反序列化器族：覆写 {@code deserializeTimeV2}，
 * 用 {@link TimeV2Decoder} 正确还原负 TIME / 超 24h TIME（连接器默认实现丢符号）。
 *
 * <p>配合共享的 tableMap（由 {@link SharedTableMapDeserializer} 填充）注册到
 * {@code EventDeserializer}，替换 WRITE/UPDATE/DELETE（含 EXT_ 变体）默认实现。
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

    public static class Write extends WriteRowsEventDataDeserializer {
        public Write(Map<Long, TableMapEventData> tableMap) {
            super(tableMap);
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return TimeV2Decoder.readSignedTime(meta, inputStream);
        }
    }

    public static class Update extends UpdateRowsEventDataDeserializer {
        public Update(Map<Long, TableMapEventData> tableMap) {
            super(tableMap);
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return TimeV2Decoder.readSignedTime(meta, inputStream);
        }
    }

    public static class Delete extends DeleteRowsEventDataDeserializer {
        public Delete(Map<Long, TableMapEventData> tableMap) {
            super(tableMap);
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return TimeV2Decoder.readSignedTime(meta, inputStream);
        }
    }
}
