package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link DataValidationService} 同步对象解析测试：三种历史格式 + 表名映射。
 * 回归：曾只支持 {"tables":[...]} / {"db":["t1"]}，当前格式 {"db":{"tables":[...]}}
 * 解析出 0 张表导致行数校验静默通过。
 */
@DisplayName("行数校验同步对象解析测试")
class DataValidationServiceParseTest {

    private final DataValidationService service = new DataValidationService();

    @Test
    @DisplayName("最老格式 {\"tables\":[...]} 应解析出表清单")
    void legacyTablesFormat() {
        List<String> tables = service.parseTablesFromSyncObjects("{\"tables\":[\"t1\",\"t2\"]}");
        assertEquals(List.of("t1", "t2"), tables);
    }

    @Test
    @DisplayName("旧格式 {\"db\":[\"t1\"]}（value 为 List）应解析出表清单")
    void legacyDbListFormat() {
        List<String> tables = service.parseTablesFromSyncObjects("{\"test1\":[\"t1\",\"t2\"]}");
        assertEquals(List.of("t1", "t2"), tables);
    }

    @Test
    @DisplayName("当前格式 {\"db\":{\"tables\":[...]}}（value 为 Map）应解析出表清单")
    void currentMapFormat() {
        List<String> tables = service.parseTablesFromSyncObjects(
                "{\"test1\":{\"tables\":[\"t1\",\"t2\"],\"dbLevel\":false}}");
        assertEquals(List.of("t1", "t2"), tables);
    }

    @Test
    @DisplayName("当前格式多库合并解析")
    void currentMapFormatMultiDb() {
        List<String> tables = service.parseTablesFromSyncObjects(
                "{\"db1\":{\"tables\":[\"a\"]},\"db2\":{\"tables\":[\"b\"]}}");
        assertEquals(2, tables.size());
        assertTrue(tables.contains("a"));
        assertTrue(tables.contains("b"));
    }

    @Test
    @DisplayName("库级 entry（无 tables）不产生表，也不报错")
    void dbLevelEntryYieldsNoTables() {
        List<String> tables = service.parseTablesFromSyncObjects("{\"test1\":{\"dbLevel\":true}}");
        assertTrue(tables.isEmpty());
    }

    @Test
    @DisplayName("表名映射应解析为 源表→目标表")
    void tableMappingParsed() {
        Map<String, String> mapping = service.parseTableMappingFromSyncObjects(
                "{\"test1\":{\"tables\":[\"t1\",\"t2\"],\"tableMapping\":{\"t1\":\"t13\",\"t2\":\"t23\"}}}");
        assertEquals("t13", mapping.get("t1"));
        assertEquals("t23", mapping.get("t2"));
        assertEquals(2, mapping.size());
    }

    @Test
    @DisplayName("无映射/旧格式/空输入时映射为空且不报错")
    void tableMappingAbsentSafe() {
        assertTrue(service.parseTableMappingFromSyncObjects("{\"test1\":{\"tables\":[\"t1\"]}}").isEmpty());
        assertTrue(service.parseTableMappingFromSyncObjects("{\"test1\":[\"t1\"]}").isEmpty());
        assertTrue(service.parseTableMappingFromSyncObjects(null).isEmpty());
        assertTrue(service.parseTableMappingFromSyncObjects("not json").isEmpty());
    }

    @Test
    @DisplayName("非法/空输入健壮返回空清单")
    void invalidInputSafe() {
        assertTrue(service.parseTablesFromSyncObjects(null).isEmpty());
        assertTrue(service.parseTablesFromSyncObjects("").isEmpty());
        assertTrue(service.parseTablesFromSyncObjects("not json").isEmpty());
    }
}
