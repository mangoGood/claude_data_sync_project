package com.migration.elastic;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElasticSyncMainTest {

    @Test
    void indexNameIsLowercasedDbUnderscoreTable() {
        assertEquals("shop_db_users", ElasticSyncMain.indexName("Shop_DB", "Users"));
        assertEquals("a_b", ElasticSyncMain.indexName("a", "b"));
    }

    @Test
    void parseEnumOptionsHandlesQuotedList() {
        assertArrayEquals(new String[]{"paid", "pending", "shipped"},
                ElasticSyncMain.parseEnumOptions("enum('paid','pending','shipped')"));
        assertArrayEquals(new String[]{"a", "b"}, ElasticSyncMain.parseEnumOptions("set('a','b')"));
        // 无括号定义（防御路径）返回空数组
        assertEquals(0, ElasticSyncMain.parseEnumOptions("enum").length);
    }

    @Test
    void bulkIndexOpProducesActionAndSourceLines() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("id", 1);
        doc.put("name", "a");
        String[] op = EsClient.indexOp("idx", "1", doc);
        assertTrue(op[0].contains("\"index\""));
        assertTrue(op[0].contains("\"_index\":\"idx\""));
        assertTrue(op[0].contains("\"_id\":\"1\""));
        assertEquals("{\"id\":1,\"name\":\"a\"}", op[1]);
    }

    @Test
    void bulkIndexOpWithoutIdOmitsIdField() {
        String[] op = EsClient.indexOp("idx", null, new LinkedHashMap<>());
        assertTrue(!op[0].contains("_id"));
    }

    @Test
    void bulkDeleteOpHasNoSourceLine() {
        String[] op = EsClient.deleteOp("idx", "5");
        assertTrue(op[0].contains("\"delete\""));
        assertTrue(op[0].contains("\"_id\":\"5\""));
        assertNull(op[1]);
    }
}
