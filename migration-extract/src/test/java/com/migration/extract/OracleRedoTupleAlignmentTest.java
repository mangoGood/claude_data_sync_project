package com.migration.extract;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link OracleRedoExtractor} tuple 解析与列/类型对齐回归测试（#5/#6）。
 *
 * <p>锁定全类型矩阵测试中发现的 bug：
 * <ul>
 *   <li>#5：部分列 tuple 的值曾按下标匹配全表 columnTypes，字符串落到数字类型槽被裸输出
 *       （syntax error at or near "increment"）。修复为 alignTypesToColumns 按实际列名对齐类型。</li>
 *   <li>#6：空 LOB 的 SQL_REDO 渲染 EMPTY_CLOB()/EMPTY_BLOB() 曾被当字符串字面量写入目标，
 *       修复为归一 NULL。</li>
 * </ul>
 * 通过反射调用私有方法（仓库既有测试模式）。
 */
@DisplayName("OracleRedoExtractor tuple 解析/对齐回归")
class OracleRedoTupleAlignmentTest {

    private OracleRedoExtractor extractor;
    private Method parseTupleData;
    private Method alignTypesToColumns;
    private Method extractTupleColumnNames;

    @BeforeEach
    void setUp() throws Exception {
        extractor = new OracleRedoExtractor();
        parseTupleData = OracleRedoExtractor.class.getDeclaredMethod("parseTupleData", String.class, List.class);
        parseTupleData.setAccessible(true);
        alignTypesToColumns = OracleRedoExtractor.class.getDeclaredMethod(
                "alignTypesToColumns", List.class, List.class, List.class);
        alignTypesToColumns.setAccessible(true);
        extractTupleColumnNames = OracleRedoExtractor.class.getDeclaredMethod(
                "extractTupleColumnNames", String.class, List.class, int.class);
        extractTupleColumnNames.setAccessible(true);
    }

    @SuppressWarnings("unchecked")
    private List<String> parse(String tuple) throws Exception {
        return (List<String>) parseTupleData.invoke(extractor, tuple, null);
    }

    @Test
    @DisplayName("tuple 值解析：引号剥离、NULL 归一")
    void parseValues() throws Exception {
        List<String> v = parse("ID:5,C_NUM:555,C_VARCHAR2:'oracle increment row'");
        assertEquals(List.of("5", "555", "oracle increment row"), v);
        // NULL 形式
        assertNull(parse("A:NULL").get(0));
        assertNull(parse("A:[null]").get(0));
    }

    @Test
    @DisplayName("#6 回归：EMPTY_CLOB()/EMPTY_BLOB() 归一为 NULL")
    void emptyLobBecomesNull() throws Exception {
        List<String> v = parse("ID:5,C_CLOB:EMPTY_CLOB(),C_BLOB:EMPTY_BLOB()");
        assertEquals("5", v.get(0));
        assertNull(v.get(1), "EMPTY_CLOB() 必须归一为 NULL");
        assertNull(v.get(2), "EMPTY_BLOB() 必须归一为 NULL");
    }

    @Test
    @DisplayName("带引号值内的逗号/转义单引号不破坏切分")
    void quotedValuesWithCommas() throws Exception {
        List<String> v = parse("A:'x, y',B:'it''s ok',C:7");
        assertEquals(3, v.size());
        assertEquals("x, y", v.get(0));
        assertEquals("7", v.get(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("#5 回归：alignTypesToColumns 按 tuple 实际列名取全表类型（字符串列拿到 VARCHAR2 而非 NUMBER）")
    void typesAlignedToActualColumns() throws Exception {
        List<String> fullCols = List.of("ID", "C_NUM", "C_NUM_PS", "C_INT", "C_VARCHAR2", "C_DATE");
        List<String> fullTypes = List.of("NUMBER", "NUMBER", "NUMBER", "NUMBER", "VARCHAR2", "DATE");
        // 部分列 INSERT：tuple 只有 3 列
        List<String> tupleCols = List.of("ID", "C_NUM", "C_VARCHAR2");
        List<String> aligned = (List<String>) alignTypesToColumns.invoke(extractor, tupleCols, fullCols, fullTypes);
        assertEquals(List.of("NUMBER", "NUMBER", "VARCHAR2"), aligned,
                "历史 bug：按下标对齐会让 C_VARCHAR2 拿到 C_NUM_PS 的 NUMBER 类型，字符串被裸输出");
        // 未知列回退空类型（formatRowData 对空类型默认加引号，安全侧）
        List<String> aligned2 = (List<String>) alignTypesToColumns.invoke(
                extractor, List.of("NOT_EXIST"), fullCols, fullTypes);
        assertEquals(List.of(""), aligned2);
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("extractTupleColumnNames：带列名 tuple 提取列名；不带列名/数量不符返回 null 回退全表")
    void tupleColumnNames() throws Exception {
        List<String> fallback = List.of("A", "B", "C");
        List<String> names = (List<String>) extractTupleColumnNames.invoke(
                extractor, "ID:5,C_NUM:555,C_VARCHAR2:'x'", fallback, 3);
        assertEquals(List.of("ID", "C_NUM", "C_VARCHAR2"), names);
        // 数量与期望不符 → null（调用方回退全表列名）
        assertNull(extractTupleColumnNames.invoke(extractor, "ID:5", fallback, 3));
    }
}
