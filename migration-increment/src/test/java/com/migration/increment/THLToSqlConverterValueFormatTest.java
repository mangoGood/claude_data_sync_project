package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link THLToSqlConverter} 增量 SQL 生成的值格式化回归测试（全类型矩阵中发现的 bug）：
 * <ul>
 *   <li>#2/#3 MySQL→PG：tinyint(1)/bool → true/false，bit(N) → bytea 十六进制字面量
 *       （历史 bug：按整数输出导致 PG 报 "column is of type boolean/bytea but expression is of type integer"）</li>
 *   <li>#5 转换端：INSERT 优先使用 insert_column_names（部分列 tuple 的列/值对齐）</li>
 *   <li>类型判定依据 mysql_column_full_types（带宽度 COLUMN_TYPE；裸 DATA_TYPE 无法区分 tinyint(1)）</li>
 * </ul>
 */
@DisplayName("THLToSqlConverter 值格式化回归")
class THLToSqlConverterValueFormatTest {

    @TempDir
    Path tempDir;

    private THLToSqlConverter mysqlToPg;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.setProperty("input.dir", tempDir.toString());
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "postgresql");
        props.setProperty("target.db.database", "tgt");
        mysqlToPg = new THLToSqlConverter(props);
    }

    private THLEvent event(long seqno, String eventType) {
        THLEvent e = new THLEvent();
        e.setSeqno(seqno);
        e.addMetadata("event_type", eventType);
        e.addMetadata("database_name", "srcdb");
        e.addMetadata("table_name", "bt");
        return e;
    }

    @Test
    @DisplayName("#2/#3 回归：INSERT 中 tinyint(1)→true/false，bit(8)→E'\\x..' bytea")
    void insertConvertsBoolAndBit() {
        THLEvent e = event(1, "INSERT");
        e.addMetadata("column_names", "id,name,c_bool,c_bit");
        e.addMetadata("mysql_column_types", "int,varchar,tinyint,bit");
        e.addMetadata("mysql_column_full_types", "int,varchar(50),tinyint(1),bit(8)");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("row_data", "1,'alice',1,0xaa");

        List<String> sql = mysqlToPg.convertToSql(e);
        assertEquals(1, sql.size());
        String insert = sql.get(0);
        assertTrue(insert.startsWith("INSERT INTO \"bt\""), insert);
        assertTrue(insert.contains("true"), "tinyint(1)=1 应输出 true: " + insert);
        assertTrue(insert.contains("E'\\\\xaa'"), "bit(8)=0xaa 应输出 bytea 字面量: " + insert);
        assertTrue(insert.contains("ON CONFLICT (\"id\") DO NOTHING"), insert);
    }

    @Test
    @DisplayName("#2/#3 回归：UPDATE 的 SET 与 WHERE 同样转换 bool/bit")
    void updateConvertsBoolAndBit() {
        THLEvent e = event(2, "UPDATE");
        e.addMetadata("column_names", "id,name,c_bool,c_bit");
        e.addMetadata("mysql_column_types", "int,varchar,tinyint,bit");
        e.addMetadata("mysql_column_full_types", "int,varchar(50),tinyint(1),bit(8)");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("row_data", "1,'alice',0,0x1");
        e.addMetadata("row_data_before", "1,'alice',1,0xaa");

        List<String> sql = mysqlToPg.convertToSql(e);
        assertEquals(1, sql.size());
        String update = sql.get(0);
        assertTrue(update.contains("\"c_bool\"=false"), "tinyint(1)=0 应输出 false: " + update);
        assertTrue(update.contains("\"c_bit\"=E'\\\\x01'"), "bit(8)=0x1 应输出 E'\\\\x01': " + update);
        assertTrue(update.contains("WHERE \"id\"=1"), update);
    }

    @Test
    @DisplayName("#5 回归：INSERT 优先使用 insert_column_names（部分列 tuple 列/值对齐）")
    void insertHonorsExplicitColumnNames() {
        THLEvent e = event(3, "INSERT");
        // 全表 5 列，但本次 INSERT 只带 3 列（Oracle redo 部分列场景）
        e.addMetadata("column_names", "id,c_num,c_num_ps,c_varchar2,c_date");
        e.addMetadata("insert_column_names", "id,c_num,c_varchar2");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("row_data", "5,555,'oracle increment row'");

        List<String> sql = mysqlToPg.convertToSql(e);
        assertEquals(1, sql.size());
        String insert = sql.get(0);
        assertTrue(insert.contains("(\"id\", \"c_num\", \"c_varchar2\")"),
                "列清单必须与 tuple 实际列一致: " + insert);
        assertTrue(insert.contains("'oracle increment row'"), "字符串必须带引号: " + insert);
    }

    @Test
    @DisplayName("普通类型不受影响：数字/字符串/NULL 原样输出")
    void ordinaryValuesUntouched() {
        THLEvent e = event(4, "INSERT");
        e.addMetadata("column_names", "id,name,amount");
        e.addMetadata("mysql_column_types", "int,varchar,decimal");
        e.addMetadata("mysql_column_full_types", "int,varchar(50),decimal(10,2)");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("row_data", "7,'bob',99.50");

        String insert = mysqlToPg.convertToSql(e).get(0);
        assertTrue(insert.contains("7,'bob',99.50"), insert);
    }
}
