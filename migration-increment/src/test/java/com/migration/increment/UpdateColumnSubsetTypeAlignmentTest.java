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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UPDATE 的 SET / WHERE 两侧列清单不同宽时，列类型必须各自对齐到自己那一侧。
 *
 * <p>两侧不同宽是常态：PG 会把"值未随事件下发"的未变更 TOAST 列从后镜像里摘掉，
 * Oracle 只下发变更列。此前两侧共用一份全宽 {@code pg_column_types} 按下标取类型，
 * 第 i 个值会配到第 i 列的类型上 —— pg→mysql 的逐值转换因此把 bool 当整数、
 * 把整数当 bool 转，转出来的是合法字面量、看不出异常，属于静默转错值。
 */
@DisplayName("UPDATE 列子集：SET/WHERE 两侧的类型各自对齐")
class UpdateColumnSubsetTypeAlignmentTest {

    @TempDir
    Path tempDir;

    private THLToSqlConverter pgToMysql;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.setProperty("input.dir", tempDir.toString());
        props.setProperty("source.db.type", "postgresql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("target.db.database", "tgt");
        pgToMysql = new THLToSqlConverter(props);
    }

    /** 全表 4 列，后镜像只带了 id/flag/note 三列（blob_txt 的值没下发被摘掉）。 */
    private THLEvent trimmedUpdate() {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", "UPDATE");
        e.addMetadata("database_name", "srcdb");
        e.addMetadata("table_name", "vals");
        e.addMetadata("column_names", "id,flag,note,blob_txt");
        e.addMetadata("pg_column_types", "integer,boolean,text,text");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("update_column_names", "id,flag,note");
        e.addMetadata("update_before_column_names", "id,flag,note");
        e.addMetadata("row_data", "1,true,'hi'");
        return e;
    }

    @Test
    @DisplayName("摘掉的列不出现在 SET 里，其余列按各自类型转换")
    void trimmedColumnsAreNotWritten() {
        List<String> sql = pgToMysql.convertToSql(trimmedUpdate());
        assertEquals(1, sql.size(), sql.toString());
        String s = sql.get(0);

        assertFalse(s.contains("blob_txt"),
                "值没下发的列绝不能出现在 SET 里（写 NULL 会抹掉目标端已有的大字段）: " + s);
        // boolean 列按 boolean 转（pg 的 true → MySQL 的 1），不是按 text 原样带引号
        assertTrue(s.contains("`flag`=1") || s.contains("`flag`=true"), s);
        assertTrue(s.contains("`note`='hi'"), s);
        assertTrue(s.contains("WHERE `id`=1"), s);
    }

    @Test
    @DisplayName("没有列子集元数据时行为不变（全宽路径）")
    void fullWidthUnchanged() {
        THLEvent e = trimmedUpdate();
        e.getMetadata().remove("update_column_names");
        e.getMetadata().remove("update_before_column_names");
        e.addMetadata("row_data", "1,true,'hi','v'");

        List<String> sql = pgToMysql.convertToSql(e);
        assertEquals(1, sql.size(), sql.toString());
        assertTrue(sql.get(0).contains("`blob_txt`='v'"), sql.get(0));
    }
}
