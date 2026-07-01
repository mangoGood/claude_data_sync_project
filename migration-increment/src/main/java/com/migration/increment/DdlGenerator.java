package com.migration.increment;

import java.util.ArrayList;
import java.util.List;

class DdlGenerator {
    private final MySqlVersion v;
    DdlGenerator(MySqlVersion v) { this.v = v; }

    List<SqlTestCaseGenerator.TestCase> generate() {
        List<SqlTestCaseGenerator.TestCase> c = new ArrayList<>();
        genCreateTable(c); genAlterTable(c); genDropTable(c); genIndex(c); genTruncate(c); genDatabase(c);
        genView(c); genProcedure(c); genFunction(c); genTrigger(c); genEvent(c);
        return c;
    }

    private SqlTestCaseGenerator.TestCase tc(String cat, String sub, String sql, String desc) {
        return new SqlTestCaseGenerator.TestCase(cat, sub, sql, desc);
    }
    private SqlTestCaseGenerator.TestCase tc(String cat, String sub, String sql, String desc, MySqlVersion min) {
        return new SqlTestCaseGenerator.TestCase(cat, sub, sql, desc, min);
    }

    private String baseCols() {
        return "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, c1 VARCHAR(100) DEFAULT NULL, c2 INT DEFAULT 0, c3 DECIMAL(15,2) DEFAULT 0.00";
    }

    private static final int TBL_ADD_COL = 0;
    private static final int TBL_MODIFY = 50;
    private static final int TBL_CHANGE = 80;
    private static final int TBL_ADD_IDX = 110;
    private static final int TBL_ADD_UQ = 140;
    private static final int TBL_RENAME = 170;
    private static final int TBL_ENGINE = 180;
    private static final int TBL_CHARSET = 185;
    private static final int TBL_ALGO = 190;
    private static final int TBL_MULTI = 195;
    private static final int TBL_DROP_IDX = 200;
    private static final int TBL_COL_POS = 210;
    private static final int TBL_FIRST = 215;
    private static final int TBL_DEFAULT = 220;
    private static final int TBL_DIS_KEYS = 225;
    private static final int TBL_EN_KEYS = 230;
    private static final int TBL_TABLESPACE = 235;
    private static final int TBL_IDX = 240;
    private static final int TBL_UQ_IDX = 260;
    private static final int TBL_COMP_IDX = 280;
    private static final int TBL_PREFIX_IDX = 290;
    private static final int TBL_FT_IDX = 300;
    private static final int TBL_DROP_IDX2 = 310;
    private static final int TBL_TRUNCATE = 320;
    private static final int TBL_VIEW = 330;
    private static final int TBL_PROC = 340;
    private static final int TBL_FUNC = 350;
    private static final int TBL_TRG = 360;
    private static final int TBL_EVT = 370;
    private static final int TBL_TOTAL = 380;

    private void genCreateTable(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < TBL_TOTAL; i++)
            c.add(tc("DDL","CREATE_TABLE","CREATE TABLE IF NOT EXISTS ddl_t_"+i+" ("+baseCols()+")","CREATE TABLE #"+i));

        String[][] engines = {{"InnoDB","InnoDB"},{"MyISAM","MyISAM"},{"MEMORY","MEMORY"},{"BLACKHOLE","BLACKHOLE"}};
        for (int i = 0; i < 40; i++)
            c.add(tc("DDL","CREATE_ENGINE","CREATE TABLE IF NOT EXISTS ddl_eng_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50)) ENGINE="+engines[i%4][1],"ENGINE="+engines[i%4][0]+" #"+i));

        String[] charsets = {"utf8","utf8mb4","latin1","ascii","binary"};
        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","CREATE_CHARSET","CREATE TABLE IF NOT EXISTS ddl_cs_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50)) CHARACTER SET "+charsets[i%5],"CHARSET="+charsets[i%5]+" #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_IFNE","CREATE TABLE IF NOT EXISTS ddl_ine_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50))","IF NOT EXISTS #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_TEMP","CREATE TEMPORARY TABLE ddl_temp_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50))","TEMPORARY #"+i));

        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","CREATE_PK","CREATE TABLE IF NOT EXISTS ddl_pk_"+i+" (id INT NOT NULL, c1 VARCHAR(50), PRIMARY KEY (id))","PK #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_CPK","CREATE TABLE IF NOT EXISTS ddl_cpk_"+i+" (uid INT NOT NULL, rid INT NOT NULL, PRIMARY KEY (uid, rid))","Composite PK #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_UQ","CREATE TABLE IF NOT EXISTS ddl_uq_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(100) UNIQUE)","UNIQUE #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_IDX_INLINE","CREATE TABLE IF NOT EXISTS ddl_idxi_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50), INDEX idx_c1 (c1))","Inline IDX #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_CHECK","CREATE TABLE IF NOT EXISTS ddl_chk_"+i+" (id INT PRIMARY KEY, age INT, CHECK (age>=0 AND age<=150))","CHECK #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_AI","CREATE TABLE IF NOT EXISTS ddl_ai_"+i+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, c1 VARCHAR(50)) AUTO_INCREMENT="+(1000+i),"AUTO_INC #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_COMMENT","CREATE TABLE IF NOT EXISTS ddl_cmt_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50) COMMENT 'user name') COMMENT='test table "+i+"'","COMMENT #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_LIKE","CREATE TABLE IF NOT EXISTS ddl_like_"+i+" LIKE ddl_pk_"+i,"LIKE #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_AS_SEL","CREATE TABLE IF NOT EXISTS ddl_sel_"+i+" AS SELECT * FROM ddl_pk_"+i,"AS SELECT #"+i));

        String[] rfs = {"COMPACT","DYNAMIC","REDUNDANT","COMPRESSED"};
        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","CREATE_RF","CREATE TABLE IF NOT EXISTS ddl_rf_"+i+" (id INT PRIMARY KEY, c1 TEXT) ROW_FORMAT="+rfs[i%4],"ROW_FORMAT="+rfs[i%4]+" #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_COLLATE","CREATE TABLE IF NOT EXISTS ddl_col_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50)) COLLATE utf8mb4_unicode_ci","COLLATE #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_ON_UPDATE","CREATE TABLE IF NOT EXISTS ddl_ou_"+i+" (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)","ON UPDATE #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_HASH_PART","CREATE TABLE IF NOT EXISTS ddl_hp_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50)) PARTITION BY HASH(id) PARTITIONS 4","HASH PARTITION #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_KEY_PART","CREATE TABLE IF NOT EXISTS ddl_kp_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50)) PARTITION BY KEY(id) PARTITIONS 4","KEY PARTITION #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_MCOL_UQ","CREATE TABLE IF NOT EXISTS ddl_muq_"+i+" (id INT PRIMARY KEY, c1 VARCHAR(50), c2 VARCHAR(50), UNIQUE KEY uq_c1c2 (c1,c2))","Multi-col UNIQUE #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_DEFAULT_EXPR","CREATE TABLE IF NOT EXISTS ddl_defe_"+i+" (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, cnt INT DEFAULT 0)","DEFAULT expr #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_RANGE_PART","CREATE TABLE IF NOT EXISTS ddl_rp_"+i+" (id INT NOT NULL AUTO_INCREMENT, c1 VARCHAR(50), cd DATE, PRIMARY KEY (id, cd)) PARTITION BY RANGE (YEAR(cd)) (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN (2025), PARTITION p2 VALUES LESS THAN MAXVALUE)","RANGE PARTITION #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_FK","CREATE TABLE IF NOT EXISTS ddl_fk_"+i+" (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES ddl_pk_"+i+"(id))","FK #"+i));

        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_FK_CASCADE","CREATE TABLE IF NOT EXISTS ddl_fkc_"+i+" (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES ddl_pk_"+i+"(id) ON DELETE CASCADE ON UPDATE RESTRICT)","FK CASCADE #"+i));

        if (v.isAtLeast(MySqlVersion.V5_7)) {
            for (int i = 0; i < 20; i++)
                c.add(tc("DDL","CREATE_JSON_57","CREATE TABLE IF NOT EXISTS ddl_json_"+i+" (id INT PRIMARY KEY, data JSON)","JSON (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 20; i++)
                c.add(tc("DDL","CREATE_GEN_57","CREATE TABLE IF NOT EXISTS ddl_gen_"+i+" (id INT PRIMARY KEY, fn VARCHAR(50), ln VARCHAR(50), full_name VARCHAR(101) GENERATED ALWAYS AS (CONCAT(fn,' ',ln)) STORED)","Generated col (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 20; i++)
                c.add(tc("DDL","CREATE_GEN_VIRTUAL_57","CREATE TABLE IF NOT EXISTS ddl_gv_"+i+" (id INT PRIMARY KEY, fn VARCHAR(50), ln VARCHAR(50), full_name VARCHAR(101) GENERATED ALWAYS AS (CONCAT(fn,' ',ln)) VIRTUAL)","Virtual gen col (5.7+) #"+i,MySqlVersion.V5_7));
        }
        if (v.isAtLeast(MySqlVersion.V8_0)) {
            for (int i = 0; i < 20; i++)
                c.add(tc("DDL","CREATE_INVIS_80","CREATE TABLE IF NOT EXISTS ddl_inv_"+i+" (id INT PRIMARY KEY, secret VARCHAR(50) INVISIBLE)","Invisible col (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 20; i++)
                c.add(tc("DDL","CREATE_CHK80_80","CREATE TABLE IF NOT EXISTS ddl_chk80_"+i+" (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age_"+i+" CHECK (age>0))","Enforced CHECK (8.0+) #"+i,MySqlVersion.V8_0));
        }
    }

    private void genAlterTable(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 50; i++)
            c.add(tc("DDL","ALTER_ADD_COL","ALTER TABLE ddl_t_"+(TBL_ADD_COL+i)+" ADD COLUMN new_c_"+i+" VARCHAR(100) DEFAULT NULL","ADD COL #"+i));

        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","ALTER_MODIFY","ALTER TABLE ddl_t_"+(TBL_MODIFY+i)+" MODIFY COLUMN c1 VARCHAR(200) NOT NULL DEFAULT ''","MODIFY #"+i));

        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","ALTER_CHANGE","ALTER TABLE ddl_t_"+(TBL_CHANGE+i)+" CHANGE COLUMN c1 renamed_c1_"+i+" VARCHAR(200)","CHANGE #"+i));

        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","ALTER_ADD_IDX","ALTER TABLE ddl_t_"+(TBL_ADD_IDX+i)+" ADD INDEX idx_new_"+i+" (c2)","ADD IDX #"+i));

        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","ALTER_ADD_UQ","ALTER TABLE ddl_t_"+(TBL_ADD_UQ+i)+" ADD UNIQUE INDEX uq_new_"+i+" (c2)","ADD UQ #"+i));

        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_RENAME","ALTER TABLE ddl_t_"+(TBL_RENAME+i)+" RENAME TO ddl_rn_"+i,"RENAME #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_ENGINE","ALTER TABLE ddl_t_"+(TBL_ENGINE+i)+" ENGINE=InnoDB","ENGINE #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_CHARSET","ALTER TABLE ddl_t_"+(TBL_CHARSET+i)+" CONVERT TO CHARACTER SET utf8mb4","CHARSET #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_ALGO","ALTER TABLE ddl_t_"+(TBL_ALGO+i)+" ALGORITHM=INPLACE, ADD COLUMN algo_c_"+i+" INT","ALGORITHM #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_MULTI","ALTER TABLE ddl_t_"+(TBL_MULTI+i)+" ADD COLUMN m1_"+i+" INT, ADD COLUMN m2_"+i+" VARCHAR(50)","Multi ops #"+i));

        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_DROP_IDX","ALTER TABLE ddl_t_"+(TBL_ADD_IDX+i+10)+" DROP INDEX idx_new_"+(i+10),"DROP IDX #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_COL_POS","ALTER TABLE ddl_t_"+(TBL_COL_POS+i)+" ADD COLUMN pos_c_"+i+" INT AFTER id","AFTER pos #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_COL_FIRST","ALTER TABLE ddl_t_"+(TBL_FIRST+i)+" ADD COLUMN first_c_"+i+" INT FIRST","FIRST pos #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_DEFAULT","ALTER TABLE ddl_t_"+(TBL_DEFAULT+i)+" ALTER COLUMN c1 SET DEFAULT 'def_"+i+"'","SET DEFAULT #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_DROP_DEFAULT","ALTER TABLE ddl_t_"+(TBL_DEFAULT+i)+" ALTER COLUMN c1 DROP DEFAULT","DROP DEFAULT #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_DIS_KEYS","ALTER TABLE ddl_t_"+(TBL_DIS_KEYS+i)+" DISABLE KEYS","DISABLE KEYS #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_EN_KEYS","ALTER TABLE ddl_t_"+(TBL_EN_KEYS+i)+" ENABLE KEYS","ENABLE KEYS #"+i));

        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_ADD_PART","ALTER TABLE ddl_rp_"+(i%20)+" REORGANIZE PARTITION p2 INTO (PARTITION p2 VALUES LESS THAN ("+(2030+i*5)+"), PARTITION p3_"+i+" VALUES LESS THAN MAXVALUE)","ADD PART #"+i));

        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_COALESCE","ALTER TABLE ddl_hp_"+(i%20)+" COALESCE PARTITION 1","COALESCE #"+i));

        for (int i = 0; i < 5; i++)
            c.add(tc("DDL","ALTER_TABLESPACE","ALTER TABLE ddl_t_"+(TBL_TABLESPACE+i)+" TABLESPACE innodb_file_per_table","TABLESPACE #"+i));

        if (v.isAtLeast(MySqlVersion.V5_7)) {
            for (int i = 0; i < 5; i++)
                c.add(tc("DDL","ALTER_RENAME_IDX_57","ALTER TABLE ddl_t_"+(TBL_ADD_IDX+i+20)+" RENAME INDEX idx_new_"+(i+20)+" TO idx_rn_"+i,"RENAME IDX (5.7+) #"+i,MySqlVersion.V5_7));
        }
        if (v.isAtLeast(MySqlVersion.V8_0)) {
            for (int i = 0; i < 5; i++)
                c.add(tc("DDL","ALTER_ALGO_INSTANT_80","ALTER TABLE ddl_t_"+(TBL_ALGO+i)+" ALGORITHM=INSTANT, ADD COLUMN inst_c_"+i+" INT","INSTANT (8.0+) #"+i,MySqlVersion.V8_0));
        }
    }

    private void genDropTable(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 50; i++)
            c.add(tc("DDL","DROP_TABLE","DROP TABLE IF EXISTS ddl_drop_"+i,"DROP TABLE #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_TEMP","DROP TEMPORARY TABLE IF EXISTS ddl_temp_"+i,"DROP TEMP #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_MULTI","DROP TABLE IF EXISTS ddl_drop_a_"+i+", ddl_drop_b_"+i,"DROP multi #"+i));
    }

    private void genIndex(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_IDX","CREATE INDEX idx_ddl_"+i+" ON ddl_t_"+(TBL_IDX+i)+" (c2)","INDEX #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_UQ_IDX","CREATE UNIQUE INDEX uidx_ddl_"+i+" ON ddl_t_"+(TBL_UQ_IDX+i)+" (c2)","UNIQUE IDX #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_COMP_IDX","CREATE INDEX cidx_ddl_"+i+" ON ddl_t_"+(TBL_COMP_IDX+i)+" (c2, c3)","Composite IDX #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_PREFIX_IDX","CREATE INDEX pidx_ddl_"+i+" ON ddl_t_"+(TBL_PREFIX_IDX+i)+" (c1(20))","Prefix IDX #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_FT_IDX","CREATE FULLTEXT INDEX fidx_ddl_"+i+" ON ddl_t_"+(TBL_FT_IDX+i)+" (c1)","Fulltext IDX #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_IDX","DROP INDEX idx_ddl_"+i+" ON ddl_t_"+(TBL_IDX+i),"DROP IDX #"+i));
        if (v.isAtLeast(MySqlVersion.V8_0)) {
            for (int i = 0; i < 10; i++)
                c.add(tc("DDL","CREATE_INVIS_IDX_80","CREATE INDEX iidx_ddl_"+i+" ON ddl_t_"+(TBL_IDX+i+10)+" (c2) INVISIBLE","Invisible IDX (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 10; i++)
                c.add(tc("DDL","CREATE_DESC_IDX_80","CREATE INDEX didx_ddl_"+i+" ON ddl_t_"+(TBL_IDX+i+15)+" (c2 DESC)","Desc IDX (8.0+) #"+i,MySqlVersion.V8_0));
        }
    }

    private void genTruncate(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 50; i++)
            c.add(tc("DDL","TRUNCATE","TRUNCATE TABLE ddl_t_"+(TBL_TRUNCATE+i),"TRUNCATE #"+i));
    }

    private void genDatabase(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","CREATE_DB","CREATE DATABASE IF NOT EXISTS test_db_"+i,"CREATE DB #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DDL","CREATE_DB_CS","CREATE DATABASE IF NOT EXISTS test_db_c_"+i+" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci","CREATE DB charset #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_DB","DROP DATABASE IF EXISTS test_db_"+i,"DROP DB #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","ALTER_DB","ALTER DATABASE test_db_c_"+i+" CHARACTER SET utf8mb4","ALTER DB #"+i));
    }

    private void genView(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_VIEW","CREATE OR REPLACE VIEW ddl_view_"+i+" AS SELECT id, c1 FROM ddl_t_"+(TBL_VIEW+i),"CREATE VIEW #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_VIEW_CHECK","CREATE OR REPLACE VIEW ddl_view_c_"+i+" AS SELECT id, c1 FROM ddl_t_"+(TBL_VIEW+i)+" WITH CHECK OPTION","VIEW CHECK #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_VIEW_CASCADED","CREATE OR REPLACE VIEW ddl_view_cs_"+i+" AS SELECT id, c1 FROM ddl_t_"+(TBL_VIEW+i+10)+" WITH CASCADED CHECK OPTION","VIEW CASCADED #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_VIEW","ALTER VIEW ddl_view_"+i+" AS SELECT id FROM ddl_t_"+(TBL_VIEW+i),"ALTER VIEW #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_VIEW","DROP VIEW IF EXISTS ddl_view_"+i,"DROP VIEW #"+i));
    }

    private void genProcedure(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_PROC","DELIMITER //\nCREATE PROCEDURE ddl_proc_"+i+" (IN p_id INT) BEGIN SELECT * FROM ddl_t_"+(TBL_PROC+i)+" WHERE id=p_id; END //\nDELIMITER ;","CREATE PROC #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_PROC_OUT","DELIMITER //\nCREATE PROCEDURE ddl_proc_out_"+i+" (IN p_id INT, OUT p_name VARCHAR(100)) BEGIN SELECT c1 INTO p_name FROM ddl_t_"+(TBL_PROC+i)+" WHERE id=p_id; END //\nDELIMITER ;","PROC OUT #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_PROC","ALTER PROCEDURE ddl_proc_"+i+" COMMENT 'updated proc "+i+"'","ALTER PROC #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_PROC","DROP PROCEDURE IF EXISTS ddl_proc_"+i,"DROP PROC #"+i));
    }

    private void genFunction(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_FUNC","DELIMITER //\nCREATE FUNCTION ddl_func_"+i+" (p_id INT) RETURNS VARCHAR(100) DETERMINISTIC READS SQL DATA BEGIN DECLARE v_name VARCHAR(100); SELECT c1 INTO v_name FROM ddl_t_"+(TBL_FUNC+i)+" WHERE id=p_id; RETURN v_name; END //\nDELIMITER ;","CREATE FUNC #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_FUNC","ALTER FUNCTION ddl_func_"+i+" COMMENT 'updated func "+i+"'","ALTER FUNC #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","DROP_FUNC","DROP FUNCTION IF EXISTS ddl_func_"+i,"DROP FUNC #"+i));
    }

    private void genTrigger(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_TRIGGER","CREATE TRIGGER ddl_trg_"+i+" BEFORE INSERT ON ddl_t_"+(TBL_TRG+i)+" FOR EACH ROW SET NEW.c1 = UPPER(NEW.c1)","TRIGGER #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_TRIGGER_AFTER","CREATE TRIGGER ddl_trg_a_"+i+" AFTER UPDATE ON ddl_t_"+(TBL_TRG+i+10)+" FOR EACH ROW INSERT INTO t_log (action) VALUES ('update')","AFTER TRIGGER #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_TRIGGER","DROP TRIGGER IF EXISTS ddl_trg_"+i,"DROP TRIGGER #"+i));
    }

    private void genEvent(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","CREATE_EVENT","CREATE EVENT IF NOT EXISTS ddl_evt_"+i+" ON SCHEDULE EVERY 1 DAY DO DELETE FROM ddl_t_"+(TBL_EVT+i)+" WHERE c2 < 0","EVENT #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","CREATE_EVENT_AT","CREATE EVENT IF NOT EXISTS ddl_evt_at_"+i+" ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO SELECT 1","EVENT AT #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DDL","ALTER_EVENT","ALTER EVENT ddl_evt_"+i+" ON SCHEDULE EVERY 2 DAY","ALTER EVENT #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DDL","DROP_EVENT","DROP EVENT IF EXISTS ddl_evt_"+i,"DROP EVENT #"+i));
    }
}
