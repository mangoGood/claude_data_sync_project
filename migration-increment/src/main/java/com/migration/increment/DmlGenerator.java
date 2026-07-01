package com.migration.increment;

import java.util.ArrayList;
import java.util.List;

class DmlGenerator {
    private final MySqlVersion v;
    DmlGenerator(MySqlVersion v) { this.v = v; }

    List<SqlTestCaseGenerator.TestCase> generate() {
        List<SqlTestCaseGenerator.TestCase> c = new ArrayList<>();
        genInsert(c); genUpdate(c); genDelete(c); genReplace(c); genEdge(c);
        return c;
    }

    private SqlTestCaseGenerator.TestCase tc(String cat, String sub, String sql, String desc) {
        return new SqlTestCaseGenerator.TestCase(cat, sub, sql, desc);
    }
    private SqlTestCaseGenerator.TestCase tc(String cat, String sub, String sql, String desc, MySqlVersion min) {
        return new SqlTestCaseGenerator.TestCase(cat, sub, sql, desc, min);
    }

    private void genInsert(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 100; i++)
            c.add(tc("DML","INSERT_SINGLE","INSERT INTO t_dml (id, name, age) VALUES ("+i+", 'user_"+i+"', "+(20+i)+")","Single INSERT #"+i));
        for (int b = 2; b <= 20; b++) {
            StringBuilder sb = new StringBuilder("INSERT INTO t_dml (id, name, age) VALUES ");
            for (int j = 0; j < b; j++) { if (j>0) sb.append(", "); sb.append("(").append(b*100+j).append(", 'batch_").append(b).append("_").append(j).append("', ").append(20+j).append(")"); }
            c.add(tc("DML","INSERT_BATCH_"+b, sb.toString(),"Batch INSERT "+b+" rows"));
        }
        for (int i = 0; i < 50; i++)
            c.add(tc("DML","INSERT_NULL","INSERT INTO t_dml (id, name, age) VALUES ("+(1000+i)+", NULL, NULL)","INSERT NULL #"+i));
        for (int i = 0; i < 50; i++) {
            String s = SqlTestCaseGenerator.specialStr(i);
            c.add(tc("DML","INSERT_SPECIAL","INSERT INTO t_dml (id, name, age) VALUES ("+(2000+i)+", '"+SqlTestCaseGenerator.escape(s)+"', "+(25+i)+")","Special chars #"+i));
        }
        for (int i = 0; i < 40; i++)
            c.add(tc("DML","INSERT_ON_DUP","INSERT INTO t_dml (id, name, age) VALUES ("+(3000+i)+", 'dup_"+i+"', "+(30+i)+") ON DUPLICATE KEY UPDATE name=VALUES(name), age=VALUES(age)","ON DUP KEY #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DML","INSERT_IGNORE","INSERT IGNORE INTO t_dml (id, name, age) VALUES ("+(4000+i)+", 'ignore_"+i+"', "+(25+i)+")","INSERT IGNORE #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_SELECT","INSERT INTO t_dml_copy (id, name, age) SELECT id, name, age FROM t_dml WHERE id > "+(i*10)+" LIMIT 5","INSERT SELECT #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_SET","INSERT INTO t_dml SET id="+(5000+i)+", name='set_"+i+"', age="+(28+i),"INSERT SET #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_DEFAULT","INSERT INTO t_dml (id) VALUES ("+(6000+i)+")","INSERT DEFAULT #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_EXPR","INSERT INTO t_dml (id, name, age) VALUES ("+(7000+i)+", CONCAT('expr_', '"+i+"'), "+i+" + 10)","INSERT expr #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_HEX","INSERT INTO t_dml (id, name, age) VALUES ("+(8000+i)+", 0x"+SqlTestCaseGenerator.hex(i)+SqlTestCaseGenerator.hex(i+1)+SqlTestCaseGenerator.hex(i+2)+", "+(30+i)+")","INSERT hex #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_BACKTICK","INSERT INTO `t_dml` (`id`, `name`, `age`) VALUES ("+(9000+i)+", 'bt_"+i+"', "+(22+i)+")","Backtick #"+i));
        for (int i = 0; i < 15; i++)
            c.add(tc("DML","INSERT_PRIORITY","INSERT "+(new String[]{"LOW_PRIORITY","HIGH_PRIORITY"})[i%2]+" INTO t_dml (id, name, age) VALUES ("+(10000+i)+", 'prio_"+i+"', "+(25+i)+")","Priority INSERT #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DML","INSERT_UNICODE","INSERT INTO t_dml (id, name, age) VALUES ("+(11000+i)+", 'unicode_"+SqlTestCaseGenerator.emoji(i)+"_"+i+"', "+(20+i)+")","Unicode #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_CJK","INSERT INTO t_dml (id, name, age) VALUES ("+(12000+i)+", '中文_"+i+"_日本語_한국어', "+(25+i)+")","CJK #"+i));
        for (int i = 0; i < 28; i++)
            c.add(tc("DML","INSERT_TIMESTAMP","INSERT INTO t_timestamp (id, ts, dt, d) VALUES ("+(i+1)+", '2024-01-"+SqlTestCaseGenerator.pad2((i%28)+1)+" 12:30:45', '2024-01-"+SqlTestCaseGenerator.pad2((i%28)+1)+" 12:30:45', '2024-01-"+SqlTestCaseGenerator.pad2((i%28)+1)+"')","Timestamp #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DML","INSERT_ZERO_DATE","INSERT INTO t_timestamp (id, ts, dt, d) VALUES ("+(100+i)+", NULL, '1970-01-01 00:00:01', '1970-01-01')","Zero date #"+i));
        if (v.isAtLeast(MySqlVersion.V8_0))
            for (int i = 0; i < 20; i++)
                c.add(tc("DML","INSERT_ROW_80","INSERT INTO t_dml (id, name, age) VALUES ROW("+(13000+i*2)+", 'row80_"+i+"', "+(20+i)+"), ROW("+(13000+i*2+1)+", 'row80_"+i+"_2', "+(21+i)+")","ROW constructor #"+i,MySqlVersion.V8_0));
        for (int i = 0; i < 50; i++)
            c.add(tc("DML","INSERT_BLOB","INSERT INTO t_blob (id, data) VALUES ("+i+", 0x"+SqlTestCaseGenerator.randomHex(32)+")","BLOB #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DML","INSERT_DECIMAL","INSERT INTO t_decimal (id, amount) VALUES ("+i+", "+SqlTestCaseGenerator.randomDecimal()+")","DECIMAL #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_NEGATIVE","INSERT INTO t_dml (id, name, age) VALUES ("+(14000+i)+", 'neg_"+i+"', "+-(i+1)+")","Negative #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DML","INSERT_BOOLEAN","INSERT INTO t_boolean (id, flag) VALUES ("+i+", "+(i%2==0?"TRUE":"FALSE")+")","BOOLEAN #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_ENUM","INSERT INTO t_enum (id, status) VALUES ("+i+", '"+(new String[]{"a","b","c"}[i%3])+"')","ENUM #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_SET","INSERT INTO t_set (id, tags) VALUES ("+i+", '"+(new String[]{"x","y","z","x,y","y,z","x,y,z"}[i%6])+"')","SET #"+i));
        if (v.isAtLeast(MySqlVersion.V5_7))
            for (int i = 0; i < 20; i++)
                c.add(tc("DML","INSERT_JSON_57","INSERT INTO t_json (id, data) VALUES ("+i+", '{\"key\": \"value_"+i+"\", \"num\": "+i+"}')","JSON #"+i,MySqlVersion.V5_7));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_YEAR","INSERT INTO t_year (id, yr) VALUES ("+i+", "+(2000+i)+")","YEAR #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_BIT","INSERT INTO t_bit (id, flags) VALUES ("+i+", b'"+SqlTestCaseGenerator.bitString(8)+"')","BIT #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DML","INSERT_LONG_STR","INSERT INTO t_dml (id, name, age) VALUES ("+(19000+i)+", '"+SqlTestCaseGenerator.randomString(200)+"', "+(25+i)+")","Long string #"+i));
        for (int i = 0; i < 10; i++)
            c.add(tc("DML","INSERT_EMPTY_STR","INSERT INTO t_dml (id, name, age) VALUES ("+(20000+i)+", '', "+(25+i)+")","Empty string #"+i));
    }

    private void genUpdate(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 100; i++) c.add(tc("DML","UPDATE_SINGLE","UPDATE t_dml SET name='upd_"+i+"' WHERE id="+i,"Single UPDATE #"+i));
        for (int i = 0; i < 50; i++) c.add(tc("DML","UPDATE_MULTI","UPDATE t_dml SET name='multi_"+i+"', age="+(30+i)+" WHERE id="+i,"Multi-col UPDATE #"+i));
        for (int i = 0; i < 50; i++) c.add(tc("DML","UPDATE_RANGE","UPDATE t_dml SET age=age+1 WHERE id BETWEEN "+(i*10)+" AND "+(i*10+9),"Range UPDATE #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","UPDATE_IN","UPDATE t_dml SET name='in_"+i+"' WHERE id IN ("+i+","+(i+100)+","+(i+200)+")","IN UPDATE #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","UPDATE_NULL","UPDATE t_dml SET name=NULL WHERE id="+i,"NULL UPDATE #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","UPDATE_EXPR","UPDATE t_dml SET age=age*2+"+i+", name=CONCAT(name,'_mod') WHERE id="+i,"Expr UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_LIMIT","UPDATE t_dml SET age="+(50+i)+" ORDER BY id LIMIT 5","LIMIT UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_JOIN","UPDATE t_dml a JOIN t_dml_copy b ON a.id=b.id SET a.name=b.name WHERE a.id>"+(i*10),"JOIN UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_SUBQ","UPDATE t_dml SET age=(SELECT MAX(age) FROM t_dml_copy) WHERE id="+i,"Subquery UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_CASE","UPDATE t_dml SET age=CASE WHEN id<"+(i*10)+" THEN 10 ELSE 20 END WHERE id<"+(i*20),"CASE UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_LOW_PRIO","UPDATE LOW_PRIORITY t_dml SET name='low_"+i+"' WHERE id="+i,"LOW_PRIORITY #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_IGNORE","UPDATE IGNORE t_dml SET id="+(5000+i)+" WHERE id="+i,"IGNORE UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_NO_WHERE","UPDATE t_dml SET age=age+1","No WHERE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_ALIAS","UPDATE t_dml AS t SET t.name='alias_"+i+"' WHERE t.id="+i,"Alias UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_TS","UPDATE t_timestamp SET ts='2024-06-"+SqlTestCaseGenerator.pad2((i%28)+1)+" 10:00:00' WHERE id="+(i+1),"Timestamp UPDATE #"+i));
        if (v.isAtLeast(MySqlVersion.V5_7))
            for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_JSON_57","UPDATE t_json SET data=JSON_SET(data,'$.key','upd_"+i+"') WHERE id="+i,"JSON UPDATE #"+i,MySqlVersion.V5_7));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_MULTI_TBL","UPDATE t_dml a, t_dml_copy b SET a.name=b.name WHERE a.id=b.id AND a.id="+i,"Multi-table UPDATE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","UPDATE_DECIMAL","UPDATE t_decimal SET amount=amount+"+SqlTestCaseGenerator.randomDecimal()+" WHERE id="+i,"Decimal UPDATE #"+i));
    }

    private void genDelete(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 100; i++) c.add(tc("DML","DELETE_PK","DELETE FROM t_dml WHERE id="+i,"DELETE PK #"+i));
        for (int i = 0; i < 50; i++) c.add(tc("DML","DELETE_RANGE","DELETE FROM t_dml WHERE id BETWEEN "+(i*10)+" AND "+(i*10+4),"Range DELETE #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","DELETE_IN","DELETE FROM t_dml WHERE id IN ("+i+","+(i+50)+","+(i+100)+")","IN DELETE #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","DELETE_LIMIT","DELETE FROM t_dml ORDER BY id LIMIT "+(i+1),"LIMIT DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_SUBQ","DELETE FROM t_dml WHERE id IN (SELECT id FROM t_dml_copy WHERE age>"+(50+i)+")","Subquery DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_JOIN","DELETE a FROM t_dml a JOIN t_dml_copy b ON a.id=b.id WHERE a.age>"+(50+i),"JOIN DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_LIKE","DELETE FROM t_dml WHERE name LIKE 'user_"+i+"%'","LIKE DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_ALL","DELETE FROM t_dml","All rows DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_IGNORE","DELETE IGNORE FROM t_dml WHERE id="+i,"IGNORE DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_MULTI","DELETE a, b FROM t_dml a JOIN t_dml_copy b ON a.id=b.id WHERE a.id>"+(i*10),"Multi DELETE #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","DELETE_ALIAS","DELETE FROM t_dml AS t WHERE t.id="+i,"Alias DELETE #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","DELETE_COMPLEX","DELETE FROM t_dml WHERE (name LIKE '%test%' OR age>"+(40+i)+") AND id NOT IN (1,2,3)","Complex DELETE #"+i));
    }

    private void genReplace(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 50; i++) c.add(tc("DML","REPLACE_SINGLE","REPLACE INTO t_dml (id, name, age) VALUES ("+i+", 'replace_"+i+"', "+(25+i)+")","Replace #"+i));
        for (int i = 0; i < 30; i++) c.add(tc("DML","REPLACE_BATCH","REPLACE INTO t_dml (id, name, age) VALUES ("+(i*2)+", 'rb1_"+i+"', "+(20+i)+"), ("+(i*2+1)+", 'rb2_"+i+"', "+(21+i)+")","Replace batch #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","REPLACE_SET","REPLACE INTO t_dml SET id="+i+", name='rset_"+i+"', age="+(30+i),"Replace SET #"+i));
    }

    private void genEdge(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 20; i++) c.add(tc("DML","TX_COMMIT","BEGIN; INSERT INTO t_dml (id, name, age) VALUES ("+(15000+i)+", 'tx_"+i+"', "+(25+i)+"); COMMIT;","TX COMMIT #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","TX_ROLLBACK","START TRANSACTION; INSERT INTO t_dml (id, name, age) VALUES ("+(16000+i)+", 'rb_"+i+"', "+(25+i)+"); ROLLBACK;","TX ROLLBACK #"+i));
        for (int i = 0; i < 10; i++) c.add(tc("DML","SAVEPOINT","BEGIN; SAVEPOINT sp_"+i+"; INSERT INTO t_dml (id, name, age) VALUES ("+(17000+i)+", 'sp_"+i+"', "+(25+i)+"); ROLLBACK TO sp_"+i+"; COMMIT;","Savepoint #"+i));
        for (int i = 0; i < 20; i++) c.add(tc("DML","MIXED_TX","BEGIN; INSERT INTO t_dml (id, name, age) VALUES ("+(18000+i)+", 'mix_"+i+"', "+(25+i)+"); UPDATE t_dml SET age="+(30+i)+" WHERE id="+(18000+i)+"; DELETE FROM t_dml WHERE id="+(18000+i)+"; COMMIT;","Mixed TX #"+i));
        for (int i = 0; i < 10; i++) c.add(tc("DML","INSERT_AUTO_INC","INSERT INTO t_auto (name, age) VALUES ('auto_"+i+"', "+(20+i)+")","Auto-inc #"+i));
        for (int i = 0; i < 10; i++) c.add(tc("DML","INSERT_DUP_KEY","INSERT IGNORE INTO t_dml (id, name, age) VALUES (1, 'dup_"+i+"', "+(25+i)+")","Dup key IGNORE #"+i));
        for (int i = 0; i < 10; i++) c.add(tc("DML","UPDATE_ZERO","UPDATE t_dml SET age=0 WHERE id="+i,"Zero value #"+i));
        for (int i = 0; i < 10; i++) c.add(tc("DML","UPDATE_EMPTY","UPDATE t_dml SET name='' WHERE id="+i,"Empty string #"+i));
    }
}
