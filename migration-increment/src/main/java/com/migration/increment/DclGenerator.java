package com.migration.increment;

import java.util.ArrayList;
import java.util.List;

class DclGenerator {
    private final MySqlVersion v;
    DclGenerator(MySqlVersion v) { this.v = v; }

    List<SqlTestCaseGenerator.TestCase> generate() {
        List<SqlTestCaseGenerator.TestCase> c = new ArrayList<>();
        genUser(c); genGrant(c); genRevoke(c); genRole(c); genOtherDcl(c);
        return c;
    }

    private SqlTestCaseGenerator.TestCase tc(String cat, String sub, String sql, String desc) {
        return new SqlTestCaseGenerator.TestCase(cat, sub, sql, desc);
    }
    private SqlTestCaseGenerator.TestCase tc(String cat, String sub, String sql, String desc, MySqlVersion min) {
        return new SqlTestCaseGenerator.TestCase(cat, sub, sql, desc, min);
    }

    private void genUser(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 200; i++)
            c.add(tc("DCL","CREATE_USER","CREATE USER IF NOT EXISTS 'testuser_"+i+"'@'%' IDENTIFIED BY 'password_"+i+"'","CREATE USER #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","CREATE_USER_HOST","CREATE USER IF NOT EXISTS 'hostuser_"+i+"'@'192.168.1."+i+"' IDENTIFIED BY 'hostpass_"+i+"'","User host #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","CREATE_USER_NO_PWD","CREATE USER IF NOT EXISTS 'nopwd_"+i+"'@'%'","User no pwd #"+i));
        for (int i = 0; i < 100; i++)
            c.add(tc("DCL","ALTER_USER_PWD","ALTER USER 'testuser_"+i+"'@'%' IDENTIFIED BY 'newpass_"+i+"'","ALTER PWD #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","ALTER_USER_RENAME","ALTER USER 'testuser_"+i+"'@'%' IDENTIFIED BY 'renamed_pass_"+i+"'","ALTER rename #"+i));
        if (v.isAtLeast(MySqlVersion.V5_7)) {
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","ALTER_EXPIRE_57","ALTER USER 'testuser_"+i+"'@'%' PASSWORD EXPIRE","Pwd expire (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","ALTER_LOCK_57","ALTER USER 'testuser_"+i+"'@'%' ACCOUNT LOCK","Account lock (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","ALTER_UNLOCK_57","ALTER USER 'testuser_"+i+"'@'%' ACCOUNT UNLOCK","Account unlock (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","ALTER_EXPIRE_NEVER_57","ALTER USER 'testuser_"+i+"'@'%' PASSWORD EXPIRE NEVER","Pwd never expire (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","ALTER_EXPIRE_INTERVAL_57","ALTER USER 'testuser_"+i+"'@'%' PASSWORD EXPIRE INTERVAL "+(30+i)+" DAY","Pwd interval (5.7+) #"+i,MySqlVersion.V5_7));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","ALTER_EXPIRE_DEFAULT_57","ALTER USER 'testuser_"+i+"'@'%' PASSWORD EXPIRE DEFAULT","Pwd default expire (5.7+) #"+i,MySqlVersion.V5_7));
        }
        if (v.isAtLeast(MySqlVersion.V8_0)) {
            for (int i = 0; i < 100; i++)
                c.add(tc("DCL","CREATE_USER_CACHING_SHA2_80","CREATE USER IF NOT EXISTS 'authuser_"+i+"'@'%' IDENTIFIED WITH caching_sha2_password BY 'pass_"+i+"'","caching_sha2 (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","CREATE_USER_SHA256_80","CREATE USER IF NOT EXISTS 'sha256user_"+i+"'@'%' IDENTIFIED WITH sha256_password BY 'pass_"+i+"'","sha256 (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","ALTER_PWD_HISTORY_80","ALTER USER 'testuser_"+i+"'@'%' PASSWORD HISTORY "+(3+(i%10)),"Pwd history (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","ALTER_PWD_REUSE_80","ALTER USER 'testuser_"+i+"'@'%' PASSWORD REUSE INTERVAL "+(30+(i%365))+" DAY","Pwd reuse (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","ALTER_FAILED_LOGIN_80","ALTER USER 'testuser_"+i+"'@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME "+(1+i%5),"Failed login (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","ALTER_USER_CERT_80","ALTER USER 'testuser_"+i+"'@'%' REQUIRE X509","Require X509 (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","ALTER_USER_SSL_80","ALTER USER 'testuser_"+i+"'@'%' REQUIRE SSL","Require SSL (8.0+) #"+i,MySqlVersion.V8_0));
        }
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","RENAME_USER","RENAME USER 'testuser_"+(i+100)+"'@'%' TO 'renamed_"+i+"'@'%'","RENAME USER #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","DROP_USER","DROP USER IF EXISTS 'testuser_"+i+"'@'%'","DROP USER #"+i));
    }

    private void genGrant(List<SqlTestCaseGenerator.TestCase> c) {
        String[] privs = {"SELECT","INSERT","UPDATE","DELETE","CREATE","DROP","ALTER","INDEX",
            "CREATE VIEW","SHOW VIEW","CREATE ROUTINE","ALTER ROUTINE","EXECUTE","TRIGGER","EVENT",
            "CREATE TEMPORARY TABLES","LOCK TABLES","REFERENCES"};
        for (int i = 0; i < 200; i++) {
            String priv = privs[i%privs.length];
            c.add(tc("DCL","GRANT_PRIV","GRANT "+priv+" ON sync_test.* TO 'grantuser_"+(i%50)+"'@'%'","GRANT "+priv+" #"+i));
        }
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","GRANT_ALL","GRANT ALL PRIVILEGES ON sync_test.* TO 'grantuser_"+(i%50)+"'@'%'","GRANT ALL #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","GRANT_TABLE","GRANT SELECT, INSERT ON sync_test.t_dml TO 'grantuser_"+(i%50)+"'@'%'","GRANT table #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","GRANT_WITH_GRANT","GRANT SELECT ON sync_test.* TO 'grantuser_"+(i%50)+"'@'%' WITH GRANT OPTION","GRANT WITH GRANT #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DCL","GRANT_PROXIES","GRANT PROXY ON ''@'' TO 'grantuser_"+(i+1)+"'@'%'","GRANT PROXY #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","GRANT_MULTI_PRIV","GRANT SELECT, INSERT, UPDATE, DELETE ON sync_test.* TO 'grantuser_"+(i%50)+"'@'%'","GRANT multi priv #"+i));
    }

    private void genRevoke(List<SqlTestCaseGenerator.TestCase> c) {
        String[] privs = {"SELECT","INSERT","UPDATE","DELETE","CREATE","DROP","ALTER","INDEX"};
        for (int i = 0; i < 100; i++) {
            String priv = privs[i%privs.length];
            c.add(tc("DCL","REVOKE_PRIV","REVOKE "+priv+" ON sync_test.* FROM 'grantuser_"+(i%50)+"'@'%'","REVOKE "+priv+" #"+i));
        }
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","REVOKE_ALL","REVOKE ALL PRIVILEGES ON sync_test.* FROM 'grantuser_"+(i%50)+"'@'%'","REVOKE ALL #"+i));
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","REVOKE_GRANT","REVOKE GRANT OPTION ON sync_test.* FROM 'grantuser_"+(i%50)+"'@'%'","REVOKE GRANT OPT #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DCL","REVOKE_TABLE","REVOKE SELECT, INSERT ON sync_test.t_dml FROM 'grantuser_"+(i%50)+"'@'%'","REVOKE table #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DCL","REVOKE_PROXIES","REVOKE PROXY ON ''@'' FROM 'grantuser_"+(i+1)+"'@'%'","REVOKE PROXY #"+i));
    }

    private void genRole(List<SqlTestCaseGenerator.TestCase> c) {
        if (v.isAtLeast(MySqlVersion.V8_0)) {
            for (int i = 0; i < 100; i++)
                c.add(tc("DCL","CREATE_ROLE_80","CREATE ROLE IF NOT EXISTS 'role_"+i+"'@'%'","CREATE ROLE (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","GRANT_ROLE_PRIV_80","GRANT SELECT ON sync_test.* TO 'role_"+i+"'@'%'","GRANT role priv (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","GRANT_ROLE_TO_USER_80","GRANT 'role_"+(i%10)+"'@'%' TO 'grantuser_"+(i%50)+"'@'%'","GRANT role to user (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 10; i++)
                c.add(tc("DCL","GRANT_ROLE_TO_ROOT_80","GRANT 'role_"+i+"'@'%' TO 'root'@'%'","GRANT role to root (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","SET_DEFAULT_ROLE_80","SET DEFAULT ROLE 'role_"+(i%10)+"'@'%' TO 'grantuser_"+(i%50)+"'@'%'","SET DEFAULT ROLE (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","SET_DEFAULT_ROLE_ALL_80","SET DEFAULT ROLE ALL TO 'grantuser_"+(i%50)+"'@'%'","DEFAULT ROLE ALL (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","SET_DEFAULT_ROLE_NONE_80","SET DEFAULT ROLE NONE TO 'grantuser_"+(i%50)+"'@'%'","DEFAULT ROLE NONE (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","SET_ROLE_80","SET ROLE 'role_"+(i%10)+"'@'%'","SET ROLE (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","SET_ROLE_ALL_80","SET ROLE ALL","SET ROLE ALL (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","SET_ROLE_NONE_80","SET ROLE NONE","SET ROLE NONE (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","REVOKE_ROLE_80","REVOKE 'role_"+(i%10)+"'@'%' FROM 'grantuser_"+(i%50)+"'@'%'","REVOKE role (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 50; i++)
                c.add(tc("DCL","DROP_ROLE_80","DROP ROLE IF EXISTS 'role_"+i+"'@'%'","DROP ROLE (8.0+) #"+i,MySqlVersion.V8_0));
        }
    }

    private void genOtherDcl(List<SqlTestCaseGenerator.TestCase> c) {
        for (int i = 0; i < 50; i++)
            c.add(tc("DCL","FLUSH_PRIV","FLUSH PRIVILEGES","FLUSH PRIV #"+i));
        String[] rwGlobalVars = {"max_connections","wait_timeout","interactive_timeout","connect_timeout",
            "thread_cache_size","table_open_cache","sort_buffer_size",
            "join_buffer_size","read_buffer_size","read_rnd_buffer_size","myisam_sort_buffer_size",
            "tmp_table_size","max_heap_table_size",
            "bulk_insert_buffer_size","innodb_lock_wait_timeout",
            "binlog_cache_size","long_query_time",
            "group_concat_max_len","max_allowed_packet","net_buffer_length"};
        for (int i = 0; i < 100; i++)
            c.add(tc("DCL","SET_GLOBAL","SET GLOBAL "+rwGlobalVars[i%rwGlobalVars.length]+" = "+(100+i*10),"SET GLOBAL #"+i));
        String[] sessionVars = {"sql_mode","autocommit","time_zone","lc_time_names",
            "sql_safe_updates","sql_select_limit",
            "optimizer_switch","optimizer_search_depth","optimizer_prune_level",
            "sort_buffer_size","join_buffer_size","read_buffer_size","tmp_table_size",
            "group_concat_max_len","max_heap_table_size","long_query_time",
            "sql_big_selects","sql_buffer_result","sql_log_off",
            "unique_checks","foreign_key_checks"};
        for (int i = 0; i < 100; i++)
            c.add(tc("DCL","SET_SESSION","SET SESSION "+sessionVars[i%sessionVars.length]+" = "+getSessionValue(sessionVars[i%sessionVars.length],i),"SET SESSION #"+i));
        String[] charsets = {"utf8","utf8mb4","latin1","ascii","binary","gbk","big5"};
        for (int i = 0; i < 60; i++)
            c.add(tc("DCL","SET_NAMES","SET NAMES "+charsets[i%charsets.length],"SET NAMES #"+i));
        for (int i = 0; i < 60; i++)
            c.add(tc("DCL","SET_CHARSET","SET CHARACTER SET "+charsets[i%charsets.length],"SET CHARSET #"+i));
        String[] isoLevels = {"READ UNCOMMITTED","READ COMMITTED","REPEATABLE READ","SERIALIZABLE"};
        for (int i = 0; i < 40; i++)
            c.add(tc("DCL","SET_TX_ISO","SET TRANSACTION ISOLATION LEVEL "+isoLevels[i%4],"SET TX ISO #"+i));
        for (int i = 0; i < 40; i++)
            c.add(tc("DCL","SET_GLOBAL_TX","SET GLOBAL TRANSACTION ISOLATION LEVEL "+isoLevels[i%4],"SET GLOBAL TX #"+i));
        for (int i = 0; i < 40; i++)
            c.add(tc("DCL","SET_TX_RW","SET TRANSACTION READ WRITE","SET TX RW #"+i));
        for (int i = 0; i < 40; i++)
            c.add(tc("DCL","SET_TX_RO","SET TRANSACTION READ ONLY","SET TX RO #"+i));
        for (int i = 0; i < 40; i++)
            c.add(tc("DCL","SET_GLOBAL_TX_RW","SET GLOBAL TRANSACTION READ WRITE","SET GLOBAL TX RW #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_AUTOCOMMIT","SET AUTOCOMMIT = "+(i%2),"SET AUTOCOMMIT #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DCL","FLUSH_TABLES","FLUSH TABLES","FLUSH TABLES #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","FLUSH_HOSTS","FLUSH HOSTS","FLUSH HOSTS #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","FLUSH_STATUS","FLUSH STATUS","FLUSH STATUS #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DCL","SET_USER_VAR","SET @user_var_"+i+" = "+(i*10),"SET user var #"+i));
        for (int i = 0; i < 30; i++)
            c.add(tc("DCL","SET_USER_VAR_STR","SET @user_str_"+i+" = 'value_"+i+"'","SET user str #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_USER_VAR_NULL","SET @user_null_"+i+" = NULL","SET user null #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_USER_VAR_EXPR","SET @user_expr_"+i+" = (SELECT COUNT(*) FROM t_dml)","SET user expr #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_SQL_LOG_BIN","SET SQL_LOG_BIN = "+(i%2),"SET SQL_LOG_BIN #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_UNIQUE_CHECKS","SET UNIQUE_CHECKS = "+(i%2),"SET UNIQUE_CHECKS #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_FK_CHECKS","SET FOREIGN_KEY_CHECKS = "+(i%2),"SET FK_CHECKS #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_AUTO_INC_INCR","SET AUTO_INCREMENT_INCREMENT = "+(1+i%5),"SET AI INCR #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_AUTO_INC_OFF","SET AUTO_INCREMENT_OFFSET = "+(1+i%10),"SET AI OFF #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_INSERT_ID","SET INSERT_ID = "+(1000+i),"SET INSERT_ID #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_LAST_INSERT_ID","SET LAST_INSERT_ID = "+(2000+i),"SET LAST_INSERT_ID #"+i));
        for (int i = 0; i < 20; i++)
            c.add(tc("DCL","SET_TIMESTAMP","SET TIMESTAMP = UNIX_TIMESTAMP()","SET TIMESTAMP #"+i));
        if (v.isAtLeast(MySqlVersion.V8_0)) {
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","PERSIST_GLOBAL_80","SET PERSIST "+rwGlobalVars[i%rwGlobalVars.length]+" = "+(200+i*10),"PERSIST (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 30; i++)
                c.add(tc("DCL","PERSIST_ONLY_80","SET PERSIST_ONLY "+rwGlobalVars[i%rwGlobalVars.length]+" = "+(50+i),"PERSIST_ONLY (8.0+) #"+i,MySqlVersion.V8_0));
            for (int i = 0; i < 20; i++)
                c.add(tc("DCL","RESET_PERSIST_80","RESET PERSIST IF EXISTS "+rwGlobalVars[i%rwGlobalVars.length],"RESET PERSIST (8.0+) #"+i,MySqlVersion.V8_0));
        }
    }

    private String getSessionValue(String var, int i) {
        switch (var) {
            case "sql_mode": return "'STRICT_TRANS_TABLES'";
            case "autocommit": return String.valueOf(i%2);
            case "time_zone": return "'+00:00'";
            case "lc_time_names": return "'en_US'";
            case "sql_safe_updates": return String.valueOf(i%2);
            case "sql_select_limit": return String.valueOf(1000+i);
            case "optimizer_switch": return "'index_merge=on'";
            case "optimizer_search_depth": return String.valueOf(i%5);
            case "optimizer_prune_level": return String.valueOf(i%2);
            case "sort_buffer_size": return String.valueOf(262144+i*1024);
            case "join_buffer_size": return String.valueOf(262144+i*1024);
            case "read_buffer_size": return String.valueOf(131072+i*1024);
            case "tmp_table_size": return String.valueOf(16777216+i*4096);
            case "group_concat_max_len": return String.valueOf(1024+i);
            case "max_heap_table_size": return String.valueOf(16777216+i*4096);
            case "long_query_time": return String.valueOf((i%10)+1);
            case "sql_big_selects": return String.valueOf(i%2);
            case "sql_buffer_result": return String.valueOf(i%2);
            case "sql_log_off": return String.valueOf(i%2);
            case "unique_checks": return String.valueOf(i%2);
            case "foreign_key_checks": return String.valueOf(i%2);
            default: return String.valueOf(i);
        }
    }
}
