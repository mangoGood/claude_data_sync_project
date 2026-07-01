package com.migration.agent.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link SyncErrorCodeMapper} 错误码映射单元测试。
 *
 * <p>验证异常消息和失败消息到错误码的映射逻辑，覆盖认证、网络、binlog、
 * 进程启动、全量迁移、主键冲突等典型场景。
 */
@DisplayName("SyncErrorCodeMapper 错误码映射测试")
class SyncErrorCodeMapperTest {

    // ============ mapExceptionToErrorCode ============

    @Test
    @DisplayName("null 异常应返回 E9999")
    void nullExceptionShouldReturnE9999() {
        assertEquals("E9999", SyncErrorCodeMapper.mapExceptionToErrorCode(null, "agent"));
    }

    @Test
    @DisplayName("null 异常消息应返回 E9999")
    void nullExceptionMessageShouldReturnE9999() {
        Exception e = new RuntimeException();
        assertEquals("E9999", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "agent"));
    }

    @Test
    @DisplayName("源库 access denied 应返回 E1003")
    void sourceAccessDeniedShouldReturnE1003() {
        Exception e = new RuntimeException("Access denied for user 'root'@'localhost'");
        assertEquals("E1003", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "source"));
    }

    @Test
    @DisplayName("目标库 access denied 应返回 E1004")
    void targetAccessDeniedShouldReturnE1004() {
        Exception e = new RuntimeException("Authentication failed for user 'app'");
        assertEquals("E1004", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "target"));
    }

    @Test
    @DisplayName("源库 connection refused 应返回 E1005")
    void sourceConnectionRefusedShouldReturnE1005() {
        Exception e = new RuntimeException("Connection refused to host 192.168.1.1");
        assertEquals("E1005", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "capture"));
    }

    @Test
    @DisplayName("目标库 connection refused 应返回 E1006")
    void targetConnectionRefusedShouldReturnE1006() {
        Exception e = new RuntimeException("Communications link failure");
        assertEquals("E1006", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "target"));
    }

    @Test
    @DisplayName("JDBC 连接关闭异常（源库上下文）应返回 E1005")
    void sourceJdbcConnectionClosedShouldReturnE1005() {
        Exception e = new RuntimeException("Unable to acquire JDBC Connection");
        assertEquals("E1005", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "checkpoint"));
    }

    @Test
    @DisplayName("Duplicate entry 异常应返回 E4004")
    void duplicateEntryShouldReturnE4004() {
        Exception e = new RuntimeException("Duplicate entry '1' for key 'PRIMARY'");
        assertEquals("E4004", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "agent"));
    }

    @Test
    @DisplayName("SQLState 28000 认证失败应返回 E1003 或 E1004")
    void sqlState28000ShouldReturnAuthError() {
        Exception e = new RuntimeException("SQLState 28000 authentication failed");
        assertEquals("E1003", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "source"));
        assertEquals("E1004", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "target"));
    }

    @Test
    @DisplayName("未知异常应返回 E9999")
    void unknownExceptionShouldReturnE9999() {
        Exception e = new RuntimeException("Some unknown error");
        assertEquals("E9999", SyncErrorCodeMapper.mapExceptionToErrorCode(e, "agent"));
    }

    // ============ mapFailureToErrorCode ============

    @Test
    @DisplayName("null 失败消息应返回 E9999")
    void nullFailureMessageShouldReturnE9999() {
        assertEquals("E9999", SyncErrorCodeMapper.mapFailureToErrorCode(null));
    }

    @Test
    @DisplayName("空失败消息应返回 E9999")
    void emptyFailureMessageShouldReturnE9999() {
        assertEquals("E9999", SyncErrorCodeMapper.mapFailureToErrorCode(""));
    }

    @Test
    @DisplayName("源数据库配置为空应返回 E5001")
    void emptySourceDbConfigShouldReturnE5001() {
        assertEquals("E5001", SyncErrorCodeMapper.mapFailureToErrorCode("源数据库配置为空"));
    }

    @Test
    @DisplayName("目标数据库配置为空应返回 E5002")
    void emptyTargetDbConfigShouldReturnE5002() {
        assertEquals("E5002", SyncErrorCodeMapper.mapFailureToErrorCode("目标数据库配置为空"));
    }

    @Test
    @DisplayName("连接串解析失败应返回 E5003")
    void connectionStringParseErrorShouldReturnE5003() {
        assertEquals("E5003", SyncErrorCodeMapper.mapFailureToErrorCode("连接串格式解析失败"));
    }

    @Test
    @DisplayName("checkpoint 失败应返回 E2005")
    void checkpointFailureShouldReturnE2005() {
        assertEquals("E2005", SyncErrorCodeMapper.mapFailureToErrorCode("初始化 checkpoint 失败"));
    }

    @Test
    @DisplayName("WAL LSN 相关失败应返回 E2006")
    void walLsnFailureShouldReturnE2006() {
        assertEquals("E2006", SyncErrorCodeMapper.mapFailureToErrorCode("无法获取 WAL LSN 位点"));
    }

    @Test
    @DisplayName("binlog 未开启应返回 E2001")
    void binlogNotEnabledShouldReturnE2001() {
        assertEquals("E2001", SyncErrorCodeMapper.mapFailureToErrorCode("MySQL binlog 未开启"));
    }

    @Test
    @DisplayName("binlog 格式错误应返回 E2002")
    void binlogFormatErrorShouldReturnE2002() {
        assertEquals("E2002", SyncErrorCodeMapper.mapFailureToErrorCode("binlog 格式不为 ROW"));
    }

    @Test
    @DisplayName("server_id 错误应返回 E2004")
    void serverIdErrorShouldReturnE2004() {
        assertEquals("E2004", SyncErrorCodeMapper.mapFailureToErrorCode("server_id 未配置或冲突"));
    }

    @Test
    @DisplayName("capture 启动失败应返回 E3001")
    void captureStartFailureShouldReturnE3001() {
        assertEquals("E3001", SyncErrorCodeMapper.mapFailureToErrorCode("capture 进程启动失败"));
    }

    @Test
    @DisplayName("capture jar 未找到应返回 E3001")
    void captureJarNotFoundShouldReturnE3001() {
        assertEquals("E3001", SyncErrorCodeMapper.mapFailureToErrorCode("capture jar file not found"));
    }

    @Test
    @DisplayName("capture 异常退出应返回 E3002")
    void captureAbnormalExitShouldReturnE3002() {
        assertEquals("E3002", SyncErrorCodeMapper.mapFailureToErrorCode("capture 进程异常退出"));
    }

    @Test
    @DisplayName("extract 启动失败应返回 E3003")
    void extractStartFailureShouldReturnE3003() {
        assertEquals("E3003", SyncErrorCodeMapper.mapFailureToErrorCode("extract 进程启动失败"));
    }

    @Test
    @DisplayName("extract jar 未找到应返回 E3003")
    void extractJarNotFoundShouldReturnE3003() {
        assertEquals("E3003", SyncErrorCodeMapper.mapFailureToErrorCode("extract jar file not found"));
    }

    @Test
    @DisplayName("增量启动失败应返回 E3004")
    void incrementStartFailureShouldReturnE3004() {
        assertEquals("E3004", SyncErrorCodeMapper.mapFailureToErrorCode("增量同步进程启动失败"));
    }

    @Test
    @DisplayName("增量异常退出应返回 E3005")
    void incrementAbnormalExitShouldReturnE3005() {
        assertEquals("E3005", SyncErrorCodeMapper.mapFailureToErrorCode("增量同步进程异常退出"));
    }

    @Test
    @DisplayName("全量失败应返回 E4001")
    void fullMigrationFailureShouldReturnE4001() {
        assertEquals("E4001", SyncErrorCodeMapper.mapFailureToErrorCode("全量迁移失败"));
    }

    @Test
    @DisplayName("全量超时应返回 E4002")
    void fullMigrationTimeoutShouldReturnE4002() {
        assertEquals("E4002", SyncErrorCodeMapper.mapFailureToErrorCode("全量迁移超时"));
    }

    @Test
    @DisplayName("写入失败应返回 E4003")
    void writeFailureShouldReturnE4003() {
        assertEquals("E4003", SyncErrorCodeMapper.mapFailureToErrorCode("SQL execution failed"));
    }

    @Test
    @DisplayName("主键冲突应返回 E4004")
    void primaryKeyConflictShouldReturnE4004() {
        assertEquals("E4004", SyncErrorCodeMapper.mapFailureToErrorCode("主键冲突: duplicate key"));
    }

    @Test
    @DisplayName("源库网络超时应返回 E1005")
    void sourceNetworkTimeoutShouldReturnE1005() {
        assertEquals("E1005", SyncErrorCodeMapper.mapFailureToErrorCode("源数据库 connection timed out"));
    }

    @Test
    @DisplayName("目标库网络超时应返回 E1006")
    void targetNetworkTimeoutShouldReturnE1006() {
        assertEquals("E1006", SyncErrorCodeMapper.mapFailureToErrorCode("目标数据库 connection timed out"));
    }

    @Test
    @DisplayName("源库认证失败应返回 E1003")
    void sourceAuthFailureShouldReturnE1003() {
        assertEquals("E1003", SyncErrorCodeMapper.mapFailureToErrorCode("源数据库 access denied"));
    }

    @Test
    @DisplayName("目标库认证失败应返回 E1004")
    void targetAuthFailureShouldReturnE1004() {
        assertEquals("E1004", SyncErrorCodeMapper.mapFailureToErrorCode("目标数据库 authentication failed"));
    }

    @Test
    @DisplayName("未知失败消息应返回 E9999")
    void unknownFailureMessageShouldReturnE9999() {
        assertEquals("E9999", SyncErrorCodeMapper.mapFailureToErrorCode("一些未知的错误消息"));
    }
}
