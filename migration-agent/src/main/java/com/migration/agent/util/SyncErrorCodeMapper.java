package com.migration.agent.util;

public class SyncErrorCodeMapper {

    public static String mapExceptionToErrorCode(Exception e, String context) {
        if (e == null || e.getMessage() == null) {
            return "E9999";
        }
        String msg = e.getMessage().toLowerCase();
        String ctx = context != null ? context.toLowerCase() : "";

        if (msg.contains("access denied") || msg.contains("authentication failed") || msg.contains("28000")) {
            if (ctx.contains("source") || ctx.contains("源")) {
                return "E1003";
            }
            return "E1004";
        }
        if (msg.contains("connection refused") || msg.contains("connect timed out") || msg.contains("no route to host")
                || msg.contains("communications link failure")) {
            if (ctx.contains("source") || ctx.contains("源") || ctx.contains("capture") || ctx.contains("checkpoint") || ctx.contains("binlog")) {
                return "E1005";
            }
            return "E1006";
        }
        if (msg.contains("unable to acquire jdbc connection") || msg.contains("could not create connection")
                || msg.contains("connection is closed") || msg.contains("connection has been closed")) {
            if (ctx.contains("source") || ctx.contains("源") || ctx.contains("capture") || ctx.contains("checkpoint")) {
                return "E1005";
            }
            return "E1006";
        }
        if (msg.contains("duplicate entry") || msg.contains("1062") || msg.contains("duplicate key")) {
            return "E4004";
        }

        return "E9999";
    }

    public static String mapFailureToErrorCode(String failureMessage) {
        if (failureMessage == null || failureMessage.isEmpty()) {
            return "E9999";
        }
        String msg = failureMessage.toLowerCase();

        if (msg.contains("源数据库配置为空")) return "E5001";
        if (msg.contains("目标数据库配置为空")) return "E5002";
        if (msg.contains("连接串") && msg.contains("解析")) return "E5003";

        if (msg.contains("checkpoint") && msg.contains("失败")) return "E2005";
        if (msg.contains("wal") && msg.contains("lsn")) return "E2006";
        if (msg.contains("binlog") && msg.contains("未开启")) return "E2001";
        if (msg.contains("binlog") && msg.contains("格式")) return "E2002";
        if (msg.contains("server_id")) return "E2004";

        if (msg.contains("capture") && msg.contains("启动失败")) return "E3001";
        if (msg.contains("capture") && (msg.contains("jar file not found") || msg.contains("jar") && msg.contains("not found"))) return "E3001";
        if (msg.contains("capture") && msg.contains("异常退出")) return "E3002";
        if (msg.contains("extract") && msg.contains("启动失败")) return "E3003";
        if (msg.contains("extract") && (msg.contains("jar file not found") || msg.contains("jar") && msg.contains("not found"))) return "E3003";
        if (msg.contains("增量") && msg.contains("启动失败")) return "E3004";
        if (msg.contains("增量") && msg.contains("异常退出")) return "E3005";
        if (msg.contains("increment") && (msg.contains("jar file not found") || msg.contains("jar") && msg.contains("not found"))) return "E3004";

        if (msg.contains("全量") && msg.contains("失败")) return "E4001";
        if (msg.contains("全量") && msg.contains("超时")) return "E4002";
        if (msg.contains("写入失败") || msg.contains("sql execution failed")) return "E4003";
        if (msg.contains("duplicate") || msg.contains("主键冲突")) return "E4004";

        if (msg.contains("connection refused") || msg.contains("timed out") || msg.contains("网络不可达")) {
            if (msg.contains("源") || msg.contains("source")) return "E1005";
            return "E1006";
        }
        if (msg.contains("access denied") || msg.contains("认证失败") || msg.contains("authentication")) {
            if (msg.contains("源") || msg.contains("source")) return "E1003";
            return "E1004";
        }

        return "E9999";
    }
}
