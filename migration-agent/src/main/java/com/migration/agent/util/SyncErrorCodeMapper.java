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

        // TLS 相关必须排在最前：驱动会把握手失败包成"网络不可达/连接失败"，
        // 落到那些泛化规则上会把人指去查地址端口——而地址端口完全是对的。
        if (msg.contains("要求加密") || msg.contains("实际未加密") || msg.contains("ssl_not_encrypted")) return "E5007";
        if (msg.contains("证书") && (msg.contains("已被删除") || msg.contains("材料")
                || msg.contains("物化失败") || msg.contains("不受支持") || msg.contains("无法解析"))) return "E5008";
        if (msg.contains("证书") && (msg.contains("校验失败") || msg.contains("主机名")
                || msg.contains("过期") || msg.contains("链"))) return "E5006";
        if (msg.contains("certpathvalidator") || msg.contains("trust anchors")
                || msg.contains("unable to find valid certification path")) return "E5006";
        if (msg.contains("tls") && (msg.contains("握手") || msg.contains("handshake"))) return "E5005";
        if (msg.contains("sslhandshakeexception") || msg.contains("ssl connection required")
                || msg.contains("不支援 ssl") || msg.contains("不支持 ssl")) return "E5005";

        if (msg.contains("源数据库配置为空")) return "E5001";
        if (msg.contains("目标数据库配置为空")) return "E5002";
        if (msg.contains("连接串") && msg.contains("解析")) return "E5003";

        // 必须排在下面那条泛化的 checkpoint 规则之前：回灌失败是"跨机接管拿不到位点"这个特定故障，
        // 落到 E2005（checkpoint 初始化失败）会让人往源库连接上查，方向就错了
        if (msg.contains("位点回灌失败")) return "E3014";
        if (msg.contains("唯一键冲突")) return "E3017";
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
