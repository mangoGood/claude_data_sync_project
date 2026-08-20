package com.migration.traffic;

import com.migration.common.io.AtomicFileWriter;

import java.io.File;

/**
 * 把错误码写给 agent。
 *
 * <p>复用既有引擎那条管道：写 {@code files/<taskId>/binlog_output/error_status}，
 * 格式 {@code 时间戳|错误码|seqno|消息|来源}，agent 的
 * {@code AbstractTaskExecutor.checkIncrementErrorStatus()} 每轮监控都会轮询它。
 *
 * <p>目录名叫 binlog_output 对流量任务是有点别扭，但另起一条上报通道意味着
 * agent 侧要再写一份轮询逻辑——而这类"第二份实现"正是本仓库里错误码会漂移的根源。
 * 沿用同一条管道，错误码就自动享有既有的展示、映射与 CI 门禁。
 */
public final class TrafficErrorStatus {

    private TrafficErrorStatus() {
    }

    public static void report(String taskId, String errorCode, String message) {
        File dir = new File("./files/" + taskId + "/binlog_output");
        if (!dir.exists() && !dir.mkdirs()) {
            return;
        }
        String line = System.currentTimeMillis() + "|" + errorCode + "|-1|"
                + (message == null ? "" : message.replace("|", "/")) + "|traffic\n";
        AtomicFileWriter.writeStringQuietly(new File(dir, "error_status"), line);
    }
}
