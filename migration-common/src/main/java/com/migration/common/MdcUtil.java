package com.migration.common;

import org.slf4j.MDC;

/**
 * MDC 上下文工具类，用于在日志中注入 taskId、processName 等结构化字段
 */
public final class MdcUtil {

    public static final String TASK_ID = "taskId";
    public static final String PROCESS_NAME = "processName";

    private MdcUtil() {}

    public static void setTaskId(String taskId) {
        if (taskId != null) {
            MDC.put(TASK_ID, taskId);
        }
    }

    public static void setProcessName(String processName) {
        if (processName != null) {
            MDC.put(PROCESS_NAME, processName);
        }
    }

    public static void put(String key, String value) {
        if (key != null && value != null) {
            MDC.put(key, value);
        }
    }

    public static void clear() {
        MDC.clear();
    }

    /**
     * 在 Runnable 执行前后自动管理 MDC 上下文
     */
    public static Runnable wrap(Runnable runnable, String taskId, String processName) {
        return () -> {
            setTaskId(taskId);
            setProcessName(processName);
            try {
                runnable.run();
            } finally {
                clear();
            }
        };
    }
}
