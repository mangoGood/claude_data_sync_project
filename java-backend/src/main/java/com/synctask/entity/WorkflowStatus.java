package com.synctask.entity;

public enum WorkflowStatus {
    CONFIGURING,        // 配置中
    PENDING,            // 启动中（等待Agent接收）
    RECEIVED,           // Agent已接收任务
    STARTING,           // 启动中
    FULL_MIGRATING,     // 全量同步中
    FULL_COMPLETED,     // 全量同步完成
    INCREMENT_RUNNING,  // 增量同步中
    SUBSCRIBE_RUNNING,  // 数据订阅中
    SWITCHING,          // 主备倒换中
    // 子进程短期重试已耗尽、agent 正在长期重连（目标库维护窗口等可自愈场景）。
    // 与 FAILED 的区别：RECONNECTING 不是终态，重连成功会自己回到 *_RUNNING，无需人工介入。
    RECONNECTING,
    COMPLETED,
    FAILED,
    PAUSED
}
