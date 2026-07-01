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
    COMPLETED,
    FAILED,
    PAUSED
}
