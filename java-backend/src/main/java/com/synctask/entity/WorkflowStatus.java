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
    PAUSED;

    /** 不参与单调性比较的控制态哨兵（倒换/重连/失败/暂停：任何方向都放行）。 */
    public static final int PHASE_CONTROL = -1;

    /**
     * 生命周期阶段序号。任务只能沿序号<b>单调前进</b>，低阶段状态不得覆盖高阶段状态。
     *
     * <p>存在的原因：agent 侧的全量进度监控线程与"全量完成"是两个发送方，实测出现过
     * {@code FULL_MIGRATING → FULL_COMPLETED → FULL_MIGRATING → INCREMENT_RUNNING}
     * ——监控线程在被 interrupt 前已进入循环体、正在构造一条 FULL_MIGRATING，于是把已经
     * 到达的 FULL_COMPLETED 顶了回去。agent 侧已补了发送前的二次确认，这里是<b>另一半</b>：
     * 消息乱序/重投/多 agent 接管都可能再次制造倒退，落库前必须自己挡一次。
     *
     * <p>{@link #PHASE_CONTROL} 表示该状态不属于线性生命周期（倒换、长期重连、失败、暂停），
     * 这些状态既可以从任何阶段进入，也可以退回任何阶段，不做比较。
     * 注意：真正的终态拦截（COMPLETED/FAILED 不再被覆盖）由消费端单独处理，不靠序号。
     */
    public int phase() {
        switch (this) {
            case CONFIGURING:        return 0;
            case PENDING:            return 10;
            case RECEIVED:           return 20;
            case STARTING:           return 30;
            case FULL_MIGRATING:     return 40;
            case FULL_COMPLETED:     return 50;
            // 增量与订阅是两条并列的"运行态"，同阶段：一个任务不会两者兼有
            case INCREMENT_RUNNING:
            case SUBSCRIBE_RUNNING:  return 60;
            case COMPLETED:          return 100;
            default:                 return PHASE_CONTROL;
        }
    }

    /** 该状态是否参与生命周期单调性比较。 */
    public boolean isLifecyclePhase() {
        return phase() != PHASE_CONTROL;
    }
}
