package com.migration.agent.checkpoint;

/**
 * {@link FullProgressStore} 的进程内持有者。
 *
 * <p>与 {@link CheckpointHydrator} 一样用静态单例：回灌发生在 {@code AbstractTaskExecutor.run()}
 * 里，而执行器是被线程池按任务创建的，没有依赖注入容器可用。
 * 未设置（中心位点关闭 / 单机部署）时返回 null，调用方按老行为走。
 */
public final class FullProgressStoreHolder {

    private static volatile FullProgressStore instance;

    private FullProgressStoreHolder() {
    }

    public static void set(FullProgressStore store) {
        instance = store;
    }

    public static FullProgressStore get() {
        return instance;
    }

    /** 给单测隔离用。 */
    public static void reset() {
        instance = null;
    }
}
