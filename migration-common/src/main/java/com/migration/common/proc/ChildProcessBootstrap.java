package com.migration.common.proc;

/**
 * 所有受 agent 管理的子进程 main() 的统一入口动作：
 * <ol>
 *   <li>{@link TaskInstanceLock} —— 同一 taskId+role 全局只允许一个实例，拿不到锁立即退出；</li>
 *   <li>{@link ParentWatchdog} —— 父 agent 消失后自杀，不留孤儿。</li>
 * </ol>
 * 两者必须成对：光有锁，孤儿会一直占着锁把恢复流程挡在门外；光有看门狗，
 * 看门狗生效前的几秒窗口仍可能双写。
 */
public final class ChildProcessBootstrap {

    private ChildProcessBootstrap() {
    }

    public static void init(String taskId, String role) {
        TaskInstanceLock.acquireOrExit(taskId, role);
        ParentWatchdog.start();
    }
}
