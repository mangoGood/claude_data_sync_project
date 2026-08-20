package com.migration.agent.service;

import com.migration.common.traffic.TrafficSourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * agent 侧的源库开关兜底还原。
 *
 * <p>捕获期间源库的 {@code general_log} 是开着的。子进程正常退出会自己关掉，
 * 但 {@code kill -9}、宿主断电、agent 硬崩这三种情形下它没有任何机会执行还原——
 * 而不还原意味着源库<b>每一条语句</b>都继续往 {@code mysql.general_log} 写，
 * 直到把源库的 datadir 写满。这是本功能最大的运维风险，所以补丁要打在多处：
 *
 * <ol>
 *   <li>子进程 shutdown hook（{@code TrafficMain}）——覆盖正常停止 / SIGTERM；</li>
 *   <li>agent 任务线程收尾（{@code TrafficCaptureTask.stop}）——覆盖任务被停/判失败；</li>
 *   <li>agent 看门狗判定守护放弃时——覆盖子进程反复起不来；</li>
 *   <li>agent 重启扫尾（{@link #sweepOnStartup}）——覆盖 agent 自己硬崩。</li>
 * </ol>
 *
 * <p>判据是录制目录里的 {@code source_state.properties}：子进程开日志时写下它，
 * 自己成功还原后删掉它。文件还在 = 还没还原干净。
 */
public class TrafficSourceGuardService {

    private static final Logger logger = LoggerFactory.getLogger(TrafficSourceGuardService.class);

    /** 还原某个任务的源库开关；没有待还原状态时静默返回 true。 */
    public boolean restoreIfPending(String taskId) {
        File dir = new File("./files/" + taskId + "/traffic");
        TrafficSourceState state = TrafficSourceState.load(dir);
        if (state == null) {
            return true;    // 子进程已经自己还原并清掉了状态文件
        }
        logger.warn("[{}] 检测到源库语句日志尚未还原，由 agent 兜底处理", taskId);
        boolean ok = state.restore();
        if (ok) {
            TrafficSourceState.clear(dir);
        } else {
            logger.error("[{}] 兜底还原失败——源库 {}:{} 的 general_log 可能仍开着，"
                    + "会持续写入日志表直到磁盘写满，请人工确认", taskId, state.host, state.port);
        }
        return ok;
    }

    /**
     * agent 启动时扫一遍所有任务目录。
     *
     * <p>这一道专治"agent 自己硬崩"：崩的那一刻没有任何代码在跑，
     * 只有下一次启动的自己能发现并收拾。
     */
    public void sweepOnStartup() {
        File root = new File("./files");
        File[] tasks = root.listFiles(File::isDirectory);
        if (tasks == null) return;
        int restored = 0;
        for (File taskDir : tasks) {
            File stateFile = TrafficSourceState.fileIn(new File(taskDir, "traffic"));
            if (!stateFile.isFile()) continue;
            logger.warn("agent 启动扫尾：任务 {} 的源库语句日志仍处于开启状态", taskDir.getName());
            if (restoreIfPending(taskDir.getName())) {
                restored++;
            }
        }
        if (restored > 0) {
            logger.info("agent 启动扫尾：已还原 {} 个源库的语句日志开关", restored);
        }
    }
}
