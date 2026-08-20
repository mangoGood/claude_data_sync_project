package com.migration.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractCapture<T> implements Capture<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Properties props;

    /** 逐行加密器（延迟初始化：未开加密时不做任何 KDF）。 */
    private com.migration.common.security.CapLineCipher capCipher;

    /**
     * 创建 {@code .cap} 写入器。四个 capture 实现都用它，而不是各自 new BufferedWriter——
     * 加密开关只在这一处生效，不会出现"某个引擎的 .cap 忘了加密"。
     *
     * <p>返回类型仍是 {@link java.io.BufferedWriter}，因此调用方的
     * {@code write/flush/close} 一行都不用改。
     */
    protected java.io.BufferedWriter newCapWriter(java.io.File outputFile) throws java.io.IOException {
        java.io.Writer raw = new java.io.OutputStreamWriter(
                new java.io.FileOutputStream(outputFile), java.nio.charset.StandardCharsets.UTF_8);
        return new com.migration.common.security.CapFileWriter(raw, capCipher());
    }

    /** 逐行加密器；未开加密时返回的实例 {@code isEnabled()==false}，写入即透传。 */
    protected com.migration.common.security.CapLineCipher capCipher() {
        if (capCipher == null) {
            capCipher = new com.migration.common.security.CapLineCipher(
                    props != null ? props : new Properties());
        }
        return capCipher;
    }
    protected String currentPosition;
    protected volatile boolean running = false;

    /**
     * 活性心跳线程：只要 capture 进程存活且 running，就每隔 {@value #LIVENESS_INTERVAL_MS}ms 改写
     * {@code <capture.output.dir>/capture_liveness}。这是<b>源无关</b>的存活信号——不依赖各源特有的
     * 业务心跳（MySQL 的 __sync_heartbeat / TiCDC watermark / Oracle LogMiner 等各不相同），只要 JVM
     * 没被冻结、线程还在调度，文件就持续刷新；进程一旦僵死/死锁/被 SIGSTOP，本线程随之停摆、文件停更，
     * agent 侧看门狗据此判活。放在基类，令所有 capture 实现统一具备该信号。
     */
    private Thread livenessThread;
    private static final long LIVENESS_INTERVAL_MS = 3000;

    @Override
    public void initialize(Properties props) throws Exception {
        this.props = props;
        String taskId = props.getProperty("task.id", "");
        if (!taskId.isEmpty()) {
            MdcUtil.setTaskId(taskId);
        }
        MdcUtil.setProcessName("capture");
        logger.info("初始化 {}, taskId={}", getClass().getSimpleName(), taskId);
        doInitialize();
    }

    @Override
    public void start() throws Exception {
        logger.info("启动 {}", getClass().getSimpleName());
        running = true;
        startLivenessHeartbeat();
        doStart();
    }

    @Override
    public void stop() throws Exception {
        logger.info("停止 {}", getClass().getSimpleName());
        running = false;
        if (livenessThread != null) {
            livenessThread.interrupt();
        }
        doStop();
        MdcUtil.clear();
    }

    private void startLivenessHeartbeat() {
        String outputDir = props.getProperty("capture.output.dir", "binlog_output");
        livenessThread = new Thread(() -> {
            java.io.File file = new java.io.File(outputDir, "capture_liveness");
            while (running) {
                try {
                    java.io.File dir = file.getParentFile();
                    if (dir != null && !dir.exists()) {
                        dir.mkdirs();
                    }
                    try (java.io.FileWriter w = new java.io.FileWriter(file, false)) {
                        w.write(Long.toString(System.currentTimeMillis()));
                    }
                } catch (Exception e) {
                    logger.debug("写 capture_liveness 失败: {}", e.getMessage());
                }
                try {
                    Thread.sleep(LIVENESS_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "capture-liveness");
        livenessThread.setDaemon(true);
        livenessThread.start();
    }

    @Override
    public String getCurrentPosition() {
        return currentPosition;
    }

    @Override
    public void setPosition(String position) throws Exception {
        this.currentPosition = position;
        logger.info("设置位点: {}", position);
    }

    protected abstract void doInitialize() throws Exception;

    protected abstract void doStart() throws Exception;

    protected abstract void doStop() throws Exception;

    public boolean isRunning() {
        return running;
    }
}
