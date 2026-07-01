package com.migration.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractCapture<T> implements Capture<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Properties props;
    protected String currentPosition;
    protected volatile boolean running = false;

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
        doStart();
    }

    @Override
    public void stop() throws Exception {
        logger.info("停止 {}", getClass().getSimpleName());
        running = false;
        doStop();
        MdcUtil.clear();
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
