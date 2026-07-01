package com.migration.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractCapture<T> implements Capture<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Properties props;
    protected String currentPosition;
    protected boolean running = false;

    @Override
    public void initialize(Properties props) throws Exception {
        this.props = props;
        logger.info("Initializing {} with properties", getClass().getSimpleName());
        doInitialize();
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting {}", getClass().getSimpleName());
        running = true;
        doStart();
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping {}", getClass().getSimpleName());
        running = false;
        doStop();
    }

    @Override
    public String getCurrentPosition() {
        return currentPosition;
    }

    @Override
    public void setPosition(String position) throws Exception {
        this.currentPosition = position;
        logger.info("Set position to: {}", position);
    }

    protected abstract void doInitialize() throws Exception;

    protected abstract void doStart() throws Exception;

    protected abstract void doStop() throws Exception;

    public boolean isRunning() {
        return running;
    }
}
