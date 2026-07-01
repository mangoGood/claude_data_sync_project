package com.migration.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractExtractor<T, E> implements Extractor<T, E> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Properties props;
    protected String lastExtractedPosition;

    @Override
    public void initialize(Properties props) throws Exception {
        this.props = props;
        logger.info("Initializing {} with properties", getClass().getSimpleName());
        doInitialize();
    }

    @Override
    public E extract(T input) throws Exception {
        logger.debug("Extracting from input: {}", input);
        return doExtract(input);
    }

    @Override
    public String getLastExtractedPosition() {
        return lastExtractedPosition;
    }

    @Override
    public void setPosition(String position) throws Exception {
        this.lastExtractedPosition = position;
        logger.info("Set position to: {}", position);
    }

    protected abstract void doInitialize() throws Exception;

    protected abstract E doExtract(T input) throws Exception;
}
