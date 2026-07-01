package com.migration.thl.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFilter implements PipelineFilter {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected String name;
    protected int priority = 100;
    protected PipelineContext context;

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name != null ? name : getClass().getSimpleName();
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void configure(PipelineContext context) {
        this.context = context;
        logger.info("Configuring filter: {}", getName());
    }

    @Override
    public void prepare(PipelineContext context) {
        logger.info("Preparing filter: {}", getName());
    }

    @Override
    public void release(PipelineContext context) {
        logger.info("Releasing filter: {}", getName());
    }
}
