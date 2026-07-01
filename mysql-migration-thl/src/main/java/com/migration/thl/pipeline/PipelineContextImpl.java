package com.migration.thl.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PipelineContextImpl implements PipelineContext {

    private static final Logger logger = LoggerFactory.getLogger(PipelineContextImpl.class);

    private final Properties properties;
    private final Map<String, Object> attributes;
    private final Map<String, AtomicLong> counters;
    private Connection sourceConnection;

    public PipelineContextImpl(Properties properties) {
        this.properties = properties;
        this.attributes = new ConcurrentHashMap<>();
        this.counters = new ConcurrentHashMap<>();
    }

    public void setSourceConnection(Connection sourceConnection) {
        this.sourceConnection = sourceConnection;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    @Override
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, Class<T> type) {
        Object value = attributes.get(key);
        if (value == null) return null;
        return (T) value;
    }

    @Override
    public void incrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
    }

    @Override
    public long getCounter(String name) {
        AtomicLong counter = counters.get(name);
        return counter == null ? 0 : counter.get();
    }

    @Override
    public Connection getSourceConnection() {
        return sourceConnection;
    }
}
