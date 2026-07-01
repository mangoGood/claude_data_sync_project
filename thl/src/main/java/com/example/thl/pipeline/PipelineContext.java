package com.example.thl.pipeline;

import java.sql.Connection;
import java.util.Properties;

public interface PipelineContext {

    Properties getProperties();

    String getProperty(String key, String defaultValue);

    void setAttribute(String key, Object value);

    Object getAttribute(String key);

    <T> T getAttribute(String key, Class<T> type);

    void incrementCounter(String name);

    long getCounter(String name);

    Connection getSourceConnection();
}
