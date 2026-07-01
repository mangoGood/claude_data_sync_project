package com.migration.common;

import java.util.Properties;

public interface Capture<T> {

    void initialize(Properties props) throws Exception;

    void start() throws Exception;

    void stop() throws Exception;

    String getCurrentPosition();

    void setPosition(String position) throws Exception;
}
