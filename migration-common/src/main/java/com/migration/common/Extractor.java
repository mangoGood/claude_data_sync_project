package com.migration.common;

import java.util.Properties;

public interface Extractor<T, E> {

    void initialize(Properties props) throws Exception;

    E extract(T input) throws Exception;

    String getLastExtractedPosition();

    void setPosition(String position) throws Exception;
}
