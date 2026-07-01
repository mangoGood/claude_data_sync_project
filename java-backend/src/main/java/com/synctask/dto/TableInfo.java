package com.synctask.dto;

public class TableInfo {
    private String name;
    private long rows;
    private String size;
    private String engine;

    public TableInfo() {}

    public TableInfo(String name, long rows, String size) {
        this.name = name;
        this.rows = rows;
        this.size = size;
    }

    public TableInfo(String name, long rows, String size, String engine) {
        this.name = name;
        this.rows = rows;
        this.size = size;
        this.engine = engine;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }
}
