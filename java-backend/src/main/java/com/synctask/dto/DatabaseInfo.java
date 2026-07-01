package com.synctask.dto;

import java.util.List;

public class DatabaseInfo {
    private String name;
    private List<TableInfo> tables;
    private boolean accessible;
    private String errorMessage;

    public DatabaseInfo() {}

    public DatabaseInfo(String name) {
        this.name = name;
        this.accessible = true;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TableInfo> getTables() {
        return tables;
    }

    public void setTables(List<TableInfo> tables) {
        this.tables = tables;
    }

    public boolean isAccessible() {
        return accessible;
    }

    public void setAccessible(boolean accessible) {
        this.accessible = accessible;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
