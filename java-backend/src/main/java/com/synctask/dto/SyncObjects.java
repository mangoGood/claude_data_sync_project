package com.synctask.dto;

import java.util.List;
import java.util.Map;

public class SyncObjects {
    private Map<String, DatabaseTables> databases;

    public SyncObjects() {}

    public Map<String, DatabaseTables> getDatabases() {
        return databases;
    }

    public void setDatabases(Map<String, DatabaseTables> databases) {
        this.databases = databases;
    }

    public static class DatabaseTables {
        private List<String> tables;

        public DatabaseTables() {}

        public DatabaseTables(List<String> tables) {
            this.tables = tables;
        }

        public List<String> getTables() {
            return tables;
        }

        public void setTables(List<String> tables) {
            this.tables = tables;
        }
    }
}
