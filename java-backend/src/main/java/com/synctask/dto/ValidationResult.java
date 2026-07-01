package com.synctask.dto;

import java.util.ArrayList;
import java.util.List;

public class ValidationResult {
    private List<CheckItem> checkItems = new ArrayList<>();
    private boolean allPassed = false;

    public static class CheckItem {
        private String name;
        private String description;
        private boolean passed;
        private String message;
        private String severity;

        public CheckItem(String name, String description, boolean passed, String message, String severity) {
            this.name = name;
            this.description = description;
            this.passed = passed;
            this.message = message;
            this.severity = severity;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public boolean isPassed() { return passed; }
        public void setPassed(boolean passed) { this.passed = passed; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }
    }

    public List<CheckItem> getCheckItems() { return checkItems; }
    public void setCheckItems(List<CheckItem> checkItems) { this.checkItems = checkItems; }
    public boolean isAllPassed() { return allPassed; }
    public void setAllPassed(boolean allPassed) { this.allPassed = allPassed; }

    public void addItem(String name, String description, boolean passed, String message, String severity) {
        checkItems.add(new CheckItem(name, description, passed, message, severity));
    }
}
