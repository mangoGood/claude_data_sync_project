package com.synctask.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class LogLevelConverter implements AttributeConverter<WorkflowLog.LogLevel, String> {
    
    @Override
    public String convertToDatabaseColumn(WorkflowLog.LogLevel level) {
        if (level == null) {
            return null;
        }
        return level.name().toLowerCase();
    }
    
    @Override
    public WorkflowLog.LogLevel convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isEmpty()) {
            return null;
        }
        return WorkflowLog.LogLevel.valueOf(dbData.toUpperCase());
    }
}
