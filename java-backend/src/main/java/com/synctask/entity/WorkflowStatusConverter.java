package com.synctask.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class WorkflowStatusConverter implements AttributeConverter<WorkflowStatus, String> {
    
    @Override
    public String convertToDatabaseColumn(WorkflowStatus status) {
        if (status == null) {
            return null;
        }
        return status.name().toLowerCase();
    }
    
    @Override
    public WorkflowStatus convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isEmpty()) {
            return null;
        }
        return WorkflowStatus.valueOf(dbData.toUpperCase());
    }
}
