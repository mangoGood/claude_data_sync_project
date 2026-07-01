package com.migration.increment.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.util.Locale;
import java.util.Map;

public class CaseMappingFilter extends AbstractFilter {

    private String schemaCase = "none";
    private String tableCase = "none";
    private String columnCase = "none";

    public void setSchemaCase(String schemaCase) {
        this.schemaCase = schemaCase;
    }

    public void setTableCase(String tableCase) {
        this.tableCase = tableCase;
    }

    public void setColumnCase(String columnCase) {
        this.columnCase = columnCase;
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);
        logger.info("CaseMappingFilter - schema: {}, table: {}, column: {}", schemaCase, tableCase, columnCase);
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null) return event;

        Map<String, Object> metadata = event.getMetadata();

        if (!"none".equalsIgnoreCase(schemaCase)) {
            String database = (String) metadata.get("database_name");
            if (database != null) {
                metadata.put("database_name", transformCase(database, schemaCase));
            }
        }

        if (!"none".equalsIgnoreCase(tableCase)) {
            String table = (String) metadata.get("table_name");
            if (table != null) {
                metadata.put("table_name", transformCase(table, tableCase));
            }
        }

        if (!"none".equalsIgnoreCase(columnCase)) {
            String columns = (String) metadata.get("columns");
            if (columns != null) {
                String[] colArr = columns.split(",");
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < colArr.length; i++) {
                    if (i > 0) sb.append(",");
                    sb.append(transformCase(colArr[i].trim(), columnCase));
                }
                metadata.put("columns", sb.toString());
            }
        }

        return event;
    }

    private String transformCase(String value, String caseType) {
        if (value == null || value.isEmpty()) return value;

        switch (caseType.toLowerCase()) {
            case "lower":
                return value.toLowerCase(Locale.ENGLISH);
            case "upper":
                return value.toUpperCase(Locale.ENGLISH);
            default:
                return value;
        }
    }
}
