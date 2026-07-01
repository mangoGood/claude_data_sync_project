package com.migration.increment.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.util.Map;

public class ColumnTransformFilter extends AbstractFilter {

    private String renameColumns;
    private String excludeColumns;
    private final Map<String, String> columnRenameMap = new java.util.HashMap<>();
    private final java.util.Set<String> excludeColumnSet = new java.util.HashSet<>();

    public void setRenameColumns(String renameColumns) {
        this.renameColumns = renameColumns;
    }

    public void setExcludeColumns(String excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);

        if (renameColumns != null && !renameColumns.trim().isEmpty()) {
            String[] pairs = renameColumns.split(",");
            for (String pair : pairs) {
                pair = pair.trim();
                if (pair.isEmpty()) continue;
                String[] kv = pair.split(":");
                if (kv.length == 2) {
                    columnRenameMap.put(kv[0].trim().toLowerCase(), kv[1].trim());
                }
            }
        }

        if (excludeColumns != null && !excludeColumns.trim().isEmpty()) {
            for (String col : excludeColumns.split(",")) {
                excludeColumnSet.add(col.trim().toLowerCase());
            }
        }

        if (!columnRenameMap.isEmpty()) {
            logger.info("ColumnTransformFilter rename mappings: {}", columnRenameMap);
        }
        if (!excludeColumnSet.isEmpty()) {
            logger.info("ColumnTransformFilter exclude columns: {}", excludeColumnSet);
        }
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null) return event;
        if (columnRenameMap.isEmpty() && excludeColumnSet.isEmpty()) return event;

        Map<String, Object> metadata = event.getMetadata();

        String columnsStr = (String) metadata.get("columns");
        if (columnsStr == null || columnsStr.isEmpty()) return event;

        String[] columns = columnsStr.split(",");
        StringBuilder newColumns = new StringBuilder();
        boolean changed = false;

        for (String col : columns) {
            String trimmed = col.trim();
            String lower = trimmed.toLowerCase();

            if (excludeColumnSet.contains(lower)) {
                changed = true;
                continue;
            }

            String newName = columnRenameMap.get(lower);
            if (newName != null) {
                newColumns.append(newName).append(",");
                changed = true;
            } else {
                newColumns.append(trimmed).append(",");
            }
        }

        if (changed) {
            String result = newColumns.toString();
            if (result.endsWith(",")) {
                result = result.substring(0, result.length() - 1);
            }
            metadata.put("columns", result);
            logger.debug("Column transform applied: seqno={}", event.getSeqno());
        }

        return event;
    }
}
