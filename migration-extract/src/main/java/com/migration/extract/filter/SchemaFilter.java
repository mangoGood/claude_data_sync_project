package com.migration.extract.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.util.*;
import java.util.regex.Pattern;

public class SchemaFilter extends AbstractFilter {

    private String doFilter;
    private String ignoreFilter;

    private final List<SchemaTablePattern> doPatterns = new ArrayList<>();
    private final List<SchemaTablePattern> ignorePatterns = new ArrayList<>();

    public void setDo(String doFilter) {
        this.doFilter = doFilter;
    }

    public void setDoFilter(String doFilter) {
        this.doFilter = doFilter;
    }

    public void setIgnore(String ignoreFilter) {
        this.ignoreFilter = ignoreFilter;
    }

    public void setIgnoreFilter(String ignoreFilter) {
        this.ignoreFilter = ignoreFilter;
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);

        if (doFilter != null && !doFilter.trim().isEmpty()) {
            parsePatterns(doFilter, doPatterns);
            logger.info("SchemaFilter 'do' patterns: {}", doPatterns);
        }

        if (ignoreFilter != null && !ignoreFilter.trim().isEmpty()) {
            parsePatterns(ignoreFilter, ignorePatterns);
            logger.info("SchemaFilter 'ignore' patterns: {}", ignorePatterns);
        }
    }

    private void parsePatterns(String config, List<SchemaTablePattern> patterns) {
        String[] parts = config.split(",");
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) continue;

            int dotIdx = part.indexOf('.');
            if (dotIdx > 0) {
                String schema = part.substring(0, dotIdx);
                String table = part.substring(dotIdx + 1);
                patterns.add(new SchemaTablePattern(schema, table));
            } else {
                patterns.add(new SchemaTablePattern(part, null));
            }
        }
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null) return null;

        Map<String, Object> metadata = event.getMetadata();
        String database = (String) metadata.getOrDefault("database_name", "");
        String table = (String) metadata.getOrDefault("table_name", "");

        if (database.isEmpty() && table.isEmpty()) {
            return event;
        }

        if (matchesAny(ignorePatterns, database, table)) {
            logger.debug("Event ignored by SchemaFilter: {}.{} (seqno={})", database, table, event.getSeqno());
            return null;
        }

        if (!doPatterns.isEmpty() && !matchesAny(doPatterns, database, table)) {
            logger.debug("Event filtered out by SchemaFilter 'do' rule: {}.{} (seqno={})", database, table, event.getSeqno());
            return null;
        }

        return event;
    }

    private boolean matchesAny(List<SchemaTablePattern> patterns, String database, String table) {
        for (SchemaTablePattern pattern : patterns) {
            if (pattern.matches(database, table)) {
                return true;
            }
        }
        return false;
    }

    private static class SchemaTablePattern {
        final Pattern schemaPattern;
        final Pattern tablePattern;
        final boolean hasTable;

        SchemaTablePattern(String schema, String table) {
            this.schemaPattern = compilePattern(schema);
            this.tablePattern = table != null ? compilePattern(table) : null;
            this.hasTable = table != null;
        }

        private Pattern compilePattern(String pattern) {
            String regex = pattern
                    .replace(".", "\\.")
                    .replace("*", ".*")
                    .replace("?", ".");
            return Pattern.compile("^" + regex + "$", Pattern.CASE_INSENSITIVE);
        }

        boolean matches(String database, String table) {
            if (!schemaPattern.matcher(database).matches()) {
                return false;
            }
            if (hasTable) {
                return table != null && tablePattern.matcher(table).matches();
            }
            return true;
        }

        @Override
        public String toString() {
            return hasTable
                    ? schemaPattern + "." + tablePattern
                    : schemaPattern.toString();
        }
    }
}
