package com.migration.increment;

import com.migration.thl.THLEvent;
import com.migration.thl.THLFileReader;
import com.migration.thl.pipeline.Pipeline;
import com.migration.thl.pipeline.PipelineConfig;
import com.migration.thl.pipeline.PipelineContext;
import com.migration.thl.pipeline.PipelineContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class ConcurrentIncrementMain {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentIncrementMain.class);

    public static void main(String[] args) {
        logger.info("Starting Concurrent Increment Module...");

        try {
            Properties props = new Properties();
            InputStream input = ConcurrentIncrementMain.class.getClassLoader()
                    .getResourceAsStream("increment.properties");
            if (input == null) {
                throw new RuntimeException("increment.properties not found in classpath");
            }
            props.load(input);
            input.close();

            String mode = props.getProperty("concurrent.mode", "batch");
            if ("batch".equalsIgnoreCase(mode)) {
                executeBatchMode(props);
            } else {
                logger.error("Unknown concurrent mode: {}", mode);
                System.exit(1);
            }

        } catch (Exception e) {
            logger.error("Error in Concurrent Increment Module", e);
            System.exit(1);
        }
    }

    public static void executeBatchMode(Properties props) throws Exception {
        String inputDir = props.getProperty("input.dir", "./thl");
        int batchSize = Integer.parseInt(props.getProperty("concurrent.batch.size", "10000"));

        File inputDirFile = new File(inputDir);
        File[] thlFiles = inputDirFile.listFiles((dir, name) ->
                name.startsWith("thl-") && name.endsWith(".thl"));

        if (thlFiles == null || thlFiles.length == 0) {
            logger.warn("No THL files found in directory: {}", inputDir);
            return;
        }

        Arrays.sort(thlFiles);

        Set<Long> executedSeqnos = loadExecutedSeqnos(inputDir);

        THLToSqlConverter sqlConverter = new THLToSqlConverter(props);
        SqlConflictKeyParser conflictKeyParser = new SqlConflictKeyParser();

        PipelineContext pipelineContext = new PipelineContextImpl(props);
        Pipeline pipeline = PipelineConfig.loadFromProperties(props, pipelineContext);
        if (pipeline != null) {
            pipeline.prepare();
            logger.info("Increment Pipeline initialized with {} filters", pipeline.getFilters().size());
        }

        List<SqlStatement> allStatements = new ArrayList<>();
        Map<Long, Long> seqnoToStatementId = new LinkedHashMap<>();
        Map<Long, String> seqnoToSql = new LinkedHashMap<>();
        int statementId = 0;

        logger.info("Phase 1: Parsing THL files and generating SQL statements...");

        for (File thlFile : thlFiles) {
            logger.info("Parsing THL file: {}", thlFile.getName());
            THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath());
            try {
                THLEvent event;
                while ((event = reader.readEvent()) != null) {
                    long seqno = event.getSeqno();

                    if (pipeline != null) {
                        event = pipeline.process(event);
                        if (event == null) {
                            logger.debug("Event filtered out by pipeline: seqno={}", seqno);
                            continue;
                        }
                    }

                    List<String> sqlStatements = sqlConverter.convertToSql(event);
                    if (sqlStatements == null || sqlStatements.isEmpty()) continue;

                    int subSeqno = 0;
                    for (String sql : sqlStatements) {
                        String executableSql = sql.trim();
                        if (executableSql.isEmpty()) continue;

                        long statementSeqno = sqlStatements.size() == 1 ? seqno : (seqno * 1000 + subSeqno);
                        subSeqno++;

                        if (executedSeqnos.contains(statementSeqno)) {
                            logger.debug("Skipping already executed statement seqno: {}", statementSeqno);
                            continue;
                        }

                        SqlStatement stmt = conflictKeyParser.parse(executableSql, statementSeqno);
                        if (stmt != null) {
                            allStatements.add(stmt);
                            seqnoToStatementId.put(statementSeqno, (long) stmt.getId());
                            seqnoToSql.put(statementSeqno, executableSql);
                        }
                    }
                }
            } finally {
                reader.close();
            }
        }

        logger.info("Phase 1 complete. Total SQL statements: {}", allStatements.size());

        if (allStatements.isEmpty()) {
            logger.info("No SQL statements to execute");
            sqlConverter.disconnect();
            return;
        }

        logger.info("Phase 2: Building dependency graph and executing concurrently...");

        DependencyGraph graph = new DependencyGraph(allStatements);

        ConcurrentSqlExecutor executor = new ConcurrentSqlExecutor(props);
        try {
            ConcurrentSqlExecutor.ExecutionResult result = executor.execute(graph);

            logger.info("Phase 2 complete. {}", result);

            if (result.getFailureCount() == 0) {
                logger.info("Phase 3: Saving executed records...");
                for (Long seqno : seqnoToSql.keySet()) {
                    sqlConverter.saveExecutedRecordDirectly(seqno, seqnoToSql.get(seqno));
                }
                logger.info("Phase 3 complete. Saved {} executed records", seqnoToSql.size());
            } else {
                logger.error("Execution had {} failures, not saving progress", result.getFailureCount());
            }

            logger.info("Concurrent Increment Module completed successfully");
            logger.info("Statistics - Success: {}, Failure: {}, Time: {}ms, Layers: {}, Throughput: {:.2f} sql/s",
                    result.getSuccessCount(), result.getFailureCount(),
                    result.getExecutionTimeMs(), result.getLayerCount(),
                    result.getExecutionTimeMs() > 0 ?
                            (double) (result.getSuccessCount() + result.getFailureCount()) * 1000 / result.getExecutionTimeMs() : 0);

        } finally {
            executor.shutdown();
            if (pipeline != null) {
                pipeline.release();
            }
            sqlConverter.disconnect();
        }
    }

    private static Set<Long> loadExecutedSeqnos(String inputDir) {
        Set<Long> executed = new HashSet<>();
        File file = new File(inputDir + "/.executed_records");
        if (!file.exists()) return executed;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                int sepIdx = line.indexOf('|');
                if (sepIdx > 0) {
                    try {
                        long seqno = Long.parseLong(line.substring(0, sepIdx));
                        executed.add(seqno);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error loading executed records", e);
        }

        logger.info("Loaded {} executed seqnos", executed.size());
        return executed;
    }
}
