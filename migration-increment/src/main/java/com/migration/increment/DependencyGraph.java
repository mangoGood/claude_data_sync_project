package com.migration.increment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DependencyGraph {

    private static final Logger logger = LoggerFactory.getLogger(DependencyGraph.class);

    private final List<SqlStatement> statements;
    private final Map<Integer, Set<Integer>> adjacencyList;
    private final Map<Integer, Integer> inDegree;
    private final Map<String, List<SqlStatement>> conflictKeyMap;
    private final List<Integer> barrierIds;

    public DependencyGraph(List<SqlStatement> statements) {
        this.statements = new ArrayList<>(statements);
        this.adjacencyList = new HashMap<>();
        this.inDegree = new HashMap<>();
        this.conflictKeyMap = new HashMap<>();
        this.barrierIds = new ArrayList<>();
        initialize();
    }

    private void initialize() {
        for (SqlStatement stmt : statements) {
            int id = stmt.getId();
            adjacencyList.put(id, new HashSet<>());
            inDegree.put(id, 0);

            if (stmt.isBarrier()) {
                barrierIds.add(id);
            } else if (stmt.getConflictKey() != null) {
                conflictKeyMap.computeIfAbsent(stmt.getConflictKey(), k -> new ArrayList<>())
                        .add(stmt);
            }
        }
    }

    public void buildDependencies() {
        long startTime = System.currentTimeMillis();

        buildDmlConflictDependencies();

        buildBarrierDependencies();

        long endTime = System.currentTimeMillis();
        logger.info("Dependency graph built in {} ms. Total edges: {}, DML conflict keys: {}, Barriers: {}",
                endTime - startTime, countEdges(), conflictKeyMap.size(), barrierIds.size());
    }

    private void buildDmlConflictDependencies() {
        for (Map.Entry<String, List<SqlStatement>> entry : conflictKeyMap.entrySet()) {
            List<SqlStatement> conflictingStatements = entry.getValue();
            for (int i = 0; i < conflictingStatements.size(); i++) {
                for (int j = i + 1; j < conflictingStatements.size(); j++) {
                    SqlStatement earlier = conflictingStatements.get(i);
                    SqlStatement later = conflictingStatements.get(j);
                    addEdge(earlier.getId(), later.getId());
                }
            }
        }
    }

    private void buildBarrierDependencies() {
        if (barrierIds.isEmpty()) return;

        for (int i = 1; i < barrierIds.size(); i++) {
            int prevBarrierId = barrierIds.get(i - 1);
            int currBarrierId = barrierIds.get(i);
            addEdge(prevBarrierId, currBarrierId);
        }

        for (SqlStatement stmt : statements) {
            if (stmt.isBarrier()) continue;

            int stmtId = stmt.getId();

            Integer nextBarrier = null;
            for (int barrierId : barrierIds) {
                if (barrierId > stmtId) {
                    nextBarrier = barrierId;
                    break;
                }
            }
            if (nextBarrier != null) {
                addEdge(stmtId, nextBarrier);
            }

            Integer prevBarrier = null;
            for (int barrierId : barrierIds) {
                if (barrierId >= stmtId) break;
                prevBarrier = barrierId;
            }
            if (prevBarrier != null) {
                addEdge(prevBarrier, stmtId);
            }
        }
    }

    private void addEdge(int from, int to) {
        if (from == to) return;
        if (!adjacencyList.get(from).contains(to)) {
            adjacencyList.get(from).add(to);
            inDegree.put(to, inDegree.getOrDefault(to, 0) + 1);
        }
    }

    public List<List<SqlStatement>> topologicalSort() {
        long startTime = System.currentTimeMillis();

        List<List<SqlStatement>> layers = new ArrayList<>();
        Map<Integer, Integer> inDegreeCopy = new HashMap<>(inDegree);
        Queue<Integer> queue = new LinkedList<>();

        for (Map.Entry<Integer, Integer> entry : inDegreeCopy.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        while (!queue.isEmpty()) {
            List<SqlStatement> currentLayer = new ArrayList<>();
            int layerSize = queue.size();

            for (int i = 0; i < layerSize; i++) {
                int nodeId = queue.poll();
                SqlStatement stmt = getStatementById(nodeId);
                if (stmt != null) {
                    currentLayer.add(stmt);
                }

                for (int neighbor : adjacencyList.get(nodeId)) {
                    int newDegree = inDegreeCopy.get(neighbor) - 1;
                    inDegreeCopy.put(neighbor, newDegree);
                    if (newDegree == 0) {
                        queue.offer(neighbor);
                    }
                }
            }

            if (!currentLayer.isEmpty()) {
                layers.add(currentLayer);
            }
        }

        int processedCount = layers.stream().mapToInt(List::size).sum();
        if (processedCount < statements.size()) {
            logger.warn("Cycle detected in dependency graph! Processed: {}/{}", processedCount, statements.size());
        }

        long endTime = System.currentTimeMillis();
        logger.info("Topological sort completed in {} ms. Total layers: {}, Total statements: {}",
                endTime - startTime, layers.size(), processedCount);

        return layers;
    }

    private SqlStatement getStatementById(int id) {
        return statements.stream()
                .filter(s -> s.getId() == id)
                .findFirst()
                .orElse(null);
    }

    private int countEdges() {
        return adjacencyList.values().stream().mapToInt(Set::size).sum();
    }

    public int getStatementCount() { return statements.size(); }
    public int getConflictKeyCount() { return conflictKeyMap.size(); }

    public double getAverageConcurrency() {
        if (statements.isEmpty() || conflictKeyMap.isEmpty()) return 0;
        return (double) statements.size() / conflictKeyMap.size();
    }

    public Map<String, List<SqlStatement>> getConflictKeyMap() {
        return Collections.unmodifiableMap(conflictKeyMap);
    }
}
