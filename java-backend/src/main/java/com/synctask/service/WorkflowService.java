package com.synctask.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.synctask.dto.TaskCreatedMessage;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class WorkflowService {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);
    private static final Gson gson = new Gson();

    /** 任务名白名单：中英文、数字、空格、常见连接符。禁止 < > " ' & 等可用于 XSS 的字符（前端渲染已转义，此为纵深防御）。 */
    private static final java.util.regex.Pattern TASK_NAME_PATTERN =
            java.util.regex.Pattern.compile("^[\\u4e00-\\u9fa5A-Za-z0-9 _\\-.()（）\\[\\]]{1,100}$");

    private void validateTaskName(String name) {
        if (name == null || name.isEmpty()) {
            throw new RuntimeException("任务名称不能为空");
        }
        if (!TASK_NAME_PATTERN.matcher(name).matches()) {
            throw new RuntimeException("任务名称包含非法字符，仅允许中英文、数字、空格及 _-.()[] 等符号，长度不超过100");
        }
    }

    /**
     * 非 SQL 管线类型的配对约束（均仅限实时同步任务，灾备/订阅链路走 SQL 管线不适用）：
     * <ul>
     *   <li>MongoDB 只支持 mongodb→mongodb（副本集到副本集）；</li>
     *   <li>Elasticsearch 只能作为目标，且源必须是 MySQL（binlog 增量捕获）。</li>
     * </ul>
     */
    private void validateMongoTypePairing(String sourceType, String targetType, String taskType) {
        boolean srcMongo = "mongodb".equalsIgnoreCase(sourceType);
        boolean tgtMongo = "mongodb".equalsIgnoreCase(targetType);
        boolean srcEs = "elasticsearch".equalsIgnoreCase(sourceType);
        boolean tgtEs = "elasticsearch".equalsIgnoreCase(targetType);
        if (!srcMongo && !tgtMongo && !srcEs && !tgtEs) {
            return;
        }
        if (srcMongo != tgtMongo) {
            throw new RuntimeException("MongoDB 只能与 MongoDB 互相同步，不支持与其它数据库类型组合");
        }
        if (srcEs) {
            throw new RuntimeException("Elasticsearch 不支持作为同步源，仅支持 MySQL 到 Elasticsearch");
        }
        if (tgtEs && !"mysql".equalsIgnoreCase(sourceType)) {
            throw new RuntimeException("到 Elasticsearch 的同步目前仅支持 MySQL 源");
        }
        if (taskType != null && !"SYNC".equals(taskType)) {
            throw new RuntimeException("MongoDB/Elasticsearch 同步目前仅支持实时同步任务，不支持灾备/订阅");
        }
    }

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowLogRepository workflowLogRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Transactional
    public Workflow createWorkflow(String name, String sourceType, String targetType, Long userId, String taskType) {
        return createWorkflow(name, sourceType, targetType, userId, taskType, null);
    }

    @Transactional
    public Workflow createWorkflow(String name, String sourceType, String targetType, Long userId,
                                   String taskType, String drMode) {
        String effectiveTaskType = taskType != null ? taskType : "SYNC";
        String effectiveName = name != null ? name.trim() : name;
        validateTaskName(effectiveName);
        if (workflowRepository.existsByUserIdAndTaskTypeAndNameAndIsDeletedFalse(userId, effectiveTaskType, effectiveName)) {
            throw new RuntimeException("已存在同名任务，请更换任务名称");
        }

        String effectiveSourceType = sourceType != null ? sourceType : "mysql";
        String effectiveTargetType = targetType != null ? targetType : "mysql";
        validateMongoTypePairing(effectiveSourceType, effectiveTargetType, effectiveTaskType);

        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID().toString());
        workflow.setName(effectiveName);
        workflow.setSourceType(effectiveSourceType);
        workflow.setTargetType(effectiveTargetType);
        workflow.setStatus(WorkflowStatus.CONFIGURING);
        workflow.setUserId(userId);
        workflow.setProgress(0);
        workflow.setIsBilling(false);
        workflow.setTaskType(effectiveTaskType);

        if ("DR".equals(taskType)) {
            workflow.setMigrationMode("fullAndIncre");
            workflow.setDrStatus("DR_CONFIGURING");
            // 灾备方向：默认单向。双向（active-active 防回环）依赖 capture 侧的 origin 标记跳过，
            // 目前 MySQL binlog 与 PostgreSQL WAL 两条 capture 均已实现，故支持 mysql↔mysql 与 pg↔pg；
            // 其它类型（Oracle/Mongo/ES）capture 侧未实现防回环，暂不支持双向。
            String effectiveDrMode = "BIDIRECTIONAL".equalsIgnoreCase(drMode) ? "BIDIRECTIONAL" : "UNIDIRECTIONAL";
            if ("BIDIRECTIONAL".equals(effectiveDrMode)) {
                String st = workflow.getSourceType();
                String tt = workflow.getTargetType();
                boolean bothMysql = "mysql".equalsIgnoreCase(st) && "mysql".equalsIgnoreCase(tt);
                boolean bothPg = "postgresql".equalsIgnoreCase(st) && "postgresql".equalsIgnoreCase(tt);
                if (!bothMysql && !bothPg) {
                    throw new RuntimeException("双向灾备目前仅支持 MySQL↔MySQL 或 PostgreSQL↔PostgreSQL");
                }
            }
            workflow.setDrMode(effectiveDrMode);
        }

        Workflow savedWorkflow = workflowRepository.save(workflow);
        addLog(savedWorkflow.getId(), WorkflowLog.LogLevel.INFO, "任务创建成功，状态: 配置中");
        return savedWorkflow;
    }

    @Transactional
    public Workflow updateConfig(String workflowId, Long userId, String sourceConnection, String targetConnection,
                                  String migrationMode, String syncObjects, String sourceDbName,
                                  String targetDbName, String sourceType, String targetType,
                                  String kafkaBootstrapServers, String kafkaTopicPrefix,
                                  String kafkaTopicStrategy, String subscribeFormat,
                                  Boolean fanoutEnabled, String targetConnections) {
        Workflow workflow = getWorkflowById(workflowId, userId);

        if (workflow.getStatus() != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能修改配置中的任务，当前状态: " + workflow.getStatus().name());
        }

        String newSourceType = sourceType != null ? sourceType : workflow.getSourceType();
        String newTargetType = targetType != null ? targetType : workflow.getTargetType();
        validateMongoTypePairing(newSourceType, newTargetType, workflow.getTaskType());

        if (sourceConnection != null) workflow.setSourceConnection(sourceConnection);
        if (targetConnection != null) workflow.setTargetConnection(targetConnection);
        if (migrationMode != null) workflow.setMigrationMode(migrationMode);
        if (syncObjects != null) workflow.setSyncObjects(syncObjects);
        if (sourceDbName != null) workflow.setSourceDbName(sourceDbName);
        if (targetDbName != null) workflow.setTargetDbName(targetDbName);
        if (sourceType != null) workflow.setSourceType(sourceType);
        if (targetType != null) workflow.setTargetType(targetType);
        if (kafkaBootstrapServers != null) workflow.setKafkaBootstrapServers(kafkaBootstrapServers);
        if (kafkaTopicPrefix != null) workflow.setKafkaTopicPrefix(kafkaTopicPrefix);
        if (kafkaTopicStrategy != null) workflow.setKafkaTopicStrategy(kafkaTopicStrategy);
        if (subscribeFormat != null) workflow.setSubscribeFormat(subscribeFormat);
        if (fanoutEnabled != null) {
            workflow.setFanoutEnabled(fanoutEnabled);
            if (fanoutEnabled && targetConnections != null) {
                workflow.setTargetConnections(targetConnections);
                int count = countTargetConnections(targetConnections);
                workflow.setFanoutTargetCount(count);
            } else if (!fanoutEnabled) {
                workflow.setFanoutTargetCount(1);
            }
        }

        addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务配置已更新");
        return workflowRepository.save(workflow);
    }

    @Transactional
    public Workflow launchWorkflow(String workflowId, Long userId) {
        Workflow workflow = getWorkflowById(workflowId, userId);
        
        if (workflow.getStatus() != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能启动配置中的任务，当前状态: " + workflow.getStatus().name());
        }
        
        if (workflow.getSourceConnection() == null || workflow.getSourceConnection().isEmpty()) {
            throw new RuntimeException("请先完成源库连接信息配置");
        }
        
        boolean isSubscribeTask = "SUBSCRIBE".equals(workflow.getTaskType());
        
        if (!isSubscribeTask) {
            if (workflow.getTargetConnection() == null || workflow.getTargetConnection().isEmpty()) {
                throw new RuntimeException("请先完成目标库连接信息配置");
            }
        }
        
        if (isSubscribeTask) {
            if (workflow.getKafkaBootstrapServers() == null || workflow.getKafkaBootstrapServers().isEmpty()) {
                throw new RuntimeException("请先配置Kafka连接地址");
            }
        }
        
        boolean isDrTask = "DR".equals(workflow.getTaskType());
        
        if (!isDrTask && !isSubscribeTask) {
            if (workflow.getSyncObjects() == null || workflow.getSyncObjects().isEmpty()) {
                throw new RuntimeException("请先选择同步对象");
            }
            if (workflow.getMigrationMode() == null || workflow.getMigrationMode().isEmpty()) {
                throw new RuntimeException("请先选择同步模式");
            }
        }
        
        if (isSubscribeTask) {
            if (workflow.getSyncObjects() == null || workflow.getSyncObjects().isEmpty()) {
                workflow.setSyncObjects("{\"_all\":true}");
            }
            workflow.setMigrationMode("subscribe");
        }

        // 双向灾备：创建隐藏的反向影子任务（B→A，仅增量）。此刻只建行不启动——
        // 若立即启动，反向全量会把尚未初始化的 B 反灌回 A；等正向进入增量同步
        // （KafkaConsumerService 监听到 INCREMENT_RUNNING）后再自动启动反向通道，
        // 反向 capture 从 B 的最新位点起步，天然跳过正向全量灌入 B 的存量数据。
        if (isDrTask && "BIDIRECTIONAL".equals(workflow.getDrMode()) && workflow.getDrPeerWorkflowId() == null) {
            Workflow shadow = new Workflow();
            shadow.setId(UUID.randomUUID().toString());
            shadow.setName(workflow.getName() + "-反向");
            shadow.setTaskType("DR_SHADOW");
            shadow.setDrMode("BIDIRECTIONAL");
            shadow.setDrPeerWorkflowId(workflow.getId());
            shadow.setUserId(workflow.getUserId());
            shadow.setStatus(WorkflowStatus.CONFIGURING);
            shadow.setProgress(0);
            shadow.setIsBilling(false);
            shadow.setMigrationMode("fullAndIncre");
            shadow.setSourceConnection(workflow.getTargetConnection());
            shadow.setTargetConnection(workflow.getSourceConnection());
            shadow.setSourceType(workflow.getTargetType());
            shadow.setTargetType(workflow.getSourceType());
            shadow.setSourceDbName(workflow.getTargetDbName());
            shadow.setTargetDbName(workflow.getSourceDbName());
            // 反向通道镜像正向的同步对象集（灾备两端库名/表集一致）；为空则由 agent 在
            // 反向源库上自动发现——继承可避免把 B 实例上无关的库卷进反向同步
            shadow.setSyncObjects(workflow.getSyncObjects());
            workflowRepository.save(shadow);
            workflow.setDrPeerWorkflowId(shadow.getId());
            addLog(workflowId, WorkflowLog.LogLevel.INFO,
                    "双向灾备：已创建反向同步通道（影子任务 " + shadow.getId() + "），将在正向进入增量同步后自动启动");
        }

        workflow.setStatus(WorkflowStatus.PENDING);
        workflow.setIsBilling(true);
        workflowRepository.save(workflow);
        
        addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务启动中，状态: 启动中");
        
        try {
            kafkaProducerService.sendTaskCreatedMessage(workflow);
            addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务消息已发送到 Kafka topic: sync-task-created，等待任务执行服务处理");
        } catch (Exception e) {
            addLog(workflowId, WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
        }
        
        return workflow;
    }

    /** 分页参数上限：防止 pageSize 传超大值一次拉全表打挂内存/DB。page 至少为 1。 */
    private static final int MAX_PAGE_SIZE = 200;

    private static int clampPageSize(int pageSize) {
        if (pageSize < 1) return 10;
        return Math.min(pageSize, MAX_PAGE_SIZE);
    }

    private static int clampPage(int page) {
        return Math.max(page, 1);
    }

    public Page<Workflow> getWorkflowsByUserId(Long userId, int page, int pageSize, String sortBy, String sortDirection) {
        String fieldName = mapSortField(sortBy);

        Sort.Direction direction = sortDirection.equalsIgnoreCase("ASC") ? Sort.Direction.ASC : Sort.Direction.DESC;
        Sort sort = Sort.by(direction, fieldName);
        Pageable pageable = PageRequest.of(clampPage(page) - 1, clampPageSize(pageSize), sort);
        return workflowRepository.findByUserId(userId, pageable);
    }

    public Page<Workflow> getWorkflowsByUserIdAndFilters(Long userId, String keyword, String status, String taskType, int page, int pageSize, String sortBy, String sortDirection) {
        String fieldName = mapSortField(sortBy);

        Sort.Direction direction = sortDirection.equalsIgnoreCase("ASC") ? Sort.Direction.ASC : Sort.Direction.DESC;
        Sort sort = Sort.by(direction, fieldName);
        Pageable pageable = PageRequest.of(clampPage(page) - 1, clampPageSize(pageSize), sort);
        
        boolean hasKeyword = keyword != null && !keyword.trim().isEmpty();
        boolean hasStatus = status != null && !status.trim().isEmpty();
        boolean hasTaskType = taskType != null && !taskType.trim().isEmpty();
        
        if (hasTaskType) {
            if (hasKeyword && hasStatus) {
                WorkflowStatus workflowStatus = WorkflowStatus.valueOf(status.toUpperCase());
                return workflowRepository.findByUserIdAndTaskTypeAndKeywordAndStatus(userId, taskType, keyword.trim(), workflowStatus, pageable);
            } else if (hasKeyword) {
                return workflowRepository.findByUserIdAndTaskTypeAndKeyword(userId, taskType, keyword.trim(), pageable);
            } else if (hasStatus) {
                WorkflowStatus workflowStatus = WorkflowStatus.valueOf(status.toUpperCase());
                return workflowRepository.findByUserIdAndTaskTypeAndStatus(userId, taskType, workflowStatus, pageable);
            } else {
                return workflowRepository.findByUserIdAndTaskType(userId, taskType, pageable);
            }
        }
        
        if (hasKeyword && hasStatus) {
            WorkflowStatus workflowStatus = WorkflowStatus.valueOf(status.toUpperCase());
            return workflowRepository.findByUserIdAndKeywordAndStatus(userId, keyword.trim(), workflowStatus, pageable);
        } else if (hasKeyword) {
            return workflowRepository.findByUserIdAndKeyword(userId, keyword.trim(), pageable);
        } else if (hasStatus) {
            WorkflowStatus workflowStatus = WorkflowStatus.valueOf(status.toUpperCase());
            return workflowRepository.findByUserIdAndStatus(userId, workflowStatus, pageable);
        } else {
            return workflowRepository.findByUserId(userId, pageable);
        }
    }
    
    public List<Workflow> getFailedWorkflowsByUserId(Long userId) {
        // DR_SHADOW 是双向灾备的隐藏反向通道，不在任何列表中直接展示。
        // 过滤下推到 DB 查询，避免先全量取回再内存 filter。
        return workflowRepository.findByUserIdAndStatusExcludingTaskType(userId, WorkflowStatus.FAILED, "DR_SHADOW");
    }
    
    private String mapSortField(String sortBy) {
        switch (sortBy) {
            case "name":
                return "name";
            case "status":
                return "status";
            case "created_at":
                return "createdAt";
            case "is_billing":
                return "isBilling";
            default:
                return "createdAt";
        }
    }

    public Workflow getWorkflowById(String id, Long userId) {
        Workflow workflow = workflowRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        
        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权访问此任务");
        }
        
        return workflow;
    }

    public List<WorkflowLog> getWorkflowLogs(String workflowId, Long userId) {
        Workflow workflow = getWorkflowById(workflowId, userId);
        return workflowLogRepository.findByWorkflowIdOrderByCreatedAtDesc(workflow.getId());
    }

    /** 构造发给 agent 的任务控制消息（stop/resume/terminate 级联共用的字段装配）。 */
    private TaskCreatedMessage buildControlMessage(Workflow w, String messageType, String currentStatus) {
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(w.getId());
        message.setTaskName(w.getName());
        message.setUserId(w.getUserId());
        message.setSourceConnection(w.getSourceConnection());
        message.setTargetConnection(w.getTargetConnection());
        message.setMigrationMode(w.getMigrationMode());
        message.setCreatedAt(w.getCreatedAt());
        message.setMessageType(messageType);
        message.setCurrentStatus(currentStatus);
        message.setSourceType(w.getSourceType());
        message.setTargetType(w.getTargetType());
        message.setSourceDbName(w.getSourceDbName());
        message.setTargetDbName(w.getTargetDbName());
        message.setTaskType(w.getTaskType());
        message.setDrMode(w.getDrMode());
        message.setSyncObjects(parseSyncObjects(w.getSyncObjects()));
        return message;
    }

    /**
     * 双向灾备：把主任务的控制操作级联到反向影子任务（用户不可见，必须跟随主任务生命周期，
     * 否则会留下无人管理的反向同步进程）。级联失败只记日志，不阻断主任务操作。
     */
    private void cascadeBidiShadow(Workflow primary, String action) {
        if (!"DR".equals(primary.getTaskType()) || !"BIDIRECTIONAL".equals(primary.getDrMode())
                || primary.getDrPeerWorkflowId() == null) {
            return;
        }
        Workflow shadow = workflowRepository.findById(primary.getDrPeerWorkflowId()).orElse(null);
        if (shadow == null) {
            return;
        }

        WorkflowStatus st = shadow.getStatus();
        boolean shadowActive = st == WorkflowStatus.PENDING || st == WorkflowStatus.RECEIVED
                || st == WorkflowStatus.STARTING || st == WorkflowStatus.INCREMENT_RUNNING
                || st == WorkflowStatus.FULL_MIGRATING || st == WorkflowStatus.FULL_COMPLETED;

        try {
            switch (action) {
                case "pause":
                    if (shadowActive) {
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "stop", st.name()));
                        shadow.setStatus(WorkflowStatus.PAUSED);
                        workflowRepository.save(shadow);
                    }
                    break;
                case "resume":
                    // 影子仍是 CONFIGURING（正向暂停/失败时还没进过增量）无需处理：
                    // 正向恢复后进入 INCREMENT_RUNNING 时会由状态消费者自动首次启动。
                    // PAUSED（正向暂停时一并停的）或 FAILED（反向通道也挂了）→ 从增量位点重新拉起。
                    if (st == WorkflowStatus.PAUSED || st == WorkflowStatus.FAILED) {
                        shadow.setStatus(WorkflowStatus.STARTING);
                        shadow.setIsBilling(true);
                        shadow.setErrorMessage(null);
                        shadow.setErrorCode(null);
                        shadow.setCompletedAt(null);
                        workflowRepository.save(shadow);
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "resume", "INCREMENT_RUNNING"));
                    }
                    break;
                case "stop":
                    if (shadowActive || st == WorkflowStatus.PAUSED) {
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "terminate", st.name()));
                    }
                    shadow.setStatus(WorkflowStatus.COMPLETED);
                    shadow.setCompletedAt(LocalDateTime.now());
                    shadow.setIsBilling(false);
                    workflowRepository.save(shadow);
                    break;
                case "delete":
                    if (shadowActive) {
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "terminate", st.name()));
                    }
                    shadow.setIsDeleted(true);
                    shadow.setIsBilling(false);
                    workflowRepository.save(shadow);
                    break;
                default:
                    return;
            }
            addLog(primary.getId(), WorkflowLog.LogLevel.INFO, "双向灾备：反向通道已级联执行 " + action);
        } catch (Exception e) {
            addLog(primary.getId(), WorkflowLog.LogLevel.WARNING,
                    "双向灾备：反向通道级联 " + action + " 失败: " + e.getMessage());
        }
    }

    @Transactional
    public void pauseWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);

        String currentStatus = workflow.getStatus().name();

        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("stop");
        message.setCurrentStatus(currentStatus);
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());

        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已暂停，发送停止消息到 Kafka，当前状态: " + currentStatus);
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
        }

        workflow.setStatus(WorkflowStatus.PAUSED);
        workflowRepository.save(workflow);
        cascadeBidiShadow(workflow, "pause");
    }

    @Transactional
    public void resumeWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);
        String previousStatus = workflow.getStatus().name();
        workflow.setStatus(WorkflowStatus.STARTING);
        workflowRepository.save(workflow);
        
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("resume");
        message.setCurrentStatus(previousStatus);
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setTaskType(workflow.getTaskType());
        
        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已恢复，发送恢复消息到 Kafka，等待任务执行服务处理");
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 恢复消息发送失败: " + e.getMessage());
        }
        cascadeBidiShadow(workflow, "resume");
    }

    @Transactional
    public void stopWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);
        
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("terminate");
        message.setCurrentStatus(workflow.getStatus().name());
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        
        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "发送终止消息到 Kafka，结束所有相关进程");
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 终止消息发送失败: " + e.getMessage());
        }
        
        workflow.setStatus(WorkflowStatus.COMPLETED);
        workflow.setCompletedAt(LocalDateTime.now());
        workflow.setIsBilling(false);
        workflowRepository.save(workflow);
        addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已结束，状态: 已完成");
        cascadeBidiShadow(workflow, "stop");
    }

    @Transactional
    public void deleteWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);

        WorkflowStatus status = workflow.getStatus();
        if (status != WorkflowStatus.COMPLETED && status != WorkflowStatus.FAILED && status != WorkflowStatus.FULL_COMPLETED && status != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能删除已完成、失败或配置中的任务，当前状态: " + status.name());
        }

        workflow.setIsDeleted(true);
        workflowRepository.save(workflow);
        addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已删除（软删除）");
        cascadeBidiShadow(workflow, "delete");
    }

    @Transactional
    public void retryWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);
        
        if (workflow.getStatus() != WorkflowStatus.FAILED) {
            throw new RuntimeException("只能重试失败的任务，当前状态: " + workflow.getStatus().name());
        }
        
        boolean incrementStarted = Boolean.TRUE.equals(workflow.getIncrementStarted());
        
        if (incrementStarted) {
            workflow.setStatus(WorkflowStatus.STARTING);
            workflow.setIsBilling(true);
            workflow.setErrorMessage(null);
            workflow.setErrorCode(null);
            workflow.setCompletedAt(null);
            workflowRepository.save(workflow);
            
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务恢复中，增量同步曾已启动，将从增量位点继续同步");
            
            TaskCreatedMessage message = new TaskCreatedMessage();
            message.setTaskId(workflow.getId());
            message.setTaskName(workflow.getName());
            message.setUserId(workflow.getUserId());
            message.setSourceConnection(workflow.getSourceConnection());
            message.setTargetConnection(workflow.getTargetConnection());
            message.setMigrationMode(workflow.getMigrationMode());
            message.setSyncObjects(parseSyncObjects(workflow.getSyncObjects()));
            message.setSourceDbName(workflow.getSourceDbName());
            message.setTargetDbName(workflow.getTargetDbName());
            message.setCreatedAt(workflow.getCreatedAt());
            message.setMessageType("resume");
            message.setCurrentStatus("INCREMENT_RUNNING");
            message.setSourceType(workflow.getSourceType());
            message.setTargetType(workflow.getTargetType());
            
            try {
                kafkaProducerService.sendControlMessage(message);
                addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务恢复消息已发送到 Kafka（跳过全量同步，从增量位点继续）");
            } catch (Exception e) {
                addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
            }
        } else {
            workflow.setStatus(WorkflowStatus.PENDING);
            workflow.setProgress(0);
            workflow.setIsBilling(true);
            workflow.setErrorMessage(null);
            workflow.setErrorCode(null);
            workflow.setCompletedAt(null);
            workflow.setTotalTables(null);
            workflow.setCompletedTables(null);
            workflow.setCurrentTable(null);
            workflow.setCurrentTableProgress(null);
            workflow.setCurrentTableRows(null);
            workflow.setCurrentTableTotalRows(null);
            workflowRepository.save(workflow);
            
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务重试中，状态重置为 PENDING");
            
            TaskCreatedMessage message = new TaskCreatedMessage();
            message.setTaskId(workflow.getId());
            message.setTaskName(workflow.getName());
            message.setUserId(workflow.getUserId());
            message.setSourceConnection(workflow.getSourceConnection());
            message.setTargetConnection(workflow.getTargetConnection());
            message.setMigrationMode(workflow.getMigrationMode());
            message.setSyncObjects(parseSyncObjects(workflow.getSyncObjects()));
            message.setSourceDbName(workflow.getSourceDbName());
            message.setCreatedAt(workflow.getCreatedAt());
            message.setMessageType("TASK_CREATED");
            message.setSourceType(workflow.getSourceType());
            message.setTargetType(workflow.getTargetType());
            
            try {
                kafkaProducerService.sendTaskCreatedMessage(workflow);
                addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务重试消息已发送到 Kafka，等待任务执行服务处理");
            } catch (Exception e) {
                addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
            }
        }

        // 双向灾备：主任务重试时把反向影子通道一并拉起，否则双向同步只剩单边。
        // resume 语义会处理影子当前处于 PAUSED 的情形；仍是 CONFIGURING（尚未进过增量）
        // 则等主任务进 INCREMENT_RUNNING 时由状态消费者自动首启（cascadeBidiShadow 内已判断）。
        cascadeBidiShadow(workflow, "resume");
    }

    private Map<String, Object> parseSyncObjects(String syncObjects) {
        if (syncObjects == null || syncObjects.isEmpty()) {
            return null;
        }
        try {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            return gson.fromJson(syncObjects, type);
        } catch (Exception e) {
            return null;
        }
    }

    private void addLog(String workflowId, WorkflowLog.LogLevel level, String message) {
        WorkflowLog log = new WorkflowLog();
        log.setWorkflowId(workflowId);
        log.setLevel(level);
        log.setMessage(message);
        workflowLogRepository.save(log);
    }

    /** 统计 targetConnections JSON 数组中的目标库数量 */
    private int countTargetConnections(String targetConnectionsJson) {
        if (targetConnectionsJson == null || targetConnectionsJson.trim().isEmpty()) return 0;
        // 简单统计：通过 "host" 字段出现次数估算
        int count = 0;
        int idx = 0;
        while ((idx = targetConnectionsJson.indexOf("\"host\"", idx)) != -1) {
            count++;
            idx += 6;
        }
        return Math.max(1, count);
    }

    @Transactional
    public Workflow failoverWorkflow(String workflowId, Long userId) {
        Workflow workflow = getWorkflowById(workflowId, userId);

        if (!"DR".equals(workflow.getTaskType())) {
            throw new RuntimeException("只有灾备任务才能执行主备倒换");
        }

        if ("BIDIRECTIONAL".equals(workflow.getDrMode())) {
            throw new RuntimeException("双向灾备两端均可读写、实时互同步，无需主备倒换");
        }

        if (workflow.getStatus() != WorkflowStatus.INCREMENT_RUNNING && workflow.getStatus() != WorkflowStatus.SWITCHING) {
            throw new RuntimeException("只有灾备中的任务才能执行主备倒换，当前状态: " + workflow.getStatus().name());
        }

        String originalSource = workflow.getSourceConnection();
        String originalTarget = workflow.getTargetConnection();
        String originalSourceType = workflow.getSourceType();
        String originalTargetType = workflow.getTargetType();
        String originalSourceDbName = workflow.getSourceDbName();
        String originalTargetDbName = workflow.getTargetDbName();

        workflow.setSourceConnection(originalTarget);
        workflow.setTargetConnection(originalSource);
        workflow.setSourceType(originalTargetType);
        workflow.setTargetType(originalSourceType);
        workflow.setSourceDbName(originalTargetDbName);
        workflow.setTargetDbName(originalSourceDbName);

        workflow.setStatus(WorkflowStatus.SWITCHING);
        workflow.setDrStatus("SWITCHING");
        workflow.setDrSwitchCount(workflow.getDrSwitchCount() != null ? workflow.getDrSwitchCount() + 1 : 1);
        workflow.setDrSwitchStartTime(java.time.LocalDateTime.now());
        workflowRepository.save(workflow);

        addLog(workflowId, WorkflowLog.LogLevel.INFO, "主备倒换开始，源库与目标库连接信息已交换，倒换次数: " + workflow.getDrSwitchCount());

        final TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setSyncObjects(parseSyncObjects(workflow.getSyncObjects()));
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("failover");
        message.setCurrentStatus("INCREMENT_RUNNING");
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());

        final String logWorkflowId = workflowId;
        org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
            new org.springframework.transaction.support.TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    new Thread(() -> {
                        boolean httpSuccess = false;
                        try {
                            httpSuccess = callAgentFailoverApi(message);
                            if (httpSuccess) {
                                addLog(logWorkflowId, WorkflowLog.LogLevel.INFO, "主备倒换命令已通过Agent HTTP API直接发送成功");
                            }
                        } catch (Exception e) {
                            addLog(logWorkflowId, WorkflowLog.LogLevel.WARNING, "Agent HTTP API 调用失败: " + e.getMessage());
                        }

                        if (!httpSuccess) {
                            try {
                                kafkaProducerService.sendControlMessage(message);
                                addLog(logWorkflowId, WorkflowLog.LogLevel.INFO, "主备倒换命令已通过Kafka发送（HTTP API不可用时的降级方案）");
                            } catch (Exception e) {
                                addLog(logWorkflowId, WorkflowLog.LogLevel.WARNING, "Kafka 主备倒换消息发送失败: " + e.getMessage());
                            }
                        }
                    }, "FailoverNotifier-" + workflowId).start();
                }
            }
        );

        return workflow;
    }

    private boolean callAgentFailoverApi(TaskCreatedMessage message) {
        // agent 地址与鉴权 token 均可配（环境变量优先），不再写死 localhost；
        // agent 侧 failover 是敏感端点，配置了 AGENT_API_TOKEN 时必须带 Bearer token。
        String agentBase = System.getenv().getOrDefault("AGENT_BASE_URL", "http://localhost:8083");
        String agentToken = System.getenv("AGENT_API_TOKEN");
        String agentUrl = agentBase + "/api/agent/failover";
        try {
            java.net.URL url = new java.net.URL(agentUrl);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            if (agentToken != null && !agentToken.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + agentToken);
            }
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(30000);
            conn.setDoOutput(true);

            com.google.gson.Gson gson = new com.google.gson.GsonBuilder()
                .registerTypeAdapter(java.time.LocalDateTime.class, (com.google.gson.JsonSerializer<java.time.LocalDateTime>) (src, typeOfSrc, context) ->
                    context.serialize(src.format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
                .create();
            String jsonBody = gson.toJson(message);

            try (java.io.OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (java.io.InputStream is = conn.getInputStream()) {
                    String response = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                    logger.info("Agent failover API response: {}", response);
                }
                return true;
            } else {
                logger.warn("Agent failover API returned status: {}", responseCode);
                return false;
            }
        } catch (java.net.ConnectException e) {
            logger.warn("Agent HTTP API not available at {}: {}", agentUrl, e.getMessage());
            return false;
        } catch (Exception e) {
            logger.warn("Error calling Agent failover API: {}", e.getMessage());
            return false;
        }
    }
}
