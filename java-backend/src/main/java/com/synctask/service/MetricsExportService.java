package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Prometheus指标导出服务
 * 为Grafana仪表盘提供指标数据
 */
@Service
public class MetricsExportService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsExportService.class);

    @Autowired
    private WorkflowRepository workflowRepository;

    /**
     * 导出Prometheus格式指标
     */
    public String exportPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();
        List<Workflow> allWorkflows = workflowRepository.findAll();

        long totalTasks = allWorkflows.stream().filter(w -> !Boolean.TRUE.equals(w.getIsDeleted())).count();
        long runningTasks = allWorkflows.stream()
                .filter(w -> !Boolean.TRUE.equals(w.getIsDeleted()))
                .filter(w -> w.getStatus() == WorkflowStatus.INCREMENT_RUNNING
                        || w.getStatus() == WorkflowStatus.FULL_MIGRATING
                        || w.getStatus() == WorkflowStatus.SUBSCRIBE_RUNNING)
                .count();
        long failedTasks = allWorkflows.stream()
                .filter(w -> !Boolean.TRUE.equals(w.getIsDeleted()))
                .filter(w -> w.getStatus() == WorkflowStatus.FAILED)
                .count();
        long completedTasks = allWorkflows.stream()
                .filter(w -> !Boolean.TRUE.equals(w.getIsDeleted()))
                .filter(w -> w.getStatus() == WorkflowStatus.COMPLETED)
                .count();

        // 总任务数
        sb.append("# HELP synctask_tasks_total Total number of tasks\n");
        sb.append("# TYPE synctask_tasks_total gauge\n");
        sb.append("synctask_tasks_total ").append(totalTasks).append("\n\n");

        // 运行中任务数
        sb.append("# HELP synctask_tasks_running Number of running tasks\n");
        sb.append("# TYPE synctask_tasks_running gauge\n");
        sb.append("synctask_tasks_running ").append(runningTasks).append("\n\n");

        // 失败任务数
        sb.append("# HELP synctask_tasks_failed Number of failed tasks\n");
        sb.append("# TYPE synctask_tasks_failed gauge\n");
        sb.append("synctask_tasks_failed ").append(failedTasks).append("\n\n");

        // 已完成任务数
        sb.append("# HELP synctask_tasks_completed Number of completed tasks\n");
        sb.append("# TYPE synctask_tasks_completed counter\n");
        sb.append("synctask_tasks_completed ").append(completedTasks).append("\n\n");

        // 各任务RPO/RTO
        sb.append("# HELP synctask_rpo_ms Recovery Point Objective in milliseconds\n");
        sb.append("# TYPE synctask_rpo_ms gauge\n");
        for (Workflow wf : allWorkflows) {
            if (Boolean.TRUE.equals(wf.getIsDeleted())) continue;
            if (wf.getRpoMs() != null) {
                sb.append("synctask_rpo_ms{task_id=\"").append(wf.getId())
                  .append("\",task_name=\"").append(escape(wf.getName())).append("\"} ")
                  .append(wf.getRpoMs()).append("\n");
            }
        }
        sb.append("\n");

        sb.append("# HELP synctask_rto_ms Recovery Time Objective in milliseconds\n");
        sb.append("# TYPE synctask_rto_ms gauge\n");
        for (Workflow wf : allWorkflows) {
            if (Boolean.TRUE.equals(wf.getIsDeleted())) continue;
            if (wf.getRtoMs() != null) {
                sb.append("synctask_rto_ms{task_id=\"").append(wf.getId())
                  .append("\",task_name=\"").append(escape(wf.getName())).append("\"} ")
                  .append(wf.getRtoMs()).append("\n");
            }
        }
        sb.append("\n");

        // 任务进度
        sb.append("# HELP synctask_task_progress Task progress percentage\n");
        sb.append("# TYPE synctask_task_progress gauge\n");
        for (Workflow wf : allWorkflows) {
            if (Boolean.TRUE.equals(wf.getIsDeleted())) continue;
            if (wf.getProgress() != null) {
                sb.append("synctask_task_progress{task_id=\"").append(wf.getId())
                  .append("\",task_name=\"").append(escape(wf.getName())).append("\"} ")
                  .append(wf.getProgress()).append("\n");
            }
        }
        sb.append("\n");

        // 任务状态分布
        sb.append("# HELP synctask_task_status Task status (1=active)\n");
        sb.append("# TYPE synctask_task_status gauge\n");
        for (Workflow wf : allWorkflows) {
            if (Boolean.TRUE.equals(wf.getIsDeleted())) continue;
            sb.append("synctask_task_status{task_id=\"").append(wf.getId())
              .append("\",task_name=\"").append(escape(wf.getName()))
              .append("\",status=\"").append(wf.getStatus().name()).append("\"} 1\n");
        }

        return sb.toString();
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
