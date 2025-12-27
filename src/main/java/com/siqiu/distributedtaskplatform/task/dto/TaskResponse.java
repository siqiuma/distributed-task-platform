package com.siqiu.distributedtaskplatform.task.dto;

import com.siqiu.distributedtaskplatform.task.Task;
import com.siqiu.distributedtaskplatform.task.TaskStatus;

import java.time.Instant;

public class TaskResponse {

    private Long id;
    private String type;
    private String payload;
    private TaskStatus status;
    private Instant createdAt;
    private Instant updatedAt;

    // Retry / execution fields
    public Integer attemptCount;
    public Integer maxAttempts;
    public Instant nextRunAt;
    public String lastError;

    public String retryState;      // human-friendly summary

    public static TaskResponse from(Task task) {
        TaskResponse r = new TaskResponse();
        r.id = task.getId();
        r.type = task.getType();
        r.payload = task.getPayload();
        r.status = task.getStatus();
        r.createdAt = task.getCreatedAt();
        r.updatedAt = task.getUpdatedAt();

        r.attemptCount = task.getAttemptCount();
        r.maxAttempts = task.getMaxAttempts();
        r.nextRunAt = task.getNextRunAt();
        r.lastError = task.getLastError();

        // Computed
        r.retryState = computeRetryState(task, Instant.now());

        return r;
    }

    private static String computeRetryState(Task t, Instant now) {
        if (t.getStatus() == TaskStatus.SUCCEEDED) return "COMPLETED";
        if (t.getStatus() == TaskStatus.CANCELED) return "CANCELED";
        if (t.getStatus() == TaskStatus.DEAD) return "DEAD (no retries)";
        if (t.getStatus() == TaskStatus.PROCESSING) return "IN_PROGRESS";

        if (t.getStatus() == TaskStatus.PENDING) return "READY_TO_PROCESS";

        if (t.getStatus() == TaskStatus.FAILED) {
            // FAILED is retryable by definition in our model
            if (t.getAttemptCount() >= t.getMaxAttempts()) {
                return "INCONSISTENT (FAILED but attempts >= max; should be DEAD)";
            }
            if (t.getNextRunAt() == null) {
                return "INCONSISTENT (FAILED but nextRunAt is null)";
            }
            return !t.getNextRunAt().isAfter(now) ? "READY_TO_RETRY" : "WAITING_FOR_RETRY";
        }
        return "UNKNOWN";
    }

}
