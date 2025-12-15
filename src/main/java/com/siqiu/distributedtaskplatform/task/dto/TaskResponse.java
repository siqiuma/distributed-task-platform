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

    public static TaskResponse from(Task task) {
        TaskResponse r = new TaskResponse();
        r.id = task.getId();
        r.type = task.getType();
        r.payload = task.getPayload();
        r.status = task.getStatus();
        r.createdAt = task.getCreatedAt();
        r.updatedAt = task.getUpdatedAt();
        return r;
    }

    public Long getId() { return id; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public TaskStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
