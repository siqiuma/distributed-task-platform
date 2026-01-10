package com.siqiu.distributedtaskplatform.queue;

public interface TaskQueueClient {
    void enqueueTask(String taskId, long scheduledForEpochMs);
}