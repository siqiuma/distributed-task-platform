package com.siqiu.distributedtaskplatform.task;

public enum TaskStatus {
    PENDING,
    ENQUEUED,
    PROCESSING,
    SUCCEEDED,
    FAILED, // retryable
    DEAD,  // terminal failure (no retries)
    CANCELED
}