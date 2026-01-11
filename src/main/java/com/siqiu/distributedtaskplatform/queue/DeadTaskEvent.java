package com.siqiu.distributedtaskplatform.queue;

import java.time.Instant;

public record DeadTaskEvent(
        long taskId,
        String workerId,
        int attemptCount,
        int maxAttempts,
        String error,
        Instant deadAt
) {}
