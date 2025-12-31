package com.siqiu.distributedtaskplatform.task;

import java.time.Instant;

public record TaskSnapshot(
        Long id,
        String type,
        TaskStatus status,
        int attemptCount,
        int maxAttempts,
        Instant nextRunAt,
        String lastError
) {}
