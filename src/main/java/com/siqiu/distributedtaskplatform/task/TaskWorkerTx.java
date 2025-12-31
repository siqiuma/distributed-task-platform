package com.siqiu.distributedtaskplatform.task;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
public class TaskWorkerTx {
    private final TaskRepository repository;

    public TaskWorkerTx(TaskRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public TaskSnapshot claimAndGetSnapshot(Long id) {
        Instant now = Instant.now();
        int updated = repository.claim(id, now);
        if (updated != 1) return null;

        return repository.findSnapshotById(id);
    }

//readOnly = true It declares intent, prevents accidental updates by disabling dirty checking in JPA,
// and improves performance for read-only operations.
    @Transactional(readOnly = true)
    public String getPayload(Long id) {
        return repository.findById(id).map(Task::getPayload).orElse(null);
    }

    @Transactional
    public TaskSnapshot markSucceeded(Long id) {
        Task task = repository.findById(id).orElseThrow();
        task.markSucceeded();
        return toSnapshot(task);
    }

    @Transactional
    public TaskSnapshot markFailed(Long id, Exception e) {
        Task task = repository.findById(id).orElseThrow();
        // backoff based on *next* attemptCount after increment
        int nextAttempt = task.getAttemptCount() + 1;
        Duration backoff = backoffForAttempt(nextAttempt);

        task.markFailed(e.getMessage(), backoff);
        return toSnapshot(task);
    }

    private TaskSnapshot toSnapshot(Task t) {
        return new TaskSnapshot(
                t.getId(),
                t.getType(),
                t.getStatus(),
                t.getAttemptCount(),
                t.getMaxAttempts(),
                t.getNextRunAt(),
                t.getLastError()
        );
    }
//We use capped exponential backoff to prevent retry storms
// and reduce pressure on failing dependencies.
    private Duration backoffForAttempt(int attemptCount) {
        // attemptCount starts at 1 after first failure
        long seconds = (long) Math.pow(2, Math.min(attemptCount, 6)); // 2s,4s,8s,... capped
        return Duration.ofSeconds(seconds);
    }

}

