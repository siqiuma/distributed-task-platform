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

    //The update query returns the number of rows affected.
    // If it’s 1, this worker atomically claimed the task;
    // if it’s 0, another worker already did.
    @Transactional
    public boolean claim(Long id) {
        return repository.claim(id, Instant.now()) == 1;
    }
//readOnly = true It declares intent, prevents accidental updates by disabling dirty checking in JPA,
// and improves performance for read-only operations.
    @Transactional(readOnly = true)
    public String getPayload(Long id) {
        return repository.findById(id).map(Task::getPayload).orElse(null);
    }

    @Transactional
    public void markSucceeded(Long id) {
        Task task = repository.findById(id).orElseThrow();
        task.markSucceeded();
    }

    @Transactional
    public void markFailed(Long id, Exception e) {
        Task task = repository.findById(id).orElseThrow();
        // backoff based on *next* attemptCount after increment
        int nextAttempt = task.getAttemptCount() + 1;
        Duration backoff = backoffForAttempt(nextAttempt);

        task.markFailed(e.getMessage(), backoff);
    }
//We use capped exponential backoff to prevent retry storms
// and reduce pressure on failing dependencies.
    private Duration backoffForAttempt(int attemptCount) {
        // attemptCount starts at 1 after first failure
        long seconds = (long) Math.pow(2, Math.min(attemptCount, 6)); // 2s,4s,8s,... capped
        return Duration.ofSeconds(seconds);
    }

}

