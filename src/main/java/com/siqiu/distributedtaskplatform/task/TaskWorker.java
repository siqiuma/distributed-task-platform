package com.siqiu.distributedtaskplatform.task;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@ConditionalOnProperty(name = "task.worker.enabled", havingValue = "true", matchIfMissing = true)
public class TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

    private final TaskRepository repository;
    private final TaskWorkerTx tx; // helper transactional bean


    public TaskWorker(TaskRepository repository, TaskWorkerTx tx) {
        this.repository = repository;
        this.tx = tx;
    }

    /*
     * Background worker that periodically processes pending tasks.
     * This simulates async task execution without queues.
     */
    //Run this method automatically, in the background, over and over.
    //There is no HTTP request, no user trigger, no controller involved.

    @Scheduled(fixedDelay = 5000)
    public void processPendingTasks() {
        Instant now = Instant.now();

        List<Task> tasks = repository.findTop5Eligible(
                now,
                org.springframework.data.domain.PageRequest.of(0, 5)
        );

        for (Task t : tasks) {
            Long id = t.getId();

            // 1) claim fast (short tx)
            boolean claimed = tx.claim(id);
            if (!claimed) {
                log.info("Task id={} already claimed/not eligible, skipping", id);
                continue;
            }

            // 2) do work OUTSIDE tx
            try {
                // Re-read a fresh copy after claim (short tx)
                String payload = tx.getPayload(id);

                log.info("Processing task id={} payload={}", id, payload);
                Thread.sleep(3000); // simulate work

                if (payload != null && payload.contains("fail")) {
                    log.info("Task id={} failed (simulated)", id);
                    throw new RuntimeException("Simulated failure");
                } else {
                    tx.markSucceeded(id);
                    log.info("Task id={} succeeded", id);
                }
            } catch (org.springframework.orm.ObjectOptimisticLockingFailureException e) {
                log.warn("Optimistic lock conflict for task id={}, skipping", id);
            } catch (Exception e) {
                log.error("Unexpected error while processing task id={}", id, e);
                try {
                    tx.markFailed(id,e);
                    log.warn("Task id={} failed, scheduled retry (if attempts remain)", id, e);
                } catch (org.springframework.orm.ObjectOptimisticLockingFailureException ignored) {
                    log.warn("Optimistic lock while marking failed for task id={}, skipping", id);
                }
            }
        }
    }
}
