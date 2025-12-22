package com.siqiu.distributedtaskplatform.task;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

    private final TaskRepository repository;

    public TaskWorker(TaskRepository repository) {
        this.repository = repository;
    }

    /*
     * Background worker that periodically processes pending tasks.
     * This simulates async task execution without queues.
     */
    //Run this method automatically, in the background, over and over.
    //There is no HTTP request, no user trigger, no controller involved.
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processPendingTasks() {
        List<Task> tasks =
                repository.findTop5ByStatusOrderByCreatedAt(TaskStatus.PENDING);

        if (tasks.isEmpty()) {
            log.debug("No pending tasks found");
            return;
        }

        for (Task task : tasks) {
            try {
                log.info("Processing task id={}", task.getId());

                task.markProcessing();

                // Simulate work
                Thread.sleep(1000);
                // Simulate work
                if (task.getPayload().contains("fail")) {
                    throw new RuntimeException("Simulated failure");
                }

                task.markSucceeded();
                log.info("Task id={} succeeded", task.getId());
            } catch (Exception e) {
                task.markFailed();
                log.error("Task id={} failed", task.getId(), e);
            }
        }
    }
}
