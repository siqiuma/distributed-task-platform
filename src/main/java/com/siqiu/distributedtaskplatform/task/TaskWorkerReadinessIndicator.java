package com.siqiu.distributedtaskplatform.task;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("worker")
public class TaskWorkerReadinessIndicator implements HealthIndicator {

    private final TaskRepository repository;

    public TaskWorkerReadinessIndicator(TaskRepository repository) {
        this.repository = repository;
    }

    @Override
    public Health health() {
        try {
            // lightweight sanity check
            repository.count();
            return Health.up()
                    .withDetail("worker", "ready")
                    .build();
        } catch (Exception e) {
            return Health.down(e)
                    .withDetail("worker", "not_ready")
                    .build();
        }
    }
}
