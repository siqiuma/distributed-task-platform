package com.siqiu.distributedtaskplatform.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class TaskMetrics {

    private final Timer scheduleLagTimer; // histogram-backed timer
    private final Counter sqsMessagesReceived;
    private final Counter sqsMessagesDeleted;
    private final Counter sqsClaimFailed;

    private final Counter tasksProcessed;
    private final Counter tasksSucceeded;
    private final Counter tasksFailed;

    public TaskMetrics(MeterRegistry registry) {
        this.scheduleLagTimer = Timer.builder("dtp_task_schedule_lag_seconds")
                .description("Lag between scheduled_for and actual processing start")
                .publishPercentileHistogram(true)   // <-- key: creates _bucket series
                .serviceLevelObjectives( // optional but helpful for resume targets
                        Duration.ofMillis(10),
                        Duration.ofMillis(50),
                        Duration.ofMillis(100),
                        Duration.ofMillis(200),
                        Duration.ofMillis(500),
                        Duration.ofSeconds(1)
                )
                .register(registry);

        this.sqsMessagesReceived = Counter.builder("dtp_sqs_messages_received_total")
                .description("Number of SQS messages received by worker")
                .register(registry);

        this.sqsMessagesDeleted = Counter.builder("dtp_sqs_messages_deleted_total")
                .description("Number of SQS messages deleted by worker")
                .register(registry);

        this.sqsClaimFailed = Counter.builder("dtp_task_claim_failed_total")
                .description("Number of tasks that failed DB claim (another worker got it, or already processed)")
                .register(registry);

        this.tasksProcessed = Counter.builder("dtp_tasks_processed_total")
                .description("Number of tasks processed by worker")
                .register(registry);

        this.tasksSucceeded = Counter.builder("dtp_tasks_succeeded_total")
                .description("Number of tasks succeeded")
                .register(registry);

        this.tasksFailed = Counter.builder("dtp_tasks_failed_total")
                .description("Number of tasks failed")
                .register(registry);
    }

    public void observeScheduleLag(Duration lag) {
        // Timer in seconds; Micrometer handles conversion.
        scheduleLagTimer.record(lag);
    }

    public void incReceived() { sqsMessagesReceived.increment(); }
    public void incDeleted() { sqsMessagesDeleted.increment(); }
    public void incClaimFailed() { sqsClaimFailed.increment(); }

    public void incTasksProcessed() { tasksProcessed.increment(); }
    public void incTasksSucceeded() { tasksSucceeded.increment(); }
    public void incTasksFailed() { tasksFailed.increment(); }
}
