package com.siqiu.distributedtaskplatform.task;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@ConditionalOnProperty(name = "dtp.queue.mode", havingValue = "db", matchIfMissing = true)
public class TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

    private final TaskRepository repository;
    private final TaskWorkerTx tx; // helper transactional bean
    private final MeterRegistry meterRegistry;

    // ---- metrics (Prometheus-friendly names) ----
    private final Counter tasksProcessed;
    private final Counter tasksSucceeded;
    private final Counter tasksFailed;
    private final Counter claimConflicts;
    private final Counter markFailedErrors;
    private final Timer processingTimer;

    public TaskWorker(
            TaskRepository repository,
            TaskWorkerTx tx,
            MeterRegistry meterRegistry
    ) {
        this.repository = repository;
        this.tx = tx;
        this.meterRegistry = meterRegistry;

        this.tasksProcessed = meterRegistry.counter("dtp_tasks_processed_total");
        this.tasksSucceeded = meterRegistry.counter("dtp_tasks_succeeded_total");
        this.tasksFailed = meterRegistry.counter("dtp_tasks_failed_total");
        this.claimConflicts = meterRegistry.counter("dtp_tasks_claim_conflicts_total");
        this.markFailedErrors = meterRegistry.counter("dtp_tasks_mark_failed_errors_total");

        this.processingTimer = Timer.builder("dtp_task_processing_duration_seconds")
                .description("Time spent processing a task")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /*
     * Background worker that periodically processes pending tasks.
     * This simulates async task execution without queues.
     */
    //Run this method automatically, in the background, over and over.
    //There is no HTTP request, no user trigger, no controller involved.

    @Scheduled(fixedDelay = 5000)
    public void processPendingTasks() {
        runOnce();
    }

    // package-private so tests in same package can call it
    void runOnce() {
        Instant now = Instant.now();

        List<Task> tasks = repository.findTop5Eligible(
                now,
                org.springframework.data.domain.PageRequest.of(0, 5)
        );

        for (Task t : tasks) {
            Long id = t.getId();

            TaskSnapshot claimed = tx.claimAndGetSnapshot(id);
            if (claimed == null) {
                claimConflicts.increment();
                log.info("task_skipped id={} reason=already_claimed_or_not_due", id);
                continue;
            }
            log.info("task_claimed id={} status={} attempt={} maxAttempts={}",
                    claimed.id(), claimed.status(), claimed.attemptCount(), claimed.maxAttempts());

            Instant startedAt = Instant.now();
            Timer.Sample sample = Timer.start(meterRegistry);

            try {
                // Re-read a fresh copy after claim (short tx)
                String payload = tx.getPayload(id);

                log.info("task_processing_started id={} type={} payloadLen={}",
                        id, claimed.type(), payload == null ? 0 : payload.length());
                Thread.sleep(3000); // simulate work

                if (payload != null && payload.contains("fail")) {
                    log.warn("task_processing_failed_expected id={} errorClass={} msg={}",
                            id, "TaskProcessingException", "Simulated failure");
                    throw new TaskProcessingException("Simulated failure");
                }
                TaskSnapshot after = tx.markSucceeded(id);
                tasksSucceeded.increment();

                log.info("task_succeeded id={} status={} attempt={} durationMs={}",
                        after.id(), after.status(), after.attemptCount(),
                        java.time.Duration.between(startedAt, Instant.now()).toMillis());

            } catch (org.springframework.orm.ObjectOptimisticLockingFailureException e) {
                claimConflicts.increment();
                log.warn("task_conflict_optimistic_lock id={} phase=processing durationMs={}",
                        id, java.time.Duration.between(startedAt, Instant.now()).toMillis());
            } catch (InterruptedException e) {
                //re-sets the interrupt flag so upper layers (Spring scheduler / thread pool)
                // can see that the thread was interrupted and can react correctly.
                Thread.currentThread().interrupt();
                log.warn("task_interrupted id={} durationMs={}",
                        id, java.time.Duration.between(startedAt, Instant.now()).toMillis());
                tryMarkFailed(id, e);
            } catch (Exception e) {
                boolean expected = (e instanceof TaskProcessingException);

                if (expected) {
                    log.warn("task_processing_failed_expected id={} errorClass={} msg={} durationMs={}",
                            id, e.getClass().getSimpleName(), e.getMessage(),
                            java.time.Duration.between(startedAt, Instant.now()).toMillis());
                } else {
                    log.error("task_processing_failed_unexpected id={} errorClass={} msg={} durationMs={}",
                            id, e.getClass().getSimpleName(), e.getMessage(),
                            java.time.Duration.between(startedAt, Instant.now()).toMillis(), e);
                }

                // ✅ important: always update retry state for any failure
                tryMarkFailed(id, e);
            } finally {
                tasksProcessed.increment();
                sample.stop(processingTimer);
            }

        }
    }

    private void tryMarkFailed(Long id, Exception e) {
        try {
            TaskSnapshot after = tx.markFailed(id, e);
            tasksFailed.increment();
            log.info("task_retry_state_updated id={} status={} attempt={} maxAttempts={} nextRunAt={} lastError={}",
                    after.id(), after.status(), after.attemptCount(), after.maxAttempts(),
                    after.nextRunAt(), after.lastError());
        } catch (ObjectOptimisticLockingFailureException ex) {
            claimConflicts.increment();
            log.warn("task_conflict_optimistic_lock id={} phase=markFailed", id);
        } catch (Exception ex) {
            markFailedErrors.increment();
            log.error("task_markFailed_failed id={} errorClass={} msg={}",
                    id, ex.getClass().getSimpleName(), ex.getMessage(), ex);
        }
    }
}
