package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.metrics.TaskMetrics;
import com.siqiu.distributedtaskplatform.queue.DeadLetterClient;
import com.siqiu.distributedtaskplatform.queue.DeadTaskEvent;
import com.siqiu.distributedtaskplatform.repo.TaskClaimRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

@ConditionalOnProperty(name = "dtp.queue.mode", havingValue = "sqs")
@Service
public class SqsWorkerLoop implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(SqsWorkerLoop.class);

    private final SqsClient sqs;
    private final String queueUrl;
    private final TaskClaimRepository claimRepo;
    private final TaskMetrics metrics;

    private final ExecutorService loop = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    private final String workerId = UUID.randomUUID().toString();

    private final boolean autoStart;

    private final DeadLetterClient dlq;

    private final TaskProcessor processor;

    public SqsWorkerLoop(
            SqsClient sqs,
            @Value("${dtp.sqs.queueName}") String queueName,
            TaskClaimRepository claimRepo,
            TaskMetrics metrics,
            @Value("${dtp.sqs.worker.autostart:true}") boolean autoStart,
            DeadLetterClient dlq,
            TaskProcessor processor
            ) {
        this.sqs = sqs;
        this.queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build()).queueUrl();
        this.claimRepo = claimRepo;
        this.metrics = metrics;
        this.autoStart = autoStart;
        this.dlq = dlq;
        this.processor = processor;

        // Start background loop only if enabled
        if (this.autoStart) {
            loop.submit(this::runLoop);
        }
    }

    private void runLoop() {
        log.info("SQS worker loop started. workerId={} queueUrl={}", workerId, queueUrl);

        while (running) {
            try {
                List<Message> messages = receiveMessages();
                for (Message msg : messages) {
                    metrics.incReceived();
                    processMessage(msg);
                }
            } catch (Exception e) {
                // Keep loop alive; don’t crash the app on transient issues
                log.error("SQS worker loop error. Will retry.", e);

                // small backoff to avoid hot-looping
                sleepQuietly(1000);
            }
        }

        log.info("SQS worker loop stopped. workerId={}", workerId);
    }

    private List<Message> receiveMessages() {
        ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(20)          // long poll
                .maxNumberOfMessages(5)       // batch
                .visibilityTimeout(30)        // seconds; tune later
                .build();

        ReceiveMessageResponse resp = sqs.receiveMessage(req);
        return resp.messages();
    }

    void processMessage(Message msg) {
        String body = msg.body(); // we expect taskId only
        long taskId;

        try {
            taskId = Long.parseLong(body.trim());
        } catch (Exception parseErr) {
            log.warn("Bad message body='{}'. Deleting message.", body);
            deleteMessage(msg);
            return;
        }

        // 1) Claim in DB (idempotency gate)
        boolean claimed = claimRepo.claimEnqueuedTask(taskId, workerId);
        if (!claimed) {
            metrics.incClaimFailed();
            log.info("Task not claimed (already claimed/processed or not due). Leaving message for retry. taskId={}", taskId);
            return;
        }
        // once claimed, we count this as "processed/started"
        metrics.incTasksProcessed();

        // 2) Observe schedule lag (for resume-grade metrics)
        claimRepo.getScheduledFor(taskId).ifPresent(scheduledFor -> {
            Duration lag = Duration.between(scheduledFor, Instant.now());
            if (!lag.isNegative()) {
                metrics.observeScheduleLag(lag);
            }
        });

        // Configure retry backoff
        long backoffSeconds = 30;

        // 3) Do the work + complete DB lifecycle
        try {
            processor.process(taskId);
            // Mark succeeded in DB
            boolean updated = claimRepo.markSucceeded(taskId, workerId);
            if (!updated) {
                // DB didn't accept update (lost lock, wrong worker_id/status, etc.)
                // Do NOT delete the message; allow retry/redelivery.
                log.warn("markSucceeded did not update row. Not deleting SQS message. taskId={} workerId={}", taskId, workerId);
                return;
            }
            metrics.incTasksSucceeded();

            // Delete only after DB succeeded
            deleteMessage(msg);

        } catch (Exception ex) {
            metrics.incTasksFailed();
            log.error("Task processing failed. taskId={}", taskId, ex);
            var outcome = claimRepo.markFailedAndRescheduleOutcome(
                    taskId,
                    workerId,
                    ex.getMessage(),
                    backoffSeconds
            );

            if (!outcome.updated()) {
                log.warn("markFailedAndRescheduleOutcome did not update row. Not deleting SQS message. taskId={} workerId={}",
                        taskId, workerId);
                return;
            }

            // If DEAD, publish a dead-task event (best effort)
            if (outcome.becameDead()) {
                metrics.incTasksDeadLettered();
                try {
                    dlq.publishDeadTask(new DeadTaskEvent(
                            taskId,
                            workerId,
                            outcome.attemptCount(),
                            outcome.maxAttempts(),
                            ex.getMessage(),
                            Instant.now()
                    ));
                } catch (Exception dlqEx) {
                    // IMPORTANT: still delete original message to avoid infinite redelivery
                    log.error("Failed to publish to DLQ. Deleting original SQS message anyway to avoid poison-loop. taskId={}", taskId, dlqEx);
                    // (optional) add a metric like metrics.incDlqPublishFailed();
                }
            }
            // Implementation Before adding DLQ
            // Record failure + reschedule in DB
//            boolean updated = claimRepo.markFailedAndReschedule(
//                    taskId,
//                    workerId,
//                    ex.getMessage(),
//                    backoffSeconds
//            );
//
//            if (!updated) {
//                // If DB update didn't happen, do NOT delete message.
//                // Otherwise you'd lose the task without recording failure/reschedule.
//                log.warn("markFailedAndReschedule did not update row. Not deleting SQS message. taskId={} workerId={}", taskId, workerId);
//                return;
//            }

            // Delete only after DB succeeded
            deleteMessage(msg);

            // Do NOT rethrow; keep worker loop healthy
        }
    }

    private void deleteMessage(Message msg) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(msg.receiptHandle())
                .build());
        metrics.incDeleted();
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // important: preserve interrupt signal
        }
    }

    @Override
    public void destroy() {
        running = false;
        loop.shutdownNow();
    }

    // for integration testing
    void runOnce() {
        List<Message> messages = receiveMessages();
        for (Message msg : messages) {
            metrics.incReceived();
            processMessage(msg);
        }
    }
}
