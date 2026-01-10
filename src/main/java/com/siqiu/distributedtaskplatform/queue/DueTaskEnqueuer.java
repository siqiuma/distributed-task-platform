package com.siqiu.distributedtaskplatform.queue;

import com.siqiu.distributedtaskplatform.repo.TaskEnqueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DueTaskEnqueuer {

    private static final Logger log = LoggerFactory.getLogger(DueTaskEnqueuer.class);

    private final TaskEnqueueRepository enqueueRepo;
    private final TaskQueueClient queueClient;

    private final String queueMode;
    private final int batchSize;
    private final long enqueueLockSeconds;

    public DueTaskEnqueuer(
            TaskEnqueueRepository enqueueRepo,
            TaskQueueClient queueClient,
            @Value("${dtp.queue.mode:sqs}") String queueMode,
            @Value("${dtp.enqueuer.batch-size:50}") int batchSize,
            @Value("${dtp.enqueuer.enqueue-lock-seconds:30}") long enqueueLockSeconds
    ) {
        this.enqueueRepo = enqueueRepo;
        this.queueClient = queueClient;
        this.queueMode = queueMode;
        this.batchSize = batchSize;
        this.enqueueLockSeconds = enqueueLockSeconds;
    }

    /**
     * Runs frequently; only does work in sqs mode.
     * You can tune delay; start with 1s.
     */
    @Scheduled(fixedDelayString = "${dtp.enqueuer.fixed-delay-ms:1000}")
    public void tick() {
        if (!"sqs".equalsIgnoreCase(queueMode)) return;

        var due = enqueueRepo.claimDueForEnqueue(batchSize, enqueueLockSeconds);
        if (due.isEmpty()) return;

        for (var task : due) {
            long scheduledEpochMs = task.scheduledFor() == null ? 0L : task.scheduledFor().toEpochMilli();
            try {
                queueClient.enqueueTask(String.valueOf(task.id()), scheduledEpochMs);
            } catch (Exception e) {
                // If enqueue failed, release lock so next tick retries quickly.
                log.error("Failed to enqueue taskId={}. Releasing enqueue lock.", task.id(), e);
                enqueueRepo.releaseEnqueueLock(task.id());
            }
        }
    }
}
