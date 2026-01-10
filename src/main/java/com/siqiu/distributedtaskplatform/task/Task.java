package com.siqiu.distributedtaskplatform.task;

import jakarta.persistence.*;
import java.time.Instant;
import java.time.Duration;


@Entity
@Table(name = "tasks")
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String type;

//    @Lob
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TaskStatus status = TaskStatus.PENDING;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    private Instant updatedAt;

    @Column(nullable = false)
    private int attemptCount = 0;

    @Column(nullable = false)
    private int maxAttempts = 3;

    private Instant nextRunAt;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    // --- SQS / distributed execution fields ---
    @Column(name = "scheduled_for")
    private Instant scheduledFor;

    @Column(name = "processing_started_at")
    private Instant processingStartedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "worker_id")
    private String workerId;
/*JPA now does this behind the scenes:
* Reads task with version = 1
* On update, executes:
UPDATE task
SET status=?, version=version+1
WHERE id=? AND version=1
* If 0 rows updated → someone else modified it → conflict detected
*This turns silent corruption into a detectable failure.
* Conflicts will throw OptimisticLockException*/
    @Version
    private Long version;

    protected Task() {}

    public Task(String type, String payload) {
        this.type = type;
        this.payload = payload;
        this.status = TaskStatus.PENDING;
    }

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;

        if (this.scheduledFor == null) this.scheduledFor = now;
    }
    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public Long getId() { return id; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public TaskStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public int getAttemptCount() { return attemptCount; }
    public int getMaxAttempts() { return maxAttempts; }
    public Instant getNextRunAt() { return nextRunAt; }
    public String getLastError() { return lastError; }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }
    public Instant getScheduledFor() {
        return scheduledFor;
    }

    public Instant getProcessingStartedAt() {
        return processingStartedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setScheduledFor(Instant scheduledFor) {
        this.scheduledFor = scheduledFor;
    }
    public void cancel() {
        if (this.status != TaskStatus.PENDING) {
            throw new InvalidTaskStateException(
                    "Cannot cancel task in state " + status
            );
        }
        this.status = TaskStatus.CANCELED;
    }

    /** Call this right after successfully sending the SQS message */
    public void markEnqueued(Instant scheduledFor) {
        if (this.status != TaskStatus.PENDING && this.status != TaskStatus.FAILED) {
            throw new InvalidTaskStateException("Cannot enqueue task in state " + status);
        }
        if (this.attemptCount >= this.maxAttempts) {
            throw new InvalidTaskStateException("Cannot enqueue task; max attempts reached");
        }
        this.status = TaskStatus.ENQUEUED;   // add this enum
        this.scheduledFor = scheduledFor;
        this.workerId = null;
        this.processingStartedAt = null;
    }

    public void markProcessing(String workerId) {
        if (this.status != TaskStatus.ENQUEUED && this.status != TaskStatus.FAILED) {
            throw new InvalidTaskStateException(
                    "Cannot process task in state " + status
            );
        }
        if (this.attemptCount >= this.maxAttempts) {
            throw new InvalidTaskStateException("Max retry attempts exceeded");
        }

        this.attemptCount++;       // increment ON ATTEMPT
        // New attempt is starting: clear old error + clear schedule
        this.lastError = null;
        this.nextRunAt = null;

        this.workerId = workerId;
        this.processingStartedAt = Instant.now();

        this.status = TaskStatus.PROCESSING;
    }

    public void markSucceeded() {
        if (this.status != TaskStatus.PROCESSING) {
            throw new InvalidTaskStateException(
                    "Cannot succeed task in state " + status
            );
        }
        this.status = TaskStatus.SUCCEEDED;
        this.completedAt = Instant.now();
    }

    public void markFailed(String errorMessage, Duration backoff) {
        if (this.status != TaskStatus.PROCESSING) {
            throw new InvalidTaskStateException(
                    "Cannot fail task in state " + status
            );
        }

        // Store the error so we can inspect/debug later
        this.lastError = (errorMessage == null || errorMessage.isBlank())
                ? "Unknown error"
                : errorMessage;

        // Decide whether it should retry
        if (this.attemptCount < this.maxAttempts) {
            // Keep FAILED status but schedule for retry
            this.status = TaskStatus.FAILED;
            this.nextRunAt = Instant.now().plus(backoff);
        } else {
            // No more retries; terminal failure
            this.status = TaskStatus.DEAD;
            this.nextRunAt = null;
        }
        this.completedAt = Instant.now(); // optional: record terminal time of this attempt
    }

}
