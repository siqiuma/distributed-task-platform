package com.siqiu.distributedtaskplatform.task;

import jakarta.persistence.*;
import java.time.Instant;

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
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
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

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public void cancel() {
        if (this.status != TaskStatus.PENDING) {
            throw new InvalidTaskStateException(
                    "Cannot cancel task in state " + status
            );
        }
        this.status = TaskStatus.CANCELED;
    }

    public void markProcessing() {
        if (this.status != TaskStatus.PENDING) {
            throw new InvalidTaskStateException(
                    "Cannot process task in state " + status
            );
        }
        this.status = TaskStatus.PROCESSING;
    }

    public void markSucceeded() {
        if (this.status != TaskStatus.PROCESSING) {
            throw new InvalidTaskStateException(
                    "Cannot succeed task in state " + status
            );
        }
        this.status = TaskStatus.SUCCEEDED;
    }

    public void markFailed() {
        if (this.status != TaskStatus.PROCESSING) {
            throw new InvalidTaskStateException(
                    "Cannot fail task in state " + status
            );
        }
        this.status = TaskStatus.FAILED;
    }

}
