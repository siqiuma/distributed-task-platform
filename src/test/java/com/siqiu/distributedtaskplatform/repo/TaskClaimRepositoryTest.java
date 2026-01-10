package com.siqiu.distributedtaskplatform.repo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class TaskClaimRepositoryTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired TaskClaimRepository claimRepo;

    private long taskId;
    private final String workerA = "worker-A";
    private final String workerB = "worker-B";

    @BeforeEach
    void setup() {
        // create a task row directly (simple + deterministic)
        // assumes your Flyway created tasks table with these cols and NOT NULL defaults for attempt_count/max_attempts.
        jdbc.update("""
            INSERT INTO tasks(type, payload, status, created_at, updated_at, attempt_count, max_attempts, scheduled_for)
            VALUES ('email', 'hello', 'ENQUEUED', now(), now(), 0, 3, now() - interval '1 second')
        """);

        taskId = jdbc.queryForObject("SELECT max(id) FROM tasks", Long.class);
        assertThat(taskId).isGreaterThan(0);
    }

    @Test
    void claim_then_markSucceeded_updates_status_and_completedAt() {
        boolean claimed = claimRepo.claimEnqueuedTask(taskId, workerA);
        assertThat(claimed).isTrue();

        boolean ok = claimRepo.markSucceeded(taskId, workerA);
        assertThat(ok).isTrue();

        var row = jdbc.queryForMap("""
            SELECT status, worker_id, processing_started_at, completed_at, attempt_count, last_error
            FROM tasks WHERE id = ?
        """, taskId);

        assertThat(row.get("status")).isEqualTo("SUCCEEDED");
        assertThat(row.get("worker_id")).isEqualTo(workerA); // we don't clear worker on success; optional either way
        assertThat(row.get("completed_at")).isNotNull();
        assertThat(((Number) row.get("attempt_count")).intValue()).isEqualTo(1);
        assertThat(row.get("last_error")).isNull();
    }

    @Test
    void markSucceeded_fails_if_workerId_does_not_match() {
        boolean claimed = claimRepo.claimEnqueuedTask(taskId, workerA);
        assertThat(claimed).isTrue();

        boolean ok = claimRepo.markSucceeded(taskId, workerB);
        assertThat(ok).isFalse();

        String status = jdbc.queryForObject("SELECT status FROM tasks WHERE id = ?", String.class, taskId);
        assertThat(status).isEqualTo("PROCESSING");
    }

    @Test
    void claim_then_markFailed_reschedules_and_returns_to_ENQUEUED() {
        boolean claimed = claimRepo.claimEnqueuedTask(taskId, workerA);
        assertThat(claimed).isTrue();

        Instant beforeFail = Instant.now();

        boolean ok = claimRepo.markFailedAndReschedule(taskId, workerA, "boom", 5);
        assertThat(ok).isTrue();

        var row = jdbc.queryForMap("""
            SELECT status, worker_id, processing_started_at, scheduled_for, completed_at, attempt_count, last_error
            FROM tasks WHERE id = ?
        """, taskId);

        assertThat(row.get("status")).isEqualTo("ENQUEUED");
        assertThat(row.get("worker_id")).isNull();
        assertThat(row.get("processing_started_at")).isNull();
        assertThat(row.get("completed_at")).isNull();
        assertThat(row.get("last_error")).isEqualTo("boom");
        assertThat(((Number) row.get("attempt_count")).intValue()).isEqualTo(1);

        Timestamp scheduledForTs = (Timestamp) row.get("scheduled_for");
        assertThat(scheduledForTs).isNotNull();
        Instant scheduledFor = scheduledForTs.toInstant();

        // should be >= now-ish (in future). Allow small clock skew.
        assertThat(scheduledFor).isAfter(beforeFail.minusSeconds(1));
    }

    @Test
    void markFailed_transitions_to_DEAD_when_attempts_exhausted() {
        // Put it at 2 attempts already (max=3), then claim increments to 3.
        jdbc.update("UPDATE tasks SET attempt_count = 2, max_attempts = 3 WHERE id = ?", taskId);

        boolean claimed = claimRepo.claimEnqueuedTask(taskId, workerA);
        assertThat(claimed).isTrue();

        boolean ok = claimRepo.markFailedAndReschedule(taskId, workerA, "final boom", 5);
        assertThat(ok).isTrue();

        var row = jdbc.queryForMap("""
            SELECT status, scheduled_for, completed_at, attempt_count, last_error, worker_id, processing_started_at
            FROM tasks WHERE id = ?
        """, taskId);

        assertThat(row.get("status")).isEqualTo("DEAD");
        assertThat(row.get("scheduled_for")).isNull();   // not retrying
        assertThat(row.get("completed_at")).isNotNull();  // set by query
        assertThat(((Number) row.get("attempt_count")).intValue()).isEqualTo(3);
        assertThat(row.get("last_error")).isEqualTo("final boom");
        assertThat(row.get("worker_id")).isNull();
        assertThat(row.get("processing_started_at")).isNull();
    }
}
