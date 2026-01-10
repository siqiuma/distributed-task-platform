package com.siqiu.distributedtaskplatform.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.time.Instant;
import java.util.Optional;
import java.sql.Timestamp;

@Repository
public class TaskClaimRepository {

    private final JdbcTemplate jdbc;

    public TaskClaimRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }
    // =========================
    // SQS MODE (queue) queries
    // =========================

    /**
     * Claim an ENQUEUED task (idempotency gate for at-least-once SQS delivery).
     * Returns true if we successfully claimed the task.
     */
    public boolean claimEnqueuedTask(long taskId, String workerId) {
        String sql = """
            UPDATE tasks
            SET status = 'PROCESSING',
                processing_started_at = now(),
                worker_id = ?,
                attempt_count = attempt_count + 1,
                last_error = NULL,
                next_run_at = NULL,
                updated_at = now()
            WHERE id = ?
              AND status = 'ENQUEUED'
              AND (scheduled_for IS NULL OR scheduled_for <= now())
              AND attempt_count < max_attempts
            """;

        int updated = jdbc.update(sql, workerId, taskId);
        return updated == 1;
    }

    /**
     * Mark task as SUCCEEDED (only the worker that claimed it can succeed it)
     */
    public boolean markSucceeded(long taskId, String workerId) {
        String sql = """
            UPDATE tasks
               SET status = 'SUCCEEDED',
                   completed_at = now(),
                   updated_at = now(),
                   last_error = NULL
             WHERE id = ?
               AND status = 'PROCESSING'
               AND worker_id = ?
            """;
        return jdbc.update(sql, taskId, workerId) == 1;
    }

    /**
     * Mark task failed. If attempts exhausted => DEAD, else reschedule by setting scheduled_for in the future.
     *
     * IMPORTANT: attempt_count is already incremented at claim time (claimEnqueuedTask),
     * so we do NOT increment it again here.
     *
     * backoffSeconds: how long to wait before retry
     */
    public boolean markFailedAndReschedule(long taskId, String workerId, String errorMsg, long backoffSeconds) {
        String sql = """
            UPDATE tasks
               SET status =
                     CASE
                       WHEN attempt_count >= max_attempts THEN 'DEAD'
                       ELSE 'ENQUEUED'
                     END,
                   last_error = ?,
                   scheduled_for =
                     CASE
                       WHEN attempt_count >= max_attempts THEN NULL
                       ELSE now() + (? * interval '1 second')
                     END,
                   completed_at =
                     CASE
                       WHEN attempt_count >= max_attempts THEN now()
                       ELSE NULL
                     END,
                   updated_at = now(),
                   processing_started_at = NULL,
                   worker_id = NULL,
                   next_run_at = NULL
             WHERE id = ?
               AND status = 'PROCESSING'
               AND worker_id = ?
            """;
        return jdbc.update(sql, errorMsg, backoffSeconds, taskId, workerId) == 1;
    }

    public Optional<Instant> getScheduledFor(long taskId) {
        String sql = "SELECT scheduled_for FROM tasks WHERE id = ?";
        return jdbc.query(sql, rs -> {
            if (!rs.next()) return Optional.empty();
            Timestamp ts = rs.getTimestamp(1);
            return ts == null ? Optional.empty() : Optional.of(ts.toInstant());
        }, taskId);
    }
}

