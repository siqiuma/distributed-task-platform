package com.siqiu.distributedtaskplatform.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class TaskEnqueueRepository {

    private final JdbcTemplate jdbc;

    public TaskEnqueueRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record TaskToEnqueue(long id, Instant scheduledFor) {}

    /**
     * Atomically selects up to {limit} ENQUEUED tasks that are due (scheduled_for <= now),
     * and "locks" them for enqueue by setting next_run_at = now() + lockSeconds.
     *
     * We re-use next_run_at as an enqueue-lock in SQS mode.
     *
     * Postgres-specific: FOR UPDATE SKIP LOCKED.
     */
    public List<TaskToEnqueue> claimDueForEnqueue(int limit, long lockSeconds) {
        String sql = """
            WITH due AS (
                SELECT id
                FROM tasks
                WHERE status = 'ENQUEUED'
                  AND (scheduled_for IS NULL OR scheduled_for <= now())
                  AND attempt_count < max_attempts
                  AND (next_run_at IS NULL OR next_run_at <= now())  -- not currently enqueue-locked
                ORDER BY scheduled_for NULLS FIRST, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks t
               SET next_run_at = now() + (? * interval '1 second'), -- enqueue lock TTL
                   updated_at = now()
              FROM due
             WHERE t.id = due.id
            RETURNING t.id, t.scheduled_for
            """;

        return jdbc.query(
                sql,
                (rs, rowNum) -> new TaskToEnqueue(
                        rs.getLong("id"),
                        rs.getTimestamp("scheduled_for") == null ? null : rs.getTimestamp("scheduled_for").toInstant()
                ),
                limit, lockSeconds
        );
    }

    /** Release enqueue-lock (so poller can try again quickly). */
    public void releaseEnqueueLock(long taskId) {
        String sql = """
            UPDATE tasks
               SET next_run_at = NULL,
                   updated_at = now()
             WHERE id = ?
               AND status = 'ENQUEUED'
            """;
        jdbc.update(sql, taskId);
    }
}
