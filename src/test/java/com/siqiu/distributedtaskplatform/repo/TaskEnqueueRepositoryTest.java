package com.siqiu.distributedtaskplatform.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class TaskEnqueueRepositoryTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired TaskEnqueueRepository enqueueRepo;

    @Test
    void claimDueForEnqueue_returnsOnlyDue_andLocksThem() {
        Instant now = Instant.now();

        long dueId = insertTask("ENQUEUED", now.minus(5, ChronoUnit.SECONDS));
        long notDueId = insertTask("ENQUEUED", now.plus(60, ChronoUnit.SECONDS));

        var claimed = enqueueRepo.claimDueForEnqueue(10, 30);

        assertThat(claimed).extracting(TaskEnqueueRepository.TaskToEnqueue::id)
                .contains(dueId)
                .doesNotContain(notDueId);

        // locked: next_run_at should be non-null for dueId
        Instant lock = jdbc.queryForObject(
                "SELECT next_run_at FROM tasks WHERE id = ?",
                (rs, rowNum) -> rs.getTimestamp(1).toInstant(),
                dueId
        );
        assertThat(lock).isAfter(now);

        // not due should still have null next_run_at
        Instant notDueLock = jdbc.query(
                "SELECT next_run_at FROM tasks WHERE id = ?",
                rs -> rs.next() && rs.getTimestamp(1) != null ? rs.getTimestamp(1).toInstant() : null,
                notDueId
        );
        assertThat(notDueLock).isNull();
    }

    private long insertTask(String status, Instant scheduledFor) {
        return jdbc.queryForObject(
                """
                INSERT INTO tasks(type, payload, status, created_at, updated_at, attempt_count, max_attempts, scheduled_for)
                VALUES ('t', 'p', ?, now(), now(), 0, 3, ?)
                RETURNING id
                """,
                Long.class,
                status,
                scheduledFor == null ? null : Timestamp.from(scheduledFor)
        );
    }
}
