package com.siqiu.distributedtaskplatform.queue;

import com.siqiu.distributedtaskplatform.repo.TaskEnqueueRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "dtp.queue.mode=sqs",
        "dtp.enqueuer.batch-size=10",
        "dtp.enqueuer.enqueue-lock-seconds=30",
        "spring.task.scheduling.enabled=false" // don’t run background scheduling in tests
})
class DueTaskEnqueuerTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DueTaskEnqueuer enqueuer;

    @MockBean TaskQueueClient queueClient;

    @Test
    void tick_enqueuesDueTasks_andReleasesLockIfEnqueueFails() {
        long id1 = insertTask("ENQUEUED", Instant.now().minusSeconds(1));
        long id2 = insertTask("ENQUEUED", Instant.now().minusSeconds(1));

        // make enqueue fail for id2
        doNothing().when(queueClient).enqueueTask(eq(String.valueOf(id1)), anyLong());
        doThrow(new RuntimeException("boom")).when(queueClient).enqueueTask(eq(String.valueOf(id2)), anyLong());

        enqueuer.tick();

        verify(queueClient, times(1)).enqueueTask(eq(String.valueOf(id1)), anyLong());
        verify(queueClient, times(1)).enqueueTask(eq(String.valueOf(id2)), anyLong());

        // id2 should have lock released (next_run_at NULL)
        Integer lockNull = jdbc.queryForObject(
                "SELECT CASE WHEN next_run_at IS NULL THEN 1 ELSE 0 END FROM tasks WHERE id = ?",
                Integer.class,
                id2
        );
        assertThat(lockNull).isEqualTo(1);
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
