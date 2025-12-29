package com.siqiu.distributedtaskplatform.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;



@SpringBootTest
@ActiveProfiles("test")
class TaskWorkerTest {

    @Autowired
    TaskWorker worker;

    @Autowired
    TaskRepository repository;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void runOnce_claimsAndSucceedsTask_exactlyOnce() {
        // given: a pending task
        Task task = new Task("email", "hello-world");
        repository.saveAndFlush(task);

        Long taskId = task.getId();

        // when: worker runs
        worker.runOnce();

        // then: task is succeeded
        Task updated = repository.findById(taskId).orElseThrow();

        assertThat(updated.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(updated.getAttemptCount()).isEqualTo(1);
        assertThat(updated.getLastError()).isNull();
        assertThat(updated.getNextRunAt()).isNull();

        // when: worker runs again
        worker.runOnce();

        // then: task is NOT processed again
        Task updatedAgain = repository.findById(taskId).orElseThrow();

        assertThat(updatedAgain.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(updatedAgain.getAttemptCount()).isEqualTo(1); // still 1
    }

    @Test
    void runOnce_failedTask_marksFailed_andSchedulesRetry() {
        // Arrange: create a task that will fail
        Task t = new Task("email", "please-fail");
        repository.saveAndFlush(t);

        // Act: run worker once
        worker.runOnce();

        // Assert: reload and verify retry state
        Task updated = repository.findById(t.getId()).orElseThrow();

        assertThat(updated.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(updated.getAttemptCount()).isEqualTo(1);
        assertThat(updated.getLastError()).isNotBlank();

        // nextRunAt should be scheduled in the future (backoff)
        assertThat(updated.getNextRunAt()).isNotNull();
        assertThat(updated.getNextRunAt()).isAfter(Instant.now().minusSeconds(1));
    }

    @Test
    void runOnce_doesNotProcessFailedTask_whenNextRunAtInFuture() {
        // Create a task that will fail and schedule retry
        Task t = repository.saveAndFlush(new Task("email", "please-fail"));

        // First run: will claim -> fail -> schedule retry
        worker.runOnce();

        Task afterFail = repository.findById(t.getId()).orElseThrow();
        assertThat(afterFail.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(afterFail.getNextRunAt()).isNotNull();

        // Force nextRunAt into the future (so it should NOT be eligible)
        Instant future = Instant.now().plus(Duration.ofMinutes(30));
        jdbc.update(
                "update tasks set next_run_at = ? where id = ?",
                Timestamp.from(future),
                t.getId()
        );

        // Second run: should NOT claim/process it (status should remain FAILED)
        worker.runOnce();

        Task stillFailed = repository.findById(t.getId()).orElseThrow();
        assertThat(stillFailed.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(stillFailed.getNextRunAt()).isNotNull();
        assertThat(stillFailed.getNextRunAt()).isAfter(Instant.now());
    }

    @Test
    void runOnce_processesFailedTask_whenNextRunAtIsDue() {
        Task t = repository.saveAndFlush(new Task("email", "test")); // success payload

        // Make it look like a retryable FAILED task due now
        Instant due = Instant.now().minusSeconds(5);
        jdbc.update(
                "update tasks set status = 'FAILED', next_run_at = ? where id = ?",
                Timestamp.from(due),
                t.getId()
        );

        worker.runOnce();

        Task updated = repository.findById(t.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void runOnce_stopsRetryingAfterMaxAttempts_andBecomesDead() {
        // Arrange: create a task that will fail
        Task t = new Task("email", "please-fail");
        t = repository.saveAndFlush(t);

        // Force max_attempts = 1 so it becomes DEAD after the first failure path
        jdbc.update("update tasks set max_attempts = ? where id = ?", 1, t.getId());

        // Act #1: run once -> claim increments attempt_count, then payload triggers simulated failure,
        // markFailed should turn it into DEAD (since max_attempts=1)
        worker.runOnce();

        // Assert after run #1
        Task after1 = repository.findById(t.getId()).orElseThrow();
        assertThat(after1.getStatus()).isEqualTo(TaskStatus.DEAD);
        assertThat(after1.getAttemptCount()).isEqualTo(1);
        assertThat(after1.getLastError()).isNotBlank();
        assertThat(after1.getNextRunAt()).isNull();

        // Act #2: run again -> DEAD must NOT be eligible, so nothing should change
        worker.runOnce();

        // Assert after run #2 (unchanged)
        Task after2 = repository.findById(t.getId()).orElseThrow();
        assertThat(after2.getStatus()).isEqualTo(TaskStatus.DEAD);
        assertThat(after2.getAttemptCount()).isEqualTo(1);
        assertThat(after2.getNextRunAt()).isNull();
    }
}
