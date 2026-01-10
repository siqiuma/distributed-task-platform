package com.siqiu.distributedtaskplatform.task;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class TaskDomainTest {

    @Test
    void markProcessing_incrementsAttempt_andClearsLastError() {
        Task t = new Task("email", "hello");

        // simulate prior failure state
        t.setStatus(TaskStatus.FAILED);

        String workerId = "test-worker-1";
        t.markProcessing(workerId);

        assertEquals(TaskStatus.PROCESSING, t.getStatus());
        assertEquals(1, t.getAttemptCount(), "attemptCount should increment when starting an attempt");
        assertNull(t.getLastError(), "lastError should be cleared on new attempt");
        assertNull(t.getNextRunAt(), "nextRunAt should be cleared once claimed/processing");
    }

    @Test
    void markFailed_schedulesRetry_whenAttemptsRemain() {
        Task t = new Task("email", "fail me");
        t.markEnqueued(Instant.now());
        String workerId = "test-worker-1";
        t.markProcessing(workerId); // attemptCount becomes 1

        Duration backoff = Duration.ofSeconds(2);
        Instant before = Instant.now();

        t.markFailed("boom", backoff);

        assertEquals(TaskStatus.FAILED, t.getStatus());
        assertNotNull(t.getNextRunAt());
        assertTrue(!t.getNextRunAt().isBefore(before.plus(backoff.minusMillis(50))),
                "nextRunAt should be roughly now + backoff");
        assertNotNull(t.getLastError());
        assertTrue(t.getLastError().contains("boom"));
    }

    @Test
    void markFailed_becomesDead_whenMaxAttemptsReached() {
        Task t = new Task("email", "fail always");
        // maxAttempts defaults to 3 in DB; ensure the entity matches that
        // Run 3 processing attempts; the 4th failure should become DEAD depending on your exact rule.
        String workerId = "test-worker-1";

        // Attempt 1
        t.markEnqueued(Instant.now());
        t.markProcessing(workerId); // attemptCount 1
        t.markFailed("boom1", Duration.ofSeconds(1)); // FAILED

        // Attempt 2
        t.markEnqueued(Instant.now());
        t.markProcessing(workerId); // attemptCount 2
        t.markFailed("boom2", Duration.ofSeconds(1));

        // Attempt 3
        t.markEnqueued(Instant.now());
        t.markProcessing(workerId); // attemptCount 3
        t.markFailed("boom3", Duration.ofSeconds(1));

        // Now depending on your business rule:
        // - If attemptCount >= maxAttempts => DEAD, then this should be DEAD now.
        assertEquals(TaskStatus.DEAD, t.getStatus(), "should become terminal DEAD after max attempts");
        assertNull(t.getNextRunAt(), "terminal failures should not be scheduled again");
        assertNotNull(t.getLastError());
    }

    @Test
    void illegalTransitions_throw() {
        Task t = new Task("email", "hello");

        // can't succeed if not PROCESSING
        assertThrows(InvalidTaskStateException.class, t::markSucceeded);

        // can't fail if not PROCESSING
        assertThrows(InvalidTaskStateException.class,
                () -> t.markFailed("x", Duration.ofSeconds(1)));
    }
}
