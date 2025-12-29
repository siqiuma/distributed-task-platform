package com.siqiu.distributedtaskplatform.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
@Transactional
class TaskRepositoryTest {

    @Autowired
    TaskRepository repository;

    @Test
    void claim_updatesExactlyOneRow_thenSecondClaimUpdatesZero() {
        // Arrange: create a PENDING task
        Task t = repository.save(new Task("email", "test"));
        Long id = t.getId();

        // Act: first claim should succeed (1 row updated)
        int first = repository.claim(id, Instant.now());

        // Act: second claim should fail (already PROCESSING => 0 rows updated)
        int second = repository.claim(id, Instant.now());

        // Assert
        assertThat(first).isEqualTo(1);
        assertThat(second).isEqualTo(0);

        Task reloaded = repository.findById(id).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(TaskStatus.PROCESSING);
    }

    @Test
    void findTop5Eligible_includesFailedWhenDue_excludesFailedWhenNotDue() {

        // FAILED but not due yet (nextRunAt in future)
        Task notDue = createFailedTaskWithNextRunAt(Duration.ofMinutes(10));

        // FAILED and due now (nextRunAt <= now)
        Task due = createFailedTaskWithNextRunAt(Duration.ZERO);

        Instant now = Instant.now();

        List<Task> eligible = repository.findTop5Eligible(now, PageRequest.of(0, 5));

        List<Long> ids = eligible.stream().map(Task::getId).toList();

        org.assertj.core.api.Assertions.assertThat(ids).contains(due.getId());
        org.assertj.core.api.Assertions.assertThat(ids).doesNotContain(notDue.getId());
    }

    private Task createFailedTaskWithNextRunAt(Duration backoff) {
        Task t = new Task("email", "will-fail");
        repository.saveAndFlush(t);

        // move it through domain transitions
        t.markProcessing();
        t.markFailed("boom", backoff);

        return repository.saveAndFlush(t);
    }

    @Test
    void findTop5Eligible_excludesDeadAlways() {
        Instant now = Instant.now();

        Task dead = createDeadTask(now);

        List<Task> eligible = repository.findTop5Eligible(now, PageRequest.of(0, 5));

        List<Long> ids = eligible.stream().map(Task::getId).toList();

        org.assertj.core.api.Assertions.assertThat(ids).doesNotContain(dead.getId());
    }

    private Task createDeadTask(Instant now) {
        Task t = new Task("email", "always-fail");
        repository.saveAndFlush(t);

        // attempt 1
        t.markProcessing();
        t.markFailed("boom1", Duration.ZERO);
        repository.saveAndFlush(t);

        // attempt 2
        t.markProcessing();
        t.markFailed("boom2", Duration.ZERO);
        repository.saveAndFlush(t);

        // attempt 3 -> should become DEAD depending on your logic
        t.markProcessing();
        t.markFailed("boom3", Duration.ZERO);

        return repository.saveAndFlush(t);
    }

    @Test
    void findTop5Eligible_ordersByCreatedAt_andRespectsLimit() throws Exception {
        Instant now = Instant.now();

        // create 7 eligible tasks (PENDING is eligible)
        Task t1 = repository.saveAndFlush(new Task("email", "p1"));
        Thread.sleep(5);
        Task t2 = repository.saveAndFlush(new Task("email", "p2"));
        Thread.sleep(5);
        Task t3 = repository.saveAndFlush(new Task("email", "p3"));
        Thread.sleep(5);
        Task t4 = repository.saveAndFlush(new Task("email", "p4"));
        Thread.sleep(5);
        Task t5 = repository.saveAndFlush(new Task("email", "p5"));
        Thread.sleep(5);
        Task t6 = repository.saveAndFlush(new Task("email", "p6"));
        Thread.sleep(5);
        Task t7 = repository.saveAndFlush(new Task("email", "p7"));

        List<Task> eligible = repository.findTop5Eligible(now, PageRequest.of(0, 5));

        // limit
        assertThat(eligible).hasSize(5);

        // ordering by createdAt ascending
        List<Long> ids = eligible.stream().map(Task::getId).toList();
        assertThat(ids).containsExactly(t1.getId(), t2.getId(), t3.getId(), t4.getId(), t5.getId());
    }

}
