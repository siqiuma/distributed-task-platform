package com.siqiu.distributedtaskplatform.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class TaskWorkerTxTest {

    @Autowired TaskWorkerTx tx;
    @Autowired TaskRepository repository;

    @Test
    void claimAndGetSnapshot_updatesExactlyOnce_thenSecondClaimReturnsNull() {
        // given
        Task t = repository.saveAndFlush(new Task("email", "hello"));
        Long id = t.getId();

        // when
        TaskSnapshot s1 = tx.claimAndGetSnapshot(id);
        TaskSnapshot s2 = tx.claimAndGetSnapshot(id);

        // then
        assertThat(s1).isNotNull();
        assertThat(s1.status()).isEqualTo(TaskStatus.PROCESSING);
        assertThat(s1.attemptCount()).isEqualTo(1);

        assertThat(s2).isNull(); // already claimed
    }
}
