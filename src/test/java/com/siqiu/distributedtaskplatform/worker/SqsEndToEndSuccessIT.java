package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.metrics.TaskMetrics;
import com.siqiu.distributedtaskplatform.queue.DeadLetterClient;
import com.siqiu.distributedtaskplatform.queue.DueTaskEnqueuer;
import com.siqiu.distributedtaskplatform.queue.SqsDeadLetterClient;
import com.siqiu.distributedtaskplatform.repo.TaskClaimRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(classes = {com.siqiu.distributedtaskplatform.DistributedTaskPlatformApplication.class, SqsEndToEndSuccessIT.CountingProcessorConfig.class},
        properties = {
                "dtp.queue.mode=sqs",
                "dtp.sqs.worker.autostart=false",
                "spring.task.scheduling.enabled=false",
                "dtp.test.taskCount=30"
        })
public class SqsEndToEndSuccessIT {
    private static final String QUEUE_NAME = "dtp-task-queue";
    private static final String DLQ_NAME   = "dtp-task-dlq";


    @Container
    static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.4.0")
    ).withServices(LocalStackContainer.Service.SQS);


    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        // SQS props (QueueConfig reads these)
        String sqsEndpoint = localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString();
        r.add("dtp.sqs.endpoint", () -> sqsEndpoint);
        r.add("dtp.sqs.region", () -> "us-east-1");
        r.add("dtp.sqs.queueName", () -> QUEUE_NAME);
        r.add("dtp.sqs.dlqName", () -> DLQ_NAME);

        // Create queue BEFORE Spring creates beans that call getQueueUrl()
        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .region(Region.US_EAST_1)
                .build()) {
            sqs.createQueue(CreateQueueRequest.builder().queueName(QUEUE_NAME).build());
            sqs.createQueue(CreateQueueRequest.builder().queueName(DLQ_NAME).build());
        }
    }

    /**
     * Processor that:
     * - counts executions per taskId (thread-safe)
     * - counts down a latch only on FIRST execution of each taskId
     */
    @TestConfiguration
    static class CountingProcessorConfig {
        @Bean
        ConcurrentHashMap<Long, AtomicInteger> processedCounts() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        CountDownLatch processedLatch(@Value("${dtp.test.taskCount:30}") int n) {
            return new CountDownLatch(n);
        }

        @Bean
        @Primary
        TaskProcessor taskProcessor(
                ConcurrentHashMap<Long, AtomicInteger> processedCounts,
                CountDownLatch processedLatch
        ) {
            return taskId -> {
                int c = processedCounts.computeIfAbsent(taskId, k -> new AtomicInteger()).incrementAndGet();
                if (c == 1) {
                    processedLatch.countDown();
                }

                // simulate some real work (helps surface concurrency issues)
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
        }
    }



    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    DueTaskEnqueuer enqueuer;
    @SpyBean
    SqsWorkerLoop worker;

    // For multi-worker construction
    @Autowired SqsClient sqs;
    @Autowired TaskClaimRepository claimRepo;
    @Autowired TaskMetrics metrics;
    @MockBean DeadLetterClient dlq;
    @Autowired TaskProcessor processor;

    @Value("${dtp.sqs.queueName}")
    String queueName;

    @Autowired ConcurrentHashMap<Long, AtomicInteger> processedCounts;
    @Autowired CountDownLatch processedLatch;

    @Value("${dtp.test.taskCount:30}")
    int taskCount;

    @Test
    void e2e_dueTask_enqueued_toSqs_consumed_marksSucceeded() throws Exception{
        long taskId = insertEnqueuedDueTask();

        // ✅ deterministically send taskId to the main queue
        sendToMainQueue(taskId);

        // Sometimes SQS receive timing can be slightly delayed; retry a bit.
        runWorkerUntilStatus(worker, taskId, "SUCCEEDED", 30, 50);

        Map<String, Object> row = jdbc.queryForMap(
                "select status, completed_at from tasks where id = ?",
                taskId
        );

        assertThat(row.get("status")).isEqualTo("SUCCEEDED");
        assertThat(row.get("completed_at")).isNotNull();
    }


    private void runWorkerUntilStatus(
            SqsWorkerLoop worker,
            long taskId,
            String expectedStatus,
            int attempts,
            long sleepMs
    ) throws InterruptedException {
        for (int i = 0; i < attempts; i++) {
            worker.runOnce();

            String status = jdbc.queryForObject(
                    "select status from tasks where id = ?",
                    String.class,
                    taskId
            );

            if (expectedStatus.equals(status)) return;
            Thread.sleep(sleepMs);
        }
    }

    private void sendToMainQueue(long taskId) {
        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .region(Region.US_EAST_1)
                .build()) {

            String queueUrl = sqs.getQueueUrl(b -> b.queueName(QUEUE_NAME)).queueUrl();

            sqs.sendMessage(b -> b.queueUrl(queueUrl).messageBody(Long.toString(taskId)));
        }
    }

    private long insertEnqueuedDueTask() {
        Instant scheduledFor = Instant.now().minusSeconds(5);

        return jdbc.queryForObject("""
        INSERT INTO tasks(
            type, payload, status,
            created_at, updated_at,
            attempt_count, max_attempts,
            scheduled_for
        )
        VALUES (
            't', 'p', 'ENQUEUED',
            now(), now(),
            0, 3,
            ?::timestamptz
        )
        RETURNING id
        """,
                Long.class,
                java.sql.Timestamp.from(scheduledFor)
        );
    }

    @Test
    void e2e_threeWorkers_noDoubleProcessing_allSucceeded() throws Exception {
        int n = taskCount;

        // 1) Insert N due tasks and send to SQS
        List<Long> taskIds = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long taskId = insertEnqueuedDueTask();
            taskIds.add(taskId);
            sendToMainQueue(taskId);
        }

        // 2) Create 3 worker instances (no autostart background loop)
        SqsWorkerLoop w1 = new SqsWorkerLoop(sqs, queueName, claimRepo, metrics, false, dlq, processor);
        SqsWorkerLoop w2 = new SqsWorkerLoop(sqs, queueName, claimRepo, metrics, false, dlq, processor);
        SqsWorkerLoop w3 = new SqsWorkerLoop(sqs, queueName, claimRepo, metrics, false, dlq, processor);

        ExecutorService pool = Executors.newFixedThreadPool(3);

        pool.submit(() -> runUntilDoneOrTimeout(w1, processedLatch, 10_000));
        pool.submit(() -> runUntilDoneOrTimeout(w2, processedLatch, 10_000));
        pool.submit(() -> runUntilDoneOrTimeout(w3, processedLatch, 10_000));

        boolean completed = processedLatch.await(10, TimeUnit.SECONDS);

        pool.shutdown(); // no interrupts
        if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
            pool.shutdownNow(); // fallback only if stuck
            pool.awaitTermination(2, TimeUnit.SECONDS);
        }

        assertThat(completed)
                .as("All %s tasks should be processed at least once within timeout", n)
                .isTrue();

        // 3) Assert: each task processed exactly once (no duplicates)
        assertThat(processedCounts).hasSize(n);
        for (Long id : taskIds) {
            AtomicInteger c = processedCounts.get(id);
            assertThat(c)
                    .as("Missing processed count for taskId=%s", id)
                    .isNotNull();
            assertThat(c.get())
                    .as("taskId=%s processed count", id)
                    .isEqualTo(1);

        }

        Integer notSucceeded = jdbc.query(con -> {
            var ps = con.prepareStatement("""
        select count(*)
          from tasks
         where status <> 'SUCCEEDED'
           and id = any(?)
    """);

            Long[] ids = taskIds.toArray(new Long[0]);
            ps.setArray(1, con.createArrayOf("bigint", ids));
            return ps;
        }, rs -> {
            rs.next();
            return rs.getInt(1);
        });

        assertThat(notSucceeded).isEqualTo(0);
    }

    private void runUntilDoneOrTimeout(SqsWorkerLoop worker, CountDownLatch latch, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && latch.getCount() > 0) {
            worker.runOnce();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

}
