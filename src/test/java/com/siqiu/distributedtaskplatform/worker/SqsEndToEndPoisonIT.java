package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.queue.DueTaskEnqueuer;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
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
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(classes = {com.siqiu.distributedtaskplatform.DistributedTaskPlatformApplication.class, SqsEndToEndPoisonIT.FailingProcessorConfig.class},
        properties = {
        "dtp.queue.mode=sqs",
        "dtp.sqs.worker.autostart=false",
        "spring.task.scheduling.enabled=false"
})
class SqsEndToEndPoisonIT {

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

    @TestConfiguration
    static class FailingProcessorConfig {
        @Bean
        @Primary
        TaskProcessor taskProcessor() {
            return taskId -> { throw new RuntimeException("boom"); };
        }
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired DueTaskEnqueuer enqueuer;
    @SpyBean SqsWorkerLoop worker;

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
    void e2e_poisonTask_becomesDead_publishesToDlq_andDeletesOriginal() {
        long taskId = insertEnqueuedDueTask();

        // send to SQS (your enqueuer puts taskId in queue)
        enqueuer.tick();

        // Run worker 3 times (maxAttempts=3) to drive it DEAD
        worker.runOnce();
        reenqueueDue(taskId); // make it due again + send again
        worker.runOnce();
        reenqueueDue(taskId);
        worker.runOnce();

        // DB should be DEAD
        Map<String, Object> row = jdbc.queryForMap("select status from tasks where id = ?", taskId);
        assertThat(row.get("status")).isEqualTo("DEAD");

        // DLQ should have exactly one message with JSON event containing taskId
        String dlqBody = receiveOneFromDlq();
        assertThat(dlqBody).contains("\"taskId\":" + taskId);
    }

    private void reenqueueDue(long taskId) {
        // make it due and re-enqueued
        jdbc.update("""
        update tasks
           set status='ENQUEUED',
               scheduled_for = now() - interval '1 second'
         where id = ?
    """, taskId);

        enqueuer.tick();
    }

    private String receiveOneFromDlq() {
        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .region(Region.US_EAST_1)
                .build()) {

            String dlqUrl = sqs.getQueueUrl(r -> r.queueName(DLQ_NAME)).queueUrl();

            ReceiveMessageResponse resp = sqs.receiveMessage(r -> r.queueUrl(dlqUrl).maxNumberOfMessages(1).waitTimeSeconds(1));
            assertThat(resp.messages()).hasSize(1);
            return resp.messages().get(0).body();
        }
    }


}
