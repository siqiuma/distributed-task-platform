package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.queue.DueTaskEnqueuer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
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
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(classes = {com.siqiu.distributedtaskplatform.DistributedTaskPlatformApplication.class, SqsEndToEndSuccessIT.SuccessProcessorConfig.class},
        properties = {
                "dtp.queue.mode=sqs",
                "dtp.sqs.worker.autostart=false",
                "spring.task.scheduling.enabled=false"
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

    @TestConfiguration
    static class SuccessProcessorConfig {
        @Bean
        @Primary
        TaskProcessor taskProcessor() {
            return taskId -> {}; // succeed
        }
    }

    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    DueTaskEnqueuer enqueuer;
    @SpyBean
    SqsWorkerLoop worker;

    @Test
    void e2e_dueTask_enqueued_toSqs_consumed_marksSucceeded() {
        long taskId = insertEnqueuedDueTask();

        // ✅ deterministically send taskId to the main queue
        sendToMainQueue(taskId);

        // consume + process + DB update + delete message
        worker.runOnce();

        Map<String, Object> row = jdbc.queryForMap(
                "select status, completed_at from tasks where id = ?",
                taskId
        );

        assertThat(row.get("status")).isEqualTo("SUCCEEDED");
        assertThat(row.get("completed_at")).isNotNull();
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
}
