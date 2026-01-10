package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.queue.DueTaskEnqueuer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "dtp.queue.mode=sqs",
        "dtp.sqs.worker.autostart=false",
        "spring.task.scheduling.enabled=false"
})
class SqsEndToEndIT {

    private static final String QUEUE_NAME = "dtp-task-queue";


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

        // Create queue BEFORE Spring creates beans that call getQueueUrl()
        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .region(Region.US_EAST_1)
                .build()) {
            sqs.createQueue(CreateQueueRequest.builder().queueName(QUEUE_NAME).build());
        }
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired DueTaskEnqueuer enqueuer;
    @Autowired SqsWorkerLoop worker;

    @Test
    void e2e_dueTask_enqueued_toSqs_consumed_marksSucceeded() {
        long taskId = insertEnqueuedDueTask();

        // send to SQS
        enqueuer.tick();

        // consume + process + DB update + delete message
        worker.runOnce();

        Map<String, Object> row = jdbc.queryForMap(
                "select status, completed_at from tasks where id = ?",
                taskId
        );

        assertThat(row.get("status")).isEqualTo("SUCCEEDED");
        assertThat(row.get("completed_at")).isNotNull();
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
