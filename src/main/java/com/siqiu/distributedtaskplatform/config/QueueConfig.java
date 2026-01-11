package com.siqiu.distributedtaskplatform.config;

import com.siqiu.distributedtaskplatform.metrics.TaskMetrics;
import com.siqiu.distributedtaskplatform.queue.DeadLetterClient;
import com.siqiu.distributedtaskplatform.queue.SqsDeadLetterClient;
import com.siqiu.distributedtaskplatform.queue.SqsTaskQueueClient;
import com.siqiu.distributedtaskplatform.queue.TaskQueueClient;
import com.siqiu.distributedtaskplatform.repo.TaskClaimRepository;
import com.siqiu.distributedtaskplatform.worker.SqsWorkerLoop;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class QueueConfig {

    @Bean
    public SqsClient sqsClient(
            @Value("${dtp.sqs.endpoint}") String endpoint,
            @Value("${dtp.sqs.region}") String region
    ) {
        return SqsClient.builder()
                .endpointOverride(java.net.URI.create(endpoint))
                .region(software.amazon.awssdk.regions.Region.of(region))
                .credentialsProvider(
                        software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("test", "test")
                        )
                )
                .build();
    }

    @Bean
    public TaskQueueClient taskQueueClient(
            SqsClient sqsClient,
            @Value("${dtp.sqs.queueName}") String queueName
    ) {
        return new SqsTaskQueueClient(sqsClient, queueName);
    }

    @Bean
    public DeadLetterClient deadLetterClient(
            SqsClient sqsClient,
            @Value("${dtp.sqs.dlqName}") String dlqName,
            com.fasterxml.jackson.databind.ObjectMapper mapper
    ) {
        return new SqsDeadLetterClient(sqsClient, dlqName, mapper);
    }

}

