package com.siqiu.distributedtaskplatform.queue;

import com.siqiu.distributedtaskplatform.task.TaskWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.net.URI;

public class SqsTaskQueueClient implements TaskQueueClient {

    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

    private final SqsClient sqs;
    private final String queueUrl;

    public SqsTaskQueueClient(SqsClient sqs, String queueName) {
        this.sqs = sqs;
        this.queueUrl = sqs.getQueueUrl(software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build()).queueUrl();
    }

    @Override
    public void enqueueTask(String taskId, long scheduledForEpochMs) {
        log.info("enqueue_task taskId={} scheduledFor={}", taskId, scheduledForEpochMs);
        sqs.sendMessage(software.amazon.awssdk.services.sqs.model.SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(taskId)
                .build());
    }
}
