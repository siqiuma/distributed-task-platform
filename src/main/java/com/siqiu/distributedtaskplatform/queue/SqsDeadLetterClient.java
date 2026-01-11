package com.siqiu.distributedtaskplatform.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsDeadLetterClient implements DeadLetterClient {

    private final SqsClient sqs;
    private final String dlqUrl;
    private final ObjectMapper mapper;

    public SqsDeadLetterClient(SqsClient sqs, @Value("${dtp.sqs.dlqName}") String dlqName, ObjectMapper mapper) {
        this.sqs = sqs;
        this.dlqUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(dlqName).build()).queueUrl();
        this.mapper = mapper;
    }

    @Override
    public void publishDeadTask(DeadTaskEvent event) {
        String body;
        try {
            body = mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize DeadTaskEvent", e);
        }

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(dlqUrl)
                .messageBody(body)
                .build());
    }
}
