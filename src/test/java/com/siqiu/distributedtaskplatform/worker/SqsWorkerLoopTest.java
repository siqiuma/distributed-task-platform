package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.metrics.TaskMetrics;
import com.siqiu.distributedtaskplatform.repo.TaskClaimRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Instant;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SqsWorkerLoopTest {

    @Mock SqsClient sqs;
    @Mock TaskClaimRepository claimRepo;
    @Mock TaskMetrics metrics;

    private SqsWorkerLoop worker;

    @BeforeEach
    void setup() {
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl("http://queue-url").build());

        // autoStart = false so it won't long-poll in tests
        worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false);
    }

    @Test
    void poisonMessage_deletesImmediately() {
        Message msg = Message.builder().body("not-a-number").receiptHandle("rh").build();

        worker.processMessage(msg);

        verify(sqs).deleteMessage(argThat(hasReceipt("rh")));
        verify(metrics).incDeleted();
        verifyNoInteractions(claimRepo);
    }

    @Test
    void claimFails_doesNotDelete() {
        Message msg = Message.builder().body("123").receiptHandle("rh").build();
        when(claimRepo.claimEnqueuedTask(anyLong(), anyString())).thenReturn(false);

        worker.processMessage(msg);

        verify(metrics).incClaimFailed();
        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(metrics, never()).incDeleted();
    }

    @Test
    void success_marksSucceeded_thenDeletesOnlyIfDbUpdated() {
        Message msg = Message.builder().body("123").receiptHandle("rh").build();

        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);
        when(claimRepo.getScheduledFor(123L)).thenReturn(Optional.of(Instant.now().minusSeconds(1)));
        when(claimRepo.markSucceeded(eq(123L), anyString())).thenReturn(true);

        worker.processMessage(msg);

        verify(claimRepo).markSucceeded(eq(123L), anyString());
        verify(sqs).deleteMessage(argThat(hasReceipt("rh")));
        verify(metrics).incDeleted();
        verify(metrics).incTasksSucceeded();
    }

    @Test
    void success_dbUpdateFails_doesNotDelete() {
        Message msg = Message.builder().body("123").receiptHandle("rh").build();

        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);
        when(claimRepo.markSucceeded(eq(123L), anyString())).thenReturn(false);

        worker.processMessage(msg);

        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(metrics, never()).incDeleted();
    }

    @Test
    void failure_marksFailedAndReschedule_thenDeletesOnlyIfDbUpdated() {
        // Override simulateWork to throw
        worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false) {
            @Override
            protected void simulateWork(long taskId) {
                throw new RuntimeException("boom");
            }
        };

        Message msg = Message.builder().body("123").receiptHandle("rh").build();

        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);
        when(claimRepo.markFailedAndReschedule(eq(123L), anyString(), anyString(), anyLong())).thenReturn(true);

        worker.processMessage(msg);

        verify(claimRepo).markFailedAndReschedule(eq(123L), anyString(), eq("boom"), eq(30L));
        verify(sqs).deleteMessage(argThat(hasReceipt("rh")));
        verify(metrics).incDeleted();
        verify(metrics).incTasksFailed();
    }

    @Test
    void failure_dbUpdateFails_doesNotDelete() {
        worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false) {
            @Override
            protected void simulateWork(long taskId) {
                throw new RuntimeException("boom");
            }
        };

        Message msg = Message.builder().body("123").receiptHandle("rh").build();

        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);
        when(claimRepo.markFailedAndReschedule(eq(123L), anyString(), anyString(), anyLong())).thenReturn(false);

        worker.processMessage(msg);

        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(metrics, never()).incDeleted();
    }

    private static ArgumentMatcher<DeleteMessageRequest> hasReceipt(String rh) {
        return r -> r != null && rh.equals(r.receiptHandle()) && "http://queue-url".equals(r.queueUrl());
    }
}
