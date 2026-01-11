package com.siqiu.distributedtaskplatform.worker;

import com.siqiu.distributedtaskplatform.metrics.TaskMetrics;
import com.siqiu.distributedtaskplatform.queue.DeadLetterClient;
import com.siqiu.distributedtaskplatform.repo.TaskClaimRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SqsWorkerLoopTest {

    @Mock SqsClient sqs;
    @Mock TaskClaimRepository claimRepo;
    @Mock TaskMetrics metrics;
    @Mock TaskProcessor processor;
    DeadLetterClient dlq = mock(DeadLetterClient.class);

    private SqsWorkerLoop worker;
    private static final String QUEUE_NAME = "dtp-task-queue";
    private static final String DLQ_NAME   = "dtp-task-dlq";

    @BeforeEach
    void setup() {
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl("http://queue-url").build());

        // autoStart = false so it won't long-poll in tests
        worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false, dlq, processor);
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
        doNothing().when(processor).process(123L);

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
        doNothing().when(processor).process(123L);

        worker.processMessage(msg);

        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(metrics, never()).incDeleted();
    }

    @Test
    void failure_marksFailedAndReschedule_thenDeletesOnlyIfDbUpdated() throws Exception {
        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);

        when(claimRepo.markFailedAndRescheduleOutcome(eq(123L), anyString(), anyString(), anyLong()))
                .thenReturn(new TaskClaimRepository.FailOutcome(true, false, 1, 3));
        doThrow(new RuntimeException("boom")).when(processor).process(123L);

        SqsWorkerLoop worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false, dlq, processor) {};

        Message msg = Message.builder().body("123").receiptHandle("rh-1").build();

        worker.processMessage(msg);

        verify(claimRepo).markFailedAndRescheduleOutcome(
                eq(123L),
                anyString(),
                contains("boom"),
                anyLong()
        );

        ArgumentCaptor<DeleteMessageRequest> captor =
                ArgumentCaptor.forClass(DeleteMessageRequest.class);

        verify(sqs).deleteMessage(captor.capture());   // ✅ unambiguous: matches request overload

        DeleteMessageRequest req = captor.getValue();
        assertEquals("http://queue-url", req.queueUrl());
        assertEquals("rh-1", req.receiptHandle());

        verify(metrics).incDeleted();
        verify(metrics).incTasksFailed();
    }

    @Test
    void failure_dbUpdateFails_doesNotDelete() throws Exception {
        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);

        when(claimRepo.markFailedAndRescheduleOutcome(eq(123L), anyString(), anyString(), anyLong()))
                .thenReturn(new TaskClaimRepository.FailOutcome(false, false, 1, 3));
        doThrow(new RuntimeException("boom")).when(processor).process(123L);

        SqsWorkerLoop worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false, dlq, processor) {};

        Message msg = Message.builder().body("123").receiptHandle("rh-2").build();

        worker.processMessage(msg);

        verify(claimRepo).markFailedAndRescheduleOutcome(eq(123L), anyString(), anyString(), anyLong());

        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(metrics, never()).incDeleted();
        verify(metrics).incTasksFailed();
    }


//    @Test
//    void failure_marksFailedAndReschedule_thenDeletesOnlyIfDbUpdated() {
//        // Override simulateWork to throw
//        worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false, dlq) {
//            @Override
//            protected void simulateWork(long taskId) {
//                throw new RuntimeException("boom");
//            }
//        };
//
//        Message msg = Message.builder().body("123").receiptHandle("rh").build();
//
//        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);
//        when(claimRepo.markFailedAndReschedule(eq(123L), anyString(), anyString(), anyLong())).thenReturn(true);
//
//        worker.processMessage(msg);
//
//        verify(claimRepo).markFailedAndReschedule(eq(123L), anyString(), eq("boom"), eq(30L));
//        verify(sqs).deleteMessage(argThat(hasReceipt("rh")));
//        verify(metrics).incDeleted();
//        verify(metrics).incTasksFailed();
//    }
//
//    @Test
//    void failure_dbUpdateFails_doesNotDelete() {
//        worker = new SqsWorkerLoop(sqs, "dtp-task-queue", claimRepo, metrics, false, dlq) {
//            @Override
//            protected void simulateWork(long taskId) {
//                throw new RuntimeException("boom");
//            }
//        };
//
//        Message msg = Message.builder().body("123").receiptHandle("rh").build();
//
//        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);
//        when(claimRepo.markFailedAndReschedule(eq(123L), anyString(), anyString(), anyLong())).thenReturn(false);
//
//        worker.processMessage(msg);
//
//        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
//        verify(metrics, never()).incDeleted();
//    }

    private static ArgumentMatcher<DeleteMessageRequest> hasReceipt(String rh) {
        return r -> r != null && rh.equals(r.receiptHandle()) && "http://queue-url".equals(r.queueUrl());
    }

    @Test
    void failure_becameDead_publishesToDlq_thenDeletes_onlyIfDbUpdated() {
        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);

        // DB records failure and marks DEAD
        when(claimRepo.markFailedAndRescheduleOutcome(eq(123L), anyString(), anyString(), anyLong()))
                .thenReturn(new TaskClaimRepository.FailOutcome(true, true, 3, 3));
        doThrow(new RuntimeException("boom")).when(processor).process(123L);

        SqsWorkerLoop worker = new SqsWorkerLoop(sqs, QUEUE_NAME, claimRepo, metrics, false, dlq, processor) {};

        Message msg = Message.builder().body("123").receiptHandle("rh-dead").build();

        worker.processMessage(msg);

        // Must publish to DLQ when becameDead=true
        verify(dlq, times(1)).publishDeadTask(argThat(ev ->
                ev.taskId() == 123L
                        && ev.error() != null
                        && ev.error().contains("boom")
                        && ev.deadAt() != null
        ));

        // Delete message only after DB update succeeded (and DLQ publish happened)
        verify(sqs).deleteMessage(argThat(hasReceipt("rh-dead")));
        verify(metrics).incDeleted();
        verify(metrics).incTasksFailed();
        verify(metrics).incTasksDeadLettered();

        // Optional but high-signal: enforce ordering (publish before delete)
        InOrder inOrder = inOrder(dlq, sqs);
        inOrder.verify(dlq).publishDeadTask(any());
        inOrder.verify(sqs).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void failure_becameDead_butDlqPublishThrows_stillDeletesMessage() {
        when(claimRepo.claimEnqueuedTask(eq(123L), anyString())).thenReturn(true);

        when(claimRepo.markFailedAndRescheduleOutcome(eq(123L), anyString(), anyString(), anyLong()))
                .thenReturn(new TaskClaimRepository.FailOutcome(true, true, 3, 3));

        doThrow(new RuntimeException("dlq-down"))
                .when(dlq).publishDeadTask(any());
        doThrow(new RuntimeException("boom")).when(processor).process(123L);

        SqsWorkerLoop worker = new SqsWorkerLoop(sqs, QUEUE_NAME, claimRepo, metrics, false, dlq, processor) {};

        Message msg = Message.builder().body("123").receiptHandle("rh-dlq-fail").build();

        worker.processMessage(msg);

        // attempted DLQ publish (best effort)
        verify(dlq).publishDeadTask(any());

        // still deletes to avoid poison-loop
        verify(sqs).deleteMessage(argThat(hasReceipt("rh-dlq-fail")));
        verify(metrics).incDeleted();
        verify(metrics).incTasksFailed();
        verify(metrics).incTasksDeadLettered();

    }

}
