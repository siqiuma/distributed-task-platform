package com.siqiu.distributedtaskplatform.task;

import com.siqiu.distributedtaskplatform.queue.TaskQueueClient;
import com.siqiu.distributedtaskplatform.task.dto.CreateTaskRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;

@Service
public class TaskService {

    private final TaskRepository repository;
    private final TaskQueueClient queueClient;
    private final String queueMode;

    public TaskService(
            TaskRepository repository,
            TaskQueueClient queueClient,
            @Value("${dtp.queue.mode:db}") String queueMode
    ) {
        this.repository = repository;
        this.queueClient = queueClient;
        this.queueMode = queueMode;
    }
    @Transactional
    public Task create(CreateTaskRequest request) {
        Instant scheduledFor = Instant.now();

        Task task = new Task(request.getType(), request.getPayload());

        if ("sqs".equalsIgnoreCase(queueMode)) {
            // use your domain method instead of setters
            task.markEnqueued(scheduledFor);
        } else {
            task.setScheduledFor(scheduledFor); // if you still want it stored for DB mode too
            // status stays PENDING for DB poller mode
        }
        Task saved = repository.save(task);

        // If we're in sqs mode, enqueue the newly created taskId
        if ("sqs".equalsIgnoreCase(queueMode)) {
//            queueClient.enqueueTask(String.valueOf(saved.getId()), saved.getScheduledFor().toEpochMilli());
            //This guarantees: DB commit happens first, then SQS.
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    queueClient.enqueueTask(String.valueOf(saved.getId()),
                            saved.getScheduledFor().toEpochMilli());
                }
            });
        }

        return saved;
    }

    public Task getOrThrow(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new TaskNotFoundException(id));
    }

    @Transactional
    public Task cancel(Long id) {
        Task task = getOrThrow(id);
        task.cancel();
        return task;
    }
}
