package com.siqiu.distributedtaskplatform.task;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;

@Service
public class TaskWorkerTx {
    private final TaskRepository repository;

    public TaskWorkerTx(TaskRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public boolean claim(Long id) {
        // implement as repository method: update where status=PENDING
        return repository.claim(id) == 1;
    }
//readOnly = true It declares intent, prevents accidental updates by disabling dirty checking in JPA,
// and improves performance for read-only operations.
    @Transactional(readOnly = true)
    public String getPayload(Long id) {
        return repository.findById(id).map(Task::getPayload).orElse(null);
    }

    @Transactional
    public void markSucceeded(Long id) {
        Task task = repository.findById(id).orElseThrow();
        task.markSucceeded();
    }

    @Transactional
    public void markFailed(Long id) {
        Task task = repository.findById(id).orElseThrow();
        task.markFailed();
    }
}

