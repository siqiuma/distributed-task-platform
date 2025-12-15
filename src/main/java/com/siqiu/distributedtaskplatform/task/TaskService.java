package com.siqiu.distributedtaskplatform.task;

import com.siqiu.distributedtaskplatform.task.dto.CreateTaskRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TaskService {

    private final TaskRepository repository;

    public TaskService(TaskRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Task create(CreateTaskRequest request) {
        Task task = new Task(request.getType(), request.getPayload());
        return repository.save(task);
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
