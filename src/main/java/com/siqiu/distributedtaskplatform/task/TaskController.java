package com.siqiu.distributedtaskplatform.task;

import com.siqiu.distributedtaskplatform.task.dto.CreateTaskRequest;
import com.siqiu.distributedtaskplatform.task.dto.TaskResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/tasks")
public class TaskController {

    private final TaskService service;

    public TaskController(TaskService service) {
        this.service = service;
    }

    //POST: Create a new resource or trigger an action that is not idempotent.
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    //@RequestBody: Take the HTTP request body (JSON) and convert it into this Java object.
    public TaskResponse create(@Valid @RequestBody CreateTaskRequest request) {
        return TaskResponse.from(service.create(request));
    }

    @GetMapping("/{id}")
    //@PathVariable: Take a value from the URL path and put it into this method parameter.
    public TaskResponse get(@PathVariable Long id) {
        return TaskResponse.from(service.getOrThrow(id));
    }

    //PUT: Update an existing resource
    @PutMapping("/{id}/cancel")
    public TaskResponse cancel(@PathVariable Long id) {
        return TaskResponse.from(service.cancel(id));
    }
}
