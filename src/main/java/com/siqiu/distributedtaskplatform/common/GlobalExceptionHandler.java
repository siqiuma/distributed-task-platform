package com.siqiu.distributedtaskplatform.common;

import com.siqiu.distributedtaskplatform.task.InvalidTaskStateException;
import com.siqiu.distributedtaskplatform.task.TaskNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(TaskNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleTaskNotFound(TaskNotFoundException ex) {
        return Map.of("message", ex.getMessage());
    }

    @ExceptionHandler(InvalidTaskStateException.class)
    //@ResponseStatus: When this exception happens, return HTTP status 409(conflict) instead of the default.
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> handleInvalidTaskState(InvalidTaskStateException ex) {
        return Map.of("message", ex.getMessage());
    }
}
