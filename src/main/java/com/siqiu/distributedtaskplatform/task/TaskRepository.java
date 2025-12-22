package com.siqiu.distributedtaskplatform.task;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TaskRepository extends JpaRepository<Task, Long> {
    List<Task> findTop5ByStatusOrderByCreatedAt(TaskStatus status);
}