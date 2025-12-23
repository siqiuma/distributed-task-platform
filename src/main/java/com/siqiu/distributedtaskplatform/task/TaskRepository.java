package com.siqiu.distributedtaskplatform.task;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TaskRepository extends JpaRepository<Task, Long> {
    List<Task> findTop5ByStatusOrderByCreatedAt(TaskStatus status);

    @Modifying
    @Query("update Task t set t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.PROCESSING " +
            "where t.id = :id and t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.PENDING")
    int claim(@Param("id") Long id);
}