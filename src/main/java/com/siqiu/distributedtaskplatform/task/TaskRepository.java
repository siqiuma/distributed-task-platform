package com.siqiu.distributedtaskplatform.task;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface TaskRepository extends JpaRepository<Task, Long> {
    List<Task> findTop5ByStatusOrderByCreatedAt(TaskStatus status);

    //It tells Spring Data that the query performs an update or delete instead of a select,
    // so it executes it as a modifying query and returns the affected row count
    @Modifying
    @Query("""
    update Task t
       set t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.PROCESSING,
           t.attemptCount = t.attemptCount + 1,
           t.nextRunAt = null,
           t.lastError = null
     where t.id = :id
       and (t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.PENDING
            or t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.FAILED)
       and (t.nextRunAt is null or t.nextRunAt <= :now)
""")
    int claim(@Param("id") Long id, @Param("now") Instant now);
//@Param binds method arguments to named parameters in JPQL queries in a type-safe way
//:id and :now are named query parameters (better than positional parameters)

    @Query("""
    select t from Task t
    where (t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.PENDING
           or t.status = com.siqiu.distributedtaskplatform.task.TaskStatus.FAILED)
      and (t.nextRunAt is null or t.nextRunAt <= :now)
    order by t.createdAt
""")
    List<Task> findTop5Eligible(@Param("now") Instant now, org.springframework.data.domain.Pageable pageable);
//Pageable: To ensure queries return bounded result sets.
// It translates to LIMIT/OFFSET at the SQL level,
// preventing large result scans and keeping worker processing efficient.
}