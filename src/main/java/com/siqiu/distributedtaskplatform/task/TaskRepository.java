package com.siqiu.distributedtaskplatform.task;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;


public interface TaskRepository extends JpaRepository<Task, Long> {
    // =========================
    // DB MODE (polling) queries
    // =========================

    /**
     * Claim a task for DB-mode worker (PENDING/FAILED that are due).
     * Returns number of updated rows (1 = claimed, 0 = someone else got it or not due).
     */

    //It tells Spring Data that the query performs an update or delete instead of a select,
    // so it executes it as a modifying query and returns the affected row count
    @Modifying(clearAutomatically = true, flushAutomatically = true)
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

    @Query("""
        select new com.siqiu.distributedtaskplatform.task.TaskSnapshot(
            t.id, t.type, t.status, t.attemptCount, t.maxAttempts, t.nextRunAt, t.lastError
        )
        from Task t
        where t.id = :id
    """)
    TaskSnapshot findSnapshotById(@Param("id") Long id);
}