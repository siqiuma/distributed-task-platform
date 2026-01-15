# Distributed Task Platform

A **production-grade distributed task processing system** built with **Spring Boot, PostgreSQL, and Amazon SQS**, designed to demonstrate **correctness under concurrency, retries, and failures**.

This project intentionally focuses on **guarantees, failure handling, and scalability** rather than superficial features — the same tradeoffs made in real-world backend systems.

---

## Why This Project Exists

Most task systems fail not because of logic bugs, but because of:
- duplicate execution
- partial failures
- race conditions
- retries after crashes
- incorrect assumptions about queues

This project was built to **explicitly solve those problems** 
---

## High-Level Architecture
Client\
|\
v\
Task API\
|\
v\
PostgreSQL (Source of Truth)\
|\
| (DueTaskEnqueuer)\
v\
Amazon SQS\
|\
v\
Worker(s)\
|\
v\
TaskProcessor


---
##Core Features
-------------

### 1\. Task Creation & Scheduling

-   Create tasks with optional delayed execution (`scheduled_for`)

-   Tasks are persisted immediately in PostgreSQL

-   Supports future scheduling and retries

**Why it matters:**\
Guarantees tasks are never lost, even if the app crashes right after creation.

* * * * *

### 2\. Database-Backed Task Lifecycle

Each task follows a strictly enforced lifecycle:
PENDING → ENQUEUED → PROCESSING → SUCCEEDED

                         ↓

                       FAILED → ENQUEUED (retry)

                         ↓

                        DEAD

-   State transitions are **atomic**

-   Illegal transitions are prevented by SQL predicates

-   Database is the **single source of truth**

### 3\. Safe Task Enqueueing (DueTaskEnqueuer)

-   Periodically scans for due tasks

-   Claims tasks for enqueue using DB locks

-   Sends task IDs to SQS

-   Releases enqueue lock on failure

**Guarantee:**\
No task is enqueued twice, even if multiple enqueuers run.

* * * * *

### 4\. At-Least-Once Queue Delivery (SQS)

-   Uses SQS standard queues

-   Long polling enabled

-   Visibility timeout configured

-   Messages may be delivered more than once

**Design choice:**\
Delivery is at-least-once, correctness is enforced downstream.

* * * * *

### 5\. Exactly-Once Task Execution (Idempotency)

-   Dedicated `task_idempotency` table

-   First execution records success

-   Retries are safely ignored

**Guarantee:**\
Side effects occur exactly once, even under retries and crashes.

* * * * *

### 6\. Safe Concurrent Processing (Horizontal Scaling)

-   Multiple worker instances can run concurrently

-   Workers compete to claim tasks via atomic SQL updates

-   Only one worker can own a task at a time

**No distributed locks, no coordination service required.**

* * * * *

### 7\. Failure Handling & Retries

-   Automatic retry with backoff

-   Attempts tracked in DB

-   Rescheduled tasks re-enter the queue

-   Retries stop at `max_attempts`

* * * * *

### 8\. Dead Letter Queue (DLQ)

-   Tasks exceeding `max_attempts` are marked `DEAD`

-   Dead task metadata is published to a DLQ

-   Original SQS message is deleted to prevent poison loops

**DLQ publishing is best-effort, system correctness is preserved regardless.**

## Core Design Principles

### 1. Database Is the Source of Truth

The database owns:
- task lifecycle
- retries
- success / failure state
- idempotency
- dead-lettering

Queues are treated as **transport only**, never as authoritative state.

---

### 2. At-Least-Once Delivery, Exactly-Once Effects

| Layer | Guarantee |
|-----|----------|
| SQS delivery | At-least-once |
| Task effects | Exactly-once |
| Failure recovery | Guaranteed |

We **do not attempt exactly-once delivery**.  
Instead, we **guarantee exactly-once side effects** using database-backed idempotency.

---

## Task Lifecycle

PENDING → ENQUEUED → PROCESSING → SUCCEEDED\
↓\
FAILED → ENQUEUED (retry)\
↓\
DEAD

## Task Claiming (Concurrency Safety)

Multiple workers may receive the same SQS message.

Only **one worker can claim the task**, enforced by an atomic SQL update

Idempotency
----------------------

SQS can redeliver messages.\
Workers can crash after doing partial work.

To prevent duplicate side effects, the system uses a **dedicated idempotency table**

Execution flow:

1.  Worker claims task

2.  Worker checks idempotency table

3.  First insert wins

4.  All retries are safely ignored

This guarantees **exactly-once execution effects**, even under retries or crashes.

Failure Handling & Retries
--------------------------

### Retry Behavior

-   Failures increment `attempt_count`

-   Tasks are rescheduled with backoff

-   Retries continue until `max_attempts`

### Dead Letter Queue (DLQ)

-   Tasks exceeding `max_attempts` are marked `DEAD`

-   A structured event is published to a DLQ

-   Original SQS message is deleted to avoid poison loops

DLQ publishing is **best-effort**:

-   Even if DLQ publish fails, the system avoids infinite redelivery

Message Deletion Rule (Critical)
--------------------------------

> **A message is deleted from SQS only after the database update succeeds.**

This guarantees:

-   No lost work

-   Safe retries

-   Database state always reflects reality

* * * * *

Worker Model & Scaling 
----------------------------------

-   Each application instance runs **one worker loop**

-   Horizontal scaling = start more instances

-   No shared memory

-   No leader election

-   No coordination services

Correctness is enforced entirely by the database.

* * * * *

Long Polling
------------

Workers use **SQS long polling**:

-   Reduces empty receives

-   Lowers AWS cost

-   Prevents CPU busy loops

-   Improves latency

* * * * *

Observability
-------------

The system exposes metrics for:

-   tasks received

-   tasks processed

-   retries

-   failures

-   dead-lettered tasks

-   schedule lag

Metrics are designed to support:

-   alerting

-   capacity planning

-   debugging production incidents

* * * * *

Testing Strategy
----------------

This project includes **end-to-end integration tests** using:

-   Testcontainers

-   LocalStack (SQS)

-   PostgreSQL

Verified behaviors:

-   No double processing with multiple workers

-   Exactly-once execution under concurrency

-   Correct retry and dead-letter behavior

-   Safe handling of poison messages

* * * * *

Tech Stack
----------

-   Java 21

-   Spring Boot

-   PostgreSQL

-   Amazon SQS

-   Testcontainers

-   LocalStack

-   JUnit 5


## Features (Current)
- Create tasks via REST API
- Retrieve tasks by ID
- Cancel tasks with valid state transitions
- Input validation using Jakarta Validation
- Global error handling with proper HTTP status codes (404, 409)

## Task Lifecycle
PENDING → CANCELED
## API Endpoints

### Create a task
POST /tasks
If `scheduledFor` is omitted:

-   Task is eligible for immediate enqueue

-   Will be picked up by the next enqueuer tick
### Get task by ID
GET /tasks/{id}
### Cancel a task
PUT /tasks/{id}/cancel

### Internal (Non-Public) Worker APIs

These are **not exposed externally**, but are core system components:

| Component | Responsibility |
| --- | --- |
| `DueTaskEnqueuer` | Claims due tasks and sends to SQS |
| `SqsWorkerLoop` | Receives messages and processes tasks |
| `TaskProcessor` | Executes task logic |
| `TaskClaimRepository` | Enforces task ownership |
| `TaskIdempotencyRepository` | Ensures exactly-once effects |

## How to Run Locally

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=postgres
```
