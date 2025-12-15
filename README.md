# Distributed Task Platform

A backend-focused Spring Boot project built to practice real-world backend engineering concepts such as RESTful API design, domain modeling, validation, and error handling.

## Tech Stack
- Java 21
- Spring Boot 4.x
- Spring Data JPA
- H2 (in-memory database)
- Maven

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
### Get task by ID
GET /tasks/{id}
### Cancel a task
PUT /tasks/{id}/cancel
## How to Run Locally

```bash
./mvnw spring-boot:run
```