ALTER TABLE tasks
  ADD COLUMN attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN max_attempts integer NOT NULL DEFAULT 3,
  ADD COLUMN next_run_at timestamp with time zone,
  ADD COLUMN last_error text;

-- Useful index for “what should run next”
CREATE INDEX idx_tasks_pending_next_run_at
    ON tasks (next_run_at)
    WHERE status = 'PENDING';