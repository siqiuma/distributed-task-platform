ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS worker_id TEXT;

-- Backfill scheduled_for for existing rows so your lag metric works
UPDATE tasks
SET scheduled_for = created_at
WHERE scheduled_for IS NULL;

-- Helpful indexes for worker claim + dashboards
CREATE INDEX IF NOT EXISTS idx_tasks_status_scheduled_for
    ON tasks (status, scheduled_for);

CREATE INDEX IF NOT EXISTS idx_tasks_next_run_at
    ON tasks (next_run_at);

CREATE INDEX IF NOT EXISTS idx_tasks_worker_id
    ON tasks (worker_id);
