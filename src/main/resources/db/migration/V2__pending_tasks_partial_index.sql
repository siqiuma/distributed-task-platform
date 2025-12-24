-- Speeds up: SELECT ... FROM tasks WHERE status='PENDING' ORDER BY created_at LIMIT N
-- Because the WHERE clause fixes status='PENDING', we only need created_at (and id as a tiebreaker).
CREATE INDEX idx_tasks_pending_created_at_id
    ON tasks (created_at, id)
    WHERE status = 'PENDING';
