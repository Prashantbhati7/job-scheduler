-- name: CreateRun :one
-- if scheduler crashes after advancing next_run_at but before finishing the run,
-- on restart it will attempt the same (job_id, scheduled_for) and get DO NOTHING.
-- RETURNING id is NULL when a conflict occurs — the caller checks for this and skips (handles the idempotency case)
INSERT INTO job_runs ( job_id, scheduled_for, status, started_at, created_at) 
VALUES ( $1,$2, 'RUNNING', now(), now())
ON CONFLICT (job_id, scheduled_for) DO NOTHING
RETURNING id;

-- name: FinishRun :exec
-- Mark run as SUCCEEDED or FAILED with the final error message.
UPDATE job_runs
SET
    status      = $2,
    finished_at = now(),
    error       = $3
WHERE id = $1;

-- name: GetRunByID :one
SELECT id, job_id, scheduled_for, status, attempt, worker_id, next_retry_at, queued_at, started_at, finished_at, error, created_at
FROM job_runs
WHERE id = $1;

-- name: ListRunsForJob :many
SELECT id, job_id, scheduled_for, status, attempt, worker_id, next_retry_at, queued_at, started_at, finished_at, error, created_at
FROM job_runs
WHERE job_id = $1
ORDER BY created_at DESC
LIMIT $2;
