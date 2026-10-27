-- name: CreateRun :one
-- if scheduler crashes after advancing next_run_at but before finishing the run,
-- on restart it will attempt the same (job_id, scheduled_for) and get DO NOTHING.
-- RETURNING id is NULL when a conflict occurs — the caller checks for this and skips (handles the idempotency case)
INSERT INTO job_runs ( job_id, scheduled_for, status, started_at, created_at) 
VALUES ( $1,$2, 'RUNNING', now(), now())
ON CONFLICT (job_id, scheduled_for) DO NOTHING
RETURNING id;

-- name: FinishRun :exec
-- Mark run as SUCCEEDED with the final status.
UPDATE job_runs
SET
    status      = $2,
    finished_at = now(),
    error       = $3
WHERE id = $1;

-- name: FailRunForRetry :exec
UPDATE job_runs
SET
    status       = 'RETRYING',
    attempt      = $2,
    next_retry_at = $3,
    error        = $4,
    finished_at  = NULL
WHERE id = $1;

-- name: MarkRunDead :exec
-- Transition a RUNNING run to DEAD (all retries exhausted).
UPDATE job_runs
SET
    status      = 'DEAD',
    finished_at = now(),
    error       = $2
WHERE id = $1;

-- name: FetchDueRetries :many
SELECT
    r.id,
    r.job_id,
    r.scheduled_for,
    r.status,
    r.attempt,
    r.error,
    j.name        AS job_name,
    j.cron_expr   AS cron_expr,
    j.timezone    AS timezone,
    j.payload     AS payload,
    j.max_retries AS max_retries,
    j.timeout_seconds AS timeout_seconds
FROM job_runs r
JOIN jobs j ON r.job_id = j.id
WHERE r.status = 'RETRYING'
  AND r.next_retry_at <= now()
  AND j.deleted_at IS NULL
ORDER BY r.next_retry_at ASC
LIMIT $1
FOR UPDATE OF r SKIP LOCKED;

-- name: MarkRunQueued :exec
-- Transition a RETRYING run back to QUEUED after the sweeper re-enqueues it.
UPDATE job_runs
SET
    status        = 'RUNNING',
    next_retry_at = NULL,
    started_at    = now()
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
