-- name: CreateRun :one
-- Atomically create a run with status PENDING within the scheduler's tick transaction.
-- If a crash happens before advancing next_run_at or if two schedulers race,
-- ON CONFLICT (job_id, scheduled_for) DO NOTHING prevents duplicate runs.
INSERT INTO job_runs (job_id, scheduled_for, status, created_at)
VALUES ($1, $2, 'PENDING', now())
ON CONFLICT (job_id, scheduled_for) DO NOTHING
RETURNING id;

-- name: MarkRunQueued :execrows
-- Transition a PENDING or RETRYING run to QUEUED after successful Kafka publish.
UPDATE job_runs
SET
    status        = 'QUEUED',
    queued_at     = now(),
    next_retry_at = NULL
WHERE id = $1 AND status IN ('PENDING', 'RETRYING');

-- name: ClaimRun :one
-- Atomically claim a run for execution when consumed by a worker.
-- Transition from QUEUED to RUNNING and assign worker_id.
UPDATE job_runs
SET
    status     = 'RUNNING',
    worker_id  = $2,
    started_at = now()
WHERE id = $1 AND status = 'QUEUED'
RETURNING id, job_id, scheduled_for, status, attempt, worker_id;

-- name: FinishRun :execrows
-- Mark run as SUCCEEDED with fencing guard: must match current worker_id and attempt.
UPDATE job_runs
SET
    status      = $2,
    finished_at = now(),
    error       = $3
WHERE id = $1
  AND status = 'RUNNING'
  AND worker_id = $4
  AND attempt = $5;

-- name: FailRunForRetry :execrows
-- Transition run to RETRYING with fencing guard: must match current worker_id and attempt.
UPDATE job_runs
SET
    status        = 'RETRYING',
    attempt       = sqlc.arg(new_attempt),
    next_retry_at = sqlc.arg(next_retry_at),
    error         = sqlc.arg(error),
    finished_at   = NULL
WHERE id = sqlc.arg(id)
  AND status = 'RUNNING'
  AND worker_id = sqlc.arg(worker_id)
  AND attempt = sqlc.arg(current_attempt);

-- name: MarkRunDead :execrows
-- Transition a RUNNING run to DEAD when retries are exhausted, with fencing guard.
UPDATE job_runs
SET
    status      = 'DEAD',
    finished_at = now(),
    error       = $2
WHERE id = $1
  AND status = 'RUNNING'
  AND worker_id = $3
  AND attempt = $4;

-- name: FetchDueRetries :many
-- Fetch runs ready to be retried (status = RETRYING and next_retry_at <= now()).
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

-- name: FetchStuckPendingRuns :many
-- Fetch runs that were committed as PENDING but never published to Kafka (older than 5s).
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
WHERE r.status = 'PENDING'
  AND r.created_at <= (now() - interval '5 seconds')
  AND j.deleted_at IS NULL
ORDER BY r.created_at ASC
LIMIT $1
FOR UPDATE OF r SKIP LOCKED;

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
