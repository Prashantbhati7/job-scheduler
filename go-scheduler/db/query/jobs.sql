-- name: FetchDueJobs :many
-- ordered by priority (lower = higher priority).
-- FOR UPDATE SKIP LOCKED: if multiple scheduler instances run, each grabs a
-- non-overlapping subset without blocking — standard Postgres advisory-locking pattern.
SELECT
    id, name, cron_expr, timezone, payload, priority, max_retries, timeout_seconds,
    enabled, next_run_at, handler, misfire_policy, overlap_policy, max_concurrency,
    consecutive_failures, owner_id, created_at, updated_at, deleted_at
FROM jobs
WHERE
    enabled = true
    AND deleted_at IS NULL
    AND next_run_at <= now()
ORDER BY priority ASC, next_run_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: AdvanceNextRun :exec
-- Always use the computed next run as the base to advance the schedule.
UPDATE jobs
SET
    next_run_at = $2,
    updated_at  = now()
WHERE id = $1;

-- name: GetJobByID :one
SELECT
    id, name, cron_expr, timezone, payload, priority, max_retries, timeout_seconds,
    enabled, next_run_at, handler, misfire_policy, overlap_policy, max_concurrency,
    consecutive_failures, owner_id, created_at, updated_at, deleted_at
FROM jobs
WHERE id = $1 AND deleted_at IS NULL;

-- name: SetInitialNextRun :exec
UPDATE jobs
SET
    next_run_at = $2,
    updated_at  = now()
WHERE id = $1 AND next_run_at IS NULL;

-- name: FetchUnscheduledJobs :many
SELECT id, cron_expr, timezone
FROM jobs
WHERE enabled = true
  AND deleted_at IS NULL
  AND next_run_at IS NULL;
