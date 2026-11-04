DROP INDEX IF EXISTS idx_job_runs_active;

ALTER TABLE job_runs
    DROP COLUMN IF EXISTS cancelled_at,
    DROP COLUMN IF EXISTS trigger_type;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS owner_id,
    DROP COLUMN IF EXISTS consecutive_failures,
    DROP COLUMN IF EXISTS max_concurrency,
    DROP COLUMN IF EXISTS overlap_policy,
    DROP COLUMN IF EXISTS misfire_policy,
    DROP COLUMN IF EXISTS handler;

