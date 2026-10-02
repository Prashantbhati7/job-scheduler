CREATE EXTENSION IF NOT EXISTS pgcrypto;


CREATE TYPE job_run_status AS ENUM (
    'PENDING',
    'QUEUED',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'RETRYING',
    'DEAD'
);

CREATE TYPE worker_status AS ENUM (
    'ACTIVE',
    'DEAD'
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    name TEXT NOT NULL,

    cron_expr TEXT NOT NULL,

    timezone TEXT NOT NULL DEFAULT 'UTC',

    payload JSONB NOT NULL,

    priority SMALLINT NOT NULL DEFAULT 5,

    max_retries INT NOT NULL DEFAULT 3,

    timeout_seconds INT NOT NULL DEFAULT 60,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    next_run_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    deleted_at TIMESTAMPTZ
);


CREATE UNIQUE INDEX idx_jobs_name_unique
ON jobs (name)
WHERE deleted_at IS NULL;


CREATE INDEX idx_jobs_enabled_next_run
ON jobs (enabled, next_run_at)
WHERE deleted_at IS NULL;



CREATE TABLE job_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id UUID NOT NULL,

    scheduled_for TIMESTAMPTZ NOT NULL,

    status job_run_status NOT NULL DEFAULT 'PENDING',

    attempt INT NOT NULL DEFAULT 0,

    worker_id TEXT,

    next_retry_at TIMESTAMPTZ,

    queued_at TIMESTAMPTZ,

    started_at TIMESTAMPTZ,

    finished_at TIMESTAMPTZ,

    error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fk_job_runs_job
        FOREIGN KEY (job_id)
        REFERENCES jobs(id)
        ON DELETE CASCADE,

    CONSTRAINT uq_job_runs_job_scheduled
        UNIQUE (job_id, scheduled_for),

    CONSTRAINT chk_job_runs_attempt
        CHECK (attempt >= 0)
);


-- Used by the retry sweeper.
CREATE INDEX idx_job_runs_status_next_retry
ON job_runs (status, next_retry_at);


-- Used for job run history / dashboard.
CREATE INDEX idx_job_runs_job_created
ON job_runs (job_id, created_at DESC);


CREATE TABLE workers (
    id TEXT PRIMARY KEY,

    host TEXT NOT NULL,

    status worker_status NOT NULL DEFAULT 'ACTIVE',

    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);



CREATE TABLE run_logs (
    id BIGSERIAL PRIMARY KEY,

    run_id UUID NOT NULL,

    attempt INT NOT NULL,

    ts TIMESTAMPTZ NOT NULL DEFAULT now(),

    level TEXT NOT NULL,

    message TEXT NOT NULL,

    CONSTRAINT fk_run_logs_run
        FOREIGN KEY (run_id)
        REFERENCES job_runs(id)
        ON DELETE CASCADE,

    CONSTRAINT chk_run_logs_attempt
        CHECK (attempt >= 0)
);


-- Used to retrieve logs for a run in chronological order.
CREATE INDEX idx_run_logs_run_ts
ON run_logs (run_id, ts);