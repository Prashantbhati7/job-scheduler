package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/cronutil"
)

// Scheduler polls the jobs table for due entries, creates a run, executes the
// handler, and updates the run status. In Step 1 this is a single-process loop;
// Kafka, Redis locks, and leader election are wired in later steps.
type Scheduler struct {
	pool         *pgxpool.Pool
	queries      db.Querier
	pollInterval time.Duration
	handler      Handler
}

// New creates a Scheduler backed by the given pool with default DummyHandler.
func New(pool *pgxpool.Pool, pollInterval time.Duration) *Scheduler {
	return NewWithQuerier(db.New(pool), pool, pollInterval, DummyHandler)
}

// NewWithQuerier creates a Scheduler with a specified Querier and Handler.
// This allows tests to supply a mock Querier and custom test Handler without
// needing a real database or network connections.
func NewWithQuerier(queries db.Querier, pool *pgxpool.Pool, pollInterval time.Duration, handler Handler) *Scheduler {
	if handler == nil {
		handler = DummyHandler
	}
	return &Scheduler{
		pool:         pool,
		queries:      queries,
		pollInterval: pollInterval,
		handler:      handler,
	}
}

// Run starts the polling loop. It returns only when ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	slog.Info("Scheduler loop starting",
		"poll_interval", s.pollInterval.String(),
	)

	// On startup, ensure every enabled job has next_run_at set.
	if err := s.bootstrapNextRuns(ctx); err != nil {
		// Not fatal — jobs may still have next_run_at from a previous run.
		slog.Warn("Bootstrap of next_run_at encountered errors", "error", err)
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Scheduler loop stopping — context cancelled")
			return nil
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick performs one poll cycle: fetch due jobs and process each one.
func (s *Scheduler) tick(ctx context.Context) {
	// Limit each tick to 100 jobs; on large backlogs we don't want one tick
	// to hold locks for too long.
	const batchSize = 100

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobs, err := s.queries.FetchDueJobs(queryCtx, batchSize)
	if err != nil {
		slog.Error("Failed to fetch due jobs", "error", err)
		return
	}

	if len(jobs) == 0 {
		return // nothing due
	}

	slog.Info("Tick: due jobs found", "count", len(jobs))

	for _, job := range jobs {
		// Each job processed in its own goroutine so one slow job doesn't
		// stall others. Step 3 will add proper worker-pool concurrency.
		go func(j db.Job) {
			if err := s.processJob(ctx, j); err != nil {
				slog.Error("Failed to process job",
					"job_id", pgUUIDStr(j.ID),
					"job_name", j.Name,
					"error", err,
				)
			}
		}(job)
	}
}

// processJob handles a single due job:
//  1. Compute and write the next cron tick (advance the schedule).
//  2. Create a job_run row (idempotent via ON CONFLICT DO NOTHING).
//  3. Execute the dummy handler inside a panic-safe wrapper.
//  4. Mark the run SUCCEEDED or FAILED.
func (s *Scheduler) processJob(ctx context.Context, job db.Job) error {
	jobIDStr := pgUUIDStr(job.ID)

	// --- 1. Advance next_run_at ---
	// Use the current scheduled tick as the base, not time.Now(), so the
	// schedule doesn't drift when the scheduler is slow or restarting.
	scheduledFor := job.NextRunAt.Time

	nextRun, err := cronutil.NextRun(job.CronExpr, job.Timezone, scheduledFor)
	if err != nil {
		return fmt.Errorf("cron parse error for job %s (%s): %w", jobIDStr, job.CronExpr, err)
	}

	advanceCtx, advCancel := context.WithTimeout(ctx, 5*time.Second)
	defer advCancel()

	if err := s.queries.AdvanceNextRun(advanceCtx, db.AdvanceNextRunParams{
		ID:        job.ID,
		NextRunAt: pgtype.Timestamptz{Time: nextRun, Valid: true},
	}); err != nil {
		return fmt.Errorf("advance next_run_at for job %s: %w", jobIDStr, err)
	}

	slog.Info("Job scheduled for next run",
		"job_id", jobIDStr,
		"job_name", job.Name,
		"next_run_at", nextRun.Format(time.RFC3339),
	)

	// --- 2. Create run (idempotent) ---
	createCtx, createCancel := context.WithTimeout(ctx, 5*time.Second)
	defer createCancel()

	runID, err := s.queries.CreateRun(createCtx, db.CreateRunParams{
		JobID:        job.ID,
		ScheduledFor: pgtype.Timestamptz{Time: scheduledFor, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create run for job %s: %w", jobIDStr, err)
	}

	// ON CONFLICT DO NOTHING returns an empty UUID when a duplicate exists.
	// This means the scheduler already processed this tick (e.g. after a crash
	// and restart). Skip handler execution — idempotency preserved.
	if !runID.Valid {
		slog.Info("Run already exists for this tick — skipping (idempotent)",
			"job_id", jobIDStr,
			"scheduled_for", scheduledFor.Format(time.RFC3339),
		)
		return nil
	}

	runIDStr := pgUUIDStr(runID)
	slog.Info("Run created",
		"run_id", runIDStr,
		"job_id", jobIDStr,
		"job_name", job.Name,
		"scheduled_for", scheduledFor.Format(time.RFC3339),
	)

	// --- 3. Execute handler ---
	handler := s.handler
	if handler == nil {
		handler = DummyHandler
	}
	handlerErr := runWithRecover(ctx, job, handler)

	// --- 4. Finish run ---
	finishCtx, finishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer finishCancel()

	status := db.JobRunStatusSUCCEEDED
	var errMsg *string

	if handlerErr != nil {
		status = db.JobRunStatusFAILED
		msg := handlerErr.Error()
		errMsg = &msg
	}

	if err := s.queries.FinishRun(finishCtx, db.FinishRunParams{
		ID:     runID,
		Status: status,
		Error:  errMsg,
	}); err != nil {
		return fmt.Errorf("finish run %s: %w", runIDStr, err)
	}

	if handlerErr != nil {
		slog.Error("Run finished with error",
			"run_id", runIDStr,
			"job_name", job.Name,
			"status", status,
			"error", handlerErr,
		)
	} else {
		slog.Info("Run finished successfully",
			"run_id", runIDStr,
			"job_name", job.Name,
			"status", status,
		)
	}

	return nil
}

// bootstrapNextRuns sets next_run_at for any enabled job where it is still NULL.
// This handles jobs inserted via psql/seed before the API is built.
func (s *Scheduler) bootstrapNextRuns(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.queries.FetchUnscheduledJobs(queryCtx)
	if err != nil {
		return fmt.Errorf("bootstrap query: %w", err)
	}

	now := time.Now().UTC()

	for _, row := range rows {
		next, err := cronutil.NextRun(row.CronExpr, row.Timezone, now)
		if err != nil {
			slog.Warn("Bootstrap: invalid cron expr",
				"job_id", pgUUIDStr(row.ID),
				"expr", row.CronExpr,
				"error", err,
			)
			continue
		}

		setCtx, setCancel := context.WithTimeout(ctx, 5*time.Second)
		defer setCancel()

		if err := s.queries.SetInitialNextRun(setCtx, db.SetInitialNextRunParams{
			ID:        row.ID,
			NextRunAt: pgtype.Timestamptz{Time: next, Valid: true},
		}); err != nil {
			slog.Warn("Bootstrap: failed to set next_run_at",
				"job_id", pgUUIDStr(row.ID),
				"error", err,
			)
		} else {
			slog.Info("Bootstrap: next_run_at initialised",
				"job_id", pgUUIDStr(row.ID),
				"next_run_at", next.Format(time.RFC3339),
			)
		}
	}

	return nil
}

// runWithRecover executes the handler and catches panics, converting them to errors.
// Why? A panicking handler would crash the entire scheduler process. In production
// each job runs in a worker (Step 3); in Step 1 we at least protect the loop.
func runWithRecover(ctx context.Context, job db.Job, fn func(context.Context, db.Job) error) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return fn(ctx, job)
}

// pgUUIDStr converts a pgtype.UUID to its canonical string representation.
func pgUUIDStr(u pgtype.UUID) string {
	if !u.Valid {
		return "<invalid>"
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// payloadPreview returns the first 80 chars of the JSON payload for logging.
func payloadPreview(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}
	s := string(raw)
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}
