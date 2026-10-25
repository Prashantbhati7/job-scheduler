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
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/cronutil"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
)


type Scheduler struct {
	pool         *pgxpool.Pool
	queries      db.Querier
	pollInterval time.Duration
	writer       *kafkago.Writer
}


func New(pool *pgxpool.Pool, pollInterval time.Duration, writer *kafkago.Writer) *Scheduler {
	return NewWithQuerier(db.New(pool), pool, pollInterval, writer)
}

// injecting a mock Querier for unit tests.
func NewWithQuerier(queries db.Querier, pool *pgxpool.Pool, pollInterval time.Duration, writer *kafkago.Writer) *Scheduler {
	return &Scheduler{
		pool:         pool,
		queries:      queries,
		pollInterval: pollInterval,
		writer:       writer,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	slog.Info("Scheduler loop starting", "poll_interval", s.pollInterval.String())
	
	if err := s.bootstrapNextRuns(ctx); err != nil {
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

func (s *Scheduler) tick(ctx context.Context) {
	const batchSize = 100

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobs, err := s.queries.FetchDueJobs(queryCtx, batchSize)
	if err != nil {
		slog.Error("Failed to fetch due jobs", "error", err)
		return
	}

	if len(jobs) == 0 {
		return
	}

	slog.Info("Tick: due jobs found", "count", len(jobs))

	for _, job := range jobs {
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


func (s *Scheduler) processJob(ctx context.Context, job db.Job) error {
	jobIDStr := pgUUIDStr(job.ID)
	scheduledFor := job.NextRunAt.Time

	nextRun, err := cronutil.NextRun(job.CronExpr, job.Timezone, scheduledFor)
	if err != nil {
		return fmt.Errorf("cron parse error for job %s (%s): %w", jobIDStr, job.CronExpr, err)
	}

	advCtx, advCancel := context.WithTimeout(ctx, 5*time.Second)
	defer advCancel()

	if err := s.queries.AdvanceNextRun(advCtx, db.AdvanceNextRunParams{
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

	createCtx, createCancel := context.WithTimeout(ctx, 5*time.Second)
	defer createCancel()

	runID, err := s.queries.CreateRun(createCtx, db.CreateRunParams{
		JobID:        job.ID,
		ScheduledFor: pgtype.Timestamptz{Time: scheduledFor, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create run for job %s: %w", jobIDStr, err)
	}

	if !runID.Valid {
		slog.Info("Run already exists for this tick — skipping (idempotent)",
			"job_id", jobIDStr,
			"scheduled_for", scheduledFor.Format(time.RFC3339),
		)
		return nil
	}

	runIDStr := pgUUIDStr(runID)
	slog.Info("Run row created — publishing to Kafka",
		"run_id", runIDStr,
		"job_id", jobIDStr,
		"job_name", job.Name,
		"scheduled_for", scheduledFor.Format(time.RFC3339),
	)

	// 3. Publish to Kafka. Workers consume and execute.
	msg := &kafka.JobMessage{
		Version:        kafka.CurrentVersion,
		RunID:          runIDStr,
		JobID:          jobIDStr,
		JobName:        job.Name,
		CronExpr:       job.CronExpr,
		Timezone:       job.Timezone,
		Payload:        json.RawMessage(job.Payload),
		MaxRetries:     job.MaxRetries,
		TimeoutSeconds: job.TimeoutSeconds,
		ScheduledFor:   scheduledFor,
		EnqueuedAt:     time.Now().UTC(),
	}

	value, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode kafka message for run %s: %w", runIDStr, err)
	}

	// Key by job_id so all runs for a given job always land on the same partition, preserving ordering within a single job.
	publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer publishCancel()

	if err := s.writer.WriteMessages(publishCtx, kafkago.Message{
		Key:   []byte(jobIDStr),
		Value: value,
	}); err != nil {
		return fmt.Errorf("publish to kafka for run %s: %w", runIDStr, err)
	}

	slog.Info("JobMessage published to Kafka",
		"run_id", runIDStr,
		"job_id", jobIDStr,
		"topic", s.writer.Topic,
	)

	return nil
}

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

func runWithRecover(ctx context.Context, job db.Job, fn func(context.Context, db.Job) error) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return fn(ctx, job)
}

func pgUUIDStr(u pgtype.UUID) string {
	if !u.Valid {
		return "<invalid>"
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

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
