package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/cronutil"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
)

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

type Scheduler struct {
	pool         *pgxpool.Pool
	queries      db.Querier
	pollInterval time.Duration
	writer       KafkaWriter
}

func New(pool *pgxpool.Pool, pollInterval time.Duration, writer KafkaWriter) *Scheduler {
	return NewWithQuerier(db.New(pool), pool, pollInterval, writer)
}

// NewWithQuerier allows injecting a mock Querier for unit tests.
func NewWithQuerier(queries db.Querier, pool *pgxpool.Pool, pollInterval time.Duration, writer KafkaWriter) *Scheduler {
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

type pendingJobToPublish struct {
	job          db.FetchDueJobsRow
	runID        pgtype.UUID
	scheduledFor time.Time
}

func (s *Scheduler) tick(ctx context.Context) {
	const batchSize = 100

	toPublish, err := s.fetchAndScheduleTx(ctx, batchSize)
	if err != nil {
		slog.Error("Failed to fetch and schedule due jobs in tx", "error", err)
		return
	}

	if len(toPublish) == 0 {
		return
	}

	slog.Info("Tick: jobs committed as PENDING — publishing to Kafka", "count", len(toPublish))

	// Bounded concurrency pool for Kafka publishing
	const maxConcurrency = 10
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, item := range toPublish {
		item := item
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if err := s.publishAndMarkQueued(ctx, item); err != nil {
				slog.Error("Failed to publish job to Kafka (will be recovered by sweeper)",
					"run_id", pgUUIDStr(item.runID),
					"job_id", pgUUIDStr(item.job.ID),
					"job_name", item.job.Name,
					"error", err,
				)
			}
		}()
	}

	wg.Wait()
}

// fetchAndScheduleTx performs atomic fetch, misfire-safe next_run_at advancement,
// and PENDING run creation within a single database transaction.
func (s *Scheduler) fetchAndScheduleTx(ctx context.Context, batchSize int32) ([]pendingJobToPublish, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var q db.Querier = s.queries
	var tx pgx.Tx
	var err error

	if s.pool != nil {
		tx, err = s.pool.Begin(queryCtx)
		if err != nil {
			return nil, fmt.Errorf("begin tx: %w", err)
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(context.Background())
			}
		}()
		q = db.New(tx)
	}

	jobs, err := q.FetchDueJobs(queryCtx, batchSize)
	if err != nil {
		return nil, fmt.Errorf("fetch due jobs: %w", err)
	}

	if len(jobs) == 0 {
		if tx != nil {
			_ = tx.Commit(queryCtx)
			tx = nil
		}
		return nil, nil
	}

	now := time.Now().UTC()
	var toPublish []pendingJobToPublish

	for _, job := range jobs {
		jobIDStr := pgUUIDStr(job.ID)
		scheduledFor := job.NextRunAt.Time

		// Misfire policy: fire once for scheduled tick, but advance next_run_at from now
		// to jump directly to the next future tick and avoid cascading bursts after downtime.
		nextRun, err := cronutil.NextRun(job.CronExpr, job.Timezone, now)
		if err != nil {
			slog.Error("Invalid cron expression on due job — skipping advance",
				"job_id", jobIDStr,
				"cron_expr", job.CronExpr,
				"error", err,
			)
			continue
		}

		if err := q.AdvanceNextRun(queryCtx, db.AdvanceNextRunParams{
			ID:        job.ID,
			NextRunAt: pgtype.Timestamptz{Time: nextRun, Valid: true},
		}); err != nil {
			return nil, fmt.Errorf("advance next_run_at for job %s: %w", jobIDStr, err)
		}

		runID, err := q.CreateRun(queryCtx, db.CreateRunParams{
			JobID:        job.ID,
			ScheduledFor: pgtype.Timestamptz{Time: scheduledFor, Valid: true},
		})
		if err != nil {
			return nil, fmt.Errorf("create run for job %s: %w", jobIDStr, err)
		}

		if runID.Valid {
			toPublish = append(toPublish, pendingJobToPublish{
				job:          job,
				runID:        runID,
				scheduledFor: scheduledFor,
			})
		}
	}

	if tx != nil {
		if err := tx.Commit(queryCtx); err != nil {
			return nil, fmt.Errorf("commit tx: %w", err)
		}
		tx = nil
	}

	return toPublish, nil
}

func (s *Scheduler) publishAndMarkQueued(ctx context.Context, item pendingJobToPublish) error {
	runIDStr := pgUUIDStr(item.runID)
	jobIDStr := pgUUIDStr(item.job.ID)

	msg := &kafka.JobMessage{
		Version:        kafka.CurrentVersion,
		RunID:          runIDStr,
		JobID:          jobIDStr,
		JobName:        item.job.Name,
		CronExpr:       item.job.CronExpr,
		Timezone:       item.job.Timezone,
		Payload:        json.RawMessage(item.job.Payload),
		MaxRetries:     item.job.MaxRetries,
		TimeoutSeconds: item.job.TimeoutSeconds,
		Attempt:        0,
		ScheduledFor:   item.scheduledFor,
		EnqueuedAt:     time.Now().UTC(),
	}

	value, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode kafka message: %w", err)
	}

	pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pubCancel()

	if err := s.writer.WriteMessages(pubCtx, kafkago.Message{
		Key:   []byte(jobIDStr),
		Value: value,
	}); err != nil {
		return fmt.Errorf("kafka write message: %w", err)
	}

	// Transition PENDING to QUEUED only after successful Kafka publish.
	markCtx, markCancel := context.WithTimeout(ctx, 5*time.Second)
	defer markCancel()

	if _, err := s.queries.MarkRunQueued(markCtx, item.runID); err != nil {
		slog.Warn("Failed to mark run QUEUED after kafka publish (sweeper will recover)",
			"run_id", runIDStr,
			"error", err,
		)
	}

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

func pgUUIDStr(u pgtype.UUID) string {
	if !u.Valid {
		return "<invalid>"
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
