package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
)

type Sweeper struct {
	pool         *pgxpool.Pool
	queries      db.Querier
	writer       KafkaWriter
	pollInterval time.Duration
}

func NewSweeper(pool *pgxpool.Pool, writer KafkaWriter, pollInterval time.Duration) *Sweeper {
	return NewSweeperWithQuerier(db.New(pool), pool, writer, pollInterval)
}

func NewSweeperWithQuerier(queries db.Querier, pool *pgxpool.Pool, writer KafkaWriter, pollInterval time.Duration) *Sweeper {
	return &Sweeper{
		pool:         pool,
		queries:      queries,
		writer:       writer,
		pollInterval: pollInterval,
	}
}

func (s *Sweeper) Run(ctx context.Context) {
	slog.Info("Outbox & retry sweeper starting", "poll_interval", s.pollInterval.String())

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Outbox & retry sweeper stopping — context done")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
	s.sweepRetries(ctx)
	s.sweepStuckPending(ctx)
}

func (s *Sweeper) sweepRetries(ctx context.Context) {
	const batchSize = 100

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.queries.FetchDueRetries(queryCtx, batchSize)
	if err != nil {
		slog.Error("Sweeper: failed to fetch due retries", "error", err)
		return
	}

	if len(rows) == 0 {
		return
	}

	slog.Info("Sweeper: due retries found", "count", len(rows))

	var wg sync.WaitGroup
	for _, row := range rows {
		row := row
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.requeueRun(ctx, row.ID, row.JobID, row.JobName, row.CronExpr, row.Timezone, row.Payload, row.MaxRetries, row.TimeoutSeconds, row.Attempt, row.ScheduledFor.Time, "retry"); err != nil {
				slog.Error("Sweeper: failed to requeue retry",
					"run_id", pgUUIDStr(row.ID),
					"error", err,
				)
			}
		}()
	}
	wg.Wait()
}

func (s *Sweeper) sweepStuckPending(ctx context.Context) {
	const batchSize = 100

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.queries.FetchStuckPendingRuns(queryCtx, batchSize)
	if err != nil {
		slog.Error("Sweeper: failed to fetch stuck pending runs", "error", err)
		return
	}

	if len(rows) == 0 {
		return
	}

	slog.Info("Sweeper: stuck PENDING runs found (outbox recovery)", "count", len(rows))

	var wg sync.WaitGroup
	for _, row := range rows {
		row := row
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.requeueRun(ctx, row.ID, row.JobID, row.JobName, row.CronExpr, row.Timezone, row.Payload, row.MaxRetries, row.TimeoutSeconds, row.Attempt, row.ScheduledFor.Time, "stuck_pending"); err != nil {
				slog.Error("Sweeper: failed to recover stuck pending run",
					"run_id", pgUUIDStr(row.ID),
					"error", err,
				)
			}
		}()
	}
	wg.Wait()
}

func (s *Sweeper) requeueRun(
	ctx context.Context,
	runID pgtype.UUID,
	jobID pgtype.UUID,
	jobName string,
	cronExpr string,
	timezone string,
	payload []byte,
	maxRetries int32,
	timeoutSeconds int32,
	attempt int32,
	scheduledFor time.Time,
	source string,
) error {
	runIDStr := pgUUIDStr(runID)
	jobIDStr := pgUUIDStr(jobID)

	msg := &kafka.JobMessage{
		Version:        kafka.CurrentVersion,
		RunID:          runIDStr,
		JobID:          jobIDStr,
		JobName:        jobName,
		CronExpr:       cronExpr,
		Timezone:       timezone,
		Payload:        json.RawMessage(payload),
		MaxRetries:     maxRetries,
		TimeoutSeconds: timeoutSeconds,
		Attempt:        attempt,
		ScheduledFor:   scheduledFor,
		EnqueuedAt:     time.Now().UTC(),
	}

	value, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode message for run %s: %w", runIDStr, err)
	}

	// 1. Publish to Kafka first!
	publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer publishCancel()

	if err := s.writer.WriteMessages(publishCtx, kafkago.Message{
		Key:   []byte(jobIDStr),
		Value: value,
	}); err != nil {
		return fmt.Errorf("publish to kafka for run %s: %w", runIDStr, err)
	}

	// 2. Only transition to QUEUED after Kafka ACK succeeds.
	markCtx, markCancel := context.WithTimeout(ctx, 5*time.Second)
	defer markCancel()

	if _, err := s.queries.MarkRunQueued(markCtx, runID); err != nil {
		return fmt.Errorf("mark run queued %s: %w", runIDStr, err)
	}

	slog.Info("Sweeper: run successfully requeued to Kafka",
		"run_id", runIDStr,
		"job_name", jobName,
		"attempt", attempt,
		"source", source,
		"scheduled_for", scheduledFor.Format(time.RFC3339),
	)

	return nil
}
