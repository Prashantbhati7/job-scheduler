package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
)

type Sweeper struct {
	pool         *pgxpool.Pool
	queries      db.Querier
	writer       *kafkago.Writer
	pollInterval time.Duration
}

func NewSweeper(pool *pgxpool.Pool, writer *kafkago.Writer, pollInterval time.Duration) *Sweeper {
	return NewSweeperWithQuerier(db.New(pool), pool, writer, pollInterval)
}

func NewSweeperWithQuerier(queries db.Querier, pool *pgxpool.Pool, writer *kafkago.Writer, pollInterval time.Duration) *Sweeper {
	return &Sweeper{
		pool:         pool,
		queries:      queries,
		writer:       writer,
		pollInterval: pollInterval,
	}
}

func (s *Sweeper) Run(ctx context.Context) {
	slog.Info("Retry sweeper starting", "poll_interval", s.pollInterval.String())

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Retry sweeper stopping — context done")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
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

	for _, row := range rows {
		row := row
		go func() {
			if err := s.requeue(ctx, row); err != nil {
				slog.Error("Sweeper: failed to requeue retry",
					"run_id", pgUUIDStr(row.ID),
					"error", err,
				)
			}
		}()
	}
}

func (s *Sweeper) requeue(ctx context.Context, row db.FetchDueRetriesRow) error {
	runIDStr := pgUUIDStr(row.ID)
	jobIDStr := pgUUIDStr(row.JobID)

	markCtx, markCancel := context.WithTimeout(ctx, 5*time.Second)
	defer markCancel()

	if err := s.queries.MarkRunQueued(markCtx, row.ID); err != nil {
		return fmt.Errorf("mark run queued %s: %w", runIDStr, err)
	}

	msg := &kafka.JobMessage{
		Version:        kafka.CurrentVersion,
		RunID:          runIDStr,
		JobID:          jobIDStr,
		JobName:        row.JobName,
		CronExpr:       row.CronExpr,
		Timezone:       row.Timezone,
		Payload:        json.RawMessage(row.Payload),
		MaxRetries:     row.MaxRetries,
		TimeoutSeconds: row.TimeoutSeconds,
		Attempt:        row.Attempt, // already incremented when FailRunForRetry was called
		ScheduledFor:   row.ScheduledFor.Time,
		EnqueuedAt:     time.Now().UTC(),
	}

	value, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode retry message for run %s: %w", runIDStr, err)
	}

	publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer publishCancel()

	if err := s.writer.WriteMessages(publishCtx, kafkago.Message{
		Key:   []byte(jobIDStr),
		Value: value,
	}); err != nil {
		return fmt.Errorf("publish retry to kafka for run %s: %w", runIDStr, err)
	}

	slog.Info("Sweeper: retry requeued to Kafka",
		"run_id", runIDStr,
		"job_name", row.JobName,
		"attempt", row.Attempt,
		"scheduled_for", row.ScheduledFor.Time.Format(time.RFC3339),
	)

	return nil
}
