package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/retry"
)

type Handler func(ctx context.Context, msg *kafka.JobMessage) error

type Consumer struct {
	reader    *kafkago.Reader
	dlqWriter *kafkago.Writer // publishes exhausted runs to the DLQ topic
	pool      *pgxpool.Pool
	queries   db.Querier
	handler   Handler
	workerID  string
}

func New(reader *kafkago.Reader, dlqWriter *kafkago.Writer, pool *pgxpool.Pool, handler Handler) *Consumer {
	return &Consumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		pool:      pool,
		queries:   db.New(pool),
		handler:   handler,
		workerID:  workerIDFromEnv(),
	}
}

func NewWithQuerier(reader *kafkago.Reader, dlqWriter *kafkago.Writer, pool *pgxpool.Pool, queries db.Querier, handler Handler) *Consumer {
	return &Consumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		pool:      pool,
		queries:   queries,
		handler:   handler,
		workerID:  workerIDFromEnv(),
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	slog.Info("Worker consumer loop starting",
		"worker_id", c.workerID,
		"topic", c.reader.Config().Topic,
		"group_id", c.reader.Config().GroupID,
	)

	for {
		kafkaMsg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.Info("Worker consumer loop stopping — context done")
				return nil
			}
			slog.Error("Failed to fetch Kafka message", "error", err)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}

		msg, err := kafka.DecodeJobMessage(kafkaMsg.Value)
		if err != nil {
			slog.Error("Malformed Kafka message — skipping",
				"offset", kafkaMsg.Offset,
				"partition", kafkaMsg.Partition,
				"error", err,
			)
			_ = c.reader.CommitMessages(ctx, kafkaMsg)
			continue
		}

		slog.Info("Worker received job",
			"run_id", msg.RunID,
			"job_id", msg.JobID,
			"job_name", msg.JobName,
			"attempt", msg.Attempt,
			"worker_id", c.workerID,
		)

		c.executeAndPersist(ctx, msg)

		// Commit offset after DB is updated — at-least-once delivery guarantee.
		if err := c.reader.CommitMessages(ctx, kafkaMsg); err != nil {
			slog.Error("Failed to commit Kafka offset",
				"run_id", msg.RunID,
				"error", err,
			)
		}
	}
}

func (c *Consumer) executeAndPersist(ctx context.Context, msg *kafka.JobMessage) {
	runID, err := parseUUID(msg.RunID)
	if err != nil {
		slog.Error("Invalid run_id in message — cannot persist result",
			"run_id", msg.RunID,
			"error", err,
		)
		return
	}

	timeout := time.Duration(msg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	handlerCtx, handlerCancel := context.WithTimeout(ctx, timeout)
	defer handlerCancel()

	handlerErr := c.runSafe(handlerCtx, msg)

	if handlerErr == nil {
		// Happy path: mark succeeded.
		c.finishSucceeded(ctx, runID, msg)
		return
	}

	// Failure path: decide between retry and dead-letter.
	nextAttempt := msg.Attempt + 1

	slog.Warn("Job handler failed",
		"run_id", msg.RunID,
		"job_id", msg.JobID,
		"job_name", msg.JobName,
		"attempt", msg.Attempt,
		"max_retries", msg.MaxRetries,
		"error", handlerErr,
	)

	if nextAttempt < msg.MaxRetries {
		// More retries allowed — schedule next attempt with backoff.
		c.scheduleRetry(ctx, runID, msg, nextAttempt, handlerErr)
	} else {
		// All retries exhausted — dead-letter the run.
		c.deadLetter(ctx, runID, msg, handlerErr)
	}
}

// finishSucceeded marks the run as SUCCEEDED in the DB.
func (c *Consumer) finishSucceeded(ctx context.Context, runID pgtype.UUID, msg *kafka.JobMessage) {
	finishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.queries.FinishRun(finishCtx, db.FinishRunParams{
		ID:     runID,
		Status: db.JobRunStatusSUCCEEDED,
		Error:  nil,
	}); err != nil {
		slog.Error("Failed to mark run SUCCEEDED", "run_id", msg.RunID, "error", err)
		return
	}
	slog.Info("Run SUCCEEDED",
		"run_id", msg.RunID,
		"job_name", msg.JobName,
		"attempt", msg.Attempt,
	)
}

// scheduleRetry transitions the run to RETRYING and writes next_retry_at.
// The sweeper (in scheduler/sweeper.go) re-enqueues it to Kafka when the delay elapses.
func (c *Consumer) scheduleRetry(ctx context.Context, runID pgtype.UUID, msg *kafka.JobMessage, nextAttempt int32, cause error) {
	// Full jitter exponential backoff — desynchronizes retries after bulk failures.
	nextRetryAt := retry.NextRetryAt(nextAttempt, retry.DefaultBase, retry.DefaultMaxCap)
	errStr := cause.Error()

	retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.queries.FailRunForRetry(retryCtx, db.FailRunForRetryParams{
		ID:          runID,
		Attempt:     nextAttempt,
		NextRetryAt: pgtype.Timestamptz{Time: nextRetryAt, Valid: true},
		Error:       &errStr,
	}); err != nil {
		slog.Error("Failed to schedule retry in DB", "run_id", msg.RunID, "error", err)
		return
	}

	slog.Warn("Run scheduled for retry (RETRYING)",
		"run_id", msg.RunID,
		"job_name", msg.JobName,
		"attempt", msg.Attempt,
		"next_attempt", nextAttempt,
		"max_retries", msg.MaxRetries,
		"next_retry_at", nextRetryAt.Format(time.RFC3339),
		"cause", cause,
	)
}

// deadLetter marks the run as DEAD in Postgres and publishes a DeadLetterMessage
// to the DLQ topic so operators can inspect or replay it.
func (c *Consumer) deadLetter(ctx context.Context, runID pgtype.UUID, msg *kafka.JobMessage, cause error) {
	errStr := cause.Error()

	deadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.queries.MarkRunDead(deadCtx, db.MarkRunDeadParams{
		ID:    runID,
		Error: &errStr,
	}); err != nil {
		slog.Error("Failed to mark run DEAD in DB", "run_id", msg.RunID, "error", err)
		return
	}

	// Publish to DLQ for operator visibility / replay.
	dlq := &kafka.DeadLetterMessage{
		Version:      kafka.CurrentVersion,
		OriginalMsg:  *msg,
		FinalError:   errStr,
		FinalAttempt: msg.Attempt,
		DiedAt:       time.Now().UTC(),
	}

	dlqBytes, encErr := dlq.Encode()
	if encErr != nil {
		slog.Error("Failed to encode DLQ message", "run_id", msg.RunID, "error", encErr)
	} else {
		publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
		defer publishCancel()

		if err := c.dlqWriter.WriteMessages(publishCtx, kafkago.Message{
			Key:   []byte(msg.JobID),
			Value: dlqBytes,
		}); err != nil {
			slog.Error("Failed to publish to DLQ", "run_id", msg.RunID, "error", err)
		}
	}

	slog.Error("Run DEAD — all retries exhausted, published to DLQ",
		"run_id", msg.RunID,
		"job_name", msg.JobName,
		"final_attempt", msg.Attempt,
		"max_retries", msg.MaxRetries,
		"cause", cause,
	)
}

func (c *Consumer) runSafe(ctx context.Context, msg *kafka.JobMessage) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return c.handler(ctx, msg)
}

func parseUUID(s string) (pgtype.UUID, error) {
	var u pgtype.UUID
	if err := u.Scan(s); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parse uuid %q: %w", s, err)
	}
	return u, nil
}

func workerIDFromEnv() string {
	if id := os.Getenv("WORKER_ID"); id != "" {
		return id
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// ExecuteAndPersistForTest exposes executeAndPersist for white-box unit testing.
// It is intentionally unexported from the consumer_test package via the _test suffix trick.
func (c *Consumer) ExecuteAndPersistForTest(ctx context.Context, msg *kafka.JobMessage) {
	c.executeAndPersist(ctx, msg)
}
