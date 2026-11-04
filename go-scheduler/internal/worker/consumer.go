package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/redis"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/retry"
)

type Handler func(ctx context.Context, msg *kafka.JobMessage) error

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

type Consumer struct {
	reader    *kafkago.Reader
	dlqWriter KafkaWriter // publishes exhausted runs to the DLQ topic
	pool      *pgxpool.Pool
	queries   db.Querier
	locker    redis.Locker
	handler   Handler
	workerID  string
}

func New(reader *kafkago.Reader, dlqWriter KafkaWriter, pool *pgxpool.Pool, locker redis.Locker, handler Handler) *Consumer {
	return &Consumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		pool:      pool,
		queries:   db.New(pool),
		locker:    locker,
		handler:   handler,
		workerID:  workerIDFromEnv(),
	}
}

func NewWithQuerier(reader *kafkago.Reader, dlqWriter KafkaWriter, pool *pgxpool.Pool, queries db.Querier, locker redis.Locker, handler Handler) *Consumer {
	return &Consumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		pool:      pool,
		queries:   queries,
		locker:    locker,
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
		// if Redis is configured but unreachable, pause consuming
		if c.locker != nil {
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			if err := c.locker.Ping(pingCtx); err != nil {
				pingCancel()
				slog.Error("Redis unreachable — pausing consumer loop to avoid running without locks", "error", err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(3 * time.Second):
				}
				continue
			}
			pingCancel()
		}

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

		persisted := c.executeAndPersist(ctx, msg)
		if !persisted {
			slog.Error("Failed to persist run state after retries — NOT committing offset to allow redelivery",
				"run_id", msg.RunID,
				"job_id", msg.JobID,
			)
			continue
		}

		// Commit offset strictly AFTER DB persistence succeeds — at-least-once delivery guarantee.
		if err := c.reader.CommitMessages(ctx, kafkaMsg); err != nil {
			slog.Error("Failed to commit Kafka offset",
				"run_id", msg.RunID,
				"error", err,
			)
		}
	}
}

const (
	defaultLockTTL = 15 * time.Second
)

func (c *Consumer) executeAndPersist(ctx context.Context, msg *kafka.JobMessage) bool {
	runID, err := parseUUID(msg.RunID)
	if err != nil {
		slog.Error("Invalid run_id in message — skipping malformed run",
			"run_id", msg.RunID,
			"error", err,
		)
		return true // Skip and commit offset so we don't poison loop on unparseable UUID
	}

	// 1. Distributed lock 
	lockKey := fmt.Sprintf("lock:run:%s", msg.RunID)
	if c.locker != nil {
		lockCtx, lockCancel := context.WithTimeout(ctx, 3*time.Second)
		acquired, lockErr := c.locker.TryLock(lockCtx, lockKey, c.workerID, defaultLockTTL)
		lockCancel()

		if lockErr != nil {
			slog.Error("Failed to contact Redis for lock — will redeliver",
				"run_id", msg.RunID,
				"error", lockErr,
			)
			return false // retry later
		}

		if !acquired {
			slog.Info("Run is already locked by another worker (idempotency check) — skipping",
				"run_id", msg.RunID,
				"worker_id", c.workerID,
			)
			return true  
		}

		// release lock via compare-and-delete Lua script when finished.
		defer func() {
			relCtx, relCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer relCancel()
			if relErr := c.locker.Release(relCtx, lockKey, c.workerID); relErr != nil && !errors.Is(relErr, redis.ErrLockNotHeld) {
				slog.Warn("Failed to release Redis lock", "key", lockKey, "error", relErr)
			}
		}()
	}

	claimCtx, claimCancel := context.WithTimeout(ctx, 5*time.Second)
	defer claimCancel()

	claim, err := c.queries.ClaimRun(claimCtx, db.ClaimRunParams{
		ID:       runID,
		WorkerID: &c.workerID,
	})
	if err != nil {
		slog.Info("Run could not be claimed in DB (already running or terminal) — skipping",
			"run_id", msg.RunID,
			"worker_id", c.workerID,
			"error", err,
		)
		return true // Commit offset for duplicate/stale messages
	}

	_ = claim

	stopRenewal := make(chan struct{})
	var renewalWg sync.WaitGroup
	if c.locker != nil {
		renewalWg.Add(1)
		go func() {
			defer renewalWg.Done()
			c.startLockRenewalWatchdog(lockKey, defaultLockTTL, stopRenewal)
		}()
	}

	timeout := time.Duration(msg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	handlerCtx, handlerCancel := context.WithTimeout(ctx, timeout)
	defer handlerCancel()

	handlerErr := c.runSafe(handlerCtx, msg)

	if c.locker != nil {
		close(stopRenewal)
		renewalWg.Wait()
	}

	if handlerErr == nil {
		// Happy path: mark succeeded
		return c.finishSucceededWithRetry(ctx, runID, msg)
	}

	// Failure path: decide between retry and dead-letter
	nextAttempt := msg.Attempt + 1

	slog.Warn("Job handler failed",
		"run_id", msg.RunID,
		"job_id", msg.JobID,
		"job_name", msg.JobName,
		"attempt", msg.Attempt,
		"next_attempt", nextAttempt,
		"max_retries", msg.MaxRetries,
		"error", handlerErr,
	)

	// total executions = 1 + max_retries, so retry if nextAttempt <= max_retries
	if nextAttempt <= msg.MaxRetries {
		return c.scheduleRetryWithRetry(ctx, runID, msg, nextAttempt, handlerErr)
	}

	return c.deadLetterWithRetry(ctx, runID, msg, handlerErr)
}

func (c *Consumer) startLockRenewalWatchdog(key string, ttl time.Duration, stopCh <-chan struct{}) {
	interval := ttl / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			renewed, err := c.locker.Renew(renewCtx, key, c.workerID, ttl)
			cancel()

			if err != nil {
				slog.Warn("Watchdog: failed to renew Redis lock", "key", key, "error", err)
			} else if !renewed {
				slog.Warn("Watchdog: lock renewal returned false (lost lock ownership)", "key", key)
				return
			}
		}
	}
}

func (c *Consumer) finishSucceededWithRetry(ctx context.Context, runID pgtype.UUID, msg *kafka.JobMessage) bool {
	for i := 0; i < 3; i++ {
		finishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rows, err := c.queries.FinishRun(finishCtx, db.FinishRunParams{
			ID:       runID,
			Status:   db.JobRunStatusSUCCEEDED,
			Error:    nil,
			WorkerID: &c.workerID,
			Attempt:  msg.Attempt,
		})
		cancel()

		if err == nil {
			if rows == 0 {
				slog.Warn("FinishRun affected 0 rows (fencing prevented zombie write)",
					"run_id", msg.RunID,
					"worker_id", c.workerID,
					"attempt", msg.Attempt,
				)
			} else {
				slog.Info("Run SUCCEEDED",
					"run_id", msg.RunID,
					"job_name", msg.JobName,
					"attempt", msg.Attempt,
					"worker_id", c.workerID,
				)
			}
			return true
		}

		slog.Warn("Failed to mark run SUCCEEDED — retrying DB write",
			"run_id", msg.RunID,
			"retry_attempt", i+1,
			"error", err,
		)
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (c *Consumer) scheduleRetryWithRetry(ctx context.Context, runID pgtype.UUID, msg *kafka.JobMessage, nextAttempt int32, cause error) bool {
	nextRetryAt := retry.NextRetryAt(nextAttempt, retry.DefaultBase, retry.DefaultMaxCap)
	errStr := cause.Error()

	for i := 0; i < 3; i++ {
		retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rows, err := c.queries.FailRunForRetry(retryCtx, db.FailRunForRetryParams{
			ID:             runID,
			NewAttempt:     nextAttempt,
			NextRetryAt:    pgtype.Timestamptz{Time: nextRetryAt, Valid: true},
			Error:          &errStr,
			WorkerID:       &c.workerID,
			CurrentAttempt: msg.Attempt,
		})
		cancel()

		if err == nil {
			if rows == 0 {
				slog.Warn("FailRunForRetry affected 0 rows (fencing prevented zombie write)",
					"run_id", msg.RunID,
					"worker_id", c.workerID,
					"attempt", msg.Attempt,
				)
			} else {
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
			return true
		}

		slog.Warn("Failed to schedule retry in DB — retrying DB write",
			"run_id", msg.RunID,
			"retry_attempt", i+1,
			"error", err,
		)
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (c *Consumer) deadLetterWithRetry(ctx context.Context, runID pgtype.UUID, msg *kafka.JobMessage, cause error) bool {
	errStr := cause.Error()

	dbSuccess := false
	for i := 0; i < 3; i++ {
		deadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rows, err := c.queries.MarkRunDead(deadCtx, db.MarkRunDeadParams{
			ID:       runID,
			Error:    &errStr,
			WorkerID: &c.workerID,
			Attempt:  msg.Attempt,
		})
		cancel()

		if err == nil {
			if rows == 0 {
				slog.Warn("MarkRunDead affected 0 rows (fencing prevented zombie write)",
					"run_id", msg.RunID,
					"worker_id", c.workerID,
					"attempt", msg.Attempt,
				)
			}
			dbSuccess = true
			break
		}

		slog.Warn("Failed to mark run DEAD in DB — retrying DB write",
			"run_id", msg.RunID,
			"retry_attempt", i+1,
			"error", err,
		)
		time.Sleep(100 * time.Millisecond)
	}

	if !dbSuccess {
		return false
	}

	// Publish to DLQ for operator visibility / replay
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

	return true
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

//exposes executeAndPersist for white-box unit testing.
func (c *Consumer) ExecuteAndPersistForTest(ctx context.Context, msg *kafka.JobMessage) bool {
	return c.executeAndPersist(ctx, msg)
}
