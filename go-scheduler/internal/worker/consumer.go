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
)


type Handler func(ctx context.Context, msg *kafka.JobMessage) error


type Consumer struct {
	reader   *kafkago.Reader
	pool     *pgxpool.Pool
	queries  db.Querier
	handler  Handler
	workerID string
}

func New(reader *kafkago.Reader, pool *pgxpool.Pool, handler Handler) *Consumer {
	workerID := workerIDFromEnv()
	return &Consumer{
		reader:   reader,
		pool:     pool,
		queries:  db.New(pool),
		handler:  handler,
		workerID: workerID,
	}
}

func NewWithQuerier(reader *kafkago.Reader, pool *pgxpool.Pool, queries db.Querier, handler Handler) *Consumer {
	return &Consumer{
		reader:   reader,
		pool:     pool,
		queries:  queries,
		handler:  handler,
		workerID: workerIDFromEnv(),
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
			"worker_id", c.workerID,
		)

		c.executeAndPersist(ctx, msg)

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
		timeout = 30 * time.Second // safe default
	}
	handlerCtx, handlerCancel := context.WithTimeout(ctx, timeout)
	defer handlerCancel()

	handlerErr := c.runSafe(handlerCtx, msg)

	status := db.JobRunStatusSUCCEEDED
	var errMsg *string
	if handlerErr != nil {
		status = db.JobRunStatusFAILED
		s := handlerErr.Error()
		errMsg = &s
		slog.Error("Job handler failed",
			"run_id", msg.RunID,
			"job_id", msg.JobID,
			"job_name", msg.JobName,
			"error", handlerErr,
		)
	} else {
		slog.Info("Job handler succeeded",
			"run_id", msg.RunID,
			"job_id", msg.JobID,
			"job_name", msg.JobName,
		)
	}

	finishCtx, finishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer finishCancel()

	if err := c.queries.FinishRun(finishCtx, db.FinishRunParams{
		ID:     runID,
		Status: status,
		Error:  errMsg,
	}); err != nil {
		slog.Error("Failed to persist run result — will retry on redelivery",
			"run_id", msg.RunID,
			"error", err,
		)
	}
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
