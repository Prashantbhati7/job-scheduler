package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
)

// DefaultHandler is a placeholder that logs and simulates work.
func DefaultHandler(ctx context.Context, msg *kafka.JobMessage) error {
	slog.Info("DefaultHandler executing job",
		"run_id", msg.RunID,
		"job_id", msg.JobID,
		"job_name", msg.JobName,
		"scheduled_for", msg.ScheduledFor.Format(time.RFC3339),
	)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}

	slog.Info("DefaultHandler finished job",
		"run_id", msg.RunID,
		"job_name", msg.JobName,
	)
	return nil
}
