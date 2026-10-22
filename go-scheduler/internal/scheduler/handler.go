package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
)

// Handler represents the execution logic for a scheduled job.
type Handler func(ctx context.Context, job db.Job) error

// DummyHandler is a placeholder executor for Step 1.
// In later milestones, jobs will be published to Kafka and consumed by workers.
// In Step 1, it logs the job execution, simulates work, and completes successfully.
func DummyHandler(ctx context.Context, job db.Job) error {
	slog.Info("DummyHandler executing job",
		"job_id", pgUUIDStr(job.ID),
		"job_name", job.Name,
		"payload", payloadPreview(job.Payload),
	)

	// Simulate work / cooperative cancellation check
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}

	slog.Info("DummyHandler finished job successfully",
		"job_id", pgUUIDStr(job.ID),
		"job_name", job.Name,
	)
	return nil
}
