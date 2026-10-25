package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/scheduler"
)

type mockQuerier struct {
	mu sync.Mutex

	fetchDueJobsFunc         func(ctx context.Context, limit int32) ([]db.Job, error)
	fetchUnscheduledJobsFunc func(ctx context.Context) ([]db.FetchUnscheduledJobsRow, error)
	advanceNextRunFunc       func(ctx context.Context, arg db.AdvanceNextRunParams) error
	createRunFunc            func(ctx context.Context, arg db.CreateRunParams) (pgtype.UUID, error)
	finishRunFunc            func(ctx context.Context, arg db.FinishRunParams) error
	setInitialNextRunFunc    func(ctx context.Context, arg db.SetInitialNextRunParams) error

	advanceNextRunCalls    []db.AdvanceNextRunParams
	createRunCalls         []db.CreateRunParams
	finishRunCalls         []db.FinishRunParams
	setInitialNextRunCalls []db.SetInitialNextRunParams
}

func (m *mockQuerier) FetchDueJobs(ctx context.Context, limit int32) ([]db.Job, error) {
	if m.fetchDueJobsFunc != nil {
		return m.fetchDueJobsFunc(ctx, limit)
	}
	return nil, nil
}

func (m *mockQuerier) FetchUnscheduledJobs(ctx context.Context) ([]db.FetchUnscheduledJobsRow, error) {
	if m.fetchUnscheduledJobsFunc != nil {
		return m.fetchUnscheduledJobsFunc(ctx)
	}
	return nil, nil
}

func (m *mockQuerier) AdvanceNextRun(ctx context.Context, arg db.AdvanceNextRunParams) error {
	m.mu.Lock()
	m.advanceNextRunCalls = append(m.advanceNextRunCalls, arg)
	m.mu.Unlock()
	if m.advanceNextRunFunc != nil {
		return m.advanceNextRunFunc(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) CreateRun(ctx context.Context, arg db.CreateRunParams) (pgtype.UUID, error) {
	m.mu.Lock()
	m.createRunCalls = append(m.createRunCalls, arg)
	m.mu.Unlock()
	if m.createRunFunc != nil {
		return m.createRunFunc(ctx, arg)
	}
	return pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4}, Valid: true}, nil
}

func (m *mockQuerier) FinishRun(ctx context.Context, arg db.FinishRunParams) error {
	m.mu.Lock()
	m.finishRunCalls = append(m.finishRunCalls, arg)
	m.mu.Unlock()
	if m.finishRunFunc != nil {
		return m.finishRunFunc(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) SetInitialNextRun(ctx context.Context, arg db.SetInitialNextRunParams) error {
	m.mu.Lock()
	m.setInitialNextRunCalls = append(m.setInitialNextRunCalls, arg)
	m.mu.Unlock()
	if m.setInitialNextRunFunc != nil {
		return m.setInitialNextRunFunc(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) GetJobByID(ctx context.Context, id pgtype.UUID) (db.Job, error) {
	return db.Job{}, nil
}

func (m *mockQuerier) GetRunByID(ctx context.Context, id pgtype.UUID) (db.JobRun, error) {
	return db.JobRun{}, nil
}

func (m *mockQuerier) ListRunsForJob(ctx context.Context, arg db.ListRunsForJobParams) ([]db.JobRun, error) {
	return nil, nil
}

var _ db.Querier = (*mockQuerier)(nil)

func TestScheduler_ProcessJob_Success(t *testing.T) {
	jobID := pgtype.UUID{Bytes: [16]byte{0xaa, 0xbb, 0xcc, 0xdd}, Valid: true}
	runUUID := pgtype.UUID{Bytes: [16]byte{0x11, 0x22, 0x33, 0x44}, Valid: true}
	scheduledTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	var handlerExecuted bool
	handler := func(ctx context.Context, job db.Job) error {
		handlerExecuted = true
		return nil
	}

	mock := &mockQuerier{
		createRunFunc: func(ctx context.Context, arg db.CreateRunParams) (pgtype.UUID, error) {
			return runUUID, nil
		},
	}

	sched := scheduler.NewWithQuerier(mock, nil, 100*time.Millisecond, handler)

	dueJob := db.Job{
		ID:        jobID,
		Name:      "test-job",
		CronExpr:  "*/5 * * * *",
		Timezone:  "UTC",
		NextRunAt: pgtype.Timestamptz{Time: scheduledTime, Valid: true},
	}

	mock.fetchDueJobsFunc = func(ctx context.Context, limit int32) ([]db.Job, error) {
		return []db.Job{dueJob}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-errCh

	if !handlerExecuted {
		t.Fatal("expected handler to be executed")
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.advanceNextRunCalls) != 1 {
		t.Fatalf("expected 1 advance call, got %d", len(mock.advanceNextRunCalls))
	}
	expectedNextTick := scheduledTime.Add(5 * time.Minute)
	actualNextTick := mock.advanceNextRunCalls[0].NextRunAt.Time
	if !actualNextTick.Equal(expectedNextTick) {
		t.Errorf("expected next_run_at %v, got %v", expectedNextTick, actualNextTick)
	}

	if len(mock.createRunCalls) != 1 {
		t.Fatalf("expected 1 create run call, got %d", len(mock.createRunCalls))
	}
	if !mock.createRunCalls[0].ScheduledFor.Time.Equal(scheduledTime) {
		t.Errorf("expected scheduled_for %v, got %v", scheduledTime, mock.createRunCalls[0].ScheduledFor.Time)
	}

	if len(mock.finishRunCalls) != 1 {
		t.Fatalf("expected 1 finish run call, got %d", len(mock.finishRunCalls))
	}
	if mock.finishRunCalls[0].Status != db.JobRunStatusSUCCEEDED {
		t.Errorf("expected status SUCCEEDED, got %v", mock.finishRunCalls[0].Status)
	}
}

func TestScheduler_ProcessJob_IdempotencyOnConflict(t *testing.T) {
	jobID := pgtype.UUID{Bytes: [16]byte{0x01}, Valid: true}
	scheduledTime := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)

	var handlerExecuted bool
	handler := func(ctx context.Context, job db.Job) error {
		handlerExecuted = true
		return nil
	}

	mock := &mockQuerier{
		createRunFunc: func(ctx context.Context, arg db.CreateRunParams) (pgtype.UUID, error) {
			return pgtype.UUID{Valid: false}, nil
		},
	}

	sched := scheduler.NewWithQuerier(mock, nil, 50*time.Millisecond, handler)

	mock.fetchDueJobsFunc = func(ctx context.Context, limit int32) ([]db.Job, error) {
		return []db.Job{
			{
				ID:        jobID,
				Name:      "conflict-job",
				CronExpr:  "@hourly",
				Timezone:  "UTC",
				NextRunAt: pgtype.Timestamptz{Time: scheduledTime, Valid: true},
			},
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = sched.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	if handlerExecuted {
		t.Fatal("handler should NOT execute when run already exists (idempotency check)")
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.finishRunCalls) != 0 {
		t.Errorf("FinishRun should not be called for duplicate runs, got %d calls", len(mock.finishRunCalls))
	}
}

func TestScheduler_ProcessJob_HandlerError(t *testing.T) {
	jobID := pgtype.UUID{Bytes: [16]byte{0x02}, Valid: true}
	runID := pgtype.UUID{Bytes: [16]byte{0x03}, Valid: true}

	expectedErr := errors.New("simulated handler timeout failure")
	handler := func(ctx context.Context, job db.Job) error {
		return expectedErr
	}

	mock := &mockQuerier{
		createRunFunc: func(ctx context.Context, arg db.CreateRunParams) (pgtype.UUID, error) {
			return runID, nil
		},
	}

	sched := scheduler.NewWithQuerier(mock, nil, 50*time.Millisecond, handler)

	mock.fetchDueJobsFunc = func(ctx context.Context, limit int32) ([]db.Job, error) {
		return []db.Job{
			{
				ID:        jobID,
				Name:      "failing-job",
				CronExpr:  "* * * * *",
				Timezone:  "UTC",
				NextRunAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
			},
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = sched.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.finishRunCalls) != 1 {
		t.Fatalf("expected 1 finish call, got %d", len(mock.finishRunCalls))
	}
	if mock.finishRunCalls[0].Status != db.JobRunStatusFAILED {
		t.Errorf("expected status FAILED, got %v", mock.finishRunCalls[0].Status)
	}
}

func TestScheduler_ProcessJob_HandlerPanicRecovery(t *testing.T) {
	jobID := pgtype.UUID{Bytes: [16]byte{0x04}, Valid: true}
	runID := pgtype.UUID{Bytes: [16]byte{0x05}, Valid: true}

	handler := func(ctx context.Context, job db.Job) error {
		panic("fatal unexpected nil dereference inside business logic")
	}

	mock := &mockQuerier{
		createRunFunc: func(ctx context.Context, arg db.CreateRunParams) (pgtype.UUID, error) {
			return runID, nil
		},
	}

	sched := scheduler.NewWithQuerier(mock, nil, 50*time.Millisecond, handler)

	mock.fetchDueJobsFunc = func(ctx context.Context, limit int32) ([]db.Job, error) {
		return []db.Job{
			{
				ID:        jobID,
				Name:      "panicking-job",
				CronExpr:  "* * * * *",
				Timezone:  "UTC",
				NextRunAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
			},
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = sched.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.finishRunCalls) != 1 {
		t.Fatalf("expected 1 finish call, got %d", len(mock.finishRunCalls))
	}
	if mock.finishRunCalls[0].Status != db.JobRunStatusFAILED {
		t.Errorf("expected status FAILED, got %v", mock.finishRunCalls[0].Status)
	}
}

func TestScheduler_BootstrapNextRuns(t *testing.T) {
	jobID := pgtype.UUID{Bytes: [16]byte{0x09}, Valid: true}
	mock := &mockQuerier{
		fetchUnscheduledJobsFunc: func(ctx context.Context) ([]db.FetchUnscheduledJobsRow, error) {
			return []db.FetchUnscheduledJobsRow{
				{
					ID:       jobID,
					CronExpr: "0 0 * * *",
					Timezone: "UTC",
				},
			}, nil
		},
	}

	sched := scheduler.NewWithQuerier(mock, nil, 100*time.Millisecond, scheduler.DummyHandler)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = sched.Run(ctx)

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.setInitialNextRunCalls) != 1 {
		t.Fatalf("expected 1 initial next run call, got %d", len(mock.setInitialNextRunCalls))
	}
}
