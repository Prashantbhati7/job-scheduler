package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/config"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/election"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/redis"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/scheduler"
)

const (
	electionKey = "/scheduler/leader"
	leaseTTLSeconds = 10
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting scheduler service...")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}
	slog.Info("Configuration loaded successfully")

	nodeID := buildNodeID()
	slog.Info("Scheduler node identity", "node_id", nodeID)

	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer initCancel()

	pgPool, err := db.Connect(initCtx, cfg.Postgres)
	if err != nil {
		slog.Error("Postgres connection failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	slog.Info("Postgres connected", "host", cfg.Postgres.Host, "port", cfg.Postgres.Port)

	rdb, err := redis.Connect(initCtx, cfg.Redis)
	if err != nil {
		slog.Error("Redis connection failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	slog.Info("Redis connected", "addr", cfg.Redis.Addr)

	kafkaClient, err := kafka.Connect(initCtx, cfg.Kafka)
	if err != nil {
		slog.Error("Kafka connection failed", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	slog.Info("Kafka connected", "brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.TopicRuns)

	etcdClient, err := election.Connect(initCtx, cfg.Etcd)
	if err != nil {
		slog.Error("etcd connection failed", "error", err)
		os.Exit(1)
	}
	defer etcdClient.Close()
	slog.Info("All dependencies connected.")

	if os.Getenv("CHECK_DEPS_ONLY") == "true" {
		slog.Info("CHECK_DEPS_ONLY is set. Exiting cleanly.")
		return
	}

	writer := kafkaClient.NewWriter(cfg.Kafka.TopicRuns)
	defer writer.Close()

	sched := scheduler.New(pgPool, 1*time.Second, writer)
	sweeper := scheduler.NewSweeper(pgPool, writer, 5*time.Second)


	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()


	elector := election.New(etcdClient, electionKey, nodeID, leaseTTLSeconds)

	slog.Info("Campaigning for leadership — will block until elected.",
		"election_key", electionKey,
		"node", nodeID,
	)

	if err := elector.Campaign(runCtx); err != nil {
		slog.Info("Campaign ended before winning leadership", "error", err)
		return
	}


	slog.Info("Elected as leader. Starting scheduler tick loop and sweeper.",
		"node", nodeID,
	)

	schedCtx, schedCancel := context.WithCancel(runCtx)
	defer schedCancel()

	// Sweeper runs alongside the tick loop, both share schedCtx so they stop
	go sweeper.Run(schedCtx)

	go func() {
		// When SIGTERM arrives, resign first so the standby wins immediately.
		<-runCtx.Done()
		slog.Info("SIGTERM received — resigning leadership before exit.", "node", nodeID)
		resignCtx, resignCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer resignCancel()
		if err := elector.Resign(resignCtx); err != nil {
			slog.Warn("Resign error (standby will still win after TTL)", "error", err)
		}
		schedCancel() // stop scheduler + sweeper
	}()

	slog.Info("Scheduler and retry sweeper started. SIGTERM to stop.")
	if err := sched.Run(schedCtx); err != nil {
		slog.Error("Scheduler loop stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("Scheduler service exited cleanly.", "node", nodeID)
}


func buildNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	if id := os.Getenv("SCHEDULER_ID"); id != "" {
		return id
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
