package main

import (
	"context"
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

	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer initCancel()

	pgPool, err := db.Connect(initCtx, cfg.Postgres)
	if err != nil {
		slog.Error("Postgres connection failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	slog.Info("Postgres connected",
		"host", cfg.Postgres.Host,
		"port", cfg.Postgres.Port,
		"database", cfg.Postgres.DB,
	)

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
	slog.Info("Kafka connected",
		"brokers", cfg.Kafka.Brokers,
		"topic", cfg.Kafka.TopicRuns,
	)

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

	// Create a Kafka writer keyed per-job for partition ordering.
	writer := kafkaClient.NewWriter(cfg.Kafka.TopicRuns)
	defer writer.Close()

	sched := scheduler.New(pgPool, 1*time.Second, writer)

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("Scheduler polling loop started. Press Ctrl+C / SIGTERM to stop.")
	if err := sched.Run(runCtx); err != nil {
		slog.Error("Scheduler loop stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("Scheduler service exited cleanly.")
}
