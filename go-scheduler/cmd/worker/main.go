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
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting worker service...")

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
	slog.Info("Kafka connected", "brokers", cfg.Kafka.Brokers)

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

	// Consumer group "workers" — Kafka auto-distributes partitions across instances.
	reader := kafkaClient.NewReader(cfg.Kafka.TopicRuns, "workers")
	defer reader.Close()

	// DLQ writer — publishes exhausted runs for operator inspection/replay.
	dlqWriter := kafkaClient.NewWriter(cfg.Kafka.TopicDLQ)
	defer dlqWriter.Close()

	consumer := worker.New(reader, dlqWriter, pgPool, worker.DefaultHandler)

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("Worker consumer loop started. Press Ctrl+C / SIGTERM to stop.")
	if err := consumer.Run(runCtx); err != nil {
		slog.Error("Worker consumer stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("Worker service exited cleanly.")
}
