package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/config"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/db"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/election"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/kafka"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/redis"
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pgPool, err := db.Connect(ctx, cfg.Postgres)
	if err != nil {
		slog.Error("Postgres connection failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	slog.Info("Postgres connection established and verified",
		"host", cfg.Postgres.Host,
		"port", cfg.Postgres.Port,
		"database", cfg.Postgres.DB,
	)

	rdb, err := redis.Connect(ctx, cfg.Redis)
	if err != nil {
		slog.Error("Redis connection failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	slog.Info("Redis connection established and verified",
		"addr", cfg.Redis.Addr,
		"db", cfg.Redis.DB,
	)


	kafkaClient, err := kafka.Connect(ctx, cfg.Kafka)
	if err != nil {
		slog.Error("Kafka connection failed", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	slog.Info("Kafka connection established and verified",
		"brokers", cfg.Kafka.Brokers,
		"topic_runs", cfg.Kafka.TopicRuns,
	)
	etcdClient, err := election.Connect(ctx, cfg.Etcd)
	if err != nil {
		slog.Error("etcd connection failed", "error", err)
		os.Exit(1)
	}
	defer etcdClient.Close()
	slog.Info("etcd connection established and verified",
		"endpoints", cfg.Etcd.Endpoints,
	)

	slog.Info("All worker dependencies connected successfully. Exiting cleanly.")
}
