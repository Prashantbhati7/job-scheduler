package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Postgres PostgresConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
	Etcd     EtcdConfig
	App      AppConfig
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
	SSLMode  string
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User, p.Password, p.Host, p.Port, p.DB, p.SSLMode)
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type KafkaConfig struct {
	Brokers   []string
	TopicRuns string
	TopicDLQ  string
}

type EtcdConfig struct {
	Endpoints []string
}

type AppConfig struct {
	Port     int
	LogLevel string
}

// Load reads environment variables and optional .env files, returning the populated Config.
func Load() (*Config, error) {
	// Attempt to load .env from current directory or parent directory if present.
	_ = godotenv.Load(".env", "../.env")

	pgPort, _ := strconv.Atoi(getEnv("POSTGRES_PORT", "5432"))
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	appPort, _ := strconv.Atoi(getEnv("API_PORT", "3000"))

	kafkaBrokersRaw := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers := splitAndTrim(kafkaBrokersRaw, ",")

	etcdEndpointsRaw := getEnv("ETCD_ENDPOINTS", "localhost:2379")
	etcdEndpoints := splitAndTrim(etcdEndpointsRaw, ",")

	cfg := &Config{
		Postgres: PostgresConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     pgPort,
			User:     getEnv("POSTGRES_USER", "myuser"),
			Password: getEnv("POSTGRES_PASSWORD", "mypassword"),
			DB:       getEnv("POSTGRES_DB", "mydb"),
			SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       redisDB,
		},
		Kafka: KafkaConfig{
			Brokers:   kafkaBrokers,
			TopicRuns: getEnv("KAFKA_TOPIC_RUNS", "job_runs"),
			TopicDLQ:  getEnv("KAFKA_TOPIC_DLQ", "job_runs.dlq"),
		},
		Etcd: EtcdConfig{
			Endpoints: etcdEndpoints,
		},
		App: AppConfig{
			Port:     appPort,
			LogLevel: getEnv("LOG_LEVEL", "info"),
		},
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	var result []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
