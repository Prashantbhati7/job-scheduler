package config

import (
	"os"
	"reflect"
	"testing"
)

func TestSplitAndTrim(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		sep      string
		expected []string
	}{
		{
			name:     "single element",
			input:    "localhost:9092",
			sep:      ",",
			expected: []string{"localhost:9092"},
		},
		{
			name:     "multiple elements with whitespace",
			input:    " localhost:9092 , localhost:9093 ,  localhost:9094 ",
			sep:      ",",
			expected: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		},
		{
			name:     "empty items filtered out",
			input:    "localhost:9092,,localhost:9093,",
			sep:      ",",
			expected: []string{"localhost:9092", "localhost:9093"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitAndTrim(tt.input, tt.sep)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("splitAndTrim() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestPostgresConfigDSN(t *testing.T) {
	cfg := PostgresConfig{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     "testuser",
		Password: "testpassword",
		DB:       "testdb",
		SSLMode:  "disable",
	}

	expected := "postgres://testuser:testpassword@127.0.0.1:5432/testdb?sslmode=disable"
	if dsn := cfg.DSN(); dsn != expected {
		t.Errorf("DSN() = %v, want %v", dsn, expected)
	}
}

func TestGetEnv(t *testing.T) {
	_ = os.Setenv("TEST_KEY_ENV", "custom_value")
	defer os.Unsetenv("TEST_KEY_ENV")

	if val := getEnv("TEST_KEY_ENV", "fallback"); val != "custom_value" {
		t.Errorf("getEnv() = %v, want custom_value", val)
	}

	if val := getEnv("NON_EXISTENT_KEY", "fallback"); val != "fallback" {
		t.Errorf("getEnv() = %v, want fallback", val)
	}
}

func TestLoad(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg == nil {
		t.Fatal("Load() returned nil config")
	}
	if cfg.Postgres.Host == "" {
		t.Errorf("expected Postgres.Host to be populated, got empty")
	}
	if cfg.Redis.Addr == "" {
		t.Errorf("expected Redis.Addr to be populated, got empty")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		t.Errorf("expected Kafka.Brokers to be populated, got empty")
	}
	if len(cfg.Etcd.Endpoints) == 0 {
		t.Errorf("expected Etcd.Endpoints to be populated, got empty")
	}
}
