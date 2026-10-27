package kafka

import (
	"encoding/json"
	"fmt"
	"time"
)

type JobMessage struct {
	Version int `json:"version"`

	RunID   string `json:"run_id"`
	JobID   string `json:"job_id"`
	JobName string `json:"job_name"`

	CronExpr       string          `json:"cron_expr"`
	Timezone       string          `json:"timezone"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     int32           `json:"max_retries"`
	TimeoutSeconds int32           `json:"timeout_seconds"`
	Attempt int32 `json:"attempt"`

	ScheduledFor time.Time `json:"scheduled_for"`
	EnqueuedAt   time.Time `json:"enqueued_at"`
}
type DeadLetterMessage struct {
	Version      int        `json:"version"`
	OriginalMsg  JobMessage `json:"original_msg"`
	FinalError   string     `json:"final_error"`
	FinalAttempt int32      `json:"final_attempt"`
	DiedAt       time.Time  `json:"died_at"`
}

const CurrentVersion = 1

// to encode the message into json to write into the kafka topic
func (m *JobMessage) Encode() ([]byte, error) {
	b, err := json.Marshal(m) // marshalling
	if err != nil {
		return nil, fmt.Errorf("kafka/message: marshal JobMessage: %w", err)
	}
	return b, nil
}

// to decode the kafka topic message into JobMessage
func DecodeJobMessage(data []byte) (*JobMessage, error) {
	var m JobMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("kafka/message: unmarshal JobMessage: %w", err)
	}
	return &m, nil
}

func (m *DeadLetterMessage) Encode() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("kafka/message: marshal DeadLetterMessage: %w", err)
	}
	return b, nil
}
