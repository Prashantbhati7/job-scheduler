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

	// Execution parameters copied from the job row so workers skip an extra DB round-trip.
	CronExpr       string          `json:"cron_expr"`
	Timezone       string          `json:"timezone"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     int32           `json:"max_retries"`
	TimeoutSeconds int32           `json:"timeout_seconds"`

	ScheduledFor time.Time `json:"scheduled_for"`
	EnqueuedAt   time.Time `json:"enqueued_at"`
}

const CurrentVersion = 1

// to encode the message into json to write into the kafka topic
func (m *JobMessage) Encode() ([]byte, error) {
	b, err := json.Marshal(m)	// marshalling 
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
