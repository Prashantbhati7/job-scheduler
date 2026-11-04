package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/config"
)

type Client struct {
	cfg  config.KafkaConfig
	conn *kafka.Conn
}
func Connect(ctx context.Context, cfg config.KafkaConfig) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}

	primaryBroker := cfg.Brokers[0]
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	conn, err := dialer.DialContext(ctx, "tcp", primaryBroker)
	if err != nil {
		return nil, fmt.Errorf("failed to dial kafka broker %s: %w", primaryBroker, err)
	}

	// Verify connectivity by fetching brokers list
	brokers, err := conn.Brokers()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to fetch brokers from %s: %w", primaryBroker, err)
	}

	if len(brokers) == 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("kafka broker %s returned empty broker list", primaryBroker)
	}

	return &Client{
		cfg:  cfg,
		conn: conn,
	}, nil
}


func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Brokers() []string {
	return c.cfg.Brokers
}

func (c *Client) NewWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(c.cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
}

func (c *Client) NewReader(topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.cfg.Brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // Disable auto-commit; commit strictly after DB persistence
		Dialer: &kafka.Dialer{
			Timeout: 10 * time.Second,
		},
	})
}

func CheckBrokerAddress(addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid broker address format: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("invalid port in broker address %s", addr)
	}
	if host == "" {
		return fmt.Errorf("broker host cannot be empty")
	}
	return nil
}
