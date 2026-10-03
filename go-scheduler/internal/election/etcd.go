package election

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/config"
)

// Connect creates an etcd client and verifies cluster connectivity.
func Connect(ctx context.Context, cfg config.EtcdConfig) (*clientv3.Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("no etcd endpoints configured")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// Verify connectivity to first endpoint
	statusCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err = cli.Status(statusCtx, cfg.Endpoints[0])
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("failed to check etcd status at %s: %w", cfg.Endpoints[0], err)
	}

	return cli, nil
}
