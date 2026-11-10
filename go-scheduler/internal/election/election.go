package election

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type LeaderElector struct {
	client   *clientv3.Client
	elecKey  string
	nodeID   string // unique ID for this instance (hostname+PID)
	ttl      int    // lease TTL in seconds

	session  *concurrency.Session
	election *concurrency.Election

	isLeader atomic.Bool
	leaderCh chan bool
}

func New(client *clientv3.Client, elecKey, nodeID string, ttl int) *LeaderElector {
	return &LeaderElector{
		client:   client,
		elecKey:  elecKey,
		nodeID:   nodeID,
		ttl:      ttl,
		leaderCh: make(chan bool, 1),
	}
}

func (le *LeaderElector) Campaign(ctx context.Context) error {

	session, err := concurrency.NewSession(le.client,
		concurrency.WithTTL(le.ttl),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("election: create session: %w", err)
	}
	le.session = session
	le.election = concurrency.NewElection(session, le.elecKey)

	slog.Info("election: campaigning for leadership",
		"key", le.elecKey,
		"node", le.nodeID,
	)

	// Campaign blocks until we win or ctx is cancelled.
	if err := le.election.Campaign(ctx, le.nodeID); err != nil {
		_ = session.Close()
		return fmt.Errorf("election: campaign failed: %w", err)
	}

	le.isLeader.Store(true)
	select {
	case le.leaderCh <- true:
	default:
	}

	slog.Info("election: became leader",
		"key", le.elecKey,
		"node", le.nodeID,
	)
	return nil
}


func (le *LeaderElector) Resign(ctx context.Context) error {
	if !le.isLeader.Load() {
		return nil
	}
	le.isLeader.Store(false)
	select {
	case le.leaderCh <- false:
	default:
	}

	if le.election != nil {
		if err := le.election.Resign(ctx); err != nil {
			slog.Warn("election: resign returned error", "error", err)
		}
	}
	if le.session != nil {
		if err := le.session.Close(); err != nil {
			slog.Warn("election: session close returned error", "error", err)
		}
	}
	slog.Info("election: resigned leadership",
		"key", le.elecKey,
		"node", le.nodeID,
	)
	return nil
}

func (le *LeaderElector) IsLeader() bool {
	return le.isLeader.Load()
}

func (le *LeaderElector) LeaderCh() <-chan bool {
	return le.leaderCh
}

