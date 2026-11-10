package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockNotHeld = errors.New("lock is not held or has been acquired by another owner")
)

type Locker interface {
	TryLock(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, owner string) error
	Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Ping(ctx context.Context) error
}

type RedisLocker struct {
	client *redis.Client
}

func NewLocker(client *redis.Client) *RedisLocker {
	return &RedisLocker{client: client}
}

const compareAndDeleteLua = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`

const compareAndRenewLua = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`

func (l *RedisLocker) TryLock(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	ok, err := l.client.SetNX(ctx, key, owner, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx %s: %w", key, err)
	}
	return ok, nil
}

func (l *RedisLocker) Release(ctx context.Context, key, owner string) error {
	res, err := l.client.Eval(ctx, compareAndDeleteLua, []string{key}, owner).Result()
	if err != nil {
		return fmt.Errorf("redis release %s: %w", key, err)
	}
	if res == int64(0) {
		return ErrLockNotHeld
	}
	return nil
}

func (l *RedisLocker) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	ttlMs := ttl.Milliseconds()
	res, err := l.client.Eval(ctx, compareAndRenewLua, []string{key}, owner, ttlMs).Result()
	if err != nil {
		return false, fmt.Errorf("redis renew %s: %w", key, err)
	}
	return res == int64(1), nil
}

func (l *RedisLocker) Ping(ctx context.Context) error {
	return l.client.Ping(ctx).Err()
}
