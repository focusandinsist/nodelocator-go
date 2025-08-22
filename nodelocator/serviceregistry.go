package nodelocator

import (
	"context"
	"time"
)

// ZMember represents a member in a sorted set, with a score.
// This is a library-agnostic version of redis.Z.
type ZMember struct {
	Score  float64
	Member string
}

// ZRangeOptions specifies the options for a sorted set range query.
// This is a library-agnostic version of redis.ZRangeBy.
type ZRangeOptions struct {
	Min string
	Max string
}

// ServiceRegistry defines the storage and communication interface required by the nodelocator.
// This abstraction allows the user to provide any backend that satisfies the interface,
// such as a standard Redis client, a cluster client, or a mock for testing.
type ServiceRegistry interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error)
	Del(ctx context.Context, keys ...string) error
	Expire(ctx context.Context, key string, expiration time.Duration) error

	Get(ctx context.Context, key string) (string, error)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)

	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	HMSet(ctx context.Context, key string, fields map[string]interface{}) error
	HSet(ctx context.Context, key, field string, value interface{}) error

	ZAdd(ctx context.Context, key string, members ...*ZMember) error
	ZRangeByScore(ctx context.Context, key string, opt *ZRangeOptions) ([]string, error)
	ZRem(ctx context.Context, key string, members ...interface{}) error
	ZRemRangeByScore(ctx context.Context, key string, min, max string) error
}
