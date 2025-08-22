package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"

	"nodelocator/nodelocator"
)

// RedisRegistry implements the nodelocator.ServiceRegistry interface using a go-redis client.
type RedisRegistry struct {
	client redis.UniversalClient // Using UniversalClient supports single-node, cluster, and sentinel modes
}

// New accepts a pre-existing client instance created by the user.
func New(client redis.UniversalClient) *RedisRegistry {
	return &RedisRegistry{
		client: client,
	}
}

// Scan .
func (r *RedisRegistry) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return r.client.Scan(ctx, cursor, match, count).Result()
}

// Del .
func (r *RedisRegistry) Del(ctx context.Context, keys ...string) error {
	return r.client.Del(ctx, keys...).Err()
}

// Expire .
func (r *RedisRegistry) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.client.Expire(ctx, key, expiration).Err()
}

// Get .
func (r *RedisRegistry) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

// SetNX .
func (r *RedisRegistry) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, value, expiration).Result()
}

// HGetAll .
func (r *RedisRegistry) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.client.HGetAll(ctx, key).Result()
}

// HDel .
func (r *RedisRegistry) HDel(ctx context.Context, key string, fields ...string) error {
	return r.client.HDel(ctx, key, fields...).Err()
}

// HMSet .
func (r *RedisRegistry) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	return r.client.HSet(ctx, key, fields).Err()
}

// HSet .
func (r *RedisRegistry) HSet(ctx context.Context, key, field string, value interface{}) error {
	return r.client.HSet(ctx, key, field, value).Err()
}

// ZAdd adds members to a sorted set, converting from nodelocator.ZMember.
func (r *RedisRegistry) ZAdd(ctx context.Context, key string, members ...*nodelocator.ZMember) error {
	redisMembers := make([]*redis.Z, len(members))
	for i, m := range members {
		redisMembers[i] = &redis.Z{
			Score:  m.Score,
			Member: m.Member,
		}
	}
	return r.client.ZAdd(ctx, key, redisMembers...).Err()
}

// ZRangeByScore gets sorted set members by score, converting from nodelocator.ZRangeOptions.
func (r *RedisRegistry) ZRangeByScore(ctx context.Context, key string, opt *nodelocator.ZRangeOptions) ([]string, error) {
	redisOpt := &redis.ZRangeBy{
		Min: opt.Min,
		Max: opt.Max,
	}
	return r.client.ZRangeByScore(ctx, key, redisOpt).Result()
}

// ZRem .
func (r *RedisRegistry) ZRem(ctx context.Context, key string, members ...interface{}) error {
	return r.client.ZRem(ctx, key, members...).Err()
}

// ZRemRangeByScore .
func (r *RedisRegistry) ZRemRangeByScore(ctx context.Context, key string, min, max string) error {
	return r.client.ZRemRangeByScore(ctx, key, min, max).Err()
}
