package sessionlocator

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	redisClient "nodelocator/redis"
)

// // RedisClient Redis client
// type RedisClient struct {
// 	client *redis.Client
// }

/*
  TODO
  Used to clean up redis instance routing table when instances go offline/fail.
  This is a temporary solution that uses redis to implement a distributed lock for leader election,
  to ensure that Cleaner is globally singleton and tasks are not executed repeatedly by other instances.
  In the future, scheduled cleanup tasks will be changed to k8s cronJob execution or a new monitor service, TBD.
*/

const (
	// LeaderLockKey Redis key for leader election lock
	LeaderLockKey = "service:logic:leader_lock"

	// LeaderElectionInterval leader election interval
	LeaderElectionInterval = 30 * time.Second

	// LeaderLockTTL TTL for leader lock
	LeaderLockTTL = 60 * time.Second

	// CleanupTaskInterval cleanup task execution interval
	CleanupTaskInterval = 5 * time.Minute
)

// Cleaner gateway instance cleaner
type Cleaner struct {
	redis      *redisClient.RedisClient
	instanceID string
	isLeader   bool
	stopCh     chan struct{}

	// Election related
	electionTicker *time.Ticker

	// Cleanup task related
	cleanupTicker *time.Ticker
}

// NewCleaner create cleaner
func NewCleaner(redis *redisClient.RedisClient, instanceID string) *Cleaner {
	return &Cleaner{
		redis:      redis,
		instanceID: instanceID,
		isLeader:   false,
		stopCh:     make(chan struct{}),
	}
}

// Start start cleaner (including leader election)
func (c *Cleaner) Start(ctx context.Context) {
	log.Printf("Starting gateway cleaner, instance ID: %s", c.instanceID)

	// Start leader election
	c.electionTicker = time.NewTicker(LeaderElectionInterval)
	go c.leaderElection(ctx)

	// Try election immediately once
	go c.tryBecomeLeader(ctx)
}

// Stop stop cleaner
func (c *Cleaner) Stop() {
	log.Printf("Stopping gateway cleaner, instance ID: %s", c.instanceID)

	close(c.stopCh)

	if c.electionTicker != nil {
		c.electionTicker.Stop()
	}

	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
	}

	// If is leader, release lock
	if c.isLeader {
		ctx := context.Background()
		c.releaseLock(ctx)
	}
}

// leaderElection leader election goroutine
func (c *Cleaner) leaderElection(ctx context.Context) {
	defer c.electionTicker.Stop()

	for {
		select {
		case <-c.electionTicker.C:
			c.tryBecomeLeader(ctx)
		case <-c.stopCh:
			return
		}
	}
}

// tryBecomeLeader try to become leader
func (c *Cleaner) tryBecomeLeader(ctx context.Context) {
	// Try to acquire leader lock
	ok, err := c.redis.SetNX(ctx, LeaderLockKey, c.instanceID, LeaderLockTTL)
	if err != nil {
		log.Printf("Leader election failed: %v", err)
		return
	}

	if ok {
		// Successfully acquired lock, become leader
		if !c.isLeader {
			log.Printf("Became leader, starting cleanup tasks")
			c.isLeader = true
			c.startCleanupTask(ctx)
		} else {
			// Already leader, renew lock
			log.Printf("Renewing leader lock")
		}
	} else {
		// Failed to acquire lock, check current leader
		currentLeader, err := c.redis.Get(ctx, LeaderLockKey)
		if err != nil {
			log.Printf("Failed to get current leader: %v", err)
		} else {
			if c.isLeader && currentLeader != c.instanceID {
				// I was leader before, but not anymore
				log.Printf("Lost leadership, stopping cleanup tasks")
				c.isLeader = false
				c.stopCleanupTask()
			}
		}
	}
}

// startCleanupTask start cleanup task
func (c *Cleaner) startCleanupTask(ctx context.Context) {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
	}

	c.cleanupTicker = time.NewTicker(CleanupTaskInterval)

	// Execute cleanup immediately once
	go c.executeCleanup(ctx)

	// Start periodic cleanup
	go func() {
		defer c.cleanupTicker.Stop()

		for {
			select {
			case <-c.cleanupTicker.C:
				if c.isLeader {
					c.executeCleanup(ctx)
				}
			case <-c.stopCh:
				return
			}
		}
	}()
}

// stopCleanupTask stop cleanup task
func (c *Cleaner) stopCleanupTask() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		c.cleanupTicker = nil
	}
}

// executeCleanup execute cleanup task
func (c *Cleaner) executeCleanup(ctx context.Context) {
	log.Printf("Executing gateway instance cleanup task...")

	// Calculate expired timestamp
	expiredBefore := time.Now().Unix() - HeartbeatWindow

	// 1. First get the number of instances to be deleted (for logging)
	expiredOpt := &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(expiredBefore, 10),
	}
	expiredInstances, err := c.redis.ZRangeByScore(ctx, ActiveGatewaysKey, expiredOpt)
	if err != nil {
		log.Printf("Failed to get expired instances: %v", err)
		return
	}

	// 2. Clean expired instances from ZSET
	if len(expiredInstances) > 0 {
		err = c.redis.ZRemRangeByScore(ctx, ActiveGatewaysKey, "0", strconv.FormatInt(expiredBefore, 10))
		if err != nil {
			log.Printf("Failed to clean expired instances from ZSET: %v", err)
			return
		}
		log.Printf("Cleaned %d expired gateway instances from ZSET: %v", len(expiredInstances), expiredInstances)
	}

	// 3. Clean orphaned hashes
	orphanedHashes, err := c.cleanupOrphanedHashes(ctx)
	if err != nil {
		log.Printf("Failed to clean orphaned hashes: %v", err)
	} else if orphanedHashes > 0 {
		log.Printf("Cleaned %d orphaned hashes", orphanedHashes)
	}

	if len(expiredInstances) > 0 || orphanedHashes > 0 {
		log.Printf("Expired instance cleanup completed: ZSET cleaned %d, Hash cleaned %d", len(expiredInstances), orphanedHashes)
	} else {
		log.Printf("Cleanup task completed, no expired instances")
	}
}

// cleanupOrphanedHashes clean orphaned hashes
func (c *Cleaner) cleanupOrphanedHashes(ctx context.Context) (int, error) {
	// Get all gateway_instances:* keys
	pattern := fmt.Sprintf(GatewayInstanceHashKeyFmt, "*")
	hashKeys, err := c.redis.Keys(ctx, pattern)
	if err != nil {
		return 0, fmt.Errorf("failed to get hash keys: %v", err)
	}

	if len(hashKeys) == 0 {
		return 0, nil
	}

	// Get all instance IDs from ZSET
	allOpt := &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}
	activeIDs, err := c.redis.ZRangeByScore(ctx, ActiveGatewaysKey, allOpt)
	if err != nil {
		return 0, fmt.Errorf("failed to get ZSET instances: %v", err)
	}

	// Build map of active instance IDs
	activeIDMap := make(map[string]bool)
	for _, id := range activeIDs {
		activeIDMap[id] = true
	}

	// Check and delete orphaned hashes
	orphanedCount := 0
	for _, hashKey := range hashKeys {
		// Extract instanceID from key
		instanceID := strings.TrimPrefix(hashKey, "gateway_instances:")

		// If this instance is not in ZSET, delete the hash
		if !activeIDMap[instanceID] {
			if err := c.redis.Del(ctx, hashKey); err != nil {
				log.Printf("Failed to delete orphaned hash %s: %v", hashKey, err)
			} else {
				orphanedCount++
				log.Printf("Deleted orphaned hash: %s", hashKey)
			}
		}
	}

	return orphanedCount, nil
}

// releaseLock release leader lock
func (c *Cleaner) releaseLock(ctx context.Context) {
	// Only release if current instance is the lock holder
	currentLeader, err := c.redis.Get(ctx, LeaderLockKey)
	if err != nil {
		log.Printf("Failed to get current leader: %v", err)
		return
	}

	if currentLeader == c.instanceID {
		if err := c.redis.Del(ctx, LeaderLockKey); err != nil {
			log.Printf("Failed to release leader lock: %v", err)
		} else {
			log.Printf("Leader lock released")
		}
	}
}

// IsLeader check if is leader
func (c *Cleaner) IsLeader() bool {
	return c.isLeader
}

// GetLeaderInfo get current leader information
func (c *Cleaner) GetLeaderInfo(ctx context.Context) (string, error) {
	return c.redis.Get(ctx, LeaderLockKey)
}
