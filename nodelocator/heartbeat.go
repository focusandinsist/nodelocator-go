package nodelocator

import (
	"context"
	"fmt"
	"log"
	"time"

	redisClient "nodelocator/redis"

	"github.com/go-redis/redis/v8"
)

// HeartbeatManager gateway heartbeat manager
// Used for gateway service to manage registration, heartbeat and deregistration by itself
type HeartbeatManager struct {
	redis      *redisClient.RedisClient
	instanceID string
	host       string
	port       int
	stopCh     chan struct{}
	ticker     *time.Ticker
}

// NewHeartbeatManager create heartbeat manager
func NewHeartbeatManager(redis *redisClient.RedisClient, instanceID, host string, port int) *HeartbeatManager {
	return &HeartbeatManager{
		redis:      redis,
		instanceID: instanceID,
		host:       host,
		port:       port,
		stopCh:     make(chan struct{}),
	}
}

// Start start heartbeat manager
func (hm *HeartbeatManager) Start(ctx context.Context) error {
	// Register gateway instance
	if err := hm.register(ctx); err != nil {
		return fmt.Errorf("failed to register gateway instance: %v", err)
	}

	// Start heartbeat
	hm.startHeartbeat()

	log.Printf("Heartbeat manager started: %s (%s:%d)", hm.instanceID, hm.host, hm.port)
	return nil
}

// Stop stop heartbeat manager
func (hm *HeartbeatManager) Stop(ctx context.Context) error {
	// Stop heartbeat
	close(hm.stopCh)
	if hm.ticker != nil {
		hm.ticker.Stop()
	}

	// Unregister gateway instance
	if err := hm.unregister(ctx); err != nil {
		log.Printf("Failed to unregister gateway instance: %v", err)
	}

	log.Printf("Heartbeat manager stopped: %s", hm.instanceID)
	return nil
}

// register register gateway instance to Redis ZSET
func (hm *HeartbeatManager) register(ctx context.Context) error {
	// Add to Redis ZSET
	score := float64(time.Now().Unix())
	z := &redis.Z{Score: score, Member: hm.instanceID}
	if err := hm.redis.ZAdd(ctx, ActiveGatewaysKey, z); err != nil {
		return fmt.Errorf("failed to register to Redis ZSET: %v", err)
	}

	// Optional: save instance details to Hash (for getting host, port and other info)
	instanceKey := fmt.Sprintf(GatewayInstanceHashKeyFmt, hm.instanceID)
	instanceInfo := map[string]interface{}{
		"id":             hm.instanceID,
		"host":           hm.host,
		"port":           hm.port,
		"registered_at":  time.Now().Unix(),
		"last_heartbeat": time.Now().Unix(),
	}

	if err := hm.redis.HMSet(ctx, instanceKey, instanceInfo); err != nil {
		log.Printf("Failed to save instance details: %v", err)
	}

	// Set Hash expiration time (heartbeat window + 30 seconds buffer)
	expireTime := time.Duration(HeartbeatWindow+30) * time.Second
	if err := hm.redis.Expire(ctx, instanceKey, expireTime); err != nil {
		log.Printf("Failed to set instance info expiration time: %v", err)
	}

	return nil
}

// unregister unregister gateway instance from Redis ZSET
func (hm *HeartbeatManager) unregister(ctx context.Context) error {
	// Remove from Redis ZSET
	if err := hm.redis.ZRem(ctx, ActiveGatewaysKey, hm.instanceID); err != nil {
		return fmt.Errorf("failed to remove from Redis ZSET: %v", err)
	}

	// Delete instance details
	instanceKey := fmt.Sprintf(GatewayInstanceHashKeyFmt, hm.instanceID)
	if err := hm.redis.Del(ctx, instanceKey); err != nil {
		log.Printf("Failed to delete instance details: %v", err)
	}

	return nil
}

// startHeartbeat start heartbeat loop
func (hm *HeartbeatManager) startHeartbeat() {
	// Use heartbeat interval defined in constants
	hm.ticker = time.NewTicker(HeartbeatInterval)

	go func() {
		defer hm.ticker.Stop()

		for {
			select {
			case <-hm.ticker.C:
				ctx := context.Background()
				if err := hm.sendHeartbeat(ctx); err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
				}
			case <-hm.stopCh:
				return
			}
		}
	}()
}

// sendHeartbeat send heartbeat
func (hm *HeartbeatManager) sendHeartbeat(ctx context.Context) error {
	// Update score (timestamp) in Redis ZSET
	score := float64(time.Now().Unix())
	z := &redis.Z{Score: score, Member: hm.instanceID}
	if err := hm.redis.ZAdd(ctx, ActiveGatewaysKey, z); err != nil {
		return fmt.Errorf("failed to update heartbeat: %v", err)
	}

	// Update heartbeat time in instance details
	instanceKey := fmt.Sprintf(GatewayInstanceHashKeyFmt, hm.instanceID)
	if err := hm.redis.HSet(ctx, instanceKey, "last_heartbeat", time.Now().Unix()); err != nil {
		log.Printf("Failed to update instance heartbeat time: %v", err)
	}

	// Renew Hash expiration (heartbeat window + 30 seconds buffer)
	expireTime := time.Duration(HeartbeatWindow+30) * time.Second
	if err := hm.redis.Expire(ctx, instanceKey, expireTime); err != nil {
		log.Printf("Failed to renew instance info expiration: %v", err)
	}

	return nil
}

// GetInstanceID get instance ID
func (hm *HeartbeatManager) GetInstanceID() string {
	return hm.instanceID
}

// GetAddress get instance address
func (hm *HeartbeatManager) GetAddress() string {
	return fmt.Sprintf("%s:%d", hm.host, hm.port)
}

// IsRunning check if heartbeat manager is running
func (hm *HeartbeatManager) IsRunning() bool {
	select {
	case <-hm.stopCh:
		return false
	default:
		return hm.ticker != nil
	}
}
