package nodelocator

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/focusandinsist/consistent-go/consistent"
)

// GatewayInstance gateway instance information
type GatewayInstance struct {
	ID            string `json:"id"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	LastHeartbeat int64  `json:"last_heartbeat"`
}

// MemberID implement consistent.Member interface
func (g *GatewayInstance) MemberID() string {
	return g.ID
}

// GetAddress get gateway address
func (g *GatewayInstance) GetAddress() string {
	return fmt.Sprintf("%s:%d", g.Host, g.Port)
}

// Locator session locator
// Responsible for monitoring Redis ZSET changes and syncing to local consistent hash ring, providing routing decisions
type Locator struct {
	registry     ServiceRegistry
	ring         *consistent.Consistent
	instances    map[string]*GatewayInstance // Instance ID -> Instance info
	mu           sync.RWMutex
	syncMu       sync.Mutex
	syncTicker   *time.Ticker
	lastSyncTime int64 // Last sync timestamp, used for detecting changes
	config       *Config
	cancelFn     context.CancelFunc
}

// NewLocator creates a new session locator.
func NewLocator(registry ServiceRegistry, consisCfg consistent.Config, cfg *Config) (*Locator, error) {

	if cfg == nil {
		cfg = DefaultConfig()
	}

	ring, err := consistent.New(consisCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consistent hash ring: %w", err)
	}
	locator := &Locator{
		registry:  registry,
		ring:      ring,
		instances: make(map[string]*GatewayInstance),
		config:    cfg,
	}

	// Sync active instances from Redis at startup
	if err := locator.syncActiveGateways(context.Background()); err != nil {
		log.Printf("Failed to sync active gateways during initialization: %v", err)
	}

	// Start background monitoring task
	locator.startMonitoring()

	return locator, nil
}

// GetGatewayForUser gets the assigned gateway instance for a given user ID.
func (l *Locator) GetGatewayForUser(ctx context.Context, userID string) (*GatewayInstance, error) {
	return l.getGateway(ctx, "user:"+userID)
}

// GetGatewayForRoom gets the assigned gateway instance for a given room ID.
func (l *Locator) GetGatewayForRoom(ctx context.Context, roomID string) (*GatewayInstance, error) {
	return l.getGateway(ctx, "room:"+roomID)
}

// getGateway is the internal implementation for locating a gateway.
// It takes a key, computes the hash, and finds the corresponding instance.
func (l *Locator) getGateway(ctx context.Context, key string) (*GatewayInstance, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.instances) == 0 {
		return nil, fmt.Errorf("no available gateway instances")
	}

	member, err := l.ring.LocateKey(ctx, []byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to locate gateway for key '%s': %w", key, err)
	}

	instance, ok := l.instances[member]
	if !ok {
		return nil, fmt.Errorf("instance %s found on hash ring does not exist in local cache, data may be inconsistent", member)
	}

	return instance, nil
}

// GetAllActiveGateways get all active gateway instances
func (l *Locator) GetAllActiveGateways() []GatewayInstance {
	l.mu.RLock()
	defer l.mu.RUnlock()

	instances := make([]GatewayInstance, 0, len(l.instances))
	for _, instance := range l.instances {
		instances = append(instances, *instance) // Return a copy
	}
	return instances
}

// GetStats get routing statistics
func (l *Locator) GetStats(ctx context.Context) map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	loadDist := l.ring.LoadDistribution(ctx)
	avgLoad, err := l.ring.AverageLoad(ctx)
	if err != nil {
		log.Printf("WARN: could not retrieve average load for stats: %v", err)
		avgLoad = 0
	}
	return map[string]interface{}{
		"active_gateways":   len(l.instances),
		"average_load":      avgLoad,
		"load_distribution": loadDist,
		"last_sync_time":    l.lastSyncTime,
	}
}

// syncActiveGateways sync active gateway instances from Redis
func (l *Locator) syncActiveGateways(ctx context.Context) error {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()

	// 1: Get all active gateway IDs from Redis (lockless)
	minScore := strconv.FormatInt(time.Now().Unix()-int64(l.config.HeartbeatWindow.Seconds()), 10)
	opt := &ZRangeOptions{Min: minScore, Max: "+inf"}
	activeIDs, err := l.registry.ZRangeByScore(ctx, ActiveGatewaysKey, opt)
	if err != nil {
		return fmt.Errorf("failed to get active gateway list: %v", err)
	}

	// 2: Build expected latest state and get detailed info for all instances (lockless)
	newState := make(map[string]*GatewayInstance)
	for _, instanceID := range activeIDs {
		instance, err := l.getInstanceDetails(ctx, instanceID)
		if err != nil {
			log.Printf("Failed to get instance %s details: %v, will skip this instance in current sync", instanceID, err)
			continue // Skip instances that failed to get details
		}
		newState[instanceID] = instance
	}

	// 3: Compare and update local state (one-time write lock)
	l.mu.Lock()
	defer l.mu.Unlock()

	var addedCount, removedCount int

	// Find and remove offline instances
	for localID := range l.instances {
		if _, existsInNewState := newState[localID]; !existsInNewState {
			if err := l.ring.Remove(ctx, localID); err != nil {
				log.Printf("Failed to remove member %s from the ring: %v", localID, err)
				continue
			}
			delete(l.instances, localID)
			removedCount++
			log.Printf("Removed gateway instance: %s", localID)
		}
	}

	// Find and add newly online instances
	for newID, newInstance := range newState {
		if _, existsLocally := l.instances[newID]; !existsLocally {
			if err := l.ring.Add(ctx, newInstance.MemberID()); err != nil {
				log.Printf("Failed to add member %s to the ring: %v", newID, err)
				continue
			}
			l.instances[newID] = newInstance
			addedCount++
			log.Printf("Added gateway instance: %s (%s)", newID, newInstance.GetAddress())
		}
	}

	if addedCount > 0 || removedCount > 0 {
		l.lastSyncTime = time.Now().Unix()
		log.Printf("Gateway instance sync completed, current active count: %d (added: %d, removed: %d)",
			len(l.instances), addedCount, removedCount)
	}

	return nil
}

// getInstanceDetails get instance details from Redis Hash
func (l *Locator) getInstanceDetails(ctx context.Context, instanceID string) (*GatewayInstance, error) {
	key := fmt.Sprintf(GatewayInstanceHashKeyFmt, instanceID)
	fields, err := l.registry.HGetAll(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("instance info does not exist")
	}

	host, ok := fields["host"]
	if !ok || host == "" {
		return nil, fmt.Errorf("instance %s is missing 'host' field", instanceID)
	}

	// Parse port number
	port := 8080
	if portStr, exists := fields["port"]; exists {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	// Parse last heartbeat time
	var lastHeartbeat int64
	if hbStr, exists := fields["last_heartbeat"]; exists {
		if hb, err := strconv.ParseInt(hbStr, 10, 64); err == nil {
			lastHeartbeat = hb
		}
	} else {
		return nil, fmt.Errorf("instance %s is missing 'last_heartbeat' field", instanceID)
	}

	return &GatewayInstance{
		ID:            instanceID,
		Host:          host,
		Port:          port,
		LastHeartbeat: lastHeartbeat,
	}, nil
}

// startMonitoring start background monitoring task
func (l *Locator) startMonitoring() {
	// Use sync interval defined in constants
	l.syncTicker = time.NewTicker(l.config.SyncInterval)

	var ctx context.Context
	ctx, l.cancelFn = context.WithCancel(context.Background())

	go l.periodicSync(ctx)
}

// periodicSync periodically sync active instances from Redis
func (l *Locator) periodicSync(ctx context.Context) {
	defer l.syncTicker.Stop()

	for {
		select {
		case <-l.syncTicker.C:
			if err := l.syncActiveGateways(ctx); err != nil {
				log.Printf("Failed to periodically sync active gateways: %v", err)
			}
		case <-ctx.Done():
			log.Printf("Periodic sync stopped due to context cancellation")
			return
		}
	}
}

// Stop stop locator
func (l *Locator) Stop() {
	if l.cancelFn != nil {
		l.cancelFn()
	}

	if l.syncTicker != nil {
		l.syncTicker.Stop()
	}
}

// ForceSync force sync (for manual trigger sync)
func (l *Locator) ForceSync(ctx context.Context) error {
	return l.syncActiveGateways(ctx)
}
