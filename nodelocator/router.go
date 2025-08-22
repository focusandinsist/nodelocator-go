package nodelocator

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"nodelocator/consistent"
)

// GatewayInstance gateway instance information
type GatewayInstance struct {
	ID            string `json:"id"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	LastHeartbeat int64  `json:"last_heartbeat"`
}

// String implement consistent.Member interface
func (g *GatewayInstance) String() string {
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
	stopCh       chan struct{}
	syncTicker   *time.Ticker
	lastSyncTime int64 // Last sync timestamp, used for detecting changes
	config       *Config
}

// NewLocator creates a new session locator.
func NewLocator(registry ServiceRegistry, consisCfg consistent.Config, cfg *Config) *Locator {

	if cfg == nil {
		cfg = DefaultConfig()
	}

	locator := &Locator{
		registry:  registry,
		ring:      consistent.New(nil, consisCfg),
		instances: make(map[string]*GatewayInstance),
		stopCh:    make(chan struct{}),
		config:    cfg,
	}

	// Sync active instances from Redis at startup
	if err := locator.syncActiveGateways(); err != nil {
		log.Printf("Failed to sync active gateways during initialization: %v", err)
	}

	// Start background monitoring task
	locator.startMonitoring()

	return locator
}

// GetGatewayForUser gets the assigned gateway instance for a given user ID.
func (l *Locator) GetGatewayForUser(userID string) (*GatewayInstance, error) {
	return l.getGateway("user:" + userID)
}

// GetGatewayForRoom gets the assigned gateway instance for a given room ID.
func (l *Locator) GetGatewayForRoom(roomID string) (*GatewayInstance, error) {
	return l.getGateway("room:" + roomID)
}

// getGateway is the internal implementation for locating a gateway.
// It takes a key, computes the hash, and finds the corresponding instance.
func (l *Locator) getGateway(key string) (*GatewayInstance, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.instances) == 0 {
		return nil, fmt.Errorf("no available gateway instances")
	}

	member := l.ring.LocateKey([]byte(key))
	if member == nil {
		// This should theoretically not happen if instances are available
		return nil, fmt.Errorf("could not locate a gateway for the given key")
	}

	instance, ok := l.instances[member.String()]
	if !ok {
		return nil, fmt.Errorf("instance %s found on hash ring does not exist in local cache, data may be inconsistent", member.String())
	}

	return instance, nil
}

// GetAllActiveGateways get all active gateway instances
func (l *Locator) GetAllActiveGateways() []*GatewayInstance {
	l.mu.RLock()
	defer l.mu.RUnlock()

	instances := make([]*GatewayInstance, 0, len(l.instances))
	for _, instance := range l.instances {
		instances = append(instances, instance)
	}
	return instances
}

// GetStats get routing statistics
func (l *Locator) GetStats() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	loadDist := l.ring.LoadDistribution()
	return map[string]interface{}{
		"active_gateways":   len(l.instances),
		"average_load":      l.ring.AverageLoad(),
		"load_distribution": loadDist,
		"last_sync_time":    l.lastSyncTime,
	}
}

// syncActiveGateways sync active gateway instances from Redis
func (l *Locator) syncActiveGateways() error {
	ctx := context.Background()

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
			l.ring.Remove(localID)
			delete(l.instances, localID)
			removedCount++
			log.Printf("Removed gateway instance: %s", localID)
		}
	}

	// Find and add newly online instances
	for newID, newInstance := range newState {
		if _, existsLocally := l.instances[newID]; !existsLocally {
			l.instances[newID] = newInstance
			l.ring.Add(newInstance)
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

	// Parse port number
	port := 8080 // Default value
	if portStr, exists := fields["port"]; exists {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	// Parse last heartbeat time
	lastHeartbeat := time.Now().Unix()
	if hbStr, exists := fields["last_ping"]; exists {
		if hb, err := strconv.ParseInt(hbStr, 10, 64); err == nil {
			lastHeartbeat = hb
		}
	}

	return &GatewayInstance{
		ID:            instanceID,
		Host:          fields["host"],
		Port:          port,
		LastHeartbeat: lastHeartbeat,
	}, nil
}

// startMonitoring start background monitoring task
func (l *Locator) startMonitoring() {
	// Use sync interval defined in constants
	l.syncTicker = time.NewTicker(l.config.SyncInterval)
	go l.periodicSync()
}

// periodicSync periodically sync active instances from Redis
func (l *Locator) periodicSync() {
	defer l.syncTicker.Stop()

	for {
		select {
		case <-l.syncTicker.C:
			if err := l.syncActiveGateways(); err != nil {
				log.Printf("Failed to periodically sync active gateways: %v", err)
			}
		case <-l.stopCh:
			return
		}
	}
}

// Stop stop locator
func (l *Locator) Stop() {
	close(l.stopCh)
	if l.syncTicker != nil {
		l.syncTicker.Stop()
	}
}

// ForceSync force sync (for manual trigger sync)
func (l *Locator) ForceSync() error {
	return l.syncActiveGateways()
}
