package nodelocator

import "time"

const (
	// ActiveGatewaysKey Redis ZSET key name for storing active gateway instances
	ActiveGatewaysKey = "active_gateways"

	// GatewayInstanceHashKeyFmt gateway instance detailed information Hash key format
	// Usage: fmt.Sprintf(GatewayInstanceHashKeyFmt, instanceID)
	GatewayInstanceHashKeyFmt = "gateway_instances:%s"

	// HeartbeatWindow heartbeat window time (seconds), instances are considered inactive if exceeded
	HeartbeatWindow = 90

	// CleanupInterval interval time for cleaning expired instances
	CleanupInterval = 60 * time.Second

	// HeartbeatInterval heartbeat sending interval time
	HeartbeatInterval = 30 * time.Second

	// SyncInterval synchronization interval time
	SyncInterval = 10 * time.Second
)
