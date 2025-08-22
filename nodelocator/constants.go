package nodelocator

const (
	// ActiveGatewaysKey Redis ZSET key name for storing active gateway instances
	ActiveGatewaysKey = "active_gateways"

	// GatewayInstanceHashKeyFmt gateway instance detailed information Hash key format
	// Usage: fmt.Sprintf(GatewayInstanceHashKeyFmt, instanceID)
	GatewayInstanceHashKeyFmt = "gateway_instances:%s"

	// LeaderLockKey Redis key for leader election lock
	LeaderLockKey = "service:logic:leader_lock"
)
