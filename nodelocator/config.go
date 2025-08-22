package nodelocator

import "time"

// Config holds all the tunable parameters for the nodelocator.
type Config struct {
	HeartbeatWindow   time.Duration
	HeartbeatInterval time.Duration
	SyncInterval      time.Duration

	// Cleaner specific
	CleanupInterval        time.Duration
	LeaderElectionInterval time.Duration
	LeaderLockTTL          time.Duration
}

// DefaultConfig returns a Config with sane defaults.
func DefaultConfig() *Config {
	return &Config{
		HeartbeatWindow:   90 * time.Second,
		HeartbeatInterval: 30 * time.Second,
		SyncInterval:      10 * time.Second,

		// Cleaner specific
		CleanupInterval:        5 * time.Minute,
		LeaderElectionInterval: 30 * time.Second,
		LeaderLockTTL:          60 * time.Second,
	}
}
