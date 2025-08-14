package consistent

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConsistentHash test basic functionality of consistent hash
func TestConsistentHash(t *testing.T) {
	// Create gateway members
	members := []Member{
		NewGatewayMember("gateway-1", "192.168.1.1", 8080),
		NewGatewayMember("gateway-2", "192.168.1.2", 8080),
		NewGatewayMember("gateway-3", "192.168.1.3", 8080),
	}

	// Create configuration
	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	// Create consistent hash ring
	ring := New(members, config)

	// Test key location
	testKeys := []string{
		"user:1001",
		"user:1002",
		"user:1003",
		"room:2001",
		"room:2002",
	}

	fmt.Println("Key distribution test:")
	for _, key := range testKeys {
		member := ring.LocateKey([]byte(key))
		if member != nil {
			fmt.Printf("Key %s -> Member %s\n", key, member.String())
		}
	}

	// Test load distribution
	fmt.Println("\nLoad distribution:")
	loadDist := ring.LoadDistribution()
	for member, load := range loadDist {
		fmt.Printf("Member %s: load %.2f\n", member, load)
	}

	// Test adding member
	fmt.Println("\nAdding new member...")
	newMember := NewGatewayMember("gateway-4", "192.168.1.4", 8080)
	ring.Add(newMember)

	// Test key distribution again
	fmt.Println("\nKey distribution after adding member:")
	for _, key := range testKeys {
		member := ring.LocateKey([]byte(key))
		if member != nil {
			fmt.Printf("Key %s -> Member %s\n", key, member.String())
		}
	}

	// Test removing member
	fmt.Println("\nRemoving member...")
	ring.Remove("gateway-2:192.168.1.2:8080")

	// Test key distribution again
	fmt.Println("\nKey distribution after removing member:")
	for _, key := range testKeys {
		member := ring.LocateKey([]byte(key))
		if member != nil {
			fmt.Printf("Key %s -> Member %s\n", key, member.String())
		}
	}

	// Test getting closest N members
	fmt.Println("\nGetting closest members:")
	closest, err := ring.GetClosestN([]byte("user:1001"), 2)
	if err != nil {
		t.Errorf("Failed to get closest members: %v", err)
	} else {
		for i, member := range closest {
			fmt.Printf("Closest member %d: %s\n", i+1, member.String())
		}
	}
}

// TestConsistentHashWithFNV test consistent hash using FNV hasher
func TestConsistentHashWithFNV(t *testing.T) {
	members := []Member{
		NewGatewayMember("gateway-1", "127.0.0.1", 8080),
		NewGatewayMember("gateway-2", "127.0.0.1", 8081),
	}

	config := Config{
		Hasher:            NewFNVHasher(),
		PartitionCount:    100,
		ReplicationFactor: 10,
		Load:              1.5,
	}

	ring := New(members, config)

	// Test average load
	avgLoad := ring.AverageLoad()
	fmt.Printf("Average load: %.2f\n", avgLoad)

	// Test member list
	allMembers := ring.GetMembers()
	fmt.Printf("Member count: %d\n", len(allMembers))
	for _, member := range allMembers {
		fmt.Printf("Member: %s\n", member.String())
	}
}

// BenchmarkLocateKey benchmark test for key location performance
func BenchmarkLocateKey(b *testing.B) {
	members := []Member{
		NewGatewayMember("gateway-1", "192.168.1.1", 8080),
		NewGatewayMember("gateway-2", "192.168.1.2", 8080),
		NewGatewayMember("gateway-3", "192.168.1.3", 8080),
		NewGatewayMember("gateway-4", "192.168.1.4", 8080),
		NewGatewayMember("gateway-5", "192.168.1.5", 8080),
	}

	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring := New(members, config)
	key := []byte("user:1001")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.LocateKey(key)
	}
}

// BenchmarkAddRemove benchmark test for add and remove member performance
func BenchmarkAddRemove(b *testing.B) {
	members := []Member{
		NewGatewayMember("gateway-1", "192.168.1.1", 8080),
		NewGatewayMember("gateway-2", "192.168.1.2", 8080),
	}

	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring := New(members, config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add member
		newMember := NewGatewayMember(fmt.Sprintf("gateway-%d", i+3), "192.168.1.10", 8080)
		ring.Add(newMember)

		// Remove member
		ring.Remove(newMember.String())
	}
}

// BenchmarkConcurrentRead benchmark test for concurrent read performance
func BenchmarkConcurrentRead(b *testing.B) {
	members := []Member{
		NewGatewayMember("gateway-1", "192.168.1.1", 8080),
		NewGatewayMember("gateway-2", "192.168.1.2", 8080),
		NewGatewayMember("gateway-3", "192.168.1.3", 8080),
		NewGatewayMember("gateway-4", "192.168.1.4", 8080),
		NewGatewayMember("gateway-5", "192.168.1.5", 8080),
	}

	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring := New(members, config)
	key := []byte("user:1001")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ring.LocateKey(key)
		}
	})
}

// BenchmarkGetMembers benchmark test for getting member list performance (test cache effect)
func BenchmarkGetMembers(b *testing.B) {
	members := []Member{
		NewGatewayMember("gateway-1", "192.168.1.1", 8080),
		NewGatewayMember("gateway-2", "192.168.1.2", 8080),
		NewGatewayMember("gateway-3", "192.168.1.3", 8080),
		NewGatewayMember("gateway-4", "192.168.1.4", 8080),
		NewGatewayMember("gateway-5", "192.168.1.5", 8080),
	}

	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring := New(members, config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetMembers()
	}
}

// BenchmarkLargeScale large scale benchmark test
func BenchmarkLargeScale(b *testing.B) {
	// Create 100 members
	var members []Member
	for i := 0; i < 100; i++ {
		members = append(members, NewGatewayMember(
			fmt.Sprintf("gateway-%d", i),
			fmt.Sprintf("192.168.%d.%d", i/254+1, i%254+1),
			8080,
		))
	}

	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    1000, // More partitions
		ReplicationFactor: 50,   // More virtual nodes
		Load:              1.25,
	}

	ring := New(members, config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("user:%d", i))
		ring.LocateKey(key)
	}
}

// TestConcurrentSafety test concurrent safety
func TestConcurrentSafety(t *testing.T) {
	members := []Member{
		NewGatewayMember("gateway-1", "192.168.1.1", 8080),
		NewGatewayMember("gateway-2", "192.168.1.2", 8080),
	}

	config := Config{
		Hasher:            NewCRC64Hasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring := New(members, config)

	// Concurrent test for GetMembers race condition fix
	t.Run("GetMembers concurrent safety", func(t *testing.T) {
		const numGoroutines = 50
		const numCalls = 100

		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numCalls; j++ {
					members := ring.GetMembers()
					if len(members) == 0 {
						t.Errorf("GetMembers returned empty list")
					}
				}
			}()
		}
		wg.Wait()
	})

	// Concurrent read-write test
	t.Run("concurrent read-write safety", func(t *testing.T) {
		const duration = 2 * time.Second
		const numReaders = 20
		const numWriters = 2

		var wg sync.WaitGroup
		stop := make(chan struct{})

		// Start reader goroutines
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						key := fmt.Sprintf("user:%d", id)
						ring.LocateKey([]byte(key))
						ring.GetMembers()
					}
				}
			}(i)
		}

		// Start writer goroutines
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				counter := 0
				for {
					select {
					case <-stop:
						return
					default:
						if counter%2 == 0 {
							member := NewGatewayMember(
								fmt.Sprintf("temp-%d-%d", id, counter),
								fmt.Sprintf("10.0.%d.%d", id, counter%254+1),
								8080,
							)
							ring.Add(member)
						} else {
							memberName := fmt.Sprintf("temp-%d-%d:10.0.%d.%d:8080",
								id, counter-1, id, (counter-1)%254+1)
							ring.Remove(memberName)
						}
						counter++
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(i)
		}

		time.Sleep(duration)
		close(stop)
		wg.Wait()
	})
}
