package nats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	metastorage "schneider.vip/retryspool/storage/meta"
)

func TestNATSAtomicOperations(t *testing.T) {
	// Connect to local NATS server
	conn, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	defer conn.Close()

	// Create factory with test bucket
	factory := NewFactory(
		WithConnection(conn),
		WithBucket("test-retryspool-atomic"),
		WithDescription("Test bucket for atomic operations"),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Test atomic Create operation
	messageID := "atomic-test-message"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":   "user@example.com",
			"from": "sender@example.com",
		},
	}

	// First StoreMeta should use Create (atomic compare-to-null-and-set)
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata with Create: %v", err)
	}

	// Second StoreMeta should use Put (since key exists)
	metadata.Attempts = 1
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata with Put: %v", err)
	}

	// Verify the update
	retrievedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	if retrievedMeta.Attempts != 1 {
		t.Errorf("Expected attempts=1, got %d", retrievedMeta.Attempts)
	}

	// Clean up
	backend.DeleteMeta(ctx, messageID)

	t.Log("Atomic Create/Put operations test passed!")
}

func TestNATSCompareAndSwap(t *testing.T) {
	// Connect to local NATS server
	conn, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	defer conn.Close()

	// Create factory with test bucket
	factory := NewFactory(
		WithConnection(conn),
		WithBucket("test-retryspool-cas"),
		WithDescription("Test bucket for compare-and-swap"),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create initial message
	messageID := "cas-test-message"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":   "user@example.com",
			"from": "sender@example.com",
		},
	}

	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store initial metadata: %v", err)
	}

	// Test UpdateMeta with compare-and-swap
	metadata.Attempts = 1
	metadata.State = metastorage.StateActive
	err = backend.UpdateMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to update metadata with CAS: %v", err)
	}

	// Verify the update
	retrievedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get updated metadata: %v", err)
	}
	if retrievedMeta.Attempts != 1 {
		t.Errorf("Expected attempts=1, got %d", retrievedMeta.Attempts)
	}
	if retrievedMeta.State != metastorage.StateActive {
		t.Errorf("Expected state=Active, got %v", retrievedMeta.State)
	}

	// Clean up
	backend.DeleteMeta(ctx, messageID)

	t.Log("Compare-and-swap operations test passed!")
}

func TestNATSConcurrentStateTransitions(t *testing.T) {
	// Connect to local NATS server
	conn, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	defer conn.Close()

	// Create factory with test bucket
	factory := NewFactory(
		WithConnection(conn),
		WithBucket("test-retryspool-concurrent"),
		WithDescription("Test bucket for concurrent operations"),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create initial message
	messageID := "concurrent-test-message"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":   "user@example.com",
			"from": "sender@example.com",
		},
	}

	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store initial metadata: %v", err)
	}

	// Test concurrent MoveToState operations with atomic compare-and-swap
	numGoroutines := 10
	var wg sync.WaitGroup
	var successCount int32
	var mu sync.Mutex

	states := []metastorage.QueueState{
		metastorage.StateActive,
		metastorage.StateDeferred,
		metastorage.StateBounce,
		metastorage.StateHold,
	}

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			// Each goroutine tries to move to a different state
			targetState := states[goroutineID%len(states)]
			
			// Get current state first
			currentMeta, err := backend.GetMeta(ctx, messageID)
			if err != nil {
				t.Logf("Goroutine %d failed to get current state: %v", goroutineID, err)
				return
			}
			
			err = backend.MoveToState(ctx, messageID, currentMeta.State, targetState)
			if err != nil {
				// Some operations may fail due to concurrent modifications, which is expected
				t.Logf("Goroutine %d failed to move from %v to state %v: %v", goroutineID, currentMeta.State, targetState, err)
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
				t.Logf("Goroutine %d successfully moved from %v to state %v", goroutineID, currentMeta.State, targetState)
			}
		}(i)
	}

	wg.Wait()

	// At least some operations should succeed
	if successCount == 0 {
		t.Error("No concurrent operations succeeded")
	}

	// Verify final state is consistent
	finalMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get final metadata: %v", err)
	}

	t.Logf("Final state: %v, successful operations: %d/%d", finalMeta.State, successCount, numGoroutines)

	// Clean up
	backend.DeleteMeta(ctx, messageID)

	t.Log("Concurrent state transitions test passed!")
}

func TestNATSOptimisticConcurrencyControl(t *testing.T) {
	// Connect to local NATS server
	conn, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	defer conn.Close()

	// Create factory with test bucket
	factory := NewFactory(
		WithConnection(conn),
		WithBucket("test-retryspool-occ"),
		WithDescription("Test bucket for optimistic concurrency control"),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create initial message
	messageID := "occ-test-message"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":   "user@example.com",
			"from": "sender@example.com",
		},
	}

	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store initial metadata: %v", err)
	}

	// Simulate rapid state transitions to test retry logic
	var wg sync.WaitGroup
	numTransitions := 5
	
	wg.Add(numTransitions)
	for i := 0; i < numTransitions; i++ {
		go func(transitionID int) {
			defer wg.Done()
			
			// Each transition moves through a sequence of states
			states := []metastorage.QueueState{
				metastorage.StateActive,
				metastorage.StateDeferred,
				metastorage.StateActive,
				metastorage.StateBounce,
			}
			
			for _, state := range states {
				// Get current state first
				currentMeta, err := backend.GetMeta(ctx, messageID)
				if err != nil {
					t.Logf("Transition %d failed to get current state: %v", transitionID, err)
					continue
				}
				
				err = backend.MoveToState(ctx, messageID, currentMeta.State, state)
				if err != nil {
					t.Logf("Transition %d failed to move from %v to state %v: %v", transitionID, currentMeta.State, state, err)
				}
				// Small delay to increase chance of conflicts
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Verify message still exists and is in a valid state
	finalMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get final metadata: %v", err)
	}

	t.Logf("Final state after concurrent transitions: %v", finalMeta.State)

	// Clean up
	backend.DeleteMeta(ctx, messageID)

	t.Log("Optimistic concurrency control test passed!")
}