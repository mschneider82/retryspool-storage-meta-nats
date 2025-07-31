package nats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	metastorage "schneider.vip/retryspool/storage/meta"
)

func TestNATSMetaBackend(t *testing.T) {
	// Connect to local NATS server
	conn, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	defer conn.Close()

	// Create factory with test bucket
	factory := NewFactory(
		WithConnection(conn),
		WithBucket("test-retryspool-meta"),
		WithDescription("Test bucket for RetrySpooL metadata"),
		WithMaxValueSize(2*1024*1024), // 2MB
		WithHistory(1),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Test metadata
	messageID := "test-message-123"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		LastError:   "",
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":      "user@example.com",
			"from":    "sender@example.com",
			"subject": "Test Message",
		},
	}

	// Test StoreMeta
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}

	// Test GetMeta
	retrievedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	// Verify metadata
	if retrievedMeta.ID != metadata.ID {
		t.Errorf("ID mismatch: expected %s, got %s", metadata.ID, retrievedMeta.ID)
	}
	if retrievedMeta.State != metadata.State {
		t.Errorf("State mismatch: expected %v, got %v", metadata.State, retrievedMeta.State)
	}
	if retrievedMeta.Headers["subject"] != metadata.Headers["subject"] {
		t.Errorf("Header mismatch: expected %s, got %s", metadata.Headers["subject"], retrievedMeta.Headers["subject"])
	}

	// Test UpdateMeta - change state
	metadata.State = metastorage.StateActive
	metadata.Attempts = 1
	metadata.Updated = time.Now()
	
	err = backend.UpdateMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}

	// Verify update
	updatedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get updated metadata: %v", err)
	}
	if updatedMeta.State != metastorage.StateActive {
		t.Errorf("State not updated: expected %v, got %v", metastorage.StateActive, updatedMeta.State)
	}
	if updatedMeta.Attempts != 1 {
		t.Errorf("Attempts not updated: expected 1, got %d", updatedMeta.Attempts)
	}

	// Test MoveToState
	err = backend.MoveToState(ctx, messageID, metastorage.StateActive, metastorage.StateDeferred)
	if err != nil {
		t.Fatalf("Failed to move to state: %v", err)
	}

	movedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get moved metadata: %v", err)
	}
	if movedMeta.State != metastorage.StateDeferred {
		t.Errorf("State not moved: expected %v, got %v", metastorage.StateDeferred, movedMeta.State)
	}

	// Test ListMessages
	result, err := backend.ListMessages(ctx, metastorage.StateDeferred, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}

	if len(result.MessageIDs) != 1 {
		t.Errorf("Expected 1 message in deferred state, got %d", len(result.MessageIDs))
	}
	if len(result.MessageIDs) > 0 && result.MessageIDs[0] != messageID {
		t.Errorf("Expected message ID %s, got %s", messageID, result.MessageIDs[0])
	}

	// Test DeleteMeta
	err = backend.DeleteMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete metadata: %v", err)
	}

	// Verify deletion
	_, err = backend.GetMeta(ctx, messageID)
	if err == nil {
		t.Error("Expected error when getting deleted metadata")
	}

	t.Log("All NATS metadata backend tests passed!")
}

func TestNATSMetaBackendMultipleMessages(t *testing.T) {
	// Connect to local NATS server
	conn, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	defer conn.Close()

	// Create factory with test bucket
	factory := NewFactory(
		WithConnection(conn),
		WithBucket("test-retryspool-meta-multi"),
		WithDescription("Test bucket for multiple messages"),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create multiple messages in different states
	messages := []struct {
		id    string
		state metastorage.QueueState
	}{
		{"msg-1", metastorage.StateIncoming},
		{"msg-2", metastorage.StateIncoming},
		{"msg-3", metastorage.StateActive},
		{"msg-4", metastorage.StateDeferred},
		{"msg-5", metastorage.StateDeferred},
	}

	// Store all messages
	for i, msg := range messages {
		metadata := metastorage.MessageMetadata{
			ID:          msg.id,
			State:       msg.state,
			Attempts:    0,
			MaxAttempts: 3,
			NextRetry:   time.Now().Add(time.Hour),
			Created:     time.Now().Add(time.Duration(i) * time.Second), // Different timestamps
			Updated:     time.Now(),
			Size:        1024,
			Priority:    5,
			Headers: map[string]string{
				"to":   "user@example.com",
				"from": "sender@example.com",
			},
		}

		err = backend.StoreMeta(ctx, msg.id, metadata)
		if err != nil {
			t.Fatalf("Failed to store metadata for %s: %v", msg.id, err)
		}
	}

	// Test listing incoming messages
	result, err := backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list incoming messages: %v", err)
	}

	if len(result.MessageIDs) != 2 {
		t.Errorf("Expected 2 incoming messages, got %d", len(result.MessageIDs))
	}

	// Test listing deferred messages
	result, err = backend.ListMessages(ctx, metastorage.StateDeferred, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list deferred messages: %v", err)
	}

	if len(result.MessageIDs) != 2 {
		t.Errorf("Expected 2 deferred messages, got %d", len(result.MessageIDs))
	}

	// Test pagination
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:     1,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages with pagination: %v", err)
	}

	if len(result.MessageIDs) != 1 {
		t.Errorf("Expected 1 message with limit=1, got %d", len(result.MessageIDs))
	}
	if !result.HasMore {
		t.Error("Expected HasMore=true with pagination")
	}

	// Clean up
	for _, msg := range messages {
		backend.DeleteMeta(ctx, msg.id)
	}

	t.Log("Multiple messages test passed!")
}