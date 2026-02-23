package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	metastorage "schneider.vip/retryspool/storage/meta"
)

// StateCounters tracks message counts per state using NATS KeyValue watch
type StateCounters struct {
	mu       sync.RWMutex
	counters map[metastorage.QueueState]int64
	watcher  nats.KeyWatcher
	ctx      context.Context
	cancel   context.CancelFunc
}

// Backend implements metastorage.Backend using NATS JetStream Key-Value store
type Backend struct {
	conn          *nats.Conn
	js            nats.JetStreamContext
	kv            nats.KeyValue
	bucket        string
	ownConn       bool // Whether we own the connection and should close it
	stateCounters *StateCounters
	mu            sync.RWMutex
	closed        bool
}

// NewBackend creates a new NATS metadata storage backend
func NewBackend(config *Config) (*Backend, error) {
	var conn *nats.Conn
	var ownConn bool
	var err error

	// Use existing connection or create new one
	if config.Connection != nil {
		conn = config.Connection
		ownConn = false
	} else {
		conn, err = nats.Connect(config.URL)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}
		ownConn = true
	}

	// Get JetStream context
	js, err := conn.JetStream()
	if err != nil {
		if ownConn {
			conn.Close()
		}
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	// Create or get KV bucket
	kvConfig := &nats.KeyValueConfig{
		Bucket:       config.Bucket,
		Description:  config.JSConfig.Description,
		MaxValueSize: int32(config.JSConfig.MaxValueSize),
		History:      config.JSConfig.History,
		Replicas:     config.JSConfig.Replicas,
	}

	if config.JSConfig.TTL > 0 {
		kvConfig.TTL = time.Duration(config.JSConfig.TTL) * time.Second
	}

	kv, err := js.CreateKeyValue(kvConfig)
	if err != nil {
		// Try to get existing bucket if creation failed
		kv, err = js.KeyValue(config.Bucket)
		if err != nil {
			if ownConn {
				conn.Close()
			}
			return nil, fmt.Errorf("failed to create or get KV bucket '%s': %w", config.Bucket, err)
		}
	}

	backend := &Backend{
		conn:    conn,
		js:      js,
		kv:      kv,
		bucket:  config.Bucket,
		ownConn: ownConn,
	}

	// Initialize state counters with watch
	if err := backend.initStateCounters(); err != nil {
		if ownConn {
			conn.Close()
		}
		return nil, fmt.Errorf("failed to initialize state counters: %w", err)
	}

	return backend, nil
}

// StoreMeta stores message metadata using atomic Create for new messages or Put for updates
func (b *Backend) StoreMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	key := b.metadataKey(messageID)

	// Check if key exists first
	_, err = b.kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			// Key doesn't exist, use atomic Create
			_, err = b.kv.Create(key, data)
			if err != nil {
				return fmt.Errorf("failed to create metadata for message %s: %w", messageID, err)
			}
		} else {
			return fmt.Errorf("failed to check existing metadata for message %s: %w", messageID, err)
		}
	} else {
		// Key exists, use Put to overwrite
		_, err = b.kv.Put(key, data)
		if err != nil {
			return fmt.Errorf("failed to update existing metadata for message %s: %w", messageID, err)
		}
	}

	// Also store state index for efficient querying
	stateKey := b.stateIndexKey(metadata.State, messageID)
	stateData := fmt.Sprintf("%d:%s", metadata.Created.Unix(), messageID)
	_, err = b.kv.Put(stateKey, []byte(stateData))
	if err != nil {
		return fmt.Errorf("failed to store state index for message %s: %w", messageID, err)
	}

	return nil
}

// GetMeta retrieves message metadata
func (b *Backend) GetMeta(ctx context.Context, messageID string) (metastorage.MessageMetadata, error) {
	key := b.metadataKey(messageID)
	entry, err := b.kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return metastorage.MessageMetadata{}, fmt.Errorf("message %s not found", messageID)
		}
		return metastorage.MessageMetadata{}, fmt.Errorf("failed to get metadata for message %s: %w", messageID, err)
	}

	var metadata metastorage.MessageMetadata
	err = json.Unmarshal(entry.Value(), &metadata)
	if err != nil {
		return metastorage.MessageMetadata{}, fmt.Errorf("failed to unmarshal metadata for message %s: %w", messageID, err)
	}

	return metadata, nil
}

// UpdateMeta updates message metadata using atomic compare-and-swap
func (b *Backend) UpdateMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	key := b.metadataKey(messageID)

	// Get current entry with revision for compare-and-swap
	currentEntry, err := b.kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return fmt.Errorf("message %s not found", messageID)
		}
		return fmt.Errorf("failed to get current metadata for message %s: %w", messageID, err)
	}

	// Parse current metadata to check if state changed
	var currentMeta metastorage.MessageMetadata
	err = json.Unmarshal(currentEntry.Value(), &currentMeta)
	if err != nil {
		return fmt.Errorf("failed to unmarshal current metadata for message %s: %w", messageID, err)
	}

	// Marshal new metadata
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Atomic compare-and-swap update
	_, err = b.kv.Update(key, data, currentEntry.Revision())
	if err != nil {
		// Check for concurrent modification error (revision mismatch)
		if strings.Contains(err.Error(), "wrong last sequence") || strings.Contains(err.Error(), "revision") {
			return fmt.Errorf("concurrent modification detected for message %s, please retry", messageID)
		}
		return fmt.Errorf("failed to update metadata for message %s: %w", messageID, err)
	}

	// If state changed, update state index atomically
	if currentMeta.State != metadata.State {
		// Remove old state index
		oldStateKey := b.stateIndexKey(currentMeta.State, messageID)
		err = b.kv.Delete(oldStateKey)
		if err != nil && err != nats.ErrKeyNotFound {
			// Log warning but don't fail the update
			fmt.Printf("Warning: failed to delete old state index %s: %v\n", oldStateKey, err)
		}

		// Add new state index
		newStateKey := b.stateIndexKey(metadata.State, messageID)
		stateData := fmt.Sprintf("%d:%s", metadata.Created.Unix(), messageID)
		_, err = b.kv.Put(newStateKey, []byte(stateData))
		if err != nil {
			return fmt.Errorf("failed to store new state index for message %s: %w", messageID, err)
		}
	}

	return nil
}

// DeleteMeta removes message metadata
func (b *Backend) DeleteMeta(ctx context.Context, messageID string) error {
	// Get current metadata to clean up state index
	metadata, err := b.GetMeta(ctx, messageID)
	if err != nil {
		return err
	}

	// Delete metadata
	key := b.metadataKey(messageID)
	err = b.kv.Delete(key)
	if err != nil && err != nats.ErrKeyNotFound {
		return fmt.Errorf("failed to delete metadata for message %s: %w", messageID, err)
	}

	// Delete state index
	stateKey := b.stateIndexKey(metadata.State, messageID)
	err = b.kv.Delete(stateKey)
	if err != nil && err != nats.ErrKeyNotFound {
		// Log warning but don't fail the delete
		fmt.Printf("Warning: failed to delete state index %s: %v\n", stateKey, err)
	}

	return nil
}

// ListMessages lists messages with pagination and filtering
func (b *Backend) ListMessages(ctx context.Context, state metastorage.QueueState, options metastorage.MessageListOptions) (metastorage.MessageListResult, error) {
	// Get all keys for the given state using efficient Keys() with filter
	statePrefix := b.statePrefix(state)

	// Use Keys() - much more efficient than WatchAll()
	keys, err := b.kv.Keys(nats.IgnoreDeletes())
	if err != nil {
		// Check if it's just "no keys found" which is not an error
		if err.Error() == "nats: no keys found" {
			return metastorage.MessageListResult{
				MessageIDs: []string{},
				Total:      0,
				HasMore:    false,
			}, nil
		}
		return metastorage.MessageListResult{}, fmt.Errorf("failed to list keys for state %s: %w", state.String(), err)
	}

	// Filter keys by prefix manually (still faster than WatchAll())
	var filteredKeys []string
	for _, key := range keys {
		if strings.HasPrefix(key, statePrefix) {
			filteredKeys = append(filteredKeys, key)
		}
	}
	keys = filteredKeys

	if len(keys) == 0 {
		return metastorage.MessageListResult{
			MessageIDs: []string{},
			Total:      0,
			HasMore:    false,
		}, nil
	}

	// Parse and sort entries
	type stateEntry struct {
		timestamp int64
		messageID string
		key       string
	}

	var entries []stateEntry
	for _, key := range keys {
		entry, err := b.kv.Get(key)
		if err != nil {
			continue // Skip if entry was deleted between listing and getting
		}

		parts := strings.SplitN(string(entry.Value()), ":", 2)
		if len(parts) != 2 {
			continue // Skip malformed entries
		}

		timestamp, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue // Skip malformed timestamps
		}

		// Apply time filter if specified
		if !options.Since.IsZero() && timestamp < options.Since.Unix() {
			continue
		}

		entries = append(entries, stateEntry{
			timestamp: timestamp,
			messageID: parts[1],
			key:       key,
		})
	}

	// Sort entries
	sort.Slice(entries, func(i, j int) bool {
		if options.SortOrder == "desc" {
			return entries[i].timestamp > entries[j].timestamp
		}
		return entries[i].timestamp < entries[j].timestamp
	})

	// Apply pagination
	total := len(entries)
	start := options.Offset
	if start > total {
		start = total
	}

	end := start + options.Limit
	if options.Limit <= 0 || end > total {
		end = total
	}

	var messageIDs []string
	for i := start; i < end; i++ {
		messageIDs = append(messageIDs, entries[i].messageID)
	}

	return metastorage.MessageListResult{
		MessageIDs: messageIDs,
		Total:      total,
		HasMore:    end < total,
	}, nil
}

// NewMessageIterator creates an iterator for messages in a specific state
func (b *Backend) NewMessageIterator(ctx context.Context, state metastorage.QueueState, batchSize int) (metastorage.MessageIterator, error) {
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	return &natsIterator{
		backend:   b,
		state:     state,
		batchSize: batchSize,
		ctx:       ctx,
		offset:    0,
	}, nil
}

// natsIterator implements MessageIterator for NATS backend
type natsIterator struct {
	backend   *Backend
	state     metastorage.QueueState
	batchSize int
	ctx       context.Context

	// Current state
	keys     []string
	current  []metastorage.MessageMetadata
	index    int
	offset   int
	finished bool
	closed   bool
	mu       sync.RWMutex
}

// Next returns the next message metadata
func (it *natsIterator) Next(ctx context.Context) (metastorage.MessageMetadata, bool, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("iterator is closed")
	}

	// Initialize or load next batch if needed
	if it.current == nil || (it.index >= len(it.current) && !it.finished) {
		if err := it.loadBatch(ctx); err != nil {
			return metastorage.MessageMetadata{}, false, err
		}
	}

	// Check if we're at the end
	if it.index >= len(it.current) {
		return metastorage.MessageMetadata{}, false, nil
	}

	// Return current message and advance
	metadata := it.current[it.index]
	it.index++

	// Check if there are more messages
	hasMore := it.index < len(it.current) || !it.finished

	return metadata, hasMore, nil
}

// loadBatch loads the next batch of messages using streaming approach
func (it *natsIterator) loadBatch(ctx context.Context) error {
	if it.finished {
		return nil
	}

	// Use Watch-based streaming instead of loading all keys at once
	statePrefix := it.backend.statePrefix(it.state)

	// Create a watcher for the state prefix
	watcher, err := it.backend.kv.Watch(statePrefix + "*")
	if err != nil {
		return fmt.Errorf("failed to create watcher for state %s: %w", it.state.String(), err)
	}
	defer watcher.Stop()

	// Collect batch of messages
	var batch []metastorage.MessageMetadata
	collected := 0
	skipped := 0

	// Process watch updates to get current state
	for update := range watcher.Updates() {
		if update == nil {
			break // No more updates
		}

		// Skip deleted entries
		if update.Operation() == nats.KeyValueDelete {
			continue
		}

		// Skip entries we've already processed (offset-based)
		if skipped < it.offset {
			skipped++
			continue
		}

		// Extract message ID from state index key
		key := update.Key()
		parts := strings.Split(key, ".")
		if len(parts) < 3 {
			continue // Skip malformed keys
		}
		messageID := parts[2]

		// Get the actual metadata using the message ID
		metadata, err := it.backend.GetMeta(ctx, messageID)
		if err != nil {
			continue // Skip if metadata not found
		}

		batch = append(batch, metadata)
		collected++

		// Stop when we have enough for this batch
		if collected >= it.batchSize {
			break
		}
	}

	// Update iterator state
	it.current = batch
	it.index = 0
	it.offset += len(batch)

	// Mark as finished if we got fewer messages than requested
	if len(batch) < it.batchSize {
		it.finished = true
	}

	return nil
}

// Close closes the iterator
func (it *natsIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.keys = nil
	it.current = nil
	return nil
}

// MoveToState moves a message to a different queue state using atomic operations
func (b *Backend) MoveToState(ctx context.Context, messageID string, fromState, toState metastorage.QueueState) error {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return metastorage.ErrBackendClosed
	}

	key := b.metadataKey(messageID)

	// Retry loop for optimistic concurrency control
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get current entry with revision
		currentEntry, err := b.kv.Get(key)
		if err != nil {
			if err == nats.ErrKeyNotFound {
				return metastorage.ErrMessageNotFound
			}
			return fmt.Errorf("failed to get current metadata for message %s: %w", messageID, err)
		}

		// Parse current metadata
		var metadata metastorage.MessageMetadata
		err = json.Unmarshal(currentEntry.Value(), &metadata)
		if err != nil {
			return fmt.Errorf("failed to unmarshal metadata for message %s: %w", messageID, err)
		}

		// Atomic check: ensure message is in expected fromState
		if metadata.State != fromState {
			return metastorage.ErrStateConflict
		}

		// Check if state is already the target state
		if metadata.State == toState {
			return nil // Already in target state
		}

		oldState := metadata.State

		// Update state and timestamp
		metadata.State = toState
		metadata.Updated = time.Now()

		// Marshal updated metadata
		data, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		// Atomic compare-and-swap update
		_, err = b.kv.Update(key, data, currentEntry.Revision())
		if err != nil {
			// Check for concurrent modification error (revision mismatch)
			if strings.Contains(err.Error(), "wrong last sequence") || strings.Contains(err.Error(), "revision") {
				// Concurrent modification, retry
				if attempt < maxRetries-1 {
					time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond) // Exponential backoff
					continue
				}
				return fmt.Errorf("failed to update message %s after %d attempts due to concurrent modifications", messageID, maxRetries)
			}
			return fmt.Errorf("failed to update metadata for message %s: %w", messageID, err)
		}

		// Successfully updated metadata, now update state indexes
		// Remove old state index
		oldStateKey := b.stateIndexKey(oldState, messageID)
		err = b.kv.Delete(oldStateKey)
		if err != nil && err != nats.ErrKeyNotFound {
			// Log warning but don't fail the operation
			fmt.Printf("Warning: failed to delete old state index %s: %v\n", oldStateKey, err)
		}

		// Add new state index
		newStateKey := b.stateIndexKey(toState, messageID)
		stateData := fmt.Sprintf("%d:%s", metadata.Created.Unix(), messageID)
		_, err = b.kv.Put(newStateKey, []byte(stateData))
		if err != nil {
			// Log warning but don't fail the operation since main update succeeded
			fmt.Printf("Warning: failed to store new state index %s: %v\n", newStateKey, err)
		}

		return nil // Success
	}

	return fmt.Errorf("failed to move message %s to state %s after %d attempts", messageID, toState, maxRetries)
}

// Close closes the NATS connection if we own it
func (b *Backend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	if b.stateCounters != nil {
		b.stateCounters.cancel()
		if b.stateCounters.watcher != nil {
			b.stateCounters.watcher.Stop()
		}
	}
	if b.ownConn && b.conn != nil {
		b.conn.Close()
	}
	return nil
}

// Helper methods for key generation

func (b *Backend) metadataKey(messageID string) string {
	return fmt.Sprintf("meta.%s", messageID)
}

func (b *Backend) statePrefix(state metastorage.QueueState) string {
	return fmt.Sprintf("state.%s.", state.String())
}

func (b *Backend) stateIndexKey(state metastorage.QueueState, messageID string) string {
	return fmt.Sprintf("state.%s.%s", state.String(), messageID)
}

// initStateCounters initializes the state counters with NATS KeyValue watch
func (b *Backend) initStateCounters() error {
	ctx, cancel := context.WithCancel(context.Background())

	b.stateCounters = &StateCounters{
		counters: make(map[metastorage.QueueState]int64),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Initialize counters by counting existing keys
	if err := b.initCountersFromExistingKeys(); err != nil {
		cancel()
		return fmt.Errorf("failed to initialize counters from existing keys: %w", err)
	}

	// Start watching for changes (including deletes for state counting)
	watcher, err := b.kv.WatchAll()
	if err != nil {
		cancel()
		return fmt.Errorf("failed to create watch: %w", err)
	}

	b.stateCounters.watcher = watcher

	// Start background goroutine to process watch events
	go b.processWatchEvents()

	return nil
}

// initCountersFromExistingKeys counts existing state keys to initialize counters
func (b *Backend) initCountersFromExistingKeys() error {
	keys, err := b.kv.Keys(nats.IgnoreDeletes())
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return nil // No existing keys, start with zero counts
		}
		return fmt.Errorf("failed to list existing keys: %w", err)
	}

	b.stateCounters.mu.Lock()
	defer b.stateCounters.mu.Unlock()

	for _, key := range keys {
		if strings.HasPrefix(key, "state.") {
			state := b.extractStateFromKey(key)
			if state != "" {
				if queueState := parseQueueState(state); state == "incoming" || state == "active" || state == "deferred" || state == "hold" || state == "bounce" {
					b.stateCounters.counters[queueState]++
				}
			}
		}
	}

	return nil
}

// processWatchEvents processes NATS KeyValue watch events to update counters
func (b *Backend) processWatchEvents() {
	for {
		select {
		case <-b.stateCounters.ctx.Done():
			return
		case entry := <-b.stateCounters.watcher.Updates():
			if entry == nil {
				continue
			}

			key := entry.Key()
			if !strings.HasPrefix(key, "state.") {
				continue
			}

			state := b.extractStateFromKey(key)
			if state == "" || (state != "incoming" && state != "active" && state != "deferred" && state != "hold" && state != "bounce") {
				continue
			}

			queueState := parseQueueState(state)

			b.stateCounters.mu.Lock()
			switch entry.Operation() {
			case nats.KeyValuePut:
				b.stateCounters.counters[queueState]++
			case nats.KeyValueDelete:
				if b.stateCounters.counters[queueState] > 0 {
					b.stateCounters.counters[queueState]--
				}
			}
			b.stateCounters.mu.Unlock()
		}
	}
}

// extractStateFromKey extracts the state from a key like "state.incoming.msg123"
func (b *Backend) extractStateFromKey(key string) string {
	parts := strings.Split(key, ".")
	if len(parts) >= 2 && parts[0] == "state" {
		return parts[1]
	}
	return ""
}

// GetStateCount returns the cached count for a specific state
func (b *Backend) GetStateCount(state metastorage.QueueState) int64 {
	if b.stateCounters == nil {
		return 0
	}

	b.stateCounters.mu.RLock()
	defer b.stateCounters.mu.RUnlock()

	return b.stateCounters.counters[state]
}

// parseQueueState converts a string to QueueState
func parseQueueState(state string) metastorage.QueueState {
	switch state {
	case "incoming":
		return metastorage.StateIncoming
	case "active":
		return metastorage.StateActive
	case "deferred":
		return metastorage.StateDeferred
	case "hold":
		return metastorage.StateHold
	case "bounce":
		return metastorage.StateBounce
	default:
		return metastorage.QueueState(0) // Invalid state
	}
}
