# NATS JetStream Key-Value Storage for RetrySpool Metadata

This package provides a NATS JetStream Key-Value store implementation for RetrySpool message metadata storage.

## Features

- **🚀 JetStream Key-Value Store**: Uses NATS JetStream KV for reliable metadata storage
- **⚡ Real-time State Counting**: Watch-based state counters for instant queue statistics
- **🔄 Compare-and-Swap Operations**: Atomic state transitions with optimistic locking
- **🎯 State Indexing**: Efficient message listing by queue state
- **📡 Streaming Batching**: KV watch-based message iteration with constant memory usage
- **👥 Multi-Worker Safe**: Optimized for concurrent worker environments across distributed nodes
- **⚙️ Configurable Storage**: TTL, replicas, value size limits, and more
- **🔗 Connection Reuse**: Support for existing NATS connections
- **🛠️ Retry Logic**: Built-in retry mechanism for concurrent modifications
- **📊 Performance Optimized**: Fast state counting with cached counters and efficient streaming

## Usage

### Basic Usage

```go
import (
    natsmetastorage "schneider.vip/retryspool/storage/meta/nats"
)

// Create factory with default settings
factory := natsmetastorage.NewFactory()

// Create backend
backend, err := factory.Create()
if err != nil {
    log.Fatal(err)
}
defer backend.Close()
```

### Advanced Configuration

```go
// Create factory with custom configuration
factory := natsmetastorage.NewFactory(
    natsmetastorage.WithURL("nats://localhost:4222"),
    natsmetastorage.WithBucket("my-retryspool-meta"),
    natsmetastorage.WithDescription("Custom metadata storage"),
    natsmetastorage.WithMaxValueSize(2*1024*1024), // 2MB
    natsmetastorage.WithHistory(3), // Keep 3 historical values
    natsmetastorage.WithTTL(3600), // 1 hour TTL
    natsmetastorage.WithReplicas(3), // 3 replicas for HA
)
```

### Using Existing Connection

```go
// Connect to NATS
conn, err := nats.Connect("nats://localhost:4222")
if err != nil {
    log.Fatal(err)
}

// Use existing connection
factory := natsmetastorage.NewFactory(
    natsmetastorage.WithConnection(conn),
    natsmetastorage.WithBucket("shared-meta-bucket"),
)
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithURL(url)` | NATS server URL | `nats://localhost:4222` |
| `WithBucket(name)` | KV bucket name | `retryspool-meta` |
| `WithConnection(conn)` | Use existing NATS connection | `nil` |
| `WithMaxValueSize(size)` | Maximum value size in bytes | `1048576` (1MB) |
| `WithHistory(count)` | Number of historical values | `1` |
| `WithTTL(seconds)` | Time to live in seconds | `0` (no TTL) |
| `WithReplicas(count)` | Number of replicas | `1` |
| `WithDescription(desc)` | Bucket description | `"RetrySpooL message metadata storage"` |

## Key Structure

The implementation uses the following key structure in the KV store:

- **Metadata**: `meta.{messageID}` - Contains the full message metadata as JSON
- **State Index**: `state.{state}.{messageID}` - Contains timestamp and message ID for efficient state-based queries

## Atomic Operations

### Compare-and-Swap State Transitions
```go
// Atomic state transitions with retry logic
err := backend.MoveToState(ctx, messageID, fromState, toState)
// - Uses NATS KV revision-based compare-and-swap
// - Retries up to 3 times on concurrent modifications
// - Exponential backoff: 10ms, 20ms, 30ms
```

### Optimistic Concurrency Control
```go
// Get current entry with revision
currentEntry, err := b.kv.Get(key)

// Atomic update with revision check
_, err = b.kv.Update(key, data, currentEntry.Revision())
if strings.Contains(err.Error(), "wrong last sequence") {
    // Concurrent modification detected, retry
    time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
    continue
}
```

## Real-time State Counting

### Watch-based Counters
The NATS backend provides real-time state counting through NATS KV watch events:

```go
// Fast state counting (cached)
count := backend.GetStateCount(metastorage.StateIncoming)

// Watch events automatically update counters:
// - PUT events increment counters
// - DELETE events decrement counters
// - All states tracked: incoming, active, deferred, hold, bounce
```

### Performance Benefits
- **Instant Results**: No need to scan/count messages
- **Real-time Updates**: Counters updated immediately on state changes
- **Efficient**: O(1) state count operations vs O(n) scanning

## Requirements

- NATS Server with JetStream enabled
- Go 1.24+
- github.com/nats-io/nats.go v1.31.0+

## Testing

```bash
# Start NATS server with JetStream
nats-server -js

# Run tests
go test -v
```

## Performance Characteristics

| Operation | Performance | Notes |
|-----------|-------------|-------|
| **State Counting** | ⭐⭐⭐⭐⭐ | O(1) cached counters via watch events |
| **State Transitions** | ⭐⭐⭐⭐ | Compare-and-swap with retry logic |
| **Message Retrieval** | ⭐⭐⭐⭐⭐ | Direct KV access by message ID |
| **State Listing** | ⭐⭐⭐ | Efficient with state indexing |
| **Concurrent Access** | ⭐⭐⭐ | Optimistic locking with retries |

## Recent Improvements

### 🐛 **Critical Bug Fix: StateIncoming Counter**
- **Issue**: `StateIncoming` (value 0) was incorrectly treated as invalid state
- **Fix**: Updated `parseQueueState()` validation logic
- **Impact**: State counters now work correctly for all states including incoming

### ⚡ **Performance Optimizations**
- **Watch Event Processing**: Optimized event handling for better throughput
- **State Counter Caching**: Real-time updates via NATS KV watch
- **Retry Logic**: Smart exponential backoff for concurrent modifications

### 🔒 **Enhanced Atomicity**
- **Compare-and-Swap**: Proper revision-based atomic updates
- **State Verification**: Double-check state consistency in transitions
- **Error Handling**: Better detection and handling of concurrent modifications

## Performance Considerations

- **Watch Events**: Automatic state counter updates provide O(1) counting
- **Batch Operations**: Use pagination for large result sets  
- **State Indexing**: Efficient querying by message state
- **Memory Usage**: Configure appropriate `MaxValueSize` based on metadata size
- **Network**: Consider replica count vs. performance trade-offs
- **Concurrent Load**: Optimistic locking performs well under moderate concurrency

## Troubleshooting

### Common Issues

#### State Counter Inconsistencies
```bash
# Symptoms: GetStateCount() returns incorrect values
# Cause: Watch events not processing correctly
# Solution: Check NATS connectivity and JetStream health
```

#### Concurrent Modification Errors
```bash
# Symptoms: "wrong last sequence" errors during high load
# Cause: Multiple processes modifying same message simultaneously  
# Solution: Normal behavior - retry logic handles this automatically
```

#### Performance Degradation
```bash
# Symptoms: Slow state transitions or counting
# Cause: Network latency or NATS server overload
# Solution: Check NATS server performance and network connectivity
```