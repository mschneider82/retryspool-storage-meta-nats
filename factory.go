package nats

import (
	"github.com/nats-io/nats.go"
	metastorage "schneider.vip/retryspool/storage/meta"
)

// Factory implements metastorage.Factory for NATS JetStream Key-Value storage
type Factory struct {
	config *Config
}

// Config holds NATS connection and bucket configuration
type Config struct {
	URL        string // NATS server URL
	Bucket     string // KV bucket name
	Connection *nats.Conn // Optional: existing connection
	// JetStream configuration
	JSConfig *JSConfig
}

// JSConfig holds JetStream specific configuration
type JSConfig struct {
	MaxValueSize int64  // Maximum value size (default: 1MB)
	History      uint8  // Number of historical values to keep (default: 1)
	TTL          int64  // Time to live in seconds (0 = no TTL)
	Replicas     int    // Number of replicas (default: 1)
	Description  string // Bucket description
}

// Option configures the NATS factory
type Option func(*Config)

// WithURL sets the NATS server URL
func WithURL(url string) Option {
	return func(c *Config) {
		c.URL = url
	}
}

// WithBucket sets the KV bucket name
func WithBucket(bucket string) Option {
	return func(c *Config) {
		c.Bucket = bucket
	}
}

// WithConnection uses an existing NATS connection
func WithConnection(conn *nats.Conn) Option {
	return func(c *Config) {
		c.Connection = conn
	}
}

// WithJSConfig sets JetStream configuration
func WithJSConfig(jsConfig *JSConfig) Option {
	return func(c *Config) {
		c.JSConfig = jsConfig
	}
}

// WithMaxValueSize sets the maximum value size for the KV bucket
func WithMaxValueSize(size int64) Option {
	return func(c *Config) {
		if c.JSConfig == nil {
			c.JSConfig = &JSConfig{}
		}
		c.JSConfig.MaxValueSize = size
	}
}

// WithHistory sets the number of historical values to keep
func WithHistory(history uint8) Option {
	return func(c *Config) {
		if c.JSConfig == nil {
			c.JSConfig = &JSConfig{}
		}
		c.JSConfig.History = history
	}
}

// WithTTL sets the time to live for values in seconds
func WithTTL(ttl int64) Option {
	return func(c *Config) {
		if c.JSConfig == nil {
			c.JSConfig = &JSConfig{}
		}
		c.JSConfig.TTL = ttl
	}
}

// WithReplicas sets the number of replicas for the KV bucket
func WithReplicas(replicas int) Option {
	return func(c *Config) {
		if c.JSConfig == nil {
			c.JSConfig = &JSConfig{}
		}
		c.JSConfig.Replicas = replicas
	}
}

// WithDescription sets the bucket description
func WithDescription(description string) Option {
	return func(c *Config) {
		if c.JSConfig == nil {
			c.JSConfig = &JSConfig{}
		}
		c.JSConfig.Description = description
	}
}

// NewFactory creates a new NATS metadata storage factory
func NewFactory(opts ...Option) *Factory {
	config := &Config{
		URL:    nats.DefaultURL, // Default to localhost:4222
		Bucket: "retryspool-meta",
		JSConfig: &JSConfig{
			MaxValueSize: 1024 * 1024, // 1MB default
			History:      1,            // Keep only current value
			TTL:          0,            // No TTL by default
			Replicas:     1,            // Single replica by default
			Description:  "RetrySpooL message metadata storage",
		},
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	return &Factory{
		config: config,
	}
}

// Create creates a new NATS metadata storage backend
func (f *Factory) Create() (metastorage.Backend, error) {
	return NewBackend(f.config)
}

// Name returns the factory name
func (f *Factory) Name() string {
	return "nats-meta"
}