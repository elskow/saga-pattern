// Package runtime provides the choreography-framework Participant runtime.
package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/choreography-framework/internal/store"
	"saga-pattern/common/events"
)

// Publisher is the framework-facing Kafka publisher interface.
// Applications implement or adapt this to their existing Kafka client.
type Publisher interface {
	Publish(ctx context.Context, message Message) error
}

type Message struct {
	Topic     string
	Key       string
	EventType string
	Payload   []byte
}

type EventHandler interface {
	HandleEvent(ctx context.Context, event events.ChoreographyEvent) error
}

// UnknownEventBehavior controls how the framework handles events on
// unregistered topics or event types that are not declared allowed.
type UnknownEventBehavior int

const (
	// IgnoreUnknown silently drops unknown events. Suitable for participant
	// services that only care about a small subset of a shared topic.
	IgnoreUnknown UnknownEventBehavior = iota
	// RejectUnknown returns an error on unknown events. Suitable for services
	// that expect strict routing (e.g. order-service treating its own inbound
	// topics as authoritative).
	RejectUnknown
)

type TopicSubscription struct {
	Topic      string
	EventTypes []string
}

// EventRegistry is the choreography analog of orchestration's Definition[D].
// It declares what this service subscribes to. There is no step ordering or
// state machine — services react independently to each event.
type EventRegistry struct {
	Subscriptions  []TopicSubscription
	OnUnknownEvent UnknownEventBehavior
}

func (r EventRegistry) Validate() error {
	if len(r.Subscriptions) == 0 {
		return fmt.Errorf("at least one topic subscription is required")
	}
	seen := make(map[string]struct{}, len(r.Subscriptions))
	for _, sub := range r.Subscriptions {
		if sub.Topic == "" {
			return fmt.Errorf("subscription topic must be non-empty")
		}
		if _, dup := seen[sub.Topic]; dup {
			return fmt.Errorf("duplicate subscription for topic %q", sub.Topic)
		}
		seen[sub.Topic] = struct{}{}
		if len(sub.EventTypes) == 0 {
			return fmt.Errorf("subscription %q must declare at least one event type", sub.Topic)
		}
	}
	return nil
}

func (r EventRegistry) Topics() []string {
	topics := make([]string, 0, len(r.Subscriptions))
	for _, sub := range r.Subscriptions {
		topics = append(topics, sub.Topic)
	}
	return topics
}

func (r EventRegistry) allowedMap() map[string]map[string]struct{} {
	m := make(map[string]map[string]struct{}, len(r.Subscriptions))
	for _, sub := range r.Subscriptions {
		inner := make(map[string]struct{}, len(sub.EventTypes))
		for _, et := range sub.EventTypes {
			inner[et] = struct{}{}
		}
		m[sub.Topic] = inner
	}
	return m
}

// Config controls Participant behavior.
// Field-for-field parallel to orchestration-framework Config where applicable.
type Config struct {
	// ServiceName labels metrics and lease ownership.
	ServiceName string
	// WorkerID identifies this replica for lease-based outbox coordination.
	// Defaults to hostname+pid if empty.
	WorkerID string
	// OutboxBatchSize caps rows claimed per tick.
	OutboxBatchSize int
	// OutboxMaxAttempts caps retries before an outbox row is marked failed.
	OutboxMaxAttempts int
	// OutboxRetryDelay is the delay applied on failure + used as claim TTL.
	OutboxRetryDelay time.Duration
	// OutboxLeaseName is the scheduler lease name (defaults to
	// "<serviceName>-outbox").
	OutboxLeaseName string
	OutboxLeaseTTL  time.Duration
	// OutboxRetention controls cleanup of sent/failed rows.
	OutboxRetention time.Duration
	// KafkaSendTimeout bounds each publish attempt.
	KafkaSendTimeout time.Duration
	CleanupInterval  time.Duration
	// ProcessedEventRetention controls how long idempotency keys are kept.
	ProcessedEventRetention time.Duration
	// ImmediateOutboxPublish, when true, tries to publish pending rows inline
	// after transactional writes. Defaults to true.
	ImmediateOutboxPublish bool
}

func DefaultConfig() Config {
	return Config{
		OutboxBatchSize:         100,
		OutboxMaxAttempts:       5,
		OutboxRetryDelay:        5 * time.Second,
		OutboxLeaseTTL:          5 * time.Second,
		OutboxRetention:         24 * time.Hour,
		KafkaSendTimeout:        10 * time.Second,
		CleanupInterval:         time.Hour,
		ProcessedEventRetention: 24 * time.Hour,
		ImmediateOutboxPublish:  true,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.OutboxBatchSize == 0 {
		c.OutboxBatchSize = d.OutboxBatchSize
	}
	if c.OutboxMaxAttempts == 0 {
		c.OutboxMaxAttempts = d.OutboxMaxAttempts
	}
	if c.OutboxRetryDelay == 0 {
		c.OutboxRetryDelay = d.OutboxRetryDelay
	}
	if c.OutboxLeaseTTL == 0 {
		c.OutboxLeaseTTL = d.OutboxLeaseTTL
	}
	if c.OutboxRetention == 0 {
		c.OutboxRetention = d.OutboxRetention
	}
	if c.KafkaSendTimeout == 0 {
		c.KafkaSendTimeout = d.KafkaSendTimeout
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = d.CleanupInterval
	}
	if c.ProcessedEventRetention == 0 {
		c.ProcessedEventRetention = d.ProcessedEventRetention
	}
	if !c.ImmediateOutboxPublish {
		// zero value defaults to true
		c.ImmediateOutboxPublish = true
	}
	return c
}

type Dependencies struct {
	Registry        EventRegistry
	Handler         EventHandler
	Store           store.Store
	Publisher       Publisher
	Config          Config
	MetricsRegistry *prometheus.Registry
	Clock           func() time.Time
	IDGenerator     func() string
}
