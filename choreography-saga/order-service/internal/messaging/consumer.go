package messaging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"

	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

type EventHandler interface {
	HandleEvent(context.Context, events.ChoreographyEvent) error
}

type DownstreamEnvelope struct {
	Topic string
	Key   string
	Value []byte
}

type DownstreamConsumer struct {
	handler EventHandler
	logger  *slog.Logger
	allowed map[string]map[string]struct{}
}

func NewDownstreamConsumer(handler EventHandler, logger *slog.Logger) (*DownstreamConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("event handler is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &DownstreamConsumer{
		handler: handler,
		logger:  logger,
		allowed: map[string]map[string]struct{}{
			commonkafka.DefaultPaymentEventsTopic: {
				events.TypePaymentCompleted: {},
				events.TypePaymentFailed:    {},
				events.TypePaymentRefunded:  {},
			},
			commonkafka.DefaultInventoryEventsTopic: {
				events.TypeInventoryReserved:          {},
				events.TypeInventoryReservationFailed: {},
			},
			commonkafka.DefaultShippingEventsTopic: {
				events.TypeShippingScheduled: {},
				events.TypeShippingFailed:    {},
			},
		},
	}, nil
}

func (c *DownstreamConsumer) Topics() []string {
	topics := make([]string, 0, len(c.allowed))
	for topic := range c.allowed {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func (c *DownstreamConsumer) Consume(ctx context.Context, envelope DownstreamEnvelope) error {
	allowedTypes, ok := c.allowed[envelope.Topic]
	if !ok {
		return fmt.Errorf("unsupported downstream topic %q", envelope.Topic)
	}

	event, err := events.DecodeChoreographyEvent(bytes.NewReader(envelope.Value))
	if err != nil {
		return fmt.Errorf("decode choreography event: %w", err)
	}
	if _, ok := allowedTypes[event.EventType()]; !ok {
		return fmt.Errorf("event type %q is not allowed on topic %q", event.EventType(), envelope.Topic)
	}

	c.logger.Debug("consume downstream choreography event", "topic", envelope.Topic, "key", envelope.Key, "eventType", event.EventType())
	return c.handler.HandleEvent(ctx, event)
}
