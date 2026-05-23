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
			commonkafka.DefaultOrderEventsTopic: {
				events.TypeOrderCreated: {},
			},
			commonkafka.DefaultPaymentEventsTopic: {
				events.TypePaymentCompleted: {},
				events.TypePaymentFailed:    {},
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

	event, err := decodeChoreographyEvent(envelope.Value)
	if err != nil {
		return err
	}
	if _, ok := allowedTypes[event.EventType()]; !ok {
		return c.ignoreUnrelatedEvent(envelope, event.EventType())
	}

	ctx = events.ContextWithMetadata(ctx, event)
	c.logger.Debug("consume downstream choreography event", "topic", envelope.Topic, "key", envelope.Key, "eventType", event.EventType())
	return c.handler.HandleEvent(ctx, event)
}

func (c *DownstreamConsumer) ignoreUnrelatedEvent(envelope DownstreamEnvelope, eventType string) error {
	c.logger.Debug("ignore choreography event for unrelated handler", "topic", envelope.Topic, "key", envelope.Key, "eventType", eventType)
	return nil
}

func decodeChoreographyEvent(payload []byte) (events.ChoreographyEvent, error) {
	event, err := events.DecodeChoreographyEvent(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("decode choreography event: %w", err)
	}
	return event, nil
}
