package messaging

import (
	"context"
	"fmt"
	"sync"

	commonkafka "saga-pattern/common/kafka"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
}

type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, string, string, any) error { return nil }

type PublishedMessage struct {
	Topic string
	Key   string
	Body  any
}

type RecordingPublisher struct {
	mu       sync.Mutex
	messages []PublishedMessage
	err      error
}

func NewRecordingPublisher() *RecordingPublisher {
	return &RecordingPublisher{}
}

func (p *RecordingPublisher) Publish(_ context.Context, topic string, key string, body any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, PublishedMessage{Topic: topic, Key: key, Body: body})
	return nil
}

func (p *RecordingPublisher) Messages() []PublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	items := make([]PublishedMessage, len(p.messages))
	copy(items, p.messages)
	return items
}

type InventoryTopicPublisher struct {
	publisher Publisher
	topic     string
}

func NewInventoryTopicPublisher(publisher Publisher) InventoryTopicPublisher {
	return InventoryTopicPublisher{publisher: publisher, topic: commonkafka.DefaultInventoryEventsTopic}
}

func (p InventoryTopicPublisher) PublishInventoryReserved(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p InventoryTopicPublisher) PublishInventoryReservationFailed(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p InventoryTopicPublisher) PublishInventoryReleased(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p InventoryTopicPublisher) Topic() string {
	return p.topic
}
