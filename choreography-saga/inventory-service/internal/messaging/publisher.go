package messaging

import (
	"context"
	"fmt"

	commonkafka "saga-pattern/common/kafka"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
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
