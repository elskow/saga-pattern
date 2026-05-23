package messaging

import (
	"context"
	"fmt"

	commonkafka "saga-pattern/common/kafka"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
}

type ShippingTopicPublisher struct {
	publisher Publisher
	topic     string
}

func NewShippingTopicPublisher(publisher Publisher) ShippingTopicPublisher {
	return ShippingTopicPublisher{publisher: publisher, topic: commonkafka.DefaultShippingEventsTopic}
}

func (p ShippingTopicPublisher) PublishShippingScheduled(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p ShippingTopicPublisher) PublishShippingCancelled(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p ShippingTopicPublisher) PublishShippingFailed(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p ShippingTopicPublisher) Topic() string {
	return p.topic
}
