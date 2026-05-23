package messaging

import (
	"context"
	"fmt"

	commonkafka "saga-pattern/common/kafka"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
}

type PaymentTopicPublisher struct {
	publisher Publisher
	topic     string
}

func NewPaymentTopicPublisher(publisher Publisher) PaymentTopicPublisher {
	return PaymentTopicPublisher{publisher: publisher, topic: commonkafka.DefaultPaymentEventsTopic}
}

func (p PaymentTopicPublisher) PublishPaymentCompleted(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p PaymentTopicPublisher) PublishPaymentFailed(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p PaymentTopicPublisher) PublishPaymentRefunded(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p PaymentTopicPublisher) Topic() string {
	return p.topic
}
