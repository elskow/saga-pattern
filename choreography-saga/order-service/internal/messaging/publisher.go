package messaging

import (
	"context"
	"fmt"

	commonkafka "saga-pattern/common/kafka"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
}

type OrderTopicPublisher struct {
	publisher Publisher
	topic     string
}

func NewOrderTopicPublisher(publisher Publisher) OrderTopicPublisher {
	return OrderTopicPublisher{publisher: publisher, topic: commonkafka.DefaultOrderEventsTopic}
}

func (p OrderTopicPublisher) PublishOrderCreated(ctx context.Context, orderID string, event any) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, event)
}

func (p OrderTopicPublisher) Topic() string {
	return p.topic
}
