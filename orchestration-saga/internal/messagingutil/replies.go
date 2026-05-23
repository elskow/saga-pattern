package messagingutil

import (
	"context"
	"fmt"

	commonreplies "saga-pattern/common/replies"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
}

type ReplyPublisher struct {
	publisher Publisher
	topic     string
}

func NewReplyPublisher(publisher Publisher, topic string) ReplyPublisher {
	return ReplyPublisher{publisher: publisher, topic: topic}
}

func (p ReplyPublisher) PublishReply(ctx context.Context, orderID string, reply commonreplies.SagaReply) error {
	if p.publisher == nil {
		return fmt.Errorf("publisher is required")
	}
	if reply == nil {
		return fmt.Errorf("reply is required")
	}
	return p.publisher.Publish(ctx, p.topic, orderID, reply)
}

func (p ReplyPublisher) Topic() string {
	return p.topic
}
