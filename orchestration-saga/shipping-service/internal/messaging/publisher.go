package messaging

import (
	"context"
	"fmt"
	"sync"

	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
)

type Publisher interface {
	Publish(context.Context, string, string, any) error
}

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

type ReplyPublisher struct {
	publisher Publisher
	topic     string
}

func NewReplyPublisher(publisher Publisher) ReplyPublisher {
	return ReplyPublisher{publisher: publisher, topic: commonkafka.DefaultShippingRepliesTopic}
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
