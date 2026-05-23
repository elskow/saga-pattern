package testutil

import (
	"context"
	"sync"

	commoncontext "saga-pattern/common/context"
)

type PublishedMessage struct {
	Topic         string
	Key           string
	Body          any
	RequestID     string
	CorrelationID string
	OrderID       string
}

type RecordingPublisher struct {
	mu       sync.Mutex
	messages []PublishedMessage
}

func NewRecordingPublisher() *RecordingPublisher {
	return &RecordingPublisher{}
}

func (p *RecordingPublisher) Publish(ctx context.Context, topic string, key string, body any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, _ := commoncontext.From(ctx)
	p.messages = append(p.messages, PublishedMessage{Topic: topic, Key: key, Body: body, RequestID: data.RequestID, CorrelationID: data.CorrelationID, OrderID: data.OrderID})
	return nil
}

func (p *RecordingPublisher) Messages() []PublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	items := make([]PublishedMessage, len(p.messages))
	copy(items, p.messages)
	return items
}
