package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-framework/internal/model"
)

type Publisher interface {
	Publish(context.Context, Message) error
}

type Message struct {
	Topic       string
	Key         string
	MessageType string
	Payload     []byte
	SagaID      string
	Step        string
	Direction   model.StepDirection
}

type ReplyEnvelope struct {
	ReplyID    string
	SagaID     string
	Topic      string
	ReceivedAt time.Time
	Payload    []byte
}

type ReplyHandler interface {
	HandleReply(context.Context, ReplyEnvelope) error
}

type ReplyConsumer struct {
	handler       ReplyHandler
	allowedTopics map[string]struct{}
}

func NewReplyConsumer(handler ReplyHandler, topics []string) (*ReplyConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("reply handler is required")
	}
	allowed := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		allowed[topic] = struct{}{}
	}
	return &ReplyConsumer{handler: handler, allowedTopics: allowed}, nil
}

func (c *ReplyConsumer) Consume(ctx context.Context, envelope ReplyEnvelope) error {
	if _, ok := c.allowedTopics[envelope.Topic]; !ok {
		return fmt.Errorf("topic %q is not configured for orchestration replies", envelope.Topic)
	}
	if _, err := commonreplies.DecodeSagaReply(bytes.NewReader(envelope.Payload)); err != nil {
		return err
	}
	return c.handler.HandleReply(ctx, envelope)
}

type PublishedMessage struct {
	Message Message
	Body    any
}

type RecordingPublisher struct {
	mu       sync.Mutex
	messages []PublishedMessage
	failures map[string]int
}

func NewRecordingPublisher() *RecordingPublisher {
	return &RecordingPublisher{failures: make(map[string]int)}
}

func (p *RecordingPublisher) Publish(_ context.Context, message Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if remaining := p.failures[message.MessageType]; remaining > 0 {
		p.failures[message.MessageType] = remaining - 1
		return fmt.Errorf("forced publish failure for %s", message.MessageType)
	}
	var body any
	if len(message.Payload) > 0 {
		_ = json.Unmarshal(message.Payload, &body)
	}
	p.messages = append(p.messages, PublishedMessage{Message: message, Body: body})
	return nil
}

func (p *RecordingPublisher) FailNext(messageType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[messageType] = count
}

func (p *RecordingPublisher) Messages() []PublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PublishedMessage(nil), p.messages...)
}
