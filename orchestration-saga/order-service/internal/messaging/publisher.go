package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	commonkafka "saga-pattern/common/kafka"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
)

type commandPublisher interface {
	Publish(context.Context, string, string, any) error
}

type KafkaRuntimePublisher struct {
	publisher commandPublisher
}

func NewKafkaRuntimePublisher(publisher *commonkafka.RuntimePublisher) (*KafkaRuntimePublisher, error) {
	if publisher == nil {
		return nil, fmt.Errorf("runtime publisher is required")
	}
	return &KafkaRuntimePublisher{publisher: publisher}, nil
}

func (p *KafkaRuntimePublisher) Publish(ctx context.Context, message sagaRuntime.Message) error {
	if p == nil || p.publisher == nil {
		return fmt.Errorf("runtime publisher is not configured")
	}
	return p.publisher.Publish(ctx, message.Topic, message.Key, json.RawMessage(message.Payload))
}

type PublishedMessage struct {
	Message sagaRuntime.Message
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

func (p *RecordingPublisher) Publish(_ context.Context, message sagaRuntime.Message) error {
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
