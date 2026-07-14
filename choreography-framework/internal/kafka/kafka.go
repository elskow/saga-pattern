package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Publisher interface {
	Publish(context.Context, Message) error
}

type Message struct {
	Topic     string
	Key       string
	EventType string
	Payload   []byte
}

type Envelope struct {
	Topic string
	Key   string
	Value []byte
}

type Handler interface {
	Consume(context.Context, Envelope) error
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
	if remaining := p.failures[message.EventType]; remaining > 0 {
		p.failures[message.EventType] = remaining - 1
		return fmt.Errorf("forced publish failure for %s", message.EventType)
	}
	var body any
	if len(message.Payload) > 0 {
		_ = json.Unmarshal(message.Payload, &body)
	}
	p.messages = append(p.messages, PublishedMessage{Message: message, Body: body})
	return nil
}

func (p *RecordingPublisher) FailNext(eventType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[eventType] = count
}

func (p *RecordingPublisher) Messages() []PublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PublishedMessage(nil), p.messages...)
}
