package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	commonreplies "saga-pattern/common/replies"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
)

type ReplyHandler interface {
	HandleReply(context.Context, sagaRuntime.ReplyEnvelope) error
}

type ReplyConsumer struct {
	handler       ReplyHandler
	allowedTopics map[string]struct{}
	topics        []string
}

func NewReplyConsumer(handler ReplyHandler, topics []string) (*ReplyConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("reply handler is required")
	}
	allowedTopics := make(map[string]struct{}, len(topics))
	clonedTopics := make([]string, 0, len(topics))
	for _, topic := range topics {
		allowedTopics[topic] = struct{}{}
		clonedTopics = append(clonedTopics, topic)
	}
	return &ReplyConsumer{handler: handler, allowedTopics: allowedTopics, topics: clonedTopics}, nil
}

func (c *ReplyConsumer) Topics() []string {
	return append([]string(nil), c.topics...)
}

func (c *ReplyConsumer) Consume(ctx context.Context, topic string, sagaID string, payload []byte, receivedAt time.Time) error {
	if _, ok := c.allowedTopics[topic]; !ok {
		return fmt.Errorf("topic %q is not configured for orchestration replies", topic)
	}
	if _, err := commonreplies.DecodeSagaReply(bytes.NewReader(payload)); err != nil {
		return err
	}
	sum := sha256.Sum256(append(append([]byte(topic+":"+sagaID+":"), payload...), byte(len(payload)%251)))
	return c.handler.HandleReply(ctx, sagaRuntime.ReplyEnvelope{
		Topic:      topic,
		SagaID:     sagaID,
		ReplyID:    fmt.Sprintf("%x", sum),
		Payload:    payload,
		ReceivedAt: receivedAt,
	})
}
