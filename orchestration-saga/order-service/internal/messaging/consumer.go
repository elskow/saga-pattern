package messaging

import (
	"bytes"
	"context"
	"fmt"
	"time"

	commonreplies "saga-pattern/common/replies"
	frameworkruntime "saga-pattern/orchestration-framework/runtime"
)

type ReplyHandler interface {
	HandleReply(context.Context, frameworkruntime.ReplyEnvelope) error
}

type ReplyEnvelope struct {
	ReplyID    string
	SagaID     string
	Topic      string
	ReceivedAt time.Time
	Payload    []byte
}

type ReplyConsumer struct {
	handler       ReplyHandler
	topics        []string
	allowedTopics map[string]struct{}
}

func NewReplyConsumer(handler ReplyHandler, topics []string) (*ReplyConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("reply handler is required")
	}
	allowedTopics := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		allowedTopics[topic] = struct{}{}
	}
	return &ReplyConsumer{handler: handler, topics: append([]string(nil), topics...), allowedTopics: allowedTopics}, nil
}

func (c *ReplyConsumer) Topics() []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.topics...)
}

func (c *ReplyConsumer) Consume(ctx context.Context, envelope ReplyEnvelope) error {
	if _, ok := c.allowedTopics[envelope.Topic]; !ok {
		return fmt.Errorf("topic %q is not configured for orchestration replies", envelope.Topic)
	}
	reply, err := commonreplies.DecodeSagaReply(bytes.NewReader(envelope.Payload))
	if err != nil {
		return err
	}
	if !replyAllowedOnTopic(reply.ReplyType(), envelope.Topic) {
		return fmt.Errorf("reply type %q is not allowed on topic %q", reply.ReplyType(), envelope.Topic)
	}
	return c.handler.HandleReply(ctx, frameworkruntime.ReplyEnvelope{
		ReplyID:    envelope.ReplyID,
		SagaID:     envelope.SagaID,
		Topic:      envelope.Topic,
		ReceivedAt: envelope.ReceivedAt,
		Payload:    envelope.Payload,
	})
}

func replyAllowedOnTopic(replyType string, topic string) bool {
	switch topic {
	case "orchestration.payment.replies":
		return replyType == commonreplies.TypePaymentCompleted || replyType == commonreplies.TypePaymentFailed || replyType == commonreplies.TypePaymentRefunded
	case "orchestration.inventory.replies":
		return replyType == commonreplies.TypeInventoryReserved || replyType == commonreplies.TypeInventoryFailed || replyType == commonreplies.TypeInventoryReleased
	case "orchestration.shipping.replies":
		return replyType == commonreplies.TypeShippingScheduled || replyType == commonreplies.TypeShippingFailed || replyType == commonreplies.TypeShippingCancelled
	default:
		return false
	}
}
