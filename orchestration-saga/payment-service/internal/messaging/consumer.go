package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/validate"
)

type PaymentCommandHandler interface {
	HandleProcessPayment(context.Context, commands.ProcessPaymentCommand) error
	HandleRefundPayment(context.Context, commands.RefundPaymentCommand) error
}

type CommandEnvelope struct {
	Topic string
	Key   string
	Value []byte
}

type CommandConsumer struct {
	handler PaymentCommandHandler
	topic   string
}

func NewCommandConsumer(handler PaymentCommandHandler) (*CommandConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("payment command handler is required")
	}
	return &CommandConsumer{handler: handler, topic: commonkafka.DefaultPaymentCommandsTopic}, nil
}

func (c *CommandConsumer) Topics() []string {
	return []string{c.topic}
}

func (c *CommandConsumer) Consume(ctx context.Context, envelope CommandEnvelope) error {
	if envelope.Topic != c.topic {
		return fmt.Errorf("unsupported payment command topic %q", envelope.Topic)
	}

	var meta struct {
		CommandType string `json:"commandType"`
	}
	if err := json.Unmarshal(envelope.Value, &meta); err != nil {
		return fmt.Errorf("decode payment command: %w", err)
	}

	switch meta.CommandType {
	case commands.CommandProcessPayment:
		var command commands.ProcessPaymentCommand
		if err := validate.UnmarshalStrictJSON(envelope.Value, &command); err != nil {
			return fmt.Errorf("decode process payment command: %w", err)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("validate process payment command: %w", err)
		}
		return c.handler.HandleProcessPayment(ctx, command)
	case commands.CommandRefundPayment:
		var command commands.RefundPaymentCommand
		if err := validate.UnmarshalStrictJSON(envelope.Value, &command); err != nil {
			return fmt.Errorf("decode refund payment command: %w", err)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("validate refund payment command: %w", err)
		}
		return c.handler.HandleRefundPayment(ctx, command)
	default:
		return fmt.Errorf("unsupported payment command type %q", meta.CommandType)
	}
}
