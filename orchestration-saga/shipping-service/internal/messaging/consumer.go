package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/validate"
)

type ShippingCommandHandler interface {
	HandleScheduleShipping(context.Context, commands.ScheduleShippingCommand) error
	HandleCancelShipping(context.Context, commands.CancelShippingCommand) error
}

type CommandEnvelope struct {
	Topic string
	Key   string
	Value []byte
}

type CommandConsumer struct {
	handler ShippingCommandHandler
	topic   string
}

func NewCommandConsumer(handler ShippingCommandHandler) (*CommandConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("shipping command handler is required")
	}
	return &CommandConsumer{handler: handler, topic: commonkafka.DefaultShippingCommandsTopic}, nil
}

func (c *CommandConsumer) Topics() []string {
	return []string{c.topic}
}

func (c *CommandConsumer) Consume(ctx context.Context, envelope CommandEnvelope) error {
	if envelope.Topic != c.topic {
		return fmt.Errorf("unsupported shipping command topic %q", envelope.Topic)
	}

	var meta struct {
		CommandType string `json:"commandType"`
	}
	if err := json.Unmarshal(envelope.Value, &meta); err != nil {
		return fmt.Errorf("decode shipping command: %w", err)
	}

	switch meta.CommandType {
	case commands.CommandScheduleShipping:
		var command commands.ScheduleShippingCommand
		if err := validate.UnmarshalStrictJSON(envelope.Value, &command); err != nil {
			return fmt.Errorf("decode schedule shipping command: %w", err)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("validate schedule shipping command: %w", err)
		}
		return c.handler.HandleScheduleShipping(ctx, command)
	case commands.CommandCancelShipping:
		var command commands.CancelShippingCommand
		if err := validate.UnmarshalStrictJSON(envelope.Value, &command); err != nil {
			return fmt.Errorf("decode cancel shipping command: %w", err)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("validate cancel shipping command: %w", err)
		}
		return c.handler.HandleCancelShipping(ctx, command)
	default:
		return fmt.Errorf("unsupported shipping command type %q", meta.CommandType)
	}
}
