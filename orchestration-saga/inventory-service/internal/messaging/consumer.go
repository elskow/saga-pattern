package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/validate"
)

type InventoryCommandHandler interface {
	HandleReserveInventory(context.Context, commands.ReserveInventoryCommand) error
	HandleReleaseInventory(context.Context, commands.ReleaseInventoryCommand) error
}

type CommandEnvelope struct {
	Topic string
	Key   string
	Value []byte
}

type CommandConsumer struct {
	handler InventoryCommandHandler
	topic   string
}

func NewCommandConsumer(handler InventoryCommandHandler) (*CommandConsumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("inventory command handler is required")
	}
	return &CommandConsumer{handler: handler, topic: commonkafka.DefaultInventoryCommandsTopic}, nil
}

func (c *CommandConsumer) Topics() []string {
	return []string{c.topic}
}

func (c *CommandConsumer) Consume(ctx context.Context, envelope CommandEnvelope) error {
	if envelope.Topic != c.topic {
		return fmt.Errorf("unsupported inventory command topic %q", envelope.Topic)
	}

	var meta struct {
		CommandType string `json:"commandType"`
	}
	if err := json.Unmarshal(envelope.Value, &meta); err != nil {
		return fmt.Errorf("decode inventory command: %w", err)
	}

	switch meta.CommandType {
	case commands.CommandReserveInventory:
		var command commands.ReserveInventoryCommand
		if err := validate.UnmarshalStrictJSON(envelope.Value, &command); err != nil {
			return fmt.Errorf("decode reserve inventory command: %w", err)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("validate reserve inventory command: %w", err)
		}
		return c.handler.HandleReserveInventory(ctx, command)
	case commands.CommandReleaseInventory:
		var command commands.ReleaseInventoryCommand
		if err := validate.UnmarshalStrictJSON(envelope.Value, &command); err != nil {
			return fmt.Errorf("decode release inventory command: %w", err)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("validate release inventory command: %w", err)
		}
		return c.handler.HandleReleaseInventory(ctx, command)
	default:
		return fmt.Errorf("unsupported inventory command type %q", meta.CommandType)
	}
}
