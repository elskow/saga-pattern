package messaging

import (
	"context"
	"fmt"
	"time"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/orchestration-saga/internal/messagingutil"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
)

type InventoryCommandHandler interface {
	HandleReserveInventory(context.Context, commands.ReserveInventoryCommand) error
	HandleCommitInventory(context.Context, commands.CommitInventoryCommand) error
	HandleReleaseInventory(context.Context, commands.ReleaseInventoryCommand) error
}

type CommandEnvelope = messagingutil.CommandEnvelope

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
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	if err := messagingutil.EnsureTopic(envelope.Topic, c.topic, "inventory command"); err != nil {
		return err
	}
	commandType, err := messagingutil.DecodeCommandType(envelope.Value, "inventory")
	if err != nil {
		return err
	}

	switch commandType {
	case commands.CommandReserveInventory:
		command, err := messagingutil.DecodeValidatedCommand[commands.ReserveInventoryCommand](envelope.Value, "reserve inventory")
		if err != nil {
			return err
		}
		return c.handler.HandleReserveInventory(ctx, command)
	case commands.CommandCommitInventory:
		command, err := messagingutil.DecodeValidatedCommand[commands.CommitInventoryCommand](envelope.Value, "commit inventory")
		if err != nil {
			return err
		}
		return c.handler.HandleCommitInventory(ctx, command)
	case commands.CommandReleaseInventory:
		command, err := messagingutil.DecodeValidatedCommand[commands.ReleaseInventoryCommand](envelope.Value, "release inventory")
		if err != nil {
			return err
		}
		return c.handler.HandleReleaseInventory(ctx, command)
	default:
		return fmt.Errorf("unsupported inventory command type %q", commandType)
	}
}
