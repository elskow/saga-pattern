package messaging

import (
	"context"
	"fmt"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/orchestration-saga/internal/messagingutil"
)

type ShippingCommandHandler interface {
	HandleScheduleShipping(context.Context, commands.ScheduleShippingCommand) error
	HandleCancelShipping(context.Context, commands.CancelShippingCommand) error
}

type CommandEnvelope = messagingutil.CommandEnvelope

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
	if err := messagingutil.EnsureTopic(envelope.Topic, c.topic, "shipping command"); err != nil {
		return err
	}
	commandType, err := messagingutil.DecodeCommandType(envelope.Value, "shipping")
	if err != nil {
		return err
	}

	switch commandType {
	case commands.CommandScheduleShipping:
		command, err := messagingutil.DecodeValidatedCommand[commands.ScheduleShippingCommand](envelope.Value, "schedule shipping")
		if err != nil {
			return err
		}
		return c.handler.HandleScheduleShipping(ctx, command)
	case commands.CommandCancelShipping:
		command, err := messagingutil.DecodeValidatedCommand[commands.CancelShippingCommand](envelope.Value, "cancel shipping")
		if err != nil {
			return err
		}
		return c.handler.HandleCancelShipping(ctx, command)
	default:
		return fmt.Errorf("unsupported shipping command type %q", commandType)
	}
}
