package messaging

import (
	"context"
	"fmt"
	"time"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/orchestration-saga/internal/messagingutil"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
)

type PaymentCommandHandler interface {
	HandleProcessPayment(context.Context, commands.ProcessPaymentCommand) error
	HandleRefundPayment(context.Context, commands.RefundPaymentCommand) error
}

type CommandEnvelope = messagingutil.CommandEnvelope

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
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	if err := messagingutil.EnsureTopic(envelope.Topic, c.topic, "payment command"); err != nil {
		return err
	}
	commandType, err := messagingutil.DecodeCommandType(envelope.Value, "payment")
	if err != nil {
		return err
	}

	switch commandType {
	case commands.CommandProcessPayment:
		command, err := messagingutil.DecodeValidatedCommand[commands.ProcessPaymentCommand](envelope.Value, "process payment")
		if err != nil {
			return err
		}
		return c.handler.HandleProcessPayment(ctx, command)
	case commands.CommandRefundPayment:
		command, err := messagingutil.DecodeValidatedCommand[commands.RefundPaymentCommand](envelope.Value, "refund payment")
		if err != nil {
			return err
		}
		return c.handler.HandleRefundPayment(ctx, command)
	default:
		return fmt.Errorf("unsupported payment command type %q", commandType)
	}
}
