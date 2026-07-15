package orders

import (
	"context"
	"database/sql"

	choreoruntime "saga-pattern/choreography-framework/runtime"
	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

type participantAdapter interface {
	EnqueueOrderCreated(ctx context.Context, tx *sql.Tx, orderID string, event events.OrderCreatedEvent) error
	EnqueueOrderCancelled(ctx context.Context, tx *sql.Tx, orderID string, event events.OrderCancelledEvent) error
	TriggerImmediatePublish(ctx context.Context)
}

type FrameworkParticipant struct {
	Participant *choreoruntime.Participant
	Topic       string
}

func (fp FrameworkParticipant) EnqueueOrderCreated(ctx context.Context, tx *sql.Tx, orderID string, event events.OrderCreatedEvent) error {
	wrapped := choreoruntime.WrapSQLTx(tx)
	return fp.Participant.EnqueueEvent(ctx, wrapped, fp.Topic, orderID, event.EventType(), event)
}

func (fp FrameworkParticipant) EnqueueOrderCancelled(ctx context.Context, tx *sql.Tx, orderID string, event events.OrderCancelledEvent) error {
	wrapped := choreoruntime.WrapSQLTx(tx)
	return fp.Participant.EnqueueEvent(ctx, wrapped, fp.Topic, orderID, event.EventType(), event)
}

func (fp FrameworkParticipant) TriggerImmediatePublish(ctx context.Context) {
	fp.Participant.TriggerImmediatePublish(ctx)
}

func NewFrameworkParticipant(p *choreoruntime.Participant) FrameworkParticipant {
	return FrameworkParticipant{Participant: p, Topic: commonkafka.DefaultOrderEventsTopic}
}

func (s *Service) buildCreateOrderHook(order domain.Order) repository.TxHook {
	return func(hctx context.Context, tx *sql.Tx) error {
		event := order.ToOrderCreatedEvent(order.CreatedAt)
		return s.participant.EnqueueOrderCreated(hctx, tx, order.OrderID, event)
	}
}
