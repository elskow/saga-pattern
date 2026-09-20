package payments

import (
	"context"
	"database/sql"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

func (s *Service) buildPaymentFailedHook(payment *domain.Payment) repository.TxHook {
	return func(hctx context.Context, tx *sql.Tx) error {
		failedEvent := events.NewPaymentFailedEvent(payment.PaymentID, payment.OrderID, payment.FailureReason, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
		return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultPaymentEventsTopic, payment.OrderID, failedEvent.EventType(), failedEvent)
	}
}

func (s *Service) buildPaymentCompletedHook(payment *domain.Payment) repository.TxHook {
	return func(hctx context.Context, tx *sql.Tx) error {
		completedEvent := events.NewPaymentCompletedEvent(payment.PaymentID, payment.OrderID, payment.Amount, payment.TransactionID, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
		return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultPaymentEventsTopic, payment.OrderID, completedEvent.EventType(), completedEvent)
	}
}

func (s *Service) buildPaymentRefundedHook(payment *domain.Payment) repository.TxHook {
	return func(hctx context.Context, tx *sql.Tx) error {
		refundedEvent := events.NewPaymentRefundedEvent(payment.PaymentID, payment.OrderID, payment.Amount, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
		return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultPaymentEventsTopic, payment.OrderID, refundedEvent.EventType(), refundedEvent)
	}
}
