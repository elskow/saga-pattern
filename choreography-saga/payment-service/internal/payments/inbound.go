package payments

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/common/events"
	commontracing "saga-pattern/common/tracing"
)

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	ctx = events.ContextWithMetadata(ctx, event)
	_, span := commontracing.Tracer("choreography/payment-service").Start(ctx, "choreography.payment.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
	switch e := event.(type) {
	case events.OrderCreatedEvent:
		return s.processOrderCreated(ctx, e)
	case events.InventoryReservationFailedEvent:
		return s.compensateInventoryReservationFailure(ctx, e)
	case events.ShippingFailedEvent:
		return s.compensateShippingFailure(ctx, e)
	default:
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) processOrderCreated(ctx context.Context, event events.OrderCreatedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/payment-service").Start(ctx, "choreography.payment.order_created",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("correlation.id", event.CorrelationID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "order-created:" + event.OrderID
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)

	payment, err := s.createPaymentAttempt(event, startedAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	payment, err = s.repo.Create(ctx, payment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	amountRat, ok := new(big.Rat).SetString(event.TotalAmount.String())
	if !ok {
		amountRat = big.NewRat(0, 1)
	}
	if !s.CheckAndDeductBalance(amountRat) {
		reason := "insufficient deposit balance"
		span.SetAttributes(attribute.String("payment.result", "failed"), attribute.String("failure.type", "insufficient_balance"))
		hook := s.buildPaymentFailedHook(&payment)
		if err := s.markPaymentFailed(ctx, &payment, reason, startedAt, hook); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		s.metrics.RecordPaymentStep(payment.UpdatedAt.Sub(startedAt))
		s.participant.TriggerImmediatePublish(ctx)
		return nil
	}

	span.SetAttributes(attribute.String("payment.result", "completed"))
	hook := s.buildPaymentCompletedHook(&payment)
	if err := s.markPaymentCompleted(ctx, &payment, startedAt, hook); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordPaymentStep(payment.UpdatedAt.Sub(startedAt))
	span.AddEvent("payment_completed_published")
	s.participant.TriggerImmediatePublish(ctx)
	return nil
}

func (s *Service) compensateInventoryReservationFailure(ctx context.Context, event events.InventoryReservationFailedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/payment-service").Start(ctx, "choreography.payment.inventory_reservation_failed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("correlation.id", event.CorrelationID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "inventory-failed:" + event.OrderID
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)

	payment, ok, err := s.loadPaymentForOrder(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !ok {
		return fmt.Errorf("payment for order %s not found", event.OrderID)
	}
	if payment.Status != domain.PaymentStatusCompleted {
		return nil
	}

	hook := s.buildPaymentRefundedHook(&payment)
	if err := s.markPaymentRefunded(ctx, &payment, startedAt, hook); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordPaymentCompensation(payment.UpdatedAt.Sub(startedAt))
	span.AddEvent("payment_refunded_published")
	s.participant.TriggerImmediatePublish(ctx)
	return nil
}

func (s *Service) compensateShippingFailure(ctx context.Context, event events.ShippingFailedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/payment-service").Start(ctx, "choreography.payment.shipping_failed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("correlation.id", event.CorrelationID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "shipping-failed:" + event.OrderID
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)

	payment, ok, err := s.loadPaymentForOrder(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !ok {
		return fmt.Errorf("payment for order %s not found", event.OrderID)
	}
	if payment.Status != domain.PaymentStatusCompleted {
		return nil
	}

	hook := s.buildPaymentRefundedHook(&payment)
	if err := s.markPaymentRefunded(ctx, &payment, startedAt, hook); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordPaymentCompensation(payment.UpdatedAt.Sub(startedAt))
	span.AddEvent("payment_refunded_published")
	s.participant.TriggerImmediatePublish(ctx)
	return nil
}

func (s *Service) claimProcessedEvent(ctx context.Context, eventKey string) (bool, error) {
	return s.repo.TryMarkProcessedEvent(ctx, eventKey)
}

func (s *Service) releaseProcessedEventOnError(ctx context.Context, eventKey string, err *error) {
	if err != nil && *err != nil {
		_ = s.repo.DeleteProcessedEvent(ctx, eventKey)
	}
}
