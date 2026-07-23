package inventory

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
	commontracing "saga-pattern/common/tracing"
)

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	ctx = events.ContextWithMetadata(ctx, event)
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	switch e := event.(type) {
	case events.OrderCreatedEvent:
		return s.handleOrderCreated(ctx, e)
	case events.PaymentCompletedEvent:
		return s.handlePaymentCompleted(ctx, e)
	case events.PaymentFailedEvent:
		return s.handlePaymentFailed(ctx, e)
	case events.ShippingScheduledEvent:
		return s.handleShippingScheduled(ctx, e)
	case events.ShippingFailedEvent:
		return s.handleShippingFailed(ctx, e)
	default:
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) handleOrderCreated(ctx context.Context, event events.OrderCreatedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.order_created",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.Int("item.count", len(event.Items))))
	defer span.End()
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
	return s.StagePendingReservation(ctx, event.OrderID, event.Items)
}

func (s *Service) handlePaymentFailed(ctx context.Context, event events.PaymentFailedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.payment_failed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("failure.type", "payment")))
	defer span.End()
	eventKey := "payment-failed:" + event.OrderID
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
	return s.ClearPendingReservation(ctx, event.OrderID)
}

func (s *Service) handlePaymentCompleted(ctx context.Context, event events.PaymentCompletedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.payment_completed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("payment.id", event.PaymentID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "payment-completed:" + event.PaymentID
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

	pendingItems, err := s.waitForPendingReservation(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(pendingItems) == 0 {
		err := fmt.Errorf("pending reservation items for order %s not found", event.OrderID)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Failure-mode inject must short-circuit like orch/shipping: never reserve stock
	// and never rely on ReserveInventory's onFail hook (only runs on real stock errors).
	if shouldFail, failureState := s.failureMode.ShouldFail(s.now()); shouldFail {
		err := fmt.Errorf("inventory failure mode enabled — reservation forced to fail")
		span.SetAttributes(attribute.String("inventory.result", "failed"), attribute.String("failure.type", "failure_mode"), attribute.String("failure.run_label", failureState.RunLabel), attribute.Int("failure.remaining", failureState.Remaining))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		failedAt := s.now()
		failedEvent := s.buildReservationFailedEvent(event, err, failedAt)
		reservationID := s.newID()
		if saveErr := s.SaveFailedReservation(ctx, reservationID, event.OrderID, pendingItems, err.Error(), failedAt); saveErr != nil {
			span.RecordError(saveErr)
			span.SetStatus(codes.Error, saveErr.Error())
			return saveErr
		}
		if enqueueErr := s.participant.EnqueueEvent(ctx, nil, commonkafka.DefaultInventoryEventsTopic, event.OrderID, failedEvent.EventType(), failedEvent); enqueueErr != nil {
			span.RecordError(enqueueErr)
			span.SetStatus(codes.Error, enqueueErr.Error())
			return enqueueErr
		}
		span.AddEvent("inventory_reservation_failed_published", trace.WithAttributes(attribute.String("failure.type", "failure_mode")))
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		s.participant.TriggerImmediatePublish(ctx)
		if clearErr := s.ClearPendingReservation(ctx, event.OrderID); clearErr != nil {
			return clearErr
		}
		return nil
	}

	reservationID := s.newID()
	reservedAt := s.now()
	reservedItems := buildReservedItems(pendingItems)
	reservedEvent := events.NewInventoryReservedEvent(reservationID, event.OrderID, reservedItems, reservedAt, event.CorrelationID, reservedAt)
	failedEvent := s.buildReservationFailedEvent(event, nil, reservedAt)
	onReserve, onFail := s.buildReserveHooks(event, &reservedEvent, failedEvent, reservedAt)
	_, err = s.ReservePendingOrderItems(ctx, event.OrderID, reservationID, pendingItems, reservedAt, onReserve, onFail)
	if err != nil {
		span.SetAttributes(attribute.String("inventory.result", "failed"), attribute.String("failure.type", "reservation_failure"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent("inventory_reservation_failed_published", trace.WithAttributes(attribute.String("failure.type", "reservation_failure")))
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		s.participant.TriggerImmediatePublish(ctx)
		if saveErr := s.SaveFailedReservation(ctx, reservationID, event.OrderID, pendingItems, err.Error(), reservedAt); saveErr != nil {
			return saveErr
		}
		if err := s.ClearPendingReservation(ctx, event.OrderID); err != nil {
			return err
		}
		return nil
	}
	span.AddEvent("inventory_reserved_published", trace.WithAttributes(attribute.String("reservation.id", reservationID)))
	s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
	s.participant.TriggerImmediatePublish(ctx)
	if err := s.ClearPendingReservation(ctx, event.OrderID); err != nil {
		return err
	}
	return nil
}

func (s *Service) handleShippingFailed(ctx context.Context, event events.ShippingFailedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.shipping_failed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("failure.type", "shipping")))
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

	hook := s.buildReleaseHook(event.OrderID, event.CorrelationID)
	reservationID, released, err := s.ReleaseInventory(ctx, event.OrderID, startedAt, hook)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !released {
		span.AddEvent("inventory_release_skipped")
		return nil
	}
	span.AddEvent("inventory_released_published", trace.WithAttributes(attribute.String("reservation.id", reservationID)))
	s.metrics.RecordInventoryCompensation(s.now().Sub(startedAt))
	s.participant.TriggerImmediatePublish(ctx)
	return nil
}

func (s *Service) handleShippingScheduled(ctx context.Context, event events.ShippingScheduledEvent) (err error) {
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.shipping_scheduled",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("shipping.id", event.ShippingID)))
	defer span.End()
	eventKey := "shipping-scheduled:" + event.ShippingID
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
	return s.CommitInventory(ctx, event.OrderID)
}

func (s *Service) claimProcessedEvent(ctx context.Context, eventKey string) (bool, error) {
	return s.repo.TryMarkProcessedEvent(ctx, eventKey)
}

func (s *Service) releaseProcessedEventOnError(ctx context.Context, eventKey string, err *error) {
	if err != nil && *err != nil {
		_ = s.repo.DeleteProcessedEvent(ctx, eventKey)
	}
}

func (s *Service) waitForPendingReservation(ctx context.Context, orderID string) ([]domain.PendingOrderItem, error) {
	deadline := time.Now().Add(pendingReservationWaitTimeout)
	for {
		pendingItems, err := s.LoadPendingReservation(ctx, orderID)
		if err != nil || len(pendingItems) > 0 || !time.Now().Before(deadline) {
			return pendingItems, err
		}
		timer := time.NewTimer(pendingReservationPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
