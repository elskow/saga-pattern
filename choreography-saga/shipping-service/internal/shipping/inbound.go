package shipping

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
	commontracing "saga-pattern/common/tracing"
)

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	ctx = events.ContextWithMetadata(ctx, event)
	_, span := commontracing.Tracer("choreography/shipping-service").Start(ctx, "choreography.shipping.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
	switch e := event.(type) {
	case events.OrderCreatedEvent:
		return s.handleOrderCreated(ctx, e)
	case events.InventoryReservedEvent:
		return s.handleInventoryReserved(ctx, e)
	case events.PaymentRefundedEvent:
		return s.handlePaymentRefunded(ctx, e)
	case events.InventoryReleasedEvent:
		return s.handleInventoryReleased(ctx, e)
	default:
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) handleOrderCreated(ctx context.Context, event events.OrderCreatedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/shipping-service").Start(ctx, "choreography.shipping.order_created",
		trace.WithAttributes(attribute.String("order.id", event.OrderID)))
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
	if err := s.StagePendingShippingAddress(ctx, event.OrderID, event.ShippingAddress); err != nil {
		return err
	}
	return nil
}

func (s *Service) handleInventoryReserved(ctx context.Context, event events.InventoryReservedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/shipping-service").Start(ctx, "choreography.shipping.inventory_reserved",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("reservation.id", event.ReservationID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "inventory-reserved:" + event.ReservationID
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

	address, ok, err := s.waitForPendingShippingAddress(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !ok || strings.TrimSpace(address) == "" {
		err := fmt.Errorf("pending shipping address for order %s not found", event.OrderID)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if shouldFail, failureState := s.failureMode.ShouldFail(s.now()); shouldFail {
		err := fmt.Errorf("shipping failure mode enabled — scheduling forced to fail")
		span.SetAttributes(attribute.String("shipping.result", "failed"), attribute.String("failure.type", "failure_mode"), attribute.String("failure.run_label", failureState.RunLabel), attribute.Int("failure.remaining", failureState.Remaining))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		failedEvent := s.buildFailedShippingEvent(event.OrderID, err.Error(), s.now(), event.CorrelationID)
		if err := s.participant.EnqueueEvent(ctx, nil, commonkafka.DefaultShippingEventsTopic, event.OrderID, failedEvent.EventType(), failedEvent); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		span.AddEvent("shipping_failed_published", trace.WithAttributes(attribute.String("failure.type", "failure_mode")))
		s.metrics.RecordShippingStep(s.now().Sub(startedAt))
		if err := s.ClearPendingShippingAddress(ctx, event.OrderID); err != nil {
			return err
		}
		return nil
	}

	buildHook := s.buildScheduledShippingHook(event.CorrelationID)
	shipment, err := s.CreateShipment(ctx, event.OrderID, address, buildHook)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.AddEvent("shipping_scheduled_published", trace.WithAttributes(attribute.String("shipping.id", shipment.ShippingID)))
	if err := s.ClearPendingShippingAddress(ctx, event.OrderID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordShippingStep(s.now().Sub(startedAt))
	return nil
}

func (s *Service) handlePaymentRefunded(ctx context.Context, event events.PaymentRefundedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/shipping-service").Start(ctx, "choreography.shipping.payment_refunded",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("payment.id", event.PaymentID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "payment-refunded:" + event.PaymentID
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
	if err := s.ClearPendingShippingAddress(ctx, event.OrderID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	buildHook := s.buildCancelledShippingHook(event.CorrelationID)
	shipment, cancelled, err := s.CancelShipment(ctx, event.OrderID, s.now(), buildHook)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !cancelled {
		span.AddEvent("shipping_cancel_skipped")
		return nil
	}
	span.AddEvent("shipping_cancelled_published", trace.WithAttributes(attribute.String("shipping.id", shipment.ShippingID)))
	s.metrics.RecordShippingCompensation(s.now().Sub(startedAt))
	return nil
}

func (s *Service) handleInventoryReleased(ctx context.Context, event events.InventoryReleasedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/shipping-service").Start(ctx, "choreography.shipping.inventory_released",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("reservation.id", event.ReservationID)))
	defer span.End()
	startedAt := s.now()
	eventKey := "inventory-released:" + event.ReservationID
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
	if err := s.ClearPendingShippingAddress(ctx, event.OrderID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	buildHook := s.buildCancelledShippingHook(event.CorrelationID)
	shipment, cancelled, err := s.CancelShipment(ctx, event.OrderID, s.now(), buildHook)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !cancelled {
		span.AddEvent("shipping_cancel_skipped")
		return nil
	}
	span.AddEvent("shipping_cancelled_published", trace.WithAttributes(attribute.String("shipping.id", shipment.ShippingID)))
	s.metrics.RecordShippingCompensation(s.now().Sub(startedAt))
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

func (s *Service) waitForPendingShippingAddress(ctx context.Context, orderID string) (string, bool, error) {
	deadline := time.Now().Add(pendingAddressWaitTimeout)
	for {
		address, ok, err := s.LoadPendingShippingAddress(ctx, orderID)
		if err != nil || ok || !time.Now().Before(deadline) {
			return address, ok, err
		}
		timer := time.NewTimer(pendingAddressPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return "", false, ctx.Err()
		case <-timer.C:
		}
	}
}
