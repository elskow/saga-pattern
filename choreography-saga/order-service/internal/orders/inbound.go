package orders

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/events"
	commontracing "saga-pattern/common/tracing"
)

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	ctx = events.ContextWithMetadata(ctx, event)
	ctx, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
	switch e := event.(type) {
	case events.PaymentCompletedEvent:
		return s.foldPaymentCompleted(ctx, e)
	case events.InventoryReservedEvent:
		return s.foldInventoryReserved(ctx, e)
	case events.ShippingScheduledEvent:
		return s.foldShippingScheduled(ctx, e)
	case events.PaymentFailedEvent:
		return s.foldTerminalFailure(ctx, e.OrderID, "payment-failed:"+e.OrderID, "payment", "Payment failed: "+e.Reason)
	case events.InventoryReservationFailedEvent:
		return s.foldTerminalFailure(ctx, e.OrderID, "inventory-failed:"+e.OrderID, "inventory", "Inventory reservation failed: "+e.Reason)
	case events.ShippingFailedEvent:
		return s.foldTerminalFailure(ctx, e.OrderID, "shipping-failed:"+e.OrderID, "shipping", "Shipping failed: "+e.Reason)
	case events.PaymentRefundedEvent:
		return s.recordCompensationAcknowledged(ctx, "payment-refunded:"+e.OrderID)
	case events.InventoryReleasedEvent:
		return s.recordCompensationAcknowledged(ctx, "inventory-released:"+e.OrderID)
	case events.ShippingCancelledEvent:
		return s.recordCompensationAcknowledged(ctx, "shipping-cancelled:"+e.OrderID)
	default:
		span.RecordError(fmt.Errorf("unsupported choreography event %T", event))
		span.SetStatus(codes.Error, "unsupported event")
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) foldPaymentCompleted(ctx context.Context, event events.PaymentCompletedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.payment_completed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("payment.id", event.PaymentID)))
	defer span.End()
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
	order, err := s.loadOrderForEvent(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_progress_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkPaymentCompleted(event.PaymentID, s.clock())
	span.AddEvent("order_payment_marked_completed", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		return err
	}
	return nil
}

func (s *Service) foldInventoryReserved(ctx context.Context, event events.InventoryReservedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.inventory_reserved",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("reservation.id", event.ReservationID)))
	defer span.End()
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
	order, err := s.loadOrderForEvent(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_progress_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkInventoryReserved(event.ReservationID, s.clock())
	span.AddEvent("order_inventory_marked_reserved", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		return err
	}
	return nil
}

func (s *Service) foldShippingScheduled(ctx context.Context, event events.ShippingScheduledEvent) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.shipping_scheduled",
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
	order, err := s.loadOrderForEvent(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_progress_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkCompleted(event.ShippingID, event.TrackingNumber, s.clock())
	span.AddEvent("order_marked_completed", trace.WithAttributes(attribute.String("shipping.id", event.ShippingID)))
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordOrderCompleted(order.UpdatedAt.Sub(order.CreatedAt))
	return nil
}

func (s *Service) foldTerminalFailure(ctx context.Context, orderID string, eventKey string, failureType string, reason string) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.failure",
		trace.WithAttributes(attribute.String("order.id", orderID), attribute.String("event.key", eventKey), attribute.String("failure.type", failureType)))
	defer span.End()
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
	order, err := s.loadOrderForEvent(ctx, orderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_failure_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkCancelled(reason, s.clock())
	span.AddEvent("order_marked_cancelled")
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordOrderFailed(order.UpdatedAt.Sub(order.CreatedAt))
	return nil
}

func (s *Service) recordCompensationAcknowledged(ctx context.Context, eventKey string) error {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.compensation_acknowledged",
		trace.WithAttributes(attribute.String("event.key", eventKey)))
	defer span.End()
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	parts := strings.SplitN(eventKey, ":", 2)
	if len(parts) != 2 {
		span.RecordError(fmt.Errorf("invalid compensation event key: %s", eventKey))
		span.SetStatus(codes.Error, "invalid event key")
		return nil
	}
	orderID := parts[1]
	order, err := s.loadOrderForEvent(ctx, orderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("compensation_acknowledged_order_already_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	span.AddEvent("compensation_acknowledged_order_not_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
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
