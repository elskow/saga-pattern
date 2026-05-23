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
	"saga-pattern/choreography-saga/shipping-service/internal/messaging"
	"saga-pattern/choreography-saga/shipping-service/internal/observability"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/events"
	"saga-pattern/common/faultinjection"
	commontracing "saga-pattern/common/tracing"
)

const (
	defaultTrackingPrefix       = "TRK-"
	defaultEstimatedDeliveryDay = 3
	pendingAddressWaitTimeout   = 2 * time.Second
	pendingAddressPollInterval  = 10 * time.Millisecond
)

type Clock func() time.Time

type IDGenerator func() string

type Service struct {
	repo        repository.Repository
	publisher   messaging.ShippingTopicPublisher
	metrics     *observability.Metrics
	clock       Clock
	newID       IDGenerator
	failureMode faultinjection.Controller
}

func NewService(repo repository.Repository, publisher messaging.ShippingTopicPublisher, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, publisher: publisher, metrics: metrics, clock: time.Now, newID: commoncontext.NewID}, nil
}

func (s *Service) FailureModeEnabled() bool {
	return s.failureMode.Snapshot(s.now()).Enabled
}

func (s *Service) SetFailureModeEnabled(enabled bool) {
	s.failureMode.SetEnabled(enabled, s.now())
}

func (s *Service) FailureModeState() faultinjection.Snapshot {
	return s.failureMode.Snapshot(s.now())
}

func (s *Service) ConfigureFailureMode(config faultinjection.Config) faultinjection.Snapshot {
	return s.failureMode.Configure(config, s.now())
}

func (s *Service) WithClock(clock Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) WithIDGenerator(generator IDGenerator) {
	if generator != nil {
		s.newID = generator
	}
}

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
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
	if err := s.stagePendingShippingAddress(ctx, event.OrderID, event.ShippingAddress); err != nil {
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
		failedEvent := events.NewShippingFailedEvent(event.OrderID, err.Error(), s.now(), event.CorrelationID, s.now())
		if publishErr := s.publisher.PublishShippingFailed(ctx, event.OrderID, failedEvent); publishErr != nil {
			span.RecordError(publishErr)
			span.SetStatus(codes.Error, publishErr.Error())
			return publishErr
		}
		span.AddEvent("shipping_failed_published", trace.WithAttributes(attribute.String("failure.type", "failure_mode")))
		s.metrics.RecordShippingStep(s.now().Sub(startedAt))
		if err := s.clearPendingShippingAddress(ctx, event.OrderID); err != nil {
			return err
		}
		return nil
	}

	shippingID := s.newID()
	scheduledAt := s.now()
	shipment, err := domain.NewPendingShipment(
		shippingID,
		event.OrderID,
		trackingNumberFor(shippingID),
		address,
		scheduledAt.Add(defaultEstimatedDeliveryDay*24*time.Hour),
		scheduledAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := s.repo.SaveShipment(ctx, shipment); err != nil {
		return err
	}
	if err := shipment.MarkScheduled(scheduledAt); err != nil {
		return err
	}
	if err := s.repo.SaveShipment(ctx, shipment); err != nil {
		return err
	}

	scheduledEvent := events.NewShippingScheduledEvent(
		shipment.ShippingID,
		shipment.OrderID,
		shipment.TrackingNumber,
		shipment.ShippingAddress,
		shipment.EstimatedDelivery,
		shipment.UpdatedAt,
		event.CorrelationID,
		shipment.CreatedAt,
	)
	if err := s.publisher.PublishShippingScheduled(ctx, shipment.OrderID, scheduledEvent); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.AddEvent("shipping_scheduled_published", trace.WithAttributes(attribute.String("shipping.id", shipment.ShippingID)))
	if err := s.clearPendingShippingAddress(ctx, event.OrderID); err != nil {
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
	if err := s.clearPendingShippingAddress(ctx, event.OrderID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	shipment, ok, err := s.repo.GetShipmentByOrderID(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !ok || !shipment.IsCancellable() {
		span.AddEvent("shipping_cancel_skipped")
		return nil
	}
	if err := shipment.Cancel(s.now()); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := s.repo.SaveShipment(ctx, shipment); err != nil {
		return err
	}

	cancelledEvent := events.NewShippingCancelledEvent(shipment.ShippingID, shipment.OrderID, shipment.UpdatedAt, event.CorrelationID, shipment.UpdatedAt)
	if err := s.publisher.PublishShippingCancelled(ctx, shipment.OrderID, cancelledEvent); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
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

func trackingNumberFor(shippingID string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(shippingID))
	trimmed = strings.ReplaceAll(trimmed, "-", "")
	if len(trimmed) > 10 {
		trimmed = trimmed[:10]
	}
	if trimmed == "" {
		trimmed = "SHIPMENT"
	}
	return defaultTrackingPrefix + trimmed
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}

func (s *Service) stagePendingShippingAddress(ctx context.Context, orderID string, shippingAddress string) error {
	return s.repo.SavePendingShippingAddress(ctx, orderID, shippingAddress)
}

func (s *Service) loadPendingShippingAddress(ctx context.Context, orderID string) (string, bool, error) {
	return s.repo.LoadPendingShippingAddress(ctx, orderID)
}

func (s *Service) waitForPendingShippingAddress(ctx context.Context, orderID string) (string, bool, error) {
	deadline := time.Now().Add(pendingAddressWaitTimeout)
	for {
		address, ok, err := s.loadPendingShippingAddress(ctx, orderID)
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

func (s *Service) clearPendingShippingAddress(ctx context.Context, orderID string) error {
	return s.repo.ClearPendingShippingAddress(ctx, orderID)
}
