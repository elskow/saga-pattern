package inventory

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
	"saga-pattern/choreography-saga/inventory-service/internal/messaging"
	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	"saga-pattern/common/faultinjection"
	commontracing "saga-pattern/common/tracing"
)

type Clock func() time.Time
type IDGenerator func() string

const (
	pendingReservationWaitTimeout  = 2 * time.Second
	pendingReservationPollInterval = 10 * time.Millisecond
)

type Service struct {
	repo        repository.Repository
	publisher   messaging.InventoryTopicPublisher
	metrics     *observability.Metrics
	clock       Clock
	newID       IDGenerator
	failureMode faultinjection.Controller
}

func NewService(repo repository.Repository, publisher messaging.InventoryTopicPublisher, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, publisher: publisher, metrics: metrics, clock: time.Now, newID: commoncontext.NewID}, nil
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

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	ctx = events.ContextWithMetadata(ctx, event)
	_, span := commontracing.Tracer("choreography/inventory-service").Start(ctx, "choreography.inventory.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
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
	if err := s.stagePendingReservation(ctx, event.OrderID, event.Items); err != nil {
		return err
	}
	return nil
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
	if err := s.clearPendingReservation(ctx, event.OrderID); err != nil {
		return err
	}
	return nil
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

	if shouldFail, failureState := s.failureMode.ShouldFail(s.now()); shouldFail {
		err := fmt.Errorf("inventory failure mode enabled — reservation forced to fail")
		span.SetAttributes(attribute.String("inventory.result", "failed"), attribute.String("failure.type", "failure_mode"), attribute.String("failure.run_label", failureState.RunLabel), attribute.Int("failure.remaining", failureState.Remaining))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		failedEvent := s.newReservationFailedEvent(event, err, s.now())
		if publishErr := s.publisher.PublishInventoryReservationFailed(ctx, event.OrderID, failedEvent); publishErr != nil {
			span.RecordError(publishErr)
			span.SetStatus(codes.Error, publishErr.Error())
			return publishErr
		}
		span.AddEvent("inventory_reservation_failed_published", trace.WithAttributes(attribute.String("failure.type", "failure_mode")))
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		if err := s.clearPendingReservation(ctx, event.OrderID); err != nil {
			return err
		}
		return nil
	}

	reservationID := s.newID()
	reservedAt := s.now()
	reservations, err := s.reservePendingOrderItems(ctx, event.OrderID, reservationID, pendingItems, reservedAt)
	if err != nil {
		span.SetAttributes(attribute.String("inventory.result", "failed"), attribute.String("failure.type", "reservation_failure"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		failedEvent := s.newReservationFailedEvent(event, err, reservedAt)
		if publishErr := s.publisher.PublishInventoryReservationFailed(ctx, event.OrderID, failedEvent); publishErr != nil {
			span.RecordError(publishErr)
			span.SetStatus(codes.Error, publishErr.Error())
			return publishErr
		}
		span.AddEvent("inventory_reservation_failed_published", trace.WithAttributes(attribute.String("failure.type", "reservation_failure")))
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		if err := s.clearPendingReservation(ctx, event.OrderID); err != nil {
			return err
		}
		return nil
	}

	reservedItems := make([]events.InventoryReservedItem, 0, len(reservations))
	for _, reservation := range reservations {
		reservedItems = append(reservedItems, events.InventoryReservedItem{ProductID: reservation.ProductID, Quantity: reservation.Quantity})
	}
	reservedEvent := events.NewInventoryReservedEvent(reservationID, event.OrderID, reservedItems, reservedAt, event.CorrelationID, reservedAt)
	if err := s.publisher.PublishInventoryReserved(ctx, event.OrderID, reservedEvent); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.AddEvent("inventory_reserved_published", trace.WithAttributes(attribute.String("reservation.id", reservationID)))
	s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
	if err := s.clearPendingReservation(ctx, event.OrderID); err != nil {
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
	reservationID, released, err := s.repo.ReleaseInventory(ctx, event.OrderID, startedAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !released {
		span.AddEvent("inventory_release_skipped")
		return nil
	}
	releasedEvent := events.NewInventoryReleasedEvent(reservationID, event.OrderID, startedAt, event.CorrelationID, startedAt)
	if err := s.publisher.PublishInventoryReleased(ctx, event.OrderID, releasedEvent); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.AddEvent("inventory_released_published", trace.WithAttributes(attribute.String("reservation.id", reservationID)))
	s.metrics.RecordInventoryCompensation(s.now().Sub(startedAt))
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
	_, _, err = s.repo.CommitInventory(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
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

func (s *Service) newReservationFailedEvent(event events.PaymentCompletedEvent, cause error, failedAt time.Time) events.InventoryReservationFailedEvent {
	productID := ""
	switch e := cause.(type) {
	case domain.ProductNotFoundError:
		productID = e.ProductID
	case domain.InsufficientStockError:
		productID = e.ProductID
	}
	return events.NewInventoryReservationFailedEvent(event.OrderID, productID, cause.Error(), failedAt, event.CorrelationID, failedAt)
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}

func (s *Service) stagePendingReservation(ctx context.Context, orderID string, items []dto.OrderItemRequest) error {
	return s.repo.SavePendingReservationItems(ctx, orderID, domain.PendingItemsFromOrder(orderID, items))
}

func (s *Service) loadPendingReservation(ctx context.Context, orderID string) ([]domain.PendingOrderItem, error) {
	return s.repo.LoadPendingReservationItems(ctx, orderID)
}

func (s *Service) clearPendingReservation(ctx context.Context, orderID string) error {
	return s.repo.ClearPendingReservationItems(ctx, orderID)
}

func (s *Service) reservePendingOrderItems(ctx context.Context, orderID string, reservationID string, pendingItems []domain.PendingOrderItem, reservedAt time.Time) ([]domain.Reservation, error) {
	return s.repo.ReserveInventory(ctx, orderID, reservationID, pendingItems, reservedAt)
}

func (s *Service) waitForPendingReservation(ctx context.Context, orderID string) ([]domain.PendingOrderItem, error) {
	deadline := time.Now().Add(pendingReservationWaitTimeout)
	for {
		pendingItems, err := s.loadPendingReservation(ctx, orderID)
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
