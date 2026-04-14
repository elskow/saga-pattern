package shipping

import (
	"context"
	"fmt"
	"strings"
	"time"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/choreography-saga/shipping-service/internal/messaging"
	"saga-pattern/choreography-saga/shipping-service/internal/observability"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/events"
)

const (
	defaultTrackingPrefix       = "TRK-"
	defaultEstimatedDeliveryDay = 3
)

type Clock func() time.Time

type IDGenerator func() string

type Service struct {
	repo      repository.Repository
	publisher messaging.ShippingTopicPublisher
	metrics   *observability.Metrics
	clock     Clock
	newID     IDGenerator
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

func (s *Service) handleOrderCreated(ctx context.Context, event events.OrderCreatedEvent) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "order-created:"+event.OrderID)
	if err != nil || !marked {
		return err
	}
	return s.repo.StorePendingAddress(ctx, event.OrderID, event.ShippingAddress)
}

func (s *Service) handleInventoryReserved(ctx context.Context, event events.InventoryReservedEvent) error {
	startedAt := s.now()
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "inventory-reserved:"+event.ReservationID)
	if err != nil || !marked {
		return err
	}

	address, ok, err := s.repo.PendingAddress(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(address) == "" {
		return fmt.Errorf("pending shipping address for order %s not found", event.OrderID)
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
		return err
	}
	if err := s.repo.DeletePendingAddress(ctx, event.OrderID); err != nil {
		return err
	}
	s.metrics.RecordShippingStep(s.now().Sub(startedAt))
	return nil
}

func (s *Service) handlePaymentRefunded(ctx context.Context, event events.PaymentRefundedEvent) error {
	startedAt := s.now()
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "payment-refunded:"+event.PaymentID)
	if err != nil || !marked {
		return err
	}
	if err := s.repo.DeletePendingAddress(ctx, event.OrderID); err != nil {
		return err
	}

	shipment, ok, err := s.repo.GetShipmentByOrderID(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if !ok || !shipment.IsCancellable() {
		return nil
	}
	if err := shipment.Cancel(s.now()); err != nil {
		return err
	}
	if err := s.repo.SaveShipment(ctx, shipment); err != nil {
		return err
	}

	cancelledEvent := events.NewShippingCancelledEvent(shipment.ShippingID, shipment.OrderID, shipment.UpdatedAt, event.CorrelationID, shipment.UpdatedAt)
	if err := s.publisher.PublishShippingCancelled(ctx, shipment.OrderID, cancelledEvent); err != nil {
		return err
	}
	s.metrics.RecordShippingCompensation(s.now().Sub(startedAt))
	return nil
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
