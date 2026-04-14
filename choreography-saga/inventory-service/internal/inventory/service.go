package inventory

import (
	"context"
	"fmt"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
	"saga-pattern/choreography-saga/inventory-service/internal/messaging"
	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/events"
)

type Clock func() time.Time
type IDGenerator func() string

type Service struct {
	repo      repository.Repository
	publisher messaging.InventoryTopicPublisher
	metrics   *observability.Metrics
	clock     Clock
	newID     IDGenerator
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

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	switch e := event.(type) {
	case events.OrderCreatedEvent:
		return s.handleOrderCreated(ctx, e)
	case events.PaymentCompletedEvent:
		return s.handlePaymentCompleted(ctx, e)
	case events.PaymentFailedEvent:
		return s.handlePaymentFailed(ctx, e)
	case events.ShippingFailedEvent:
		return s.handleShippingFailed(ctx, e)
	default:
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) handleOrderCreated(ctx context.Context, event events.OrderCreatedEvent) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "order-created:"+event.OrderID)
	if err != nil || !marked {
		return err
	}
	return s.repo.StorePendingItems(ctx, event.OrderID, domain.PendingItemsFromOrder(event.OrderID, event.Items))
}

func (s *Service) handlePaymentFailed(ctx context.Context, event events.PaymentFailedEvent) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "payment-failed:"+event.OrderID)
	if err != nil || !marked {
		return err
	}
	return s.repo.DeletePendingItems(ctx, event.OrderID)
}

func (s *Service) handlePaymentCompleted(ctx context.Context, event events.PaymentCompletedEvent) error {
	startedAt := s.now()
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "payment-completed:"+event.PaymentID)
	if err != nil || !marked {
		return err
	}

	items, err := s.repo.PendingItems(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return fmt.Errorf("pending items for order %s not found", event.OrderID)
	}

	reservationID := s.newID()
	reservedAt := s.now()
	reservations, err := s.repo.ReserveInventory(ctx, event.OrderID, reservationID, items, reservedAt)
	if err != nil {
		failedEvent := s.newReservationFailedEvent(event, err, reservedAt)
		if publishErr := s.publisher.PublishInventoryReservationFailed(ctx, event.OrderID, failedEvent); publishErr != nil {
			return publishErr
		}
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		return s.repo.DeletePendingItems(ctx, event.OrderID)
	}

	reservedItems := make([]events.InventoryReservedItem, 0, len(reservations))
	for _, reservation := range reservations {
		reservedItems = append(reservedItems, events.InventoryReservedItem{ProductID: reservation.ProductID, Quantity: reservation.Quantity})
	}
	reservedEvent := events.NewInventoryReservedEvent(reservationID, event.OrderID, reservedItems, reservedAt, event.CorrelationID, reservedAt)
	if err := s.publisher.PublishInventoryReserved(ctx, event.OrderID, reservedEvent); err != nil {
		return err
	}
	s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
	return s.repo.DeletePendingItems(ctx, event.OrderID)
}

func (s *Service) handleShippingFailed(ctx context.Context, event events.ShippingFailedEvent) error {
	startedAt := s.now()
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "shipping-failed:"+event.OrderID)
	if err != nil || !marked {
		return err
	}
	reservationID, released, err := s.repo.ReleaseInventory(ctx, event.OrderID, startedAt)
	if err != nil {
		return err
	}
	if !released {
		return nil
	}
	releasedEvent := events.NewInventoryReleasedEvent(reservationID, event.OrderID, startedAt, event.CorrelationID, startedAt)
	if err := s.publisher.PublishInventoryReleased(ctx, event.OrderID, releasedEvent); err != nil {
		return err
	}
	s.metrics.RecordInventoryCompensation(s.now().Sub(startedAt))
	return nil
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
