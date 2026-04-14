package orders

import (
	"context"
	"errors"
	"fmt"
	"time"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/choreography-saga/order-service/internal/messaging"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
)

var ErrOrderNotFound = errors.New("order not found")

type Clock func() time.Time

type Service struct {
	repo      repository.Repository
	publisher messaging.OrderTopicPublisher
	metrics   *observability.Metrics
	clock     Clock
}

func NewService(repo repository.Repository, publisher messaging.OrderTopicPublisher, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, publisher: publisher, metrics: metrics, clock: time.Now}, nil
}

func (s *Service) WithClock(clock Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) CreateOrder(ctx context.Context, request dto.ChoreographyCreateOrderRequest, idempotencyKey string) (dto.OrderResponse, bool, error) {
	now := s.clock().UTC()
	orderID := commoncontext.NewID()
	order, err := domain.NewOrderFromRequest(orderID, request, idempotencyKey, now)
	if err != nil {
		return dto.OrderResponse{}, false, err
	}

	created := true
	if idempotencyKey == "" {
		order, err = s.repo.Create(ctx, order)
	} else {
		order, created, err = s.repo.CreateIfAbsent(ctx, idempotencyKey, order)
	}
	if err != nil {
		return dto.OrderResponse{}, false, err
	}

	if created {
		event := order.ToOrderCreatedEvent(now)
		if err := s.publisher.PublishOrderCreated(ctx, order.OrderID, event); err != nil {
			return dto.OrderResponse{}, false, err
		}
		s.metrics.RecordOrderCreated()
	}

	return order.Response(), created, nil
}

func (s *Service) GetOrder(ctx context.Context, orderID string) (dto.OrderResponse, error) {
	order, ok, err := s.repo.Get(ctx, orderID)
	if err != nil {
		return dto.OrderResponse{}, err
	}
	if !ok {
		return dto.OrderResponse{}, ErrOrderNotFound
	}
	return order.Response(), nil
}

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	switch e := event.(type) {
	case events.PaymentCompletedEvent:
		return s.handlePaymentCompleted(ctx, e)
	case events.InventoryReservedEvent:
		return s.handleInventoryReserved(ctx, e)
	case events.ShippingScheduledEvent:
		return s.handleShippingScheduled(ctx, e)
	case events.PaymentFailedEvent:
		return s.handleFailure(ctx, e.OrderID, "payment-failed:"+e.OrderID, "Payment failed: "+e.Reason)
	case events.InventoryReservationFailedEvent:
		return s.handleFailure(ctx, e.OrderID, "inventory-failed:"+e.OrderID, "Inventory reservation failed: "+e.Reason)
	case events.ShippingFailedEvent:
		return s.handleFailure(ctx, e.OrderID, "shipping-failed:"+e.OrderID, "Shipping failed: "+e.Reason)
	case events.PaymentRefundedEvent:
		_, err := s.repo.TryMarkProcessedEvent(ctx, "payment-refunded:"+e.OrderID)
		return err
	default:
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) handlePaymentCompleted(ctx context.Context, event events.PaymentCompletedEvent) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "payment-completed:"+event.PaymentID)
	if err != nil || !marked {
		return err
	}
	order, err := s.lookupOrder(ctx, event.OrderID)
	if err != nil {
		return err
	}
	order.MarkPaymentCompleted(event.PaymentID, s.clock())
	return s.repo.Save(ctx, order)
}

func (s *Service) handleInventoryReserved(ctx context.Context, event events.InventoryReservedEvent) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "inventory-reserved:"+event.ReservationID)
	if err != nil || !marked {
		return err
	}
	order, err := s.lookupOrder(ctx, event.OrderID)
	if err != nil {
		return err
	}
	order.MarkInventoryReserved(event.ReservationID, s.clock())
	return s.repo.Save(ctx, order)
}

func (s *Service) handleShippingScheduled(ctx context.Context, event events.ShippingScheduledEvent) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "shipping-scheduled:"+event.ShippingID)
	if err != nil || !marked {
		return err
	}
	order, err := s.lookupOrder(ctx, event.OrderID)
	if err != nil {
		return err
	}
	order.MarkCompleted(event.ShippingID, event.TrackingNumber, s.clock())
	if err := s.repo.Save(ctx, order); err != nil {
		return err
	}
	s.metrics.RecordOrderCompleted(order.UpdatedAt.Sub(order.CreatedAt))
	return nil
}

func (s *Service) handleFailure(ctx context.Context, orderID string, eventKey string, reason string) error {
	marked, err := s.repo.TryMarkProcessedEvent(ctx, eventKey)
	if err != nil || !marked {
		return err
	}
	order, err := s.lookupOrder(ctx, orderID)
	if err != nil {
		return err
	}
	order.MarkCancelled(reason, s.clock())
	if err := s.repo.Save(ctx, order); err != nil {
		return err
	}
	s.metrics.RecordOrderFailed(order.UpdatedAt.Sub(order.CreatedAt))
	return nil
}

func (s *Service) lookupOrder(ctx context.Context, orderID string) (domain.Order, error) {
	order, ok, err := s.repo.Get(ctx, orderID)
	if err != nil {
		return domain.Order{}, err
	}
	if !ok {
		return domain.Order{}, ErrOrderNotFound
	}
	return order, nil
}
