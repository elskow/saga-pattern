package payments

import (
	"context"
	"fmt"
	"strings"
	"time"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/choreography-saga/payment-service/internal/messaging"
	"saga-pattern/choreography-saga/payment-service/internal/observability"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
)

const premiumFailureProductID = "PROD-PREMIUM-001"

type Clock func() time.Time

type IDGenerator func() string

type Service struct {
	repo      repository.Repository
	publisher messaging.PaymentTopicPublisher
	metrics   *observability.Metrics
	clock     Clock
	newID     IDGenerator
}

func NewService(repo repository.Repository, publisher messaging.PaymentTopicPublisher, metrics *observability.Metrics) (*Service, error) {
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
	case events.InventoryReservationFailedEvent:
		return s.handleInventoryReservationFailed(ctx, e)
	default:
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) handleOrderCreated(ctx context.Context, event events.OrderCreatedEvent) error {
	startedAt := s.now()
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "order-created:"+event.OrderID)
	if err != nil || !marked {
		return err
	}

	payment, err := domain.NewPayment(s.newID(), event.OrderID, event.CustomerID, event.TotalAmount, event.CorrelationID, startedAt)
	if err != nil {
		return err
	}
	payment, err = s.repo.Create(ctx, payment)
	if err != nil {
		return err
	}

	if reason, failed := paymentFailureReason(event.Items); failed {
		if err := payment.MarkFailed(reason, s.now()); err != nil {
			return err
		}
		if err := s.repo.Save(ctx, payment); err != nil {
			return err
		}
		failedEvent := events.NewPaymentFailedEvent(payment.PaymentID, payment.OrderID, payment.FailureReason, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
		if err := s.publisher.PublishPaymentFailed(ctx, payment.OrderID, failedEvent); err != nil {
			return err
		}
		s.metrics.RecordPaymentStep(payment.UpdatedAt.Sub(startedAt))
		return nil
	}

	if err := payment.MarkCompleted(generateTransactionID(s.newID()), s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, payment); err != nil {
		return err
	}
	completedEvent := events.NewPaymentCompletedEvent(payment.PaymentID, payment.OrderID, payment.Amount, payment.TransactionID, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
	if err := s.publisher.PublishPaymentCompleted(ctx, payment.OrderID, completedEvent); err != nil {
		return err
	}
	s.metrics.RecordPaymentStep(payment.UpdatedAt.Sub(startedAt))
	return nil
}

func (s *Service) handleInventoryReservationFailed(ctx context.Context, event events.InventoryReservationFailedEvent) error {
	startedAt := s.now()
	marked, err := s.repo.TryMarkProcessedEvent(ctx, "inventory-failed:"+event.OrderID)
	if err != nil || !marked {
		return err
	}

	payment, ok, err := s.repo.GetByOrderID(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("payment for order %s not found", event.OrderID)
	}
	if payment.Status != domain.PaymentStatusCompleted {
		return nil
	}

	if err := payment.MarkRefunded(s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, payment); err != nil {
		return err
	}
	refundedEvent := events.NewPaymentRefundedEvent(payment.PaymentID, payment.OrderID, payment.Amount, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
	if err := s.publisher.PublishPaymentRefunded(ctx, payment.OrderID, refundedEvent); err != nil {
		return err
	}
	s.metrics.RecordPaymentCompensation(payment.UpdatedAt.Sub(startedAt))
	return nil
}

func paymentFailureReason(items []dto.OrderItemRequest) (string, bool) {
	for _, item := range items {
		if item.ProductID == premiumFailureProductID {
			return fmt.Sprintf("payment failure fixture triggered for product %s", premiumFailureProductID), true
		}
	}
	return "", false
}

func generateTransactionID(seed string) string {
	seed = strings.ToUpper(strings.TrimSpace(seed))
	if len(seed) > 8 {
		seed = seed[:8]
	}
	if seed == "" {
		seed = "PAYMENT"
	}
	return "TX-" + seed
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
