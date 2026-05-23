package payments

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/choreography-saga/payment-service/internal/messaging"
	"saga-pattern/choreography-saga/payment-service/internal/observability"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/events"
	commontracing "saga-pattern/common/tracing"
)

type Clock func() time.Time

type IDGenerator func() string

type Service struct {
	repo           repository.Repository
	publisher      messaging.PaymentTopicPublisher
	metrics        *observability.Metrics
	clock          Clock
	newID          IDGenerator
	depositMu      sync.Mutex
	depositBalance *big.Rat
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

func (s *Service) GetDepositBalance() *big.Rat {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if s.depositBalance == nil {
		return nil
	}
	return new(big.Rat).Set(s.depositBalance)
}

func (s *Service) SetDepositBalance(balance *big.Rat) {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if balance != nil {
		s.depositBalance = new(big.Rat).Set(balance)
	} else {
		s.depositBalance = nil
	}
}

func (s *Service) CheckAndDeductBalance(amount *big.Rat) bool {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if s.depositBalance == nil {
		return true
	}
	if s.depositBalance.Cmp(amount) < 0 {
		return false
	}
	s.depositBalance = new(big.Rat).Sub(s.depositBalance, amount)
	return true
}

func (s *Service) RefundBalance(amount *big.Rat) {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if s.depositBalance == nil {
		return
	}
	s.depositBalance = new(big.Rat).Add(s.depositBalance, amount)
}

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	ctx = events.ContextWithMetadata(ctx, event)
	_, span := commontracing.Tracer("choreography/payment-service").Start(ctx, "choreography.payment.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
	switch e := event.(type) {
	case events.OrderCreatedEvent:
		return s.processOrderCreated(ctx, e)
	case events.InventoryReservationFailedEvent:
		return s.compensateInventoryReservationFailure(ctx, e)
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
		if err := s.failPaymentAttempt(ctx, &payment, reason, startedAt); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		return nil
	}

	span.SetAttributes(attribute.String("payment.result", "completed"))
	if err := s.completePaymentAttempt(ctx, &payment, startedAt); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.AddEvent("payment_completed_published")
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

	if err := s.refundCompletedPayment(ctx, &payment, startedAt); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.AddEvent("payment_refunded_published")
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

func (s *Service) createPaymentAttempt(event events.OrderCreatedEvent, startedAt time.Time) (domain.Payment, error) {
	return domain.NewPayment(s.newID(), event.OrderID, event.CustomerID, event.TotalAmount, event.CorrelationID, startedAt)
}

func (s *Service) loadPaymentForOrder(ctx context.Context, orderID string) (domain.Payment, bool, error) {
	return s.repo.GetByOrderID(ctx, orderID)
}

func (s *Service) savePayment(ctx context.Context, payment domain.Payment) error {
	return s.repo.Save(ctx, payment)
}

func (s *Service) failPaymentAttempt(ctx context.Context, payment *domain.Payment, reason string, startedAt time.Time) error {
	if err := payment.MarkFailed(reason, s.now()); err != nil {
		return err
	}
	if err := s.savePayment(ctx, *payment); err != nil {
		return err
	}
	failedEvent := events.NewPaymentFailedEvent(payment.PaymentID, payment.OrderID, payment.FailureReason, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
	if err := s.publisher.PublishPaymentFailed(ctx, payment.OrderID, failedEvent); err != nil {
		return err
	}
	s.metrics.RecordPaymentStep(payment.UpdatedAt.Sub(startedAt))
	return nil
}

func (s *Service) completePaymentAttempt(ctx context.Context, payment *domain.Payment, startedAt time.Time) error {
	if err := payment.MarkCompleted(generateTransactionID(s.newID()), s.now()); err != nil {
		return err
	}
	if err := s.savePayment(ctx, *payment); err != nil {
		return err
	}
	completedEvent := events.NewPaymentCompletedEvent(payment.PaymentID, payment.OrderID, payment.Amount, payment.TransactionID, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
	if err := s.publisher.PublishPaymentCompleted(ctx, payment.OrderID, completedEvent); err != nil {
		return err
	}
	s.metrics.RecordPaymentStep(payment.UpdatedAt.Sub(startedAt))
	return nil
}

func (s *Service) refundCompletedPayment(ctx context.Context, payment *domain.Payment, startedAt time.Time) error {
	amountRat, ok := new(big.Rat).SetString(payment.Amount.String())
	if err := payment.MarkRefunded(s.now()); err != nil {
		return err
	}
	if err := s.savePayment(ctx, *payment); err != nil {
		return err
	}
	if ok {
		s.RefundBalance(amountRat)
	}
	refundedEvent := events.NewPaymentRefundedEvent(payment.PaymentID, payment.OrderID, payment.Amount, payment.UpdatedAt, payment.CorrelationID, payment.CreatedAt)
	if err := s.publisher.PublishPaymentRefunded(ctx, payment.OrderID, refundedEvent); err != nil {
		return err
	}
	s.metrics.RecordPaymentCompensation(payment.UpdatedAt.Sub(startedAt))
	return nil
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
