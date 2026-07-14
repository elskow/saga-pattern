package payments

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/choreography-saga/payment-service/internal/observability"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/events"
)

type Clock func() time.Time

type IDGenerator func() string

type participantAdapter interface {
	EnqueueEvent(ctx context.Context, tx *sql.Tx, topic, key, eventType string, payload any) error
	TriggerImmediatePublish(ctx context.Context)
}

type Service struct {
	repo           repository.Repository
	participant    participantAdapter
	metrics        *observability.Metrics
	clock          Clock
	newID          IDGenerator
	depositMu      sync.Mutex
	depositBalance *big.Rat
}

func NewService(repo repository.Repository, participant participantAdapter, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, participant: participant, metrics: metrics, clock: time.Now, newID: commoncontext.NewID}, nil
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

func (s *Service) createPaymentAttempt(event events.OrderCreatedEvent, startedAt time.Time) (domain.Payment, error) {
	return domain.NewPayment(s.newID(), event.OrderID, event.CustomerID, event.TotalAmount, event.CorrelationID, startedAt)
}

func (s *Service) loadPaymentForOrder(ctx context.Context, orderID string) (domain.Payment, bool, error) {
	return s.repo.GetByOrderID(ctx, orderID)
}

func (s *Service) markPaymentFailed(ctx context.Context, payment *domain.Payment, reason string, startedAt time.Time, hook repository.TxHook) error {
	if err := payment.MarkFailed(reason, s.now()); err != nil {
		return err
	}
	return s.repo.Save(ctx, *payment, hook)
}

func (s *Service) markPaymentCompleted(ctx context.Context, payment *domain.Payment, startedAt time.Time, hook repository.TxHook) error {
	if err := payment.MarkCompleted(generateTransactionID(s.newID()), s.now()); err != nil {
		return err
	}
	return s.repo.Save(ctx, *payment, hook)
}

func (s *Service) markPaymentRefunded(ctx context.Context, payment *domain.Payment, startedAt time.Time, hook repository.TxHook) error {
	amountRat, ok := new(big.Rat).SetString(payment.Amount.String())
	if err := payment.MarkRefunded(s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, *payment, hook); err != nil {
		return err
	}
	if ok {
		s.RefundBalance(amountRat)
	}
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
