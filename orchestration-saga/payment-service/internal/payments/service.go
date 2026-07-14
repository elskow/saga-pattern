package payments

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"saga-pattern/orchestration-saga/payment-service/internal/messaging"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
	"saga-pattern/orchestration-saga/payment-service/internal/repository"
)

type Clock func() time.Time

type Service struct {
	repo           repository.Repository
	publisher      messaging.ReplyPublisher
	metrics        *observability.Metrics
	clock          Clock
	processMu      sync.Mutex
	depositMu      sync.Mutex
	depositBalance *big.Rat // nil means unlimited
}

func NewService(repo repository.Repository, publisher messaging.ReplyPublisher, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	if publisher.Topic() == "" {
		return nil, fmt.Errorf("reply publisher is required")
	}
	return &Service{repo: repo, publisher: publisher, metrics: metrics, clock: time.Now}, nil
}

func (s *Service) WithClock(clock Clock) {
	if clock != nil {
		s.clock = clock
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

func (s *Service) HealthStatus(ctx context.Context) error {
	return s.repo.PingContext(ctx)
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
