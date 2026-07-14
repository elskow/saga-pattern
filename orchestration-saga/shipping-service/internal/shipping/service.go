package shipping

import (
	"context"
	"fmt"
	"time"

	"saga-pattern/common/faultinjection"
	"saga-pattern/orchestration-saga/shipping-service/internal/messaging"
	"saga-pattern/orchestration-saga/shipping-service/internal/observability"
	"saga-pattern/orchestration-saga/shipping-service/internal/repository"
)

type Clock func() time.Time

type Service struct {
	repo        repository.Repository
	publisher   messaging.ReplyPublisher
	metrics     *observability.Metrics
	clock       Clock
	failureMode faultinjection.Controller
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

func (s *Service) HealthStatus(ctx context.Context) error {
	return s.repo.PingContext(ctx)
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
