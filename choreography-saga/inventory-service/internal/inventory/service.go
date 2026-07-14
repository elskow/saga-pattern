package inventory

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/faultinjection"
)

type Clock func() time.Time
type IDGenerator func() string

const (
	pendingReservationWaitTimeout  = 2 * time.Second
	pendingReservationPollInterval = 10 * time.Millisecond
)

type participantAdapter interface {
	EnqueueEvent(ctx context.Context, tx *sql.Tx, topic, key, eventType string, payload any) error
	TriggerImmediatePublish(ctx context.Context)
}

type Service struct {
	repo        repository.Repository
	participant participantAdapter
	metrics     *observability.Metrics
	clock       Clock
	newID       IDGenerator
	failureMode faultinjection.Controller
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

func (s *Service) StagePendingReservation(ctx context.Context, orderID string, items []dto.OrderItemRequest) error {
	return s.repo.SavePendingReservationItems(ctx, orderID, domain.PendingItemsFromOrder(orderID, items))
}

func (s *Service) LoadPendingReservation(ctx context.Context, orderID string) ([]domain.PendingOrderItem, error) {
	return s.repo.LoadPendingReservationItems(ctx, orderID)
}

func (s *Service) ClearPendingReservation(ctx context.Context, orderID string) error {
	return s.repo.ClearPendingReservationItems(ctx, orderID)
}

func (s *Service) ReservePendingOrderItems(ctx context.Context, orderID string, reservationID string, pendingItems []domain.PendingOrderItem, reservedAt time.Time, onReserve repository.TxHook, onFail repository.TxHook) ([]domain.Reservation, error) {
	return s.repo.ReserveInventory(ctx, orderID, reservationID, pendingItems, reservedAt, onReserve, onFail)
}

func (s *Service) ReleaseInventory(ctx context.Context, orderID string, releasedAt time.Time, hook repository.TxHook) (string, bool, error) {
	return s.repo.ReleaseInventory(ctx, orderID, releasedAt, hook)
}

func (s *Service) CommitInventory(ctx context.Context, orderID string) error {
	_, _, err := s.repo.CommitInventory(ctx, orderID)
	return err
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
