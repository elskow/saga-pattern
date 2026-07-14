package shipping

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/choreography-saga/shipping-service/internal/observability"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/faultinjection"
)

const (
	defaultTrackingPrefix       = "TRK-"
	defaultEstimatedDeliveryDay = 3
	pendingAddressWaitTimeout   = 2 * time.Second
	pendingAddressPollInterval  = 10 * time.Millisecond
)

type Clock func() time.Time

type IDGenerator func() string

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

// CreateShipment buildHook is called with the created shipment to produce the outbox TxHook; pass nil to skip.
func (s *Service) CreateShipment(ctx context.Context, orderID, address string, buildHook func(domain.Shipment) repository.TxHook) (domain.Shipment, error) {
	shippingID := s.newID()
	scheduledAt := s.now()
	shipment, err := domain.NewPendingShipment(
		shippingID,
		orderID,
		trackingNumberFor(shippingID),
		address,
		scheduledAt.Add(defaultEstimatedDeliveryDay*24*time.Hour),
		scheduledAt,
	)
	if err != nil {
		return domain.Shipment{}, err
	}
	if err := s.repo.SaveShipment(ctx, shipment, nil); err != nil {
		return domain.Shipment{}, err
	}
	if err := shipment.MarkScheduled(scheduledAt); err != nil {
		return domain.Shipment{}, err
	}
	var hook repository.TxHook
	if buildHook != nil {
		hook = buildHook(shipment)
	}
	if err := s.repo.SaveShipment(ctx, shipment, hook); err != nil {
		return domain.Shipment{}, err
	}
	return shipment, nil
}

func (s *Service) CancelShipment(ctx context.Context, orderID string, now time.Time, buildHook func(domain.Shipment) repository.TxHook) (domain.Shipment, bool, error) {
	shipment, ok, err := s.repo.GetShipmentByOrderID(ctx, orderID)
	if err != nil {
		return domain.Shipment{}, false, err
	}
	if !ok || !shipment.IsCancellable() {
		return domain.Shipment{}, false, nil
	}
	if err := shipment.Cancel(now); err != nil {
		return domain.Shipment{}, false, err
	}
	var hook repository.TxHook
	if buildHook != nil {
		hook = buildHook(shipment)
	}
	if err := s.repo.SaveShipment(ctx, shipment, hook); err != nil {
		return domain.Shipment{}, false, err
	}
	return shipment, true, nil
}

func (s *Service) StagePendingShippingAddress(ctx context.Context, orderID string, shippingAddress string) error {
	return s.repo.SavePendingShippingAddress(ctx, orderID, shippingAddress)
}

func (s *Service) LoadPendingShippingAddress(ctx context.Context, orderID string) (string, bool, error) {
	return s.repo.LoadPendingShippingAddress(ctx, orderID)
}

func (s *Service) ClearPendingShippingAddress(ctx context.Context, orderID string) error {
	return s.repo.ClearPendingShippingAddress(ctx, orderID)
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
