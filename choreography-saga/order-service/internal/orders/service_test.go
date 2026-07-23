package orders

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
)

func TestFoldInventoryReservedDoesNotReopenCancelledOrder(t *testing.T) {
	now := time.Date(2026, 5, 14, 4, 3, 8, 0, time.UTC)
	repo := &recordingRepo{
		order: domain.Order{
			OrderID:       "ORDER-CANCELLED",
			Status:        dto.OrderStatusCancelled,
			FailureReason: "Shipping failed: failure mode",
			CreatedAt:     now.Add(-time.Second),
			UpdatedAt:     now,
		},
	}
	service := &Service{repo: repo, clock: func() time.Time { return now.Add(time.Second) }}
	event := events.NewInventoryReservedEvent(
		"RES-LATE",
		repo.order.OrderID,
		[]events.InventoryReservedItem{{ProductID: "PROD-001", Quantity: 1}},
		now,
		"corr-late",
		now,
	)

	if err := service.foldInventoryReserved(context.Background(), event); err != nil {
		t.Fatalf("fold inventory reserved: %v", err)
	}

	if repo.saved {
		t.Fatalf("terminal order should not be saved with progress status")
	}
	if repo.order.Status != dto.OrderStatusCancelled {
		t.Fatalf("order status = %s, want %s", repo.order.Status, dto.OrderStatusCancelled)
	}
	if !repo.processed["inventory-reserved:RES-LATE"] {
		t.Fatalf("late event should still be marked processed")
	}
}

type recordingRepo struct {
	order     domain.Order
	stuck     []domain.Order
	saved     bool
	processed map[string]bool
}

func (r *recordingRepo) Create(_ context.Context, order domain.Order, _ repository.TxHook) (domain.Order, error) {
	return order, nil
}

func (r *recordingRepo) CreateIfAbsent(_ context.Context, _ string, order domain.Order, _ repository.TxHook) (domain.Order, bool, error) {
	return order, false, nil
}

func (r *recordingRepo) Get(context.Context, string) (domain.Order, bool, error) {
	return r.order, true, nil
}

func (r *recordingRepo) List(context.Context) ([]domain.Order, error) {
	return nil, nil
}

func (r *recordingRepo) Save(_ context.Context, order domain.Order) error {
	r.saved = true
	r.order = order
	return nil
}

func (r *recordingRepo) SaveWithHook(_ context.Context, order domain.Order, _ repository.TxHook) error {
	return r.Save(nil, order)
}

func (r *recordingRepo) FindStuckOrders(_ context.Context, _ time.Time) ([]domain.Order, error) {
	if r.stuck != nil {
		return append([]domain.Order(nil), r.stuck...), nil
	}
	return nil, nil
}

func TestCancelTimeoutOrderRecordsFailedMetrics(t *testing.T) {
	now := time.Date(2026, 5, 14, 4, 3, 8, 0, time.UTC)
	created := now.Add(-90 * time.Second)
	order := domain.Order{
		OrderID:       "ORDER-TIMEOUT",
		Status:        dto.OrderStatusPaymentCompleted,
		CorrelationID: "corr-timeout",
		CreatedAt:     created,
		UpdatedAt:     created,
	}
	repo := &recordingRepo{order: order, stuck: []domain.Order{order}}
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scanAndCancel(context.Background(), logger, repo, stubParticipant{}, metrics)

	if !repo.saved {
		t.Fatalf("timeout cancel should save order")
	}
	if repo.order.Status != dto.OrderStatusCancelled {
		t.Fatalf("order status = %s, want %s", repo.order.Status, dto.OrderStatusCancelled)
	}
	if repo.order.FailureReason != "saga timeout" {
		t.Fatalf("failure reason = %q, want saga timeout", repo.order.FailureReason)
	}

	failed := counterValue(t, metrics.Registry(), "saga_orders_failed_total")
	if failed != 1 {
		t.Fatalf("saga_orders_failed_total = %v, want 1", failed)
	}
	durationCount := histogramCount(t, metrics.Registry(), "saga_total_duration_seconds")
	if durationCount != 1 {
		t.Fatalf("saga_total_duration_seconds count = %v, want 1", durationCount)
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				return float64(h.GetSampleCount())
			}
		}
	}
	t.Fatalf("histogram %s not found", name)
	return 0
}

func (r *recordingRepo) CancelOrder(_ context.Context, _ string) error {
	return nil
}

func (r *recordingRepo) TryMarkProcessedEvent(_ context.Context, key string) (bool, error) {
	if r.processed == nil {
		r.processed = make(map[string]bool)
	}
	if r.processed[key] {
		return false, nil
	}
	r.processed[key] = true
	return true, nil
}

func (r *recordingRepo) DeleteProcessedEvent(_ context.Context, key string) error {
	delete(r.processed, key)
	return nil
}

// stubParticipant is a no-op participantAdapter for service tests that don't
// exercise the publishing path.
type stubParticipant struct{}

func (stubParticipant) EnqueueOrderCreated(context.Context, *sql.Tx, string, events.OrderCreatedEvent) error {
	return nil
}

func (stubParticipant) EnqueueOrderCancelled(context.Context, *sql.Tx, string, events.OrderCancelledEvent) error {
	return nil
}

func (stubParticipant) TriggerImmediatePublish(context.Context) {}

var _ participantAdapter = stubParticipant{}
