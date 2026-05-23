package orders

import (
	"context"
	"testing"
	"time"

	"saga-pattern/choreography-saga/order-service/internal/domain"
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
	saved     bool
	processed map[string]bool
}

func (r *recordingRepo) Create(context.Context, domain.Order) (domain.Order, error) {
	return domain.Order{}, nil
}

func (r *recordingRepo) CreateIfAbsent(context.Context, string, domain.Order) (domain.Order, bool, error) {
	return domain.Order{}, false, nil
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

func (r *recordingRepo) ClaimPendingOrderEvents(context.Context, int) ([]repository.OrderOutboxMessage, error) {
	return nil, nil
}

func (r *recordingRepo) MarkOrderEventPublished(context.Context, int64) error {
	return nil
}

func (r *recordingRepo) MarkOrderEventPublishFailed(context.Context, int64, string) error {
	return nil
}
