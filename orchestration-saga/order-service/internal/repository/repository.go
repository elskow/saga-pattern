package repository

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	frameworkruntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/domain"
)

type Repository interface {
	SaveAccepted(context.Context, domain.Order) error
	GetVisible(context.Context, string) (domain.Order, bool, error)
	FinalizeFromSnapshot(context.Context, frameworkruntime.Snapshot, time.Time) (domain.Order, error)
	ListVisible(context.Context) ([]domain.Order, error)
	ListVisibleByCustomer(context.Context, string) ([]domain.Order, error)
}

type MemoryRepository struct {
	mu     sync.RWMutex
	orders map[string]domain.Order
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{orders: make(map[string]domain.Order)}
}

func (r *MemoryRepository) SaveAccepted(_ context.Context, order domain.Order) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.orders[order.OrderID]; exists {
		return fmt.Errorf("order %s already exists", order.OrderID)
	}
	r.orders[order.OrderID] = order
	return nil
}

func (r *MemoryRepository) GetVisible(_ context.Context, orderID string) (domain.Order, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	order, ok := r.orders[orderID]
	if !ok || !order.Visible {
		return domain.Order{}, false, nil
	}
	return order, true, nil
}

func (r *MemoryRepository) FinalizeFromSnapshot(_ context.Context, snapshot frameworkruntime.Snapshot, now time.Time) (domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	order, ok := r.orders[snapshot.OrderID]
	if !ok {
		order = domain.FromSnapshot(snapshot, now)
		r.orders[order.OrderID] = order
		return order, nil
	}
	order.Visible = true
	order.Status = terminalStatus(snapshot.State)
	order.FailureReason = snapshot.LastError
	order.UpdatedAt = now
	if order.CreatedAt.IsZero() {
		order.CreatedAt = snapshot.StartedAt
	}
	r.orders[order.OrderID] = order
	return order, nil
}

func (r *MemoryRepository) ListVisible(_ context.Context) ([]domain.Order, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	orders := make([]domain.Order, 0, len(r.orders))
	for _, order := range r.orders {
		if order.Visible {
			orders = append(orders, order)
		}
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].CreatedAt.Before(orders[j].CreatedAt) })
	return orders, nil
}

func (r *MemoryRepository) ListVisibleByCustomer(_ context.Context, customerID string) ([]domain.Order, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	orders := make([]domain.Order, 0)
	for _, order := range r.orders {
		if order.Visible && order.CustomerID == customerID {
			orders = append(orders, order)
		}
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].CreatedAt.Before(orders[j].CreatedAt) })
	return orders, nil
}

func terminalStatus(state string) string {
	switch state {
	case domain.StatusCompleted:
		return domain.StatusCompleted
	case domain.StatusCancelled:
		return domain.StatusCancelled
	default:
		return domain.StatusFailed
	}
}
