package repository

import (
	"context"
	"fmt"
	"sync"

	"saga-pattern/choreography-saga/order-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Order) (domain.Order, error)
	CreateIfAbsent(context.Context, string, domain.Order) (domain.Order, bool, error)
	Get(context.Context, string) (domain.Order, bool, error)
	Save(context.Context, domain.Order) error
	TryMarkProcessedEvent(context.Context, string) (bool, error)
}

type MemoryRepository struct {
	mu              sync.RWMutex
	orders          map[string]domain.Order
	byIdempotency   map[string]string
	processedEvents map[string]struct{}
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		orders:          make(map[string]domain.Order),
		byIdempotency:   make(map[string]string),
		processedEvents: make(map[string]struct{}),
	}
}

func (r *MemoryRepository) Create(_ context.Context, order domain.Order) (domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.orders[order.OrderID]; exists {
		return domain.Order{}, fmt.Errorf("order %s already exists", order.OrderID)
	}
	r.orders[order.OrderID] = order
	return order, nil
}

func (r *MemoryRepository) CreateIfAbsent(_ context.Context, idempotencyKey string, order domain.Order) (domain.Order, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existingOrderID, ok := r.byIdempotency[idempotencyKey]; ok {
		existing, exists := r.orders[existingOrderID]
		if !exists {
			return domain.Order{}, false, fmt.Errorf("idempotency key %s points to missing order", idempotencyKey)
		}
		return existing, false, nil
	}
	if _, exists := r.orders[order.OrderID]; exists {
		return domain.Order{}, false, fmt.Errorf("order %s already exists", order.OrderID)
	}
	r.orders[order.OrderID] = order
	r.byIdempotency[idempotencyKey] = order.OrderID
	return order, true, nil
}

func (r *MemoryRepository) Get(_ context.Context, orderID string) (domain.Order, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	order, ok := r.orders[orderID]
	return order, ok, nil
}

func (r *MemoryRepository) Save(_ context.Context, order domain.Order) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.orders[order.OrderID]; !exists {
		return fmt.Errorf("order %s not found", order.OrderID)
	}
	r.orders[order.OrderID] = order
	if order.IdempotencyKey != "" {
		r.byIdempotency[order.IdempotencyKey] = order.OrderID
	}
	return nil
}

func (r *MemoryRepository) TryMarkProcessedEvent(_ context.Context, key string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.processedEvents[key]; exists {
		return false, nil
	}
	r.processedEvents[key] = struct{}{}
	return true, nil
}
