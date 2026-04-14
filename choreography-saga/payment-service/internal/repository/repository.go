package repository

import (
	"context"
	"fmt"
	"sync"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Payment) (domain.Payment, error)
	Save(context.Context, domain.Payment) error
	GetByOrderID(context.Context, string) (domain.Payment, bool, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
}

type MemoryRepository struct {
	mu              sync.RWMutex
	payments        map[string]domain.Payment
	byOrderID       map[string]string
	processedEvents map[string]struct{}
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		payments:        make(map[string]domain.Payment),
		byOrderID:       make(map[string]string),
		processedEvents: make(map[string]struct{}),
	}
}

func (r *MemoryRepository) Create(_ context.Context, payment domain.Payment) (domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.payments[payment.PaymentID]; exists {
		return domain.Payment{}, fmt.Errorf("payment %s already exists", payment.PaymentID)
	}
	if _, exists := r.byOrderID[payment.OrderID]; exists {
		return domain.Payment{}, fmt.Errorf("payment for order %s already exists", payment.OrderID)
	}
	r.payments[payment.PaymentID] = payment
	r.byOrderID[payment.OrderID] = payment.PaymentID
	return payment, nil
}

func (r *MemoryRepository) Save(_ context.Context, payment domain.Payment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.payments[payment.PaymentID]; !exists {
		return fmt.Errorf("payment %s not found", payment.PaymentID)
	}
	r.payments[payment.PaymentID] = payment
	r.byOrderID[payment.OrderID] = payment.PaymentID
	return nil
}

func (r *MemoryRepository) GetByOrderID(_ context.Context, orderID string) (domain.Payment, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	paymentID, ok := r.byOrderID[orderID]
	if !ok {
		return domain.Payment{}, false, nil
	}
	payment, exists := r.payments[paymentID]
	if !exists {
		return domain.Payment{}, false, fmt.Errorf("payment index for order %s points to missing payment", orderID)
	}
	return payment, true, nil
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
