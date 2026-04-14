package repository

import (
	"context"
	"sync"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
)

type Repository interface {
	StorePendingAddress(context.Context, string, string) error
	PendingAddress(context.Context, string) (string, bool, error)
	DeletePendingAddress(context.Context, string) error
	SaveShipment(context.Context, domain.Shipment) error
	GetShipmentByOrderID(context.Context, string) (domain.Shipment, bool, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
}

type MemoryRepository struct {
	mu              sync.RWMutex
	pending         map[string]string
	shipments       map[string]domain.Shipment
	byOrderID       map[string]string
	processedEvents map[string]struct{}
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		pending:         make(map[string]string),
		shipments:       make(map[string]domain.Shipment),
		byOrderID:       make(map[string]string),
		processedEvents: make(map[string]struct{}),
	}
}

func (r *MemoryRepository) StorePendingAddress(_ context.Context, orderID string, shippingAddress string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[orderID] = shippingAddress
	return nil
}

func (r *MemoryRepository) PendingAddress(_ context.Context, orderID string) (string, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	address, ok := r.pending[orderID]
	return address, ok, nil
}

func (r *MemoryRepository) DeletePendingAddress(_ context.Context, orderID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, orderID)
	return nil
}

func (r *MemoryRepository) SaveShipment(_ context.Context, shipment domain.Shipment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shipments[shipment.ShippingID] = shipment
	r.byOrderID[shipment.OrderID] = shipment.ShippingID
	return nil
}

func (r *MemoryRepository) GetShipmentByOrderID(_ context.Context, orderID string) (domain.Shipment, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	shippingID, ok := r.byOrderID[orderID]
	if !ok {
		return domain.Shipment{}, false, nil
	}
	shipment, exists := r.shipments[shippingID]
	return shipment, exists, nil
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
