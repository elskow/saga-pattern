package repository

import (
	"context"
	"sync"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
)

type Repository interface {
	StorePendingItems(context.Context, string, []domain.PendingOrderItem) error
	PendingItems(context.Context, string) ([]domain.PendingOrderItem, error)
	DeletePendingItems(context.Context, string) error
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	ReserveInventory(context.Context, string, string, []domain.PendingOrderItem, time.Time) ([]domain.Reservation, error)
	ReleaseInventory(context.Context, string, time.Time) (string, bool, error)
	Product(context.Context, string) (domain.Product, bool, error)
}

type MemoryRepository struct {
	mu              sync.RWMutex
	products        map[string]domain.Product
	pending         map[string][]domain.PendingOrderItem
	reservations    map[string][]domain.Reservation
	processedEvents map[string]struct{}
}

func NewMemoryRepository() *MemoryRepository {
	products := make(map[string]domain.Product)
	for _, product := range domain.DefaultProducts() {
		products[product.ProductID] = product
	}
	return &MemoryRepository{
		products:        products,
		pending:         make(map[string][]domain.PendingOrderItem),
		reservations:    make(map[string][]domain.Reservation),
		processedEvents: make(map[string]struct{}),
	}
}

func (r *MemoryRepository) StorePendingItems(_ context.Context, orderID string, items []domain.PendingOrderItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cloned := make([]domain.PendingOrderItem, len(items))
	copy(cloned, items)
	r.pending[orderID] = cloned
	return nil
}

func (r *MemoryRepository) PendingItems(_ context.Context, orderID string) ([]domain.PendingOrderItem, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.pending[orderID]
	cloned := make([]domain.PendingOrderItem, len(items))
	copy(cloned, items)
	return cloned, nil
}

func (r *MemoryRepository) DeletePendingItems(_ context.Context, orderID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, orderID)
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

func (r *MemoryRepository) ReserveInventory(_ context.Context, orderID string, reservationID string, items []domain.PendingOrderItem, at time.Time) ([]domain.Reservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	products := make(map[string]domain.Product, len(items))
	for _, item := range items {
		product, ok := r.products[item.ProductID]
		if !ok {
			return nil, domain.ProductNotFoundError{ProductID: item.ProductID}
		}
		products[item.ProductID] = product
	}

	for _, item := range items {
		product := products[item.ProductID]
		if err := product.Reserve(item.Quantity, at); err != nil {
			return nil, err
		}
		products[item.ProductID] = product
	}

	reservations := make([]domain.Reservation, 0, len(items))
	for _, item := range items {
		product := products[item.ProductID]
		r.products[item.ProductID] = product
		reservations = append(reservations, domain.Reservation{
			ReservationID: reservationID,
			OrderID:       orderID,
			ProductID:     item.ProductID,
			Quantity:      item.Quantity,
			Status:        domain.ReservationStatusReserved,
			CreatedAt:     at.UTC(),
		})
	}
	r.reservations[orderID] = reservations
	return cloneReservations(reservations), nil
}

func (r *MemoryRepository) ReleaseInventory(_ context.Context, orderID string, at time.Time) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reservations := r.reservations[orderID]
	if len(reservations) == 0 {
		return "", false, nil
	}

	reservationID := reservations[0].ReservationID
	updated := false
	for idx := range reservations {
		reservation := reservations[idx]
		if reservation.Status != domain.ReservationStatusReserved {
			continue
		}
		product, ok := r.products[reservation.ProductID]
		if !ok {
			return "", false, domain.ProductNotFoundError{ProductID: reservation.ProductID}
		}
		if err := product.Release(reservation.Quantity, at); err != nil {
			return "", false, err
		}
		reservations[idx].Release(at)
		r.products[reservation.ProductID] = product
		updated = true
	}

	r.reservations[orderID] = reservations
	if !updated {
		return reservationID, false, nil
	}
	return reservationID, true, nil
}

func (r *MemoryRepository) Product(_ context.Context, productID string) (domain.Product, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	product, ok := r.products[productID]
	return product, ok, nil
}

func cloneReservations(items []domain.Reservation) []domain.Reservation {
	cloned := make([]domain.Reservation, len(items))
	copy(cloned, items)
	return cloned
}
