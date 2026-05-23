package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"saga-pattern/common/dto"
)

type ReservationStatus string

const (
	ReservationStatusReserved  ReservationStatus = "RESERVED"
	ReservationStatusCommitted ReservationStatus = "COMMITTED"
	ReservationStatusReleased  ReservationStatus = "RELEASED"
)

type Product struct {
	ProductID         string
	ProductName       string
	Description       string
	Price             json.Number
	Image             string
	Category          string
	Visible           bool
	QuantityAvailable int
	QuantityReserved  int
	LastRestockedAt   time.Time
	LastReservationAt time.Time
	LastReleaseAt     time.Time
}

func (p *Product) SetTotalStock(total int, at time.Time) error {
	if total < 0 {
		return fmt.Errorf("total stock cannot be negative")
	}
	if total < p.QuantityReserved {
		return fmt.Errorf("total stock cannot be less than reserved quantity")
	}
	p.QuantityAvailable = total - p.QuantityReserved
	p.LastRestockedAt = at.UTC()
	return nil
}

func (p *Product) Reserve(quantity int, at time.Time) error {
	if quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if p.QuantityAvailable < quantity {
		return InsufficientStockError{ProductID: p.ProductID, Requested: quantity, Available: p.QuantityAvailable}
	}
	p.QuantityAvailable -= quantity
	p.QuantityReserved += quantity
	p.LastReservationAt = at.UTC()
	return nil
}

func (p *Product) Release(quantity int, at time.Time) error {
	if quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if p.QuantityReserved < quantity {
		return fmt.Errorf("reserved quantity for product %s would become negative", p.ProductID)
	}
	p.QuantityReserved -= quantity
	p.QuantityAvailable += quantity
	p.LastReleaseAt = at.UTC()
	return nil
}

type Reservation struct {
	ReservationID string
	OrderID       string
	ProductID     string
	Quantity      int
	Status        ReservationStatus
	CreatedAt     time.Time
	ReleasedAt    time.Time
}

func (r *Reservation) Release(at time.Time) bool {
	if r.Status == ReservationStatusReleased || r.Status == ReservationStatusCommitted {
		return false
	}
	r.Status = ReservationStatusReleased
	r.ReleasedAt = at.UTC()
	return true
}

func (r *Reservation) Commit() bool {
	if r.Status != ReservationStatusReserved {
		return false
	}
	r.Status = ReservationStatusCommitted
	return true
}

type PendingOrderItem struct {
	OrderID   string
	ProductID string
	Quantity  int
}

func PendingItemsFromOrder(orderID string, items []dto.OrderItemRequest) []PendingOrderItem {
	pending := make([]PendingOrderItem, 0, len(items))
	for _, item := range items {
		pending = append(pending, PendingOrderItem{OrderID: orderID, ProductID: item.ProductID, Quantity: item.Quantity})
	}
	return pending
}

type ProductNotFoundError struct {
	ProductID string
}

func (e ProductNotFoundError) Error() string {
	return fmt.Sprintf("product %s not found", e.ProductID)
}

type InsufficientStockError struct {
	ProductID string
	Requested int
	Available int
}

func (e InsufficientStockError) Error() string {
	return fmt.Sprintf("insufficient stock for product %s: requested %d, available %d", e.ProductID, e.Requested, e.Available)
}
