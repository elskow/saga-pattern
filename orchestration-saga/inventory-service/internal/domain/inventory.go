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
	ReservationStatusFailed    ReservationStatus = "FAILED"
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
	Quantity          int
	ReservedQuantity  int
	LastRestockedAt   time.Time
	LastReservationAt time.Time
	LastReleaseAt     time.Time
}

func (p *Product) AvailableQuantity() int {
	return p.Quantity - p.ReservedQuantity
}

func (p *Product) Reserve(quantity int, at time.Time) error {
	if quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if p.AvailableQuantity() < quantity {
		return InsufficientStockError{ProductID: p.ProductID, Requested: quantity, Available: p.AvailableQuantity()}
	}
	p.ReservedQuantity += quantity
	p.LastReservationAt = at.UTC()
	return nil
}

func (p *Product) Release(quantity int, at time.Time) error {
	if quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if p.ReservedQuantity < quantity {
		return fmt.Errorf("reserved quantity for product %s would become negative", p.ProductID)
	}
	p.ReservedQuantity -= quantity
	p.LastReleaseAt = at.UTC()
	return nil
}

func (p *Product) SetTotalStock(total int, at time.Time) error {
	if total < 0 {
		return fmt.Errorf("total stock cannot be negative")
	}
	if total < p.ReservedQuantity {
		return fmt.Errorf("total stock cannot be less than reserved quantity")
	}
	p.Quantity = total
	p.LastRestockedAt = at.UTC()
	return nil
}

type Reservation struct {
	ReservationID string
	OrderID       string
	Items         []dto.OrderItemRequest
	Status        ReservationStatus
	FailureReason string
	ReleaseReason string
	CreatedAt     time.Time
	ReservedAt    time.Time
	ReleasedAt    time.Time
}

func (r *Reservation) Release(at time.Time, reason string) bool {
	if r.Status == ReservationStatusReleased || r.Status == ReservationStatusCommitted {
		return false
	}
	r.Status = ReservationStatusReleased
	r.ReleaseReason = reason
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

func (r Reservation) Clone() Reservation {
	cloned := make([]dto.OrderItemRequest, len(r.Items))
	copy(cloned, r.Items)
	r.Items = cloned

	return r
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
