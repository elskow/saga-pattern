package domain

import (
	"fmt"
	"time"

	"saga-pattern/common/dto"
)

const (
	lowStockProductOne = "PROD-LOW-001"
	lowStockProductTwo = "PROD-LOW-002"
)

type ReservationStatus string

const (
	ReservationStatusReserved ReservationStatus = "RESERVED"
	ReservationStatusFailed   ReservationStatus = "FAILED"
	ReservationStatusReleased ReservationStatus = "RELEASED"
)

type Product struct {
	ProductID         string
	ProductName       string
	Quantity          int
	ReservedQuantity  int
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
	if r.Status == ReservationStatusReleased {
		return false
	}
	r.Status = ReservationStatusReleased
	r.ReleaseReason = reason
	r.ReleasedAt = at.UTC()
	return true
}

func (r Reservation) Clone() Reservation {
	r.Items = cloneItems(r.Items)
	return r
}

func cloneItems(items []dto.OrderItemRequest) []dto.OrderItemRequest {
	cloned := make([]dto.OrderItemRequest, len(items))
	copy(cloned, items)
	return cloned
}

func DefaultProducts() []Product {
	return []Product{
		{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 100},
		{ProductID: "PROD-002", ProductName: "Smartphone", Quantity: 200},
		{ProductID: "PROD-003", ProductName: "Headphones", Quantity: 500},
		{ProductID: "PROD-004", ProductName: "Tablet", Quantity: 150},
		{ProductID: "PROD-005", ProductName: "Monitor", Quantity: 120},
		{ProductID: "PROD-006", ProductName: "Webcam", Quantity: 300},
		{ProductID: "PROD-007", ProductName: "USB Hub", Quantity: 400},
		{ProductID: "PROD-008", ProductName: "Mousepad", Quantity: 500},
		{ProductID: lowStockProductOne, ProductName: "Rare Item", Quantity: 10},
		{ProductID: lowStockProductTwo, ProductName: "Limited Edition", Quantity: 15},
		{ProductID: "PROD-PREMIUM-001", ProductName: "Premium Item", Quantity: 100},
	}
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
