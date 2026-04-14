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
	ReservationStatusReleased ReservationStatus = "RELEASED"
)

type Product struct {
	ProductID         string
	ProductName       string
	QuantityAvailable int
	QuantityReserved  int
	LastReservationAt time.Time
	LastReleaseAt     time.Time
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
	if r.Status == ReservationStatusReleased {
		return false
	}
	r.Status = ReservationStatusReleased
	r.ReleasedAt = at.UTC()
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

func DefaultProducts() []Product {
	return []Product{
		{ProductID: "PROD-001", ProductName: "Laptop", QuantityAvailable: 100},
		{ProductID: "PROD-002", ProductName: "Smartphone", QuantityAvailable: 200},
		{ProductID: "PROD-003", ProductName: "Headphones", QuantityAvailable: 500},
		{ProductID: "PROD-004", ProductName: "Tablet", QuantityAvailable: 150},
		{ProductID: "PROD-005", ProductName: "Monitor", QuantityAvailable: 120},
		{ProductID: "PROD-006", ProductName: "Webcam", QuantityAvailable: 300},
		{ProductID: "PROD-007", ProductName: "USB Hub", QuantityAvailable: 400},
		{ProductID: "PROD-008", ProductName: "Mousepad", QuantityAvailable: 500},
		{ProductID: lowStockProductOne, ProductName: "Rare Item", QuantityAvailable: 10},
		{ProductID: lowStockProductTwo, ProductName: "Limited Edition", QuantityAvailable: 15},
		{ProductID: "PROD-PREMIUM-001", ProductName: "Premium Item", QuantityAvailable: 100},
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
