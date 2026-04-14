package domain

import (
	"fmt"
	"strings"
	"time"
)

type ShipmentStatus string

const (
	ShipmentStatusPending   ShipmentStatus = "PENDING"
	ShipmentStatusScheduled ShipmentStatus = "SCHEDULED"
	ShipmentStatusCancelled ShipmentStatus = "CANCELLED"
)

type Shipment struct {
	ShippingID        string
	OrderID           string
	TrackingNumber    string
	ShippingAddress   string
	Status            ShipmentStatus
	EstimatedDelivery time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func NewPendingShipment(shippingID, orderID, trackingNumber, shippingAddress string, estimatedDelivery, createdAt time.Time) (Shipment, error) {
	if strings.TrimSpace(shippingID) == "" {
		return Shipment{}, fmt.Errorf("shipping id is required")
	}
	if strings.TrimSpace(orderID) == "" {
		return Shipment{}, fmt.Errorf("order id is required")
	}
	if strings.TrimSpace(trackingNumber) == "" {
		return Shipment{}, fmt.Errorf("tracking number is required")
	}
	if strings.TrimSpace(shippingAddress) == "" {
		return Shipment{}, fmt.Errorf("shipping address is required")
	}
	if estimatedDelivery.IsZero() {
		return Shipment{}, fmt.Errorf("estimated delivery is required")
	}
	if createdAt.IsZero() {
		return Shipment{}, fmt.Errorf("created at is required")
	}

	createdAt = createdAt.UTC()
	return Shipment{
		ShippingID:        shippingID,
		OrderID:           orderID,
		TrackingNumber:    trackingNumber,
		ShippingAddress:   shippingAddress,
		Status:            ShipmentStatusPending,
		EstimatedDelivery: estimatedDelivery.UTC(),
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
	}, nil
}

func (s *Shipment) MarkScheduled(at time.Time) error {
	if s == nil {
		return fmt.Errorf("shipment is required")
	}
	if at.IsZero() {
		return fmt.Errorf("scheduled time is required")
	}
	s.Status = ShipmentStatusScheduled
	s.UpdatedAt = at.UTC()
	return nil
}

func (s *Shipment) Cancel(at time.Time) error {
	if s == nil {
		return fmt.Errorf("shipment is required")
	}
	if at.IsZero() {
		return fmt.Errorf("cancelled time is required")
	}
	if !s.IsCancellable() {
		return nil
	}
	s.Status = ShipmentStatusCancelled
	s.UpdatedAt = at.UTC()
	return nil
}

func (s Shipment) IsCancellable() bool {
	return s.Status == ShipmentStatusPending || s.Status == ShipmentStatusScheduled
}
