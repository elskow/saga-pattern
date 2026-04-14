package domain

import (
	"fmt"
	"strings"
	"time"
)

type ShipmentStatus string

const (
	ShipmentStatusScheduled ShipmentStatus = "SCHEDULED"
	ShipmentStatusFailed    ShipmentStatus = "FAILED"
	ShipmentStatusCancelled ShipmentStatus = "CANCELLED"
)

type Shipment struct {
	ShippingID         string
	OrderID            string
	ShippingAddress    string
	Status             ShipmentStatus
	FailureReason      string
	CancellationReason string
	CreatedAt          time.Time
	ScheduledAt        time.Time
	CancelledAt        time.Time
	UpdatedAt          time.Time
}

func NewScheduledShipment(shippingID, orderID, shippingAddress string, at time.Time) (Shipment, error) {
	if err := validateShipmentIdentity(shippingID, orderID, shippingAddress, at); err != nil {
		return Shipment{}, err
	}
	at = at.UTC()
	return Shipment{
		ShippingID:      shippingID,
		OrderID:         orderID,
		ShippingAddress: shippingAddress,
		Status:          ShipmentStatusScheduled,
		CreatedAt:       at,
		ScheduledAt:     at,
		UpdatedAt:       at,
	}, nil
}

func NewFailedShipment(shippingID, orderID, shippingAddress, reason string, at time.Time) (Shipment, error) {
	if err := validateShipmentIdentity(shippingID, orderID, shippingAddress, at); err != nil {
		return Shipment{}, err
	}
	if strings.TrimSpace(reason) == "" {
		return Shipment{}, fmt.Errorf("failure reason is required")
	}
	at = at.UTC()
	return Shipment{
		ShippingID:      shippingID,
		OrderID:         orderID,
		ShippingAddress: shippingAddress,
		Status:          ShipmentStatusFailed,
		FailureReason:   reason,
		CreatedAt:       at,
		UpdatedAt:       at,
	}, nil
}

func (s *Shipment) Cancel(reason string, at time.Time) error {
	if s == nil {
		return fmt.Errorf("shipment is required")
	}
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("cancellation reason is required")
	}
	if at.IsZero() {
		return fmt.Errorf("cancelled time is required")
	}
	if s.Status == ShipmentStatusCancelled {
		return nil
	}
	at = at.UTC()
	s.Status = ShipmentStatusCancelled
	s.CancellationReason = reason
	s.CancelledAt = at
	s.UpdatedAt = at
	return nil
}

func validateShipmentIdentity(shippingID, orderID, shippingAddress string, at time.Time) error {
	if strings.TrimSpace(shippingID) == "" {
		return fmt.Errorf("shipping id is required")
	}
	if strings.TrimSpace(orderID) == "" {
		return fmt.Errorf("order id is required")
	}
	if strings.TrimSpace(shippingAddress) == "" {
		return fmt.Errorf("shipping address is required")
	}
	if at.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}
