package repository

import (
	"context"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
)

type Repository interface {
	SavePendingShippingAddress(context.Context, string, string) error
	LoadPendingShippingAddress(context.Context, string) (string, bool, error)
	ClearPendingShippingAddress(context.Context, string) error
	SaveShipment(context.Context, domain.Shipment) error
	GetShipmentByOrderID(context.Context, string) (domain.Shipment, bool, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ListShipments(context.Context) ([]domain.Shipment, error)
	UpdateStatus(context.Context, string, domain.ShipmentStatus) error
}
