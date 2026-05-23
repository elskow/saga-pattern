package repository

import (
	"context"
	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Shipment) (domain.Shipment, error)
	Save(context.Context, domain.Shipment) error
	GetByShippingID(context.Context, string) (domain.Shipment, bool, error)
	PingContext(context.Context) error
	ListShipments(context.Context) ([]domain.Shipment, error)
	UpdateStatus(context.Context, string, domain.ShipmentStatus) error
}
