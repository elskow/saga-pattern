package repository

import (
	"context"
	"database/sql"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
)

// TxHook is invoked inside repo transactions to let callers write additional
// rows atomically. Used by the service to enqueue outbox events.
type TxHook func(context.Context, *sql.Tx) error

type Repository interface {
	SavePendingShippingAddress(context.Context, string, string) error
	LoadPendingShippingAddress(context.Context, string) (string, bool, error)
	ClearPendingShippingAddress(context.Context, string) error
	SaveShipment(context.Context, domain.Shipment, TxHook) error
	GetShipmentByOrderID(context.Context, string) (domain.Shipment, bool, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ListShipments(context.Context) ([]domain.Shipment, error)
	UpdateStatus(context.Context, string, domain.ShipmentStatus) error
}
