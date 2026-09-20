package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
)

type TxHook func(ctx context.Context, tx *sql.Tx, id string, hookErr error) error

type Repository interface {
	SavePendingReservationItems(context.Context, string, []domain.PendingOrderItem) error
	LoadPendingReservationItems(context.Context, string) ([]domain.PendingOrderItem, error)
	ClearPendingReservationItems(context.Context, string) error
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ReserveInventory(ctx context.Context, orderID, reservationID string, items []domain.PendingOrderItem, at time.Time, onReserve TxHook, onFail TxHook) ([]domain.Reservation, error)
	SaveFailedReservation(ctx context.Context, reservationID, orderID string, items []domain.PendingOrderItem, reason string, at time.Time) error
	CommitInventory(context.Context, string) (string, bool, error)
	ReleaseInventory(ctx context.Context, orderID string, at time.Time, hook TxHook) (string, bool, error)
	UpdateTotalStock(context.Context, string, int, time.Time) (domain.Product, error)
	UpdateVisibility(context.Context, string, bool) (domain.Product, error)
	CreateProduct(ctx context.Context, p domain.Product) (domain.Product, error)
	UpdateProductMeta(ctx context.Context, productId, name, description, category, image string, price json.Number) (domain.Product, error)
	DeleteProduct(ctx context.Context, productId string) error
	Product(context.Context, string) (domain.Product, bool, error)
	ListProducts(context.Context) ([]domain.Product, error)
	ListReservations(context.Context) ([]domain.Reservation, error)
}
