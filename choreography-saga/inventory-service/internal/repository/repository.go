package repository

import (
	"context"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
)

type Repository interface {
	SavePendingReservationItems(context.Context, string, []domain.PendingOrderItem) error
	LoadPendingReservationItems(context.Context, string) ([]domain.PendingOrderItem, error)
	ClearPendingReservationItems(context.Context, string) error
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ReserveInventory(context.Context, string, string, []domain.PendingOrderItem, time.Time) ([]domain.Reservation, error)
	CommitInventory(context.Context, string) (string, bool, error)
	ReleaseInventory(context.Context, string, time.Time) (string, bool, error)
	UpdateTotalStock(context.Context, string, int, time.Time) (domain.Product, error)
	UpdateVisibility(context.Context, string, bool) (domain.Product, error)
	Product(context.Context, string) (domain.Product, bool, error)
	ListProducts(context.Context) ([]domain.Product, error)
	ListReservations(context.Context) ([]domain.Reservation, error)
}
