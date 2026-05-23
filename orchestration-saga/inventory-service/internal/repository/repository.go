package repository

import (
	"context"
	"time"

	"saga-pattern/common/dto"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
)

type Repository interface {
	GetReservation(context.Context, string) (domain.Reservation, bool, error)
	ReserveInventory(context.Context, string, string, []dto.OrderItemRequest, time.Time) (domain.Reservation, error)
	CommitReservation(context.Context, string, time.Time) (domain.Reservation, bool, error)
	SaveFailedReservation(context.Context, string, string, []dto.OrderItemRequest, string, time.Time) (domain.Reservation, error)
	ReleaseReservation(context.Context, string, time.Time, string) (domain.Reservation, error)
	UpdateTotalStock(context.Context, string, int, time.Time) (domain.Product, error)
	UpdateVisibility(context.Context, string, bool) (domain.Product, error)
	Product(context.Context, string) (domain.Product, bool, error)
	PingContext(context.Context) error
	ListProducts(context.Context) ([]domain.Product, error)
	ListReservations(context.Context) ([]domain.Reservation, error)
}
