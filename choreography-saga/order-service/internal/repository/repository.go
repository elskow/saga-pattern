package repository

import (
	"context"
	"database/sql"
	"time"

	"saga-pattern/choreography-saga/order-service/internal/domain"
)

type TxHook func(context.Context, *sql.Tx) error

type Repository interface {
	Create(ctx context.Context, order domain.Order, hook TxHook) (domain.Order, error)
	CreateIfAbsent(ctx context.Context, idempotencyKey string, order domain.Order, hook TxHook) (domain.Order, bool, error)
	Get(context.Context, string) (domain.Order, bool, error)
	List(context.Context) ([]domain.Order, error)
	Save(context.Context, domain.Order) error
	SaveWithHook(ctx context.Context, order domain.Order, hook TxHook) error
	CancelOrder(context.Context, string) error
	FindStuckOrders(ctx context.Context, olderThan time.Time) ([]domain.Order, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
}
