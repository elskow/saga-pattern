package repository

import (
	"context"
	"database/sql"

	"saga-pattern/choreography-saga/order-service/internal/domain"
)

// TxHook is invoked inside repo transactions to let callers write additional
// rows atomically. Used by the service layer to enqueue outbox events via the
// choreography-framework in the same transaction as the domain insert.
type TxHook func(context.Context, *sql.Tx) error

type Repository interface {
	Create(ctx context.Context, order domain.Order, hook TxHook) (domain.Order, error)
	CreateIfAbsent(ctx context.Context, idempotencyKey string, order domain.Order, hook TxHook) (domain.Order, bool, error)
	Get(context.Context, string) (domain.Order, bool, error)
	List(context.Context) ([]domain.Order, error)
	Save(context.Context, domain.Order) error
	CancelOrder(context.Context, string) error
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
}
