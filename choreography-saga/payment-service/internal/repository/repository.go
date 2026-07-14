package repository

import (
	"context"
	"database/sql"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
)

// TxHook is invoked inside repo transactions to let callers write additional
// rows atomically. Used by the service to enqueue outbox events.
type TxHook func(context.Context, *sql.Tx) error

type Repository interface {
	Create(context.Context, domain.Payment) (domain.Payment, error)
	Save(ctx context.Context, payment domain.Payment, hook TxHook) error
	GetByOrderID(context.Context, string) (domain.Payment, bool, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ListPayments(context.Context) ([]domain.Payment, error)
}
