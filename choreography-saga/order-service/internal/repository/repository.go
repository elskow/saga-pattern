package repository

import (
	"context"

	"saga-pattern/choreography-saga/order-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Order) (domain.Order, error)
	CreateIfAbsent(context.Context, string, domain.Order) (domain.Order, bool, error)
	Get(context.Context, string) (domain.Order, bool, error)
	List(context.Context) ([]domain.Order, error)
	Save(context.Context, domain.Order) error
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ClaimPendingOrderEvents(context.Context, int) ([]OrderOutboxMessage, error)
	MarkOrderEventPublished(context.Context, int64) error
	MarkOrderEventPublishFailed(context.Context, int64, string) error
}

type OrderOutboxMessage struct {
	ID          int64
	OrderID     string
	EventType   string
	PayloadJSON string
	Attempts    int
}
