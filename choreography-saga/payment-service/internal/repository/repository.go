package repository

import (
	"context"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Payment) (domain.Payment, error)
	Save(context.Context, domain.Payment) error
	GetByOrderID(context.Context, string) (domain.Payment, bool, error)
	TryMarkProcessedEvent(context.Context, string) (bool, error)
	DeleteProcessedEvent(context.Context, string) error
	ListPayments(context.Context) ([]domain.Payment, error)
}
