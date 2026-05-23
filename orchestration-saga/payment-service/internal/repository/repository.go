package repository

import (
	"context"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Payment) (domain.Payment, error)
	Save(context.Context, domain.Payment) error
	GetByPaymentID(context.Context, string) (domain.Payment, bool, error)
	PingContext(context.Context) error
	ListPayments(context.Context) ([]domain.Payment, error)
}
