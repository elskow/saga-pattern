package repository

import (
	"context"
	"time"

	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/domain"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
)

type Repository interface {
	AcknowledgeAcceptedLifecycle(context.Context) error
	GetFinalized(context.Context, string) (domain.Order, bool, error)
	UpsertFinalizedFromRuntimeView(context.Context, sagaRuntime.View[ordersaga.Data], time.Time) (domain.Order, error)
	ListFinalized(context.Context) ([]domain.Order, error)
	ListFinalizedByCustomer(context.Context, string) ([]domain.Order, error)
}
