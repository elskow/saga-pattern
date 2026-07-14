package store

import (
	"context"
	"time"

	"saga-pattern/choreography-framework/internal/model"
)

type Store interface {
	WithinTx(context.Context, func(Tx) error) error
	AcquireLease(ctx context.Context, name string, owner string, now time.Time, ttl time.Duration) (bool, error)
	ClaimOutbox(ctx context.Context, owner string, now time.Time, batchSize int, retryDelay time.Duration) ([]model.OutboxRow, error)
	MarkOutboxSent(ctx context.Context, outboxID string, sentAt time.Time) error
	MarkOutboxFailed(ctx context.Context, outboxID string, nextAttemptAt time.Time, errMsg string, terminal bool) error
	CleanupProcessedEvents(ctx context.Context, before time.Time) (int, error)
	CleanupOutbox(ctx context.Context, before time.Time) (int, error)
	ListOutbox(ctx context.Context) ([]model.OutboxRow, error)
}

type Tx interface {
	InsertOutbox(context.Context, model.OutboxRow) error
	TryMarkProcessedEvent(ctx context.Context, key string) (bool, error)
	DeleteProcessedEvent(ctx context.Context, key string) error
}
