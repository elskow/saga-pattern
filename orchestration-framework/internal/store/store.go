package store

import (
	"context"
	"time"

	"saga-pattern/orchestration-framework/internal/model"
)

type Store interface {
	WithinTx(context.Context, func(Tx) error) error
	AcquireLease(ctx context.Context, name string, owner string, now time.Time, ttl time.Duration) (bool, error)
	ClaimOutbox(ctx context.Context, owner string, now time.Time, batchSize int, retryDelay time.Duration) ([]model.OutboxRow, error)
	MarkOutboxSent(ctx context.Context, outboxID string, sentAt time.Time) error
	MarkOutboxFailed(ctx context.Context, outboxID string, nextAttemptAt time.Time, errMsg string, terminal bool) error
	ListExpiredSagas(ctx context.Context, now time.Time, limit int) ([]string, error)
	CleanupProcessedReplies(ctx context.Context, before time.Time) (int, error)
	CleanupOutbox(ctx context.Context, before time.Time) (int, error)
	GetSaga(ctx context.Context, sagaID string) (model.SagaInstanceRow, bool, error)
	ListStepHistory(ctx context.Context, sagaID string) ([]model.StepHistoryRow, error)
	ListOutbox(ctx context.Context, sagaID string) ([]model.OutboxRow, error)
}

type Tx interface {
	InsertSaga(context.Context, model.SagaInstanceRow) error
	LockSaga(context.Context, string) (model.SagaInstanceRow, bool, error)
	UpdateSaga(context.Context, model.SagaInstanceRow) error
	InsertStepHistory(context.Context, model.StepHistoryRow) error
	UpdateStepHistory(context.Context, model.StepHistoryRow) error
	ListStepHistory(context.Context, string) ([]model.StepHistoryRow, error)
	InsertOutbox(context.Context, model.OutboxRow) error
	InsertProcessedReply(context.Context, model.ProcessedReplyRow) (bool, error)
}
