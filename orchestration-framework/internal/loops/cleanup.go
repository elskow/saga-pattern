package loops

import (
	"context"
	"time"

	"saga-pattern/orchestration-framework/internal/store"
)

type CleanupLoop struct {
	Store                   store.Store
	ProcessedReplyRetention time.Duration
	OutboxRetention         time.Duration
}

func (l *CleanupLoop) RunOnce(ctx context.Context, now time.Time) error {
	if _, err := l.Store.CleanupProcessedReplies(ctx, now.Add(-l.ProcessedReplyRetention)); err != nil {
		return err
	}
	if _, err := l.Store.CleanupOutbox(ctx, now.Add(-l.OutboxRetention)); err != nil {
		return err
	}
	return nil
}
