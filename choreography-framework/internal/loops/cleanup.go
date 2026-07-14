package loops

import (
	"context"
	"time"

	"saga-pattern/choreography-framework/internal/store"
)

type CleanupLoop struct {
	Store                   store.Store
	ProcessedEventRetention time.Duration
	OutboxRetention         time.Duration
}

func (l *CleanupLoop) RunOnce(ctx context.Context, now time.Time) error {
	if _, err := l.Store.CleanupProcessedEvents(ctx, now.Add(-l.ProcessedEventRetention)); err != nil {
		return err
	}
	if _, err := l.Store.CleanupOutbox(ctx, now.Add(-l.OutboxRetention)); err != nil {
		return err
	}
	return nil
}
