package loops

import (
	"context"
	"fmt"
	"time"

	"saga-pattern/orchestration-framework/internal/kafka"
	"saga-pattern/orchestration-framework/internal/store"
)

type OutboxLoop struct {
	LeaseName   string
	LeaseTTL    time.Duration
	RetryDelay  time.Duration
	BatchSize   int
	SendTimeout time.Duration
	Store       store.Store
	Publisher   kafka.Publisher
	WorkerID    string
}

func (l *OutboxLoop) RunOnce(ctx context.Context, now time.Time) error {
	ok, err := l.Store.AcquireLease(ctx, l.LeaseName, l.WorkerID, now, l.LeaseTTL)
	if err != nil || !ok {
		return err
	}
	rows, err := l.Store.ClaimOutbox(ctx, l.WorkerID, now, l.BatchSize, l.RetryDelay)
	if err != nil {
		return err
	}
	for _, row := range rows {
		publishCtx, cancel := context.WithTimeout(ctx, l.SendTimeout)
		err := publishWithTimeout(publishCtx, l.Publisher, kafka.Message{
			Topic:       row.Topic,
			Key:         row.Key,
			MessageType: row.MessageType,
			Payload:     row.Payload,
			SagaID:      row.SagaID,
			Step:        row.Step,
			Direction:   row.Direction,
		})
		cancel()
		if err != nil {
			terminal := row.AttemptCount >= row.MaxAttempts
			if markErr := l.Store.MarkOutboxFailed(ctx, row.ID, now.Add(l.RetryDelay), err.Error(), terminal); markErr != nil {
				return markErr
			}
			continue
		}
		if err := l.Store.MarkOutboxSent(ctx, row.ID, now); err != nil {
			return err
		}
	}
	return nil
}

func publishWithTimeout(ctx context.Context, publisher kafka.Publisher, message kafka.Message) error {
	result := make(chan error, 1)
	go func() {
		result <- publisher.Publish(ctx, message)
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("publish %s to %s timed out: %w", message.MessageType, message.Topic, ctx.Err())
	}
}
