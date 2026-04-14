package loops

import (
	"context"
	"time"

	"saga-pattern/orchestration-framework/internal/store"
)

type TimeoutHandler interface {
	RecoverExpiredSaga(context.Context, string, time.Time) error
}

type TimeoutLoop struct {
	Store   store.Store
	Handler TimeoutHandler
	Limit   int
}

func (l *TimeoutLoop) RunOnce(ctx context.Context, now time.Time) error {
	ids, err := l.Store.ListExpiredSagas(ctx, now, l.Limit)
	if err != nil {
		return err
	}
	for _, sagaID := range ids {
		if err := l.Handler.RecoverExpiredSaga(ctx, sagaID, now); err != nil {
			return err
		}
	}
	return nil
}
