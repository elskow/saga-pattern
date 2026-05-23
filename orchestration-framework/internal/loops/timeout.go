package loops

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/orchestration-framework/internal/store"
	commontracing "saga-pattern/common/tracing"
)

type TimeoutHandler interface {
	RecoverExpiredSaga(context.Context, string, time.Time) error
}

type TimeoutLoop struct {
	Store   store.Store
	Handler TimeoutHandler
	Limit   int
}

func (l *TimeoutLoop) RunOnce(ctx context.Context, now time.Time) (err error) {
	ctx, span := commontracing.Tracer("orchestration-framework/timeout").Start(ctx, "orchestration.timeout.run_once",
		trace.WithAttributes(
			attribute.Int("limit", l.Limit),
			attribute.String("checked_at", now.UTC().Format(time.RFC3339Nano)),
		),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	ids, err := l.Store.ListExpiredSagas(ctx, now, l.Limit)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int("expired.count", len(ids)))
	if len(ids) == 0 {
		span.AddEvent("no_expired_sagas")
	}
	for _, sagaID := range ids {
		if err := l.Handler.RecoverExpiredSaga(ctx, sagaID, now); err != nil {
			return err
		}
	}
	return nil
}
