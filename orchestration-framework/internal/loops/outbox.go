package loops

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	commontracing "saga-pattern/common/tracing"
	"saga-pattern/orchestration-framework/internal/kafka"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

type OutboxLoop struct {
	LeaseName       string
	LeaseTTL        time.Duration
	RetryDelay      time.Duration
	BatchSize       int
	SendTimeout     time.Duration
	Store           store.Store
	Publisher       kafka.Publisher
	WorkerID        string
	OnPublishFailed func(retryDelay time.Duration)
}

func (l *OutboxLoop) RunOnce(ctx context.Context, now time.Time) (err error) {
	ctx, span := commontracing.Tracer("orchestration-framework/outbox").Start(ctx, "orchestration.outbox.run_once",
		trace.WithAttributes(
			attribute.String("lease.name", l.LeaseName),
			attribute.String("worker.id", l.WorkerID),
			attribute.Int("batch.size", l.BatchSize),
			attribute.String("retry.delay", l.RetryDelay.String()),
		),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	ok, err := l.Store.AcquireLease(ctx, l.LeaseName, l.WorkerID, now, l.LeaseTTL)
	span.SetAttributes(attribute.Bool("lease.acquired", ok))
	if err != nil || !ok {
		if err == nil {
			span.AddEvent("lease_not_acquired")
		}
		return err
	}
	rows, err := l.Store.ClaimOutbox(ctx, l.WorkerID, now, l.BatchSize, l.RetryDelay)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int("outbox.claimed_count", len(rows)))
	for _, row := range rows {
		linkRunOnceToRowTrace(now, row.ID, row.TraceHeaders)
	}
	for _, row := range rows {
		publishAttrs := []attribute.KeyValue{
			attribute.String("outbox.id", row.ID),
			attribute.String("saga.id", row.SagaID),
			attribute.String("saga.type", row.SagaType),
			attribute.String("saga.step", row.Step),
			attribute.String("saga.pending.direction", string(row.Direction)),
			attribute.String("topic", row.Topic),
			attribute.String("message.type", row.MessageType),
			attribute.Int("messaging.message.payload_size_bytes", len(row.Payload)),
			attribute.Int("attempt.count", row.AttemptCount),
			attribute.Int("max.attempts", row.MaxAttempts),
			attribute.String("outbox.publish.status", "attempting"),
		}
		if row.RequestID != "" {
			publishAttrs = append(publishAttrs, attribute.String("request.id", row.RequestID))
		}
		if row.CorrelationID != "" {
			publishAttrs = append(publishAttrs, attribute.String("correlation.id", row.CorrelationID))
		}
		if row.BenchmarkRun != "" {
			publishAttrs = append(publishAttrs, attribute.String("benchmark.run_label", row.BenchmarkRun))
		}
		if row.BenchmarkScene != "" {
			publishAttrs = append(publishAttrs, attribute.String("benchmark.scenario", row.BenchmarkScene))
		}
		if row.BenchmarkPhase != "" {
			publishAttrs = append(publishAttrs, attribute.String("benchmark.phase", row.BenchmarkPhase))
		}
		rowCtx := contextWithOutboxTraceHeaders(ctx, row)
		links := traceLinksFromContext(ctx)
		rowCtx, rowSpan := commontracing.Tracer("orchestration-framework/outbox").Start(rowCtx, "orchestration.outbox.publish",
			trace.WithAttributes(publishAttrs...),
			trace.WithLinks(links...),
		)
		publishCtx := contextWithOutboxRequestData(rowCtx, row)
		publishCtx, cancel := context.WithTimeout(publishCtx, l.SendTimeout)
		err := l.Publisher.Publish(publishCtx, kafka.Message{
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
			rowSpan.SetAttributes(
				attribute.String("outbox.publish.event", "outbox_publish_failed"),
				attribute.String("outbox.publish.status", "failed"),
			)
			rowSpan.RecordError(err)
			rowSpan.SetStatus(codes.Error, err.Error())
			rowSpan.End()
			terminal := row.AttemptCount >= row.MaxAttempts
			span.AddEvent("outbox_publish_failed",
				trace.WithAttributes(outboxEventAttributes(row,
					attribute.Bool("terminal", terminal),
					attribute.String("error", err.Error()),
				)...),
			)
			if markErr := l.Store.MarkOutboxFailed(ctx, row.ID, now.Add(l.RetryDelay), err.Error(), terminal); markErr != nil {
				l.notifyPublishFailed()
				return markErr
			}
			l.notifyPublishFailed()
			continue
		}
		if err := l.Store.MarkOutboxSent(ctx, row.ID, now); err != nil {
			rowSpan.RecordError(err)
			rowSpan.SetStatus(codes.Error, err.Error())
			rowSpan.End()
			return err
		}
		rowSpan.SetAttributes(
			attribute.String("outbox.publish.event", "outbox_published"),
			attribute.String("outbox.publish.status", "published"),
		)
		rowSpan.AddEvent("outbox_published",
			trace.WithAttributes(outboxEventAttributes(row)...),
		)
		rowSpan.End()
	}
	return nil
}

func (l *OutboxLoop) notifyPublishFailed() {
	if l.OnPublishFailed != nil {
		l.OnPublishFailed(l.RetryDelay)
	}
}

func linkRunOnceToRowTrace(relayStart time.Time, outboxID string, traceHeaders map[string]string) {
	rowCtx := commontracing.ExtractContext(context.Background(), traceHeaders)
	if sc := trace.SpanContextFromContext(rowCtx); !sc.IsValid() {
		return
	}
	_, child := commontracing.Tracer("orchestration-framework/outbox").Start(rowCtx, "orchestration.outbox.relayed",
		trace.WithTimestamp(relayStart),
		trace.WithAttributes(attribute.String("outbox.id", outboxID)),
	)
	child.End()
}

func outboxEventAttributes(row model.OutboxRow, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("outbox.id", row.ID),
		attribute.String("saga.id", row.SagaID),
		attribute.String("saga.type", row.SagaType),
		attribute.String("saga.step", row.Step),
		attribute.String("saga.pending.direction", string(row.Direction)),
		attribute.String("topic", row.Topic),
		attribute.String("message.type", row.MessageType),
		attribute.Int("messaging.message.payload_size_bytes", len(row.Payload)),
		attribute.Int("attempt.count", row.AttemptCount),
		attribute.Int("max.attempts", row.MaxAttempts),
	}
	if row.LastError != "" {
		attrs = append(attrs, attribute.String("last.error", row.LastError))
	}
	if row.RequestID != "" {
		attrs = append(attrs, attribute.String("request.id", row.RequestID))
	}
	if row.CorrelationID != "" {
		attrs = append(attrs, attribute.String("correlation.id", row.CorrelationID))
	}
	if row.BenchmarkRun != "" {
		attrs = append(attrs, attribute.String("benchmark.run_label", row.BenchmarkRun))
	}
	if row.BenchmarkScene != "" {
		attrs = append(attrs, attribute.String("benchmark.scenario", row.BenchmarkScene))
	}
	if row.BenchmarkPhase != "" {
		attrs = append(attrs, attribute.String("benchmark.phase", row.BenchmarkPhase))
	}
	return append(attrs, extra...)
}

func contextWithOutboxTraceHeaders(ctx context.Context, row model.OutboxRow) context.Context {
	if len(row.TraceHeaders) == 0 {
		return ctx
	}
	return commontracing.ExtractContext(ctx, row.TraceHeaders)
}

func traceLinksFromContext(ctx context.Context) []trace.Link {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return nil
	}
	return []trace.Link{{SpanContext: spanContext}}
}

func contextWithOutboxRequestData(ctx context.Context, row model.OutboxRow) context.Context {
	if row.RequestID == "" && row.CorrelationID == "" && row.BenchmarkRun == "" && row.BenchmarkScene == "" && row.BenchmarkPhase == "" {
		return ctx
	}
	if row.RequestID != "" && row.CorrelationID == "" {
		return ctx
	}
	return commoncontext.With(ctx, commoncontext.Data{
		RequestID:      row.RequestID,
		CorrelationID:  row.CorrelationID,
		BenchmarkRun:   row.BenchmarkRun,
		BenchmarkScene: row.BenchmarkScene,
		BenchmarkPhase: row.BenchmarkPhase,
	})
}
