package loops

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-framework/internal/kafka"
	"saga-pattern/choreography-framework/internal/model"
	"saga-pattern/choreography-framework/internal/observability"
	"saga-pattern/choreography-framework/internal/store"
	commontracing "saga-pattern/common/tracing"
)

type OutboxLoop struct {
	ServiceName string
	LeaseName   string
	LeaseTTL    time.Duration
	RetryDelay  time.Duration
	BatchSize   int
	SendTimeout time.Duration
	Store       store.Store
	Publisher   kafka.Publisher
	WorkerID    string
	Metrics     *observability.Metrics
}

func (l *OutboxLoop) RunOnce(ctx context.Context, now time.Time) (err error) {
	ctx, span := commontracing.Tracer("choreography-framework/outbox").Start(ctx, "choreography.outbox.run_once",
		trace.WithAttributes(
			attribute.String("service.name", l.ServiceName),
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
		if err := l.publishRow(ctx, span, now, row); err != nil {
			return err
		}
	}
	return nil
}

func (l *OutboxLoop) publishRow(ctx context.Context, parent trace.Span, now time.Time, row model.OutboxRow) error {
	publishAttrs := []attribute.KeyValue{
		attribute.String("outbox.id", row.ID),
		attribute.String("service.name", l.ServiceName),
		attribute.String("topic", row.Topic),
		attribute.String("event.type", row.EventType),
		attribute.Int("messaging.message.payload_size_bytes", len(row.Payload)),
		attribute.Int("attempt.count", row.AttemptCount),
		attribute.Int("max.attempts", row.MaxAttempts),
		attribute.String("outbox.publish.status", "attempting"),
	}
	rowCtx := contextWithOutboxTraceHeaders(ctx, row)
	links := traceLinksFromContext(ctx)
	rowCtx, rowSpan := commontracing.Tracer("choreography-framework/outbox").Start(rowCtx, "choreography.outbox.publish",
		trace.WithAttributes(publishAttrs...),
		trace.WithLinks(links...),
	)
	publishCtx, cancel := context.WithTimeout(rowCtx, l.SendTimeout)
	start := now
	err := l.Publisher.Publish(publishCtx, kafka.Message{
		Topic:     row.Topic,
		Key:       row.Key,
		EventType: row.EventType,
		Payload:   row.Payload,
	})
	cancel()
	duration := time.Since(start)
	if err != nil {
		rowSpan.SetAttributes(
			attribute.String("outbox.publish.event", "outbox_publish_failed"),
			attribute.String("outbox.publish.status", "failed"),
		)
		rowSpan.RecordError(err)
		rowSpan.SetStatus(codes.Error, err.Error())
		rowSpan.End()
		terminal := row.AttemptCount >= row.MaxAttempts
		parent.AddEvent("outbox_publish_failed",
			trace.WithAttributes(outboxEventAttributes(row,
				attribute.Bool("terminal", terminal),
				attribute.String("error", err.Error()),
			)...),
		)
		if l.Metrics != nil {
			l.Metrics.RecordOutboxPublishFailed(l.ServiceName, row.Topic, row.EventType)
			l.Metrics.RecordOutboxSendDuration(l.ServiceName, row.Topic, row.EventType, "failed", duration)
		}
		if markErr := l.Store.MarkOutboxFailed(ctx, row.ID, now.Add(l.RetryDelay), err.Error(), terminal); markErr != nil {
			return markErr
		}
		return nil
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
	if l.Metrics != nil {
		l.Metrics.RecordEventPublished(l.ServiceName, row.Topic, row.EventType)
		l.Metrics.RecordOutboxAttempts(l.ServiceName, row.Topic, row.EventType, row.AttemptCount)
		l.Metrics.RecordOutboxSendDuration(l.ServiceName, row.Topic, row.EventType, "success", duration)
	}
	return nil
}

func outboxEventAttributes(row model.OutboxRow, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("outbox.id", row.ID),
		attribute.String("topic", row.Topic),
		attribute.String("event.type", row.EventType),
		attribute.Int("messaging.message.payload_size_bytes", len(row.Payload)),
		attribute.Int("attempt.count", row.AttemptCount),
		attribute.Int("max.attempts", row.MaxAttempts),
	}
	if row.LastError != "" {
		attrs = append(attrs, attribute.String("last.error", row.LastError))
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
