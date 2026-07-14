package orders

import (
    "bytes"
    "context"

    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/trace"

    commonreplies "saga-pattern/common/replies"
    commontracing "saga-pattern/common/tracing"
    sagaRuntime "saga-pattern/orchestration-framework/runtime"
    "saga-pattern/orchestration-saga/order-service/internal/domain"
)

// HandleReply consumes a reply envelope from the runtime, materializes the
// finalized order projection when the saga reaches a terminal state.
func (s *Service) HandleReply(ctx context.Context, envelope sagaRuntime.ReplyEnvelope) (err error) {
    ctx, span := commontracing.Tracer("orchestration/order-service").Start(ctx, "orchestration.order.handle_reply",
        trace.WithAttributes(
            attribute.String("saga.id", envelope.SagaID),
            attribute.String("reply.topic", envelope.Topic),
            attribute.Int("messaging.message.payload_size_bytes", len(envelope.Payload)),
        ))
    defer finishOrderSpan(span, &err)
    if err := s.runtime.ConsumeReply(ctx, envelope); err != nil {
        return err
    }
    reply, err := commonreplies.DecodeSagaReply(bytes.NewReader(envelope.Payload))
    if err != nil {
        return err
    }
    span.SetAttributes(attribute.String("reply.type", reply.ReplyType()))
    s.recordCompensationMetric(reply.ReplyType())
    runtimeView, ok, err := s.runtime.View(ctx, envelope.SagaID)
    if err != nil || !ok {
        return err
    }

    // The runtime remains the source of truth for in-flight orchestration state.
    // The orders table only becomes queryable once a terminal runtime view is
    // materialized into the finalized projection.
    if runtimeView.State != domain.StatusCompleted && runtimeView.State != domain.StatusCancelled {
        span.SetAttributes(attribute.String("saga.state", string(runtimeView.State)), attribute.String("order.result", "in_flight"))
        return nil
    }

    finalizedOrder, err := s.repo.UpsertFinalizedFromRuntimeView(ctx, runtimeView, s.clock().UTC())
    if err != nil {
        return err
    }
    duration := finalizedOrder.UpdatedAt.Sub(finalizedOrder.CreatedAt)
    if finalizedOrder.Status == domain.StatusCompleted {
        s.metrics.RecordOrderCompleted(duration)
        span.SetAttributes(attribute.String("saga.state", string(runtimeView.State)), attribute.String("order.result", "completed"))
        span.AddEvent("orchestration.saga.completed", trace.WithAttributes(attribute.String("saga.id", envelope.SagaID)))
        return nil
    }
    s.metrics.RecordOrderFailed(duration)
    span.SetAttributes(attribute.String("saga.state", string(runtimeView.State)), attribute.String("order.result", "cancelled"))
    span.AddEvent("orchestration.saga.cancelled", trace.WithAttributes(attribute.String("saga.id", envelope.SagaID)))
    return nil
}

func (s *Service) recordCompensationMetric(replyType string) {
    switch replyType {
    case commonreplies.TypePaymentRefunded:
        s.metrics.RecordPaymentCompensation()
    case commonreplies.TypeInventoryReleased:
        s.metrics.RecordInventoryCompensation()
    case commonreplies.TypeShippingCancelled:
        s.metrics.RecordShippingCompensation()
    }
}
