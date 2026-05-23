package loops

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/orchestration-framework/internal/model"
)

func TestOutboxEventAttributesIncludeQueryableContext(t *testing.T) {
	attrs := loopAttributesByKey(outboxEventAttributes(model.OutboxRow{
		ID:             "outbox-1",
		SagaID:         "order-1",
		SagaType:       "OrderSaga",
		Step:           "Payment",
		Direction:      model.StepDirectionForward,
		Topic:          "orchestration.payment.commands",
		MessageType:    "PROCESS_PAYMENT",
		Payload:        []byte(`{"orderId":"order-1"}`),
		AttemptCount:   2,
		MaxAttempts:    5,
		LastError:      "temporary failure",
		RequestID:      "request-1",
		CorrelationID:  "correlation-1",
		BenchmarkRun:   "run-1",
		BenchmarkScene: "failure",
		BenchmarkPhase: "create",
	}))

	assertLoopAttr(t, attrs, "outbox.id", "outbox-1")
	assertLoopAttr(t, attrs, "saga.id", "order-1")
	assertLoopAttr(t, attrs, "saga.type", "OrderSaga")
	assertLoopAttr(t, attrs, "saga.step", "Payment")
	assertLoopAttr(t, attrs, "saga.pending.direction", "forward")
	assertLoopAttr(t, attrs, "topic", "orchestration.payment.commands")
	assertLoopAttr(t, attrs, "message.type", "PROCESS_PAYMENT")
	assertLoopAttr(t, attrs, "messaging.message.payload_size_bytes", "21")
	assertLoopAttr(t, attrs, "attempt.count", "2")
	assertLoopAttr(t, attrs, "max.attempts", "5")
	assertLoopAttr(t, attrs, "last.error", "temporary failure")
	assertLoopAttr(t, attrs, "request.id", "request-1")
	assertLoopAttr(t, attrs, "correlation.id", "correlation-1")
	assertLoopAttr(t, attrs, "benchmark.run_label", "run-1")
	assertLoopAttr(t, attrs, "benchmark.scenario", "failure")
	assertLoopAttr(t, attrs, "benchmark.phase", "create")
}

func loopAttributesByKey(attrs []attribute.KeyValue) map[string]string {
	byKey := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		byKey[string(attr.Key)] = attr.Value.Emit()
	}
	return byKey
}

func TestContextWithOutboxRequestDataRestoresBenchmarkMetadata(t *testing.T) {
	ctx := contextWithOutboxRequestData(context.Background(), model.OutboxRow{
		RequestID:      "request-1",
		CorrelationID:  "correlation-1",
		BenchmarkRun:   "run-1",
		BenchmarkScene: "failure",
		BenchmarkPhase: "create",
	})

	data, ok := commoncontext.From(ctx)
	if !ok {
		t.Fatalf("expected common context")
	}
	if data.RequestID != "request-1" || data.CorrelationID != "correlation-1" || data.BenchmarkRun != "run-1" || data.BenchmarkScene != "failure" || data.BenchmarkPhase != "create" {
		t.Fatalf("context data = %+v", data)
	}
}

func assertLoopAttr(t *testing.T, attrs map[string]string, key string, want string) {
	t.Helper()
	got, ok := attrs[key]
	if !ok {
		t.Fatalf("missing attribute %q in %#v", key, attrs)
	}
	if got != want {
		t.Fatalf("attribute %q = %q, want %q", key, got, want)
	}
}

func TestContextWithOutboxTraceHeadersRestoresSpanContext(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx := contextWithOutboxTraceHeaders(context.Background(), model.OutboxRow{TraceHeaders: map[string]string{
		"traceparent": "00-958b17a8fdf7a6b3efa0d85c7685cda4-ac7a5dd8008f57cb-01",
	}})

	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		t.Fatalf("expected valid restored span context")
	}
	if got := spanContext.TraceID().String(); got != "958b17a8fdf7a6b3efa0d85c7685cda4" {
		t.Fatalf("trace id = %q", got)
	}
	if got := spanContext.SpanID().String(); got != "ac7a5dd8008f57cb" {
		t.Fatalf("span id = %q", got)
	}
}
