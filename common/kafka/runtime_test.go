package kafka

import (
	"context"
	"strconv"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"

	commoncontext "saga-pattern/common/context"
)

func TestPayloadAttributesExtractsChoreographyEventFields(t *testing.T) {
	payload := []byte(`{"type":"PAYMENT_FAILED","orderId":"order-1","paymentId":"payment-1","correlationId":"corr-1","reason":"card declined"}`)

	attrs := attributesByKey(payloadAttributes(payload))

	assertAttr(t, attrs, "messaging.message.payload_size_bytes", strconv.Itoa(len(payload)))
	assertAttr(t, attrs, "message.type", "PAYMENT_FAILED")
	assertAttr(t, attrs, "event.type", "PAYMENT_FAILED")
	assertAttr(t, attrs, "order.id", "order-1")
	assertAttr(t, attrs, "payment.id", "payment-1")
	assertAttr(t, attrs, "correlation.id", "corr-1")
	assertNoAttr(t, attrs, "failure.reason")
}

func TestPayloadAttributesExtractsOrchestrationCommandFields(t *testing.T) {
	payload := []byte(`{"commandType":"RESERVE_INVENTORY","orderId":"order-2","reservationId":"reservation-1","status":"pending"}`)

	attrs := attributesByKey(payloadAttributes(payload))

	assertAttr(t, attrs, "messaging.message.payload_size_bytes", strconv.Itoa(len(payload)))
	assertAttr(t, attrs, "saga.command.type", "RESERVE_INVENTORY")
	assertAttr(t, attrs, "order.id", "order-2")
	assertAttr(t, attrs, "reservation.id", "reservation-1")
	assertAttr(t, attrs, "saga.status", "pending")
}

func TestPayloadAttributesExtractsReplyOrGenericTypeFields(t *testing.T) {
	payload := []byte(`{"type":"SHIPPING_CANCELLED","orderId":"order-3","shippingId":"shipping-1","reason":"rollback"}`)

	attrs := attributesByKey(payloadAttributes(payload))

	assertAttr(t, attrs, "messaging.message.payload_size_bytes", strconv.Itoa(len(payload)))
	assertAttr(t, attrs, "message.type", "SHIPPING_CANCELLED")
	assertAttr(t, attrs, "event.type", "SHIPPING_CANCELLED")
	assertAttr(t, attrs, "order.id", "order-3")
	assertAttr(t, attrs, "shipping.id", "shipping-1")
	assertNoAttr(t, attrs, "failure.reason")
}

func TestPayloadAttributesExtractsEventTypeField(t *testing.T) {
	payload := []byte(`{"eventType":"ORDER_ACCEPTED","orderId":"order-4"}`)

	attrs := attributesByKey(payloadAttributes(payload))

	assertAttr(t, attrs, "event.type", "ORDER_ACCEPTED")
	assertAttr(t, attrs, "message.type", "ORDER_ACCEPTED")
	assertAttr(t, attrs, "order.id", "order-4")
}

func TestKafkaSpanNameUsesMessageTypeBeforeTopic(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		topic   string
		payload []byte
		want    string
	}{
		{name: "choreography event", prefix: "kafka.publish", topic: "order-events", payload: []byte(`{"type":"ORDER_CREATED"}`), want: "kafka.publish ORDER_CREATED"},
		{name: "event type", prefix: "kafka.consume", topic: "inventory-events", payload: []byte(`{"eventType":"PAYMENT_COMPLETED"}`), want: "kafka.consume PAYMENT_COMPLETED"},
		{name: "orchestration command", prefix: "kafka.publish", topic: "commands", payload: []byte(`{"commandType":"RESERVE_INVENTORY"}`), want: "kafka.publish RESERVE_INVENTORY"},
		{name: "topic fallback", prefix: "kafka.consume", topic: "reply-topic", payload: []byte(`{"unknown":"value"}`), want: "kafka.consume reply-topic"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := kafkaSpanName(test.prefix, test.topic, test.payload); got != test.want {
				t.Fatalf("kafkaSpanName() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPayloadAttributesKeepsOnlyPayloadSizeForMalformedOrNonObjectJSON(t *testing.T) {
	for _, payload := range [][]byte{[]byte(`not-json`), []byte(`["not", "an", "object"]`)} {
		attrs := attributesByKey(payloadAttributes(payload))
		if len(attrs) != 1 {
			t.Fatalf("payloadAttributes(%s) returned %d attrs, want 1", payload, len(attrs))
		}
		assertAttr(t, attrs, "messaging.message.payload_size_bytes", strconv.Itoa(len(payload)))
	}
}

func TestKafkaHeadersFromContextPropagatesAppHeadersWithoutIdempotencyKey(t *testing.T) {
	ctx := commoncontext.With(context.Background(), commoncontext.Data{
		OrderID:        "order-1",
		RequestID:      "request-1",
		CorrelationID:  "correlation-1",
		SagaID:         "saga-1",
		SagaType:       "OrderSaga",
		BenchmarkRun:   "run-1",
		BenchmarkScene: "failure",
		BenchmarkPhase: "create",
	})

	headers := kafkaHeadersByKey(kafkaHeadersFromContext(ctx))

	if got := headers[commoncontext.HeaderOrderID]; got != "order-1" {
		t.Fatalf("%s = %q, want order-1", commoncontext.HeaderOrderID, got)
	}
	if got := headers[commoncontext.HeaderRequestID]; got != "request-1" {
		t.Fatalf("%s = %q, want request-1", commoncontext.HeaderRequestID, got)
	}
	if got := headers[commoncontext.HeaderCorrelationID]; got != "correlation-1" {
		t.Fatalf("%s = %q, want correlation-1", commoncontext.HeaderCorrelationID, got)
	}
	if got := headers[commoncontext.HeaderIdempotencyKey]; got != "" {
		t.Fatalf("%s = %q, want empty", commoncontext.HeaderIdempotencyKey, got)
	}
	if got := headers[commoncontext.HeaderSagaID]; got != "saga-1" {
		t.Fatalf("%s = %q, want saga-1", commoncontext.HeaderSagaID, got)
	}
	if got := headers[commoncontext.HeaderSagaType]; got != "OrderSaga" {
		t.Fatalf("%s = %q, want OrderSaga", commoncontext.HeaderSagaType, got)
	}
	if got := headers[commoncontext.HeaderBenchmarkRun]; got != "run-1" {
		t.Fatalf("%s = %q, want run-1", commoncontext.HeaderBenchmarkRun, got)
	}
	if got := headers[commoncontext.HeaderBenchmarkScene]; got != "failure" {
		t.Fatalf("%s = %q, want failure", commoncontext.HeaderBenchmarkScene, got)
	}
	if got := headers[commoncontext.HeaderBenchmarkPhase]; got != "create" {
		t.Fatalf("%s = %q, want create", commoncontext.HeaderBenchmarkPhase, got)
	}
}

func TestKafkaHeadersFromContextDoesNotCreateRequestIDWithoutContextData(t *testing.T) {
	headers := kafkaHeadersByKey(kafkaHeadersFromContext(context.Background()))

	if got := headers[commoncontext.HeaderRequestID]; got != "" {
		t.Fatalf("%s = %q, want empty", commoncontext.HeaderRequestID, got)
	}
	if got := headers[commoncontext.HeaderCorrelationID]; got != "" {
		t.Fatalf("%s = %q, want empty", commoncontext.HeaderCorrelationID, got)
	}
}

func TestContextFromKafkaHeadersStoresExistingAppHeaders(t *testing.T) {
	ctx := contextFromKafkaHeaders(context.Background(), map[string]string{
		commoncontext.HeaderOrderID:        " order-2 ",
		commoncontext.HeaderRequestID:      " request-2 ",
		commoncontext.HeaderCorrelationID:  " correlation-2 ",
		commoncontext.HeaderSagaID:         " saga-2 ",
		commoncontext.HeaderSagaType:       " OrderSaga ",
		commoncontext.HeaderBenchmarkRun:   " run-2 ",
		commoncontext.HeaderBenchmarkScene: " contention ",
		commoncontext.HeaderBenchmarkPhase: " poll ",
	})

	data, ok := commoncontext.From(ctx)
	if !ok {
		t.Fatal("context data was not stored")
	}
	if data.OrderID != "order-2" {
		t.Fatalf("OrderID = %q, want order-2", data.OrderID)
	}
	if data.RequestID != "request-2" {
		t.Fatalf("RequestID = %q, want request-2", data.RequestID)
	}
	if data.CorrelationID != "correlation-2" {
		t.Fatalf("CorrelationID = %q, want correlation-2", data.CorrelationID)
	}
	if data.SagaID != "saga-2" {
		t.Fatalf("SagaID = %q, want saga-2", data.SagaID)
	}
	if data.SagaType != "OrderSaga" {
		t.Fatalf("SagaType = %q, want OrderSaga", data.SagaType)
	}
	if data.BenchmarkRun != "run-2" {
		t.Fatalf("BenchmarkRun = %q, want run-2", data.BenchmarkRun)
	}
	if data.BenchmarkScene != "contention" {
		t.Fatalf("BenchmarkScene = %q, want contention", data.BenchmarkScene)
	}
	if data.BenchmarkPhase != "poll" {
		t.Fatalf("BenchmarkPhase = %q, want poll", data.BenchmarkPhase)
	}
}

func TestContextFromKafkaHeadersDoesNotStoreDataWithoutAppHeaders(t *testing.T) {
	ctx := contextFromKafkaHeaders(context.Background(), map[string]string{})

	if data, ok := commoncontext.From(ctx); ok {
		t.Fatalf("context data = %#v, want absent", data)
	}
}

func attributesByKey(attrs []attribute.KeyValue) map[string]string {
	byKey := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		byKey[string(attr.Key)] = attr.Value.Emit()
	}
	return byKey
}

func kafkaHeadersByKey(headers []kafkago.Header) map[string]string {
	byKey := make(map[string]string, len(headers))
	for _, header := range headers {
		byKey[header.Key] = string(header.Value)
	}
	return byKey
}

func assertAttr(t *testing.T, attrs map[string]string, key string, want string) {
	t.Helper()
	got, ok := attrs[key]
	if !ok {
		t.Fatalf("missing attribute %q in %#v", key, attrs)
	}
	if got != want {
		t.Fatalf("attribute %q = %q, want %q", key, got, want)
	}
}

func assertNoAttr(t *testing.T, attrs map[string]string, key string) {
	t.Helper()
	if _, ok := attrs[key]; ok {
		t.Fatalf("unexpected attribute %q in %#v", key, attrs)
	}
}
