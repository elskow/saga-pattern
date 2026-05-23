package events_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	"saga-pattern/common/replies"
)

func TestEventAndReplyJSONRoundTrip(t *testing.T) {
	t.Parallel()

	createdAt := time.Unix(1710000000, 0).UTC()
	event := events.NewOrderCreatedEvent(
		"ORDER-123",
		"CUST-001",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-123",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Sample Product", Quantity: 2, Price: json.Number("799000")}},
		json.Number("99.98"),
		createdAt,
	)

	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if !bytes.Contains(eventJSON, []byte(`"type":"ORDER_CREATED"`)) {
		t.Fatalf("event JSON missing discriminator: %s", eventJSON)
	}

	decodedEvent, err := events.DecodeChoreographyEvent(bytes.NewReader(eventJSON))
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	orderCreated, ok := decodedEvent.(events.OrderCreatedEvent)
	if !ok {
		t.Fatalf("decoded event type = %T, want OrderCreatedEvent", decodedEvent)
	}
	if orderCreated.OrderID != event.OrderID || orderCreated.CorrelationID != event.CorrelationID || orderCreated.Type != events.TypeOrderCreated {
		t.Fatalf("decoded event mismatch: %+v", orderCreated)
	}

	reply := replies.NewPaymentCompletedReply("PAY-123", "ORDER-123")
	replyJSON, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	if !bytes.Contains(replyJSON, []byte(`"type":"PAYMENT_COMPLETED"`)) {
		t.Fatalf("reply JSON missing discriminator: %s", replyJSON)
	}

	decodedReply, err := replies.DecodeSagaReply(bytes.NewReader(replyJSON))
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	paymentCompleted, ok := decodedReply.(replies.PaymentCompletedReply)
	if !ok {
		t.Fatalf("decoded reply type = %T, want PaymentCompletedReply", decodedReply)
	}
	if paymentCompleted.PaymentID != reply.PaymentID || paymentCompleted.OrderID != reply.OrderID || paymentCompleted.Type != replies.TypePaymentCompleted {
		t.Fatalf("decoded reply mismatch: %+v", paymentCompleted)
	}
}

func TestContextWithMetadataPreservesRequestIDAndUsesEventCorrelation(t *testing.T) {
	ctx := commoncontext.With(context.Background(), commoncontext.Data{RequestID: "request-1", CorrelationID: "http-correlation"})
	event := events.NewPaymentCompletedEvent("PAY-1", "ORDER-1", json.Number("799000"), "TX-1", time.Now().UTC(), "event-correlation", time.Now().UTC())

	ctx = events.ContextWithMetadata(ctx, event)
	data, ok := commoncontext.From(ctx)
	if !ok {
		t.Fatal("context data was not stored")
	}
	if data.RequestID != "request-1" {
		t.Fatalf("RequestID = %q, want request-1", data.RequestID)
	}
	if data.OrderID != "ORDER-1" {
		t.Fatalf("OrderID = %q, want ORDER-1", data.OrderID)
	}
	if data.CorrelationID != "event-correlation" {
		t.Fatalf("CorrelationID = %q, want event-correlation", data.CorrelationID)
	}
}
