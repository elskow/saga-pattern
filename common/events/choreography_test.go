package events_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

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
		"123 Main Street",
		"corr-123",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Sample Product", Quantity: 2, Price: json.Number("49.99")}},
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
