package payments

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/choreography-saga/payment-service/internal/messaging"
	"saga-pattern/choreography-saga/payment-service/internal/observability"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

func TestOrderCreatedPublishesPaymentCompleted(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)

	event := events.NewOrderCreatedEvent(
		"ORDER-123",
		"CUST-001",
		"123 Main Street",
		"corr-123",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Widget", Quantity: 1, Price: json.Number("99.99")}},
		json.Number("99.99"),
		now,
	)

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, event.OrderID, event); err != nil {
		t.Fatalf("consume order created: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published message count = %d, want 1", len(messages))
	}
	if messages[0].Topic != messaging.NewPaymentTopicPublisher(publisher).Topic() {
		t.Fatalf("published topic = %q", messages[0].Topic)
	}
	completed, ok := messages[0].Body.(events.PaymentCompletedEvent)
	if !ok {
		t.Fatalf("published body type = %T, want PaymentCompletedEvent", messages[0].Body)
	}
	if completed.OrderID != event.OrderID {
		t.Fatalf("completed order id = %q, want %q", completed.OrderID, event.OrderID)
	}
	if completed.TransactionID != "TX-TX-SEED-" {
		t.Fatalf("transaction id = %q, want %q", completed.TransactionID, "TX-TX-SEED-")
	}
	payment, ok, err := repo.GetByOrderID(context.Background(), event.OrderID)
	if err != nil {
		t.Fatalf("get payment by order id: %v", err)
	}
	if !ok {
		t.Fatalf("payment not stored for order %s", event.OrderID)
	}
	if payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment status = %s, want %s", payment.Status, domain.PaymentStatusCompleted)
	}
}

func TestPremiumProductPublishesPaymentFailed(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 13, 0, 0, 0, time.UTC)

	event := events.NewOrderCreatedEvent(
		"ORDER-PREMIUM",
		"CUST-999",
		"9 Premium Avenue",
		"corr-premium",
		[]dto.OrderItemRequest{{ProductID: premiumFailureProductID, ProductName: "Premium Item", Quantity: 1, Price: json.Number("10000.00")}},
		json.Number("10000.00"),
		now,
	)

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, event.OrderID, event); err != nil {
		t.Fatalf("consume premium order created: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published message count = %d, want 1", len(messages))
	}
	failed, ok := messages[0].Body.(events.PaymentFailedEvent)
	if !ok {
		t.Fatalf("published body type = %T, want PaymentFailedEvent", messages[0].Body)
	}
	if !strings.Contains(failed.Reason, premiumFailureProductID) {
		t.Fatalf("failure reason = %q, want fixture product id", failed.Reason)
	}
	payment, ok, err := repo.GetByOrderID(context.Background(), event.OrderID)
	if err != nil {
		t.Fatalf("get payment by order id: %v", err)
	}
	if !ok {
		t.Fatalf("payment not stored for order %s", event.OrderID)
	}
	if payment.Status != domain.PaymentStatusFailed {
		t.Fatalf("payment status = %s, want %s", payment.Status, domain.PaymentStatusFailed)
	}
}

func TestDuplicateOrderCreatedReplayIsSafe(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 14, 0, 0, 0, time.UTC)

	event := events.NewOrderCreatedEvent(
		"ORDER-DUPE",
		"CUST-002",
		"22 Replay Road",
		"corr-dupe",
		[]dto.OrderItemRequest{{ProductID: "PROD-002", ProductName: "Basic", Quantity: 2, Price: json.Number("149.99")}},
		json.Number("299.98"),
		now,
	)

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, event.OrderID, event); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, event.OrderID, event); err != nil {
		t.Fatalf("duplicate consume should be ignored: %v", err)
	}

	if len(publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(publisher.Messages()))
	}
	payment, ok, err := repo.GetByOrderID(context.Background(), event.OrderID)
	if err != nil {
		t.Fatalf("get payment by order id: %v", err)
	}
	if !ok {
		t.Fatalf("payment not stored for order %s", event.OrderID)
	}
	if payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment status = %s, want %s", payment.Status, domain.PaymentStatusCompleted)
	}
}

func TestInventoryFailurePublishesPaymentRefunded(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 15, 0, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-REFUND",
		"CUST-003",
		"7 Refund Street",
		"corr-refund",
		[]dto.OrderItemRequest{{ProductID: "PROD-003", ProductName: "Refundable", Quantity: 1, Price: json.Number("79.99")}},
		json.Number("79.99"),
		now,
	)

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderEvent.OrderID, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	refundEvent := events.NewInventoryReservationFailedEvent(orderEvent.OrderID, "PROD-003", "out of stock", now.Add(time.Minute), "corr-refund", now.Add(time.Minute))
	if err := deliverEvent(consumer, commonkafka.DefaultInventoryEventsTopic, orderEvent.OrderID, refundEvent); err != nil {
		t.Fatalf("consume inventory failure: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 2 {
		t.Fatalf("published message count = %d, want 2", len(messages))
	}
	refunded, ok := messages[1].Body.(events.PaymentRefundedEvent)
	if !ok {
		t.Fatalf("published body type = %T, want PaymentRefundedEvent", messages[1].Body)
	}
	if refunded.OrderID != orderEvent.OrderID {
		t.Fatalf("refunded order id = %q, want %q", refunded.OrderID, orderEvent.OrderID)
	}
	payment, ok, err := repo.GetByOrderID(context.Background(), orderEvent.OrderID)
	if err != nil {
		t.Fatalf("get payment by order id: %v", err)
	}
	if !ok {
		t.Fatalf("payment not stored for order %s", orderEvent.OrderID)
	}
	if payment.Status != domain.PaymentStatusRefunded {
		t.Fatalf("payment status = %s, want %s", payment.Status, domain.PaymentStatusRefunded)
	}
}

func newTestService(t *testing.T) (*messaging.DownstreamConsumer, *repository.MemoryRepository, *messaging.RecordingPublisher) {
	t.Helper()
	repo := repository.NewMemoryRepository()
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	publisher := messaging.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewPaymentTopicPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 13, 16, 0, 0, 0, time.UTC) })
	service.WithIDGenerator(sequenceIDs("pay-1", "tx-seed-1", "pay-2", "tx-seed-2", "pay-3", "tx-seed-3", "pay-4", "tx-seed-4"))
	consumer, err := messaging.NewDownstreamConsumer(service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new downstream consumer: %v", err)
	}
	return consumer, repo, publisher
}

func sequenceIDs(values ...string) func() string {
	idx := 0
	return func() string {
		if idx >= len(values) {
			return "fallback-id"
		}
		value := values[idx]
		idx++
		return value
	}
}

func deliverEvent(consumer *messaging.DownstreamConsumer, topic string, key string, event events.ChoreographyEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return consumer.Consume(context.Background(), messaging.DownstreamEnvelope{Topic: topic, Key: key, Value: payload})
}
