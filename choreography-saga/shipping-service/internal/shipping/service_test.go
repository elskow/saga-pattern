package shipping

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/choreography-saga/shipping-service/internal/messaging"
	"saga-pattern/choreography-saga/shipping-service/internal/observability"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

func TestScheduleShippingPublishesShippingScheduled(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 14, 9, 0, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-1",
		"CUST-1",
		"123 Main Street",
		"corr-ship-1",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Widget", Quantity: 1, Price: json.Number("49.99")}},
		json.Number("49.99"),
		now,
	)
	inventoryReserved := events.NewInventoryReservedEvent(
		"RES-SHIP-1",
		orderCreated.OrderID,
		[]events.InventoryReservedItem{{ProductID: "PROD-001", Quantity: 1}},
		now.Add(time.Minute),
		orderCreated.CorrelationID,
		now.Add(time.Minute),
	)

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderCreated.OrderID, orderCreated); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultInventoryEventsTopic, orderCreated.OrderID, inventoryReserved); err != nil {
		t.Fatalf("consume inventory reserved: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published message count = %d, want 1", len(messages))
	}
	if messages[0].Topic != messaging.NewShippingTopicPublisher(publisher).Topic() {
		t.Fatalf("published topic = %q", messages[0].Topic)
	}
	scheduled, ok := messages[0].Body.(events.ShippingScheduledEvent)
	if !ok {
		t.Fatalf("published body type = %T, want ShippingScheduledEvent", messages[0].Body)
	}
	if scheduled.ShippingID != "ship-1" {
		t.Fatalf("shipping id = %q, want %q", scheduled.ShippingID, "ship-1")
	}
	if scheduled.OrderID != orderCreated.OrderID {
		t.Fatalf("scheduled order id = %q, want %q", scheduled.OrderID, orderCreated.OrderID)
	}
	if scheduled.Address != orderCreated.ShippingAddress {
		t.Fatalf("scheduled address = %q, want %q", scheduled.Address, orderCreated.ShippingAddress)
	}
	if scheduled.TrackingNumber != "TRK-SHIP1" {
		t.Fatalf("tracking number = %q, want %q", scheduled.TrackingNumber, "TRK-SHIP1")
	}
	shipment, ok, err := repo.GetShipmentByOrderID(context.Background(), orderCreated.OrderID)
	if err != nil {
		t.Fatalf("get shipment by order id: %v", err)
	}
	if !ok {
		t.Fatalf("shipment missing for order %s", orderCreated.OrderID)
	}
	if shipment.Status != domain.ShipmentStatusScheduled {
		t.Fatalf("shipment status = %s, want %s", shipment.Status, domain.ShipmentStatusScheduled)
	}
	if _, ok, err := repo.PendingAddress(context.Background(), orderCreated.OrderID); err != nil {
		t.Fatalf("pending address lookup: %v", err)
	} else if ok {
		t.Fatalf("pending address should be deleted after scheduling")
	}
}

func TestCancelShippingCompensationIsIdempotent(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-CANCEL",
		"CUST-2",
		"8 Refund Lane",
		"corr-ship-cancel",
		[]dto.OrderItemRequest{{ProductID: "PROD-002", ProductName: "Phone", Quantity: 1, Price: json.Number("149.99")}},
		json.Number("149.99"),
		now,
	)
	inventoryReserved := events.NewInventoryReservedEvent(
		"RES-SHIP-CANCEL",
		orderCreated.OrderID,
		[]events.InventoryReservedItem{{ProductID: "PROD-002", Quantity: 1}},
		now.Add(time.Minute),
		orderCreated.CorrelationID,
		now.Add(time.Minute),
	)
	refunded := events.NewPaymentRefundedEvent(
		"PAY-SHIP-CANCEL",
		orderCreated.OrderID,
		json.Number("149.99"),
		now.Add(2*time.Minute),
		orderCreated.CorrelationID,
		now.Add(2*time.Minute),
	)

	for _, delivery := range []struct {
		topic string
		key   string
		event events.ChoreographyEvent
	}{
		{topic: commonkafka.DefaultOrderEventsTopic, key: orderCreated.OrderID, event: orderCreated},
		{topic: commonkafka.DefaultInventoryEventsTopic, key: orderCreated.OrderID, event: inventoryReserved},
		{topic: commonkafka.DefaultPaymentEventsTopic, key: orderCreated.OrderID, event: refunded},
		{topic: commonkafka.DefaultPaymentEventsTopic, key: orderCreated.OrderID, event: refunded},
	} {
		if err := deliverEvent(consumer, delivery.topic, delivery.key, delivery.event); err != nil {
			t.Fatalf("consume %s: %v", delivery.topic, err)
		}
	}

	messages := publisher.Messages()
	if len(messages) != 2 {
		t.Fatalf("published message count = %d, want 2", len(messages))
	}
	cancelled, ok := messages[1].Body.(events.ShippingCancelledEvent)
	if !ok {
		t.Fatalf("published body type = %T, want ShippingCancelledEvent", messages[1].Body)
	}
	if cancelled.ShippingID != "ship-1" {
		t.Fatalf("cancelled shipping id = %q, want %q", cancelled.ShippingID, "ship-1")
	}
	shipment, ok, err := repo.GetShipmentByOrderID(context.Background(), orderCreated.OrderID)
	if err != nil {
		t.Fatalf("get shipment by order id: %v", err)
	}
	if !ok {
		t.Fatalf("shipment missing for order %s", orderCreated.OrderID)
	}
	if shipment.Status != domain.ShipmentStatusCancelled {
		t.Fatalf("shipment status = %s, want %s", shipment.Status, domain.ShipmentStatusCancelled)
	}
}

func TestDuplicateInventoryReservedReplayIsSafe(t *testing.T) {
	consumer, _, publisher := newTestService(t)
	now := time.Date(2026, 4, 14, 11, 0, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-DUPE",
		"CUST-3",
		"12 Replay Road",
		"corr-ship-dupe",
		[]dto.OrderItemRequest{{ProductID: "PROD-003", ProductName: "Headphones", Quantity: 2, Price: json.Number("79.99")}},
		json.Number("159.98"),
		now,
	)
	inventoryReserved := events.NewInventoryReservedEvent(
		"RES-SHIP-DUPE",
		orderCreated.OrderID,
		[]events.InventoryReservedItem{{ProductID: "PROD-003", Quantity: 2}},
		now.Add(time.Minute),
		orderCreated.CorrelationID,
		now.Add(time.Minute),
	)

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderCreated.OrderID, orderCreated); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultInventoryEventsTopic, orderCreated.OrderID, inventoryReserved); err != nil {
		t.Fatalf("first inventory reserved consume: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultInventoryEventsTopic, orderCreated.OrderID, inventoryReserved); err != nil {
		t.Fatalf("duplicate inventory reserved should be ignored: %v", err)
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(publisher.Messages()))
	}
}

func newTestService(t *testing.T) (*messaging.DownstreamConsumer, repository.Repository, *messaging.RecordingPublisher) {
	t.Helper()
	repo := repository.NewMemoryRepository()
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	publisher := messaging.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewShippingTopicPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC) })
	service.WithIDGenerator(sequenceIDs("ship-1", "ship-2", "ship-3"))
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
