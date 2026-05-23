package inventory

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/messaging"
	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/testutil"
)

func TestReserveInventoryPublishesReservedEvent(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 17, 0, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-INV-1",
		"CUST-INV",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-inventory-1",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 2, Price: json.Number("15999000")}},
		json.Number("31998000"),
		now,
	)
	paymentEvent := events.NewPaymentCompletedEvent("PAY-INV-1", orderEvent.OrderID, json.Number("31998000"), "TX-INV-1", now.Add(time.Minute), orderEvent.CorrelationID, now.Add(time.Minute))

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderEvent.OrderID, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderEvent.OrderID, paymentEvent); err != nil {
		t.Fatalf("consume payment completed: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published message count = %d, want 1", len(messages))
	}
	reserved, ok := messages[0].Body.(events.InventoryReservedEvent)
	if !ok {
		t.Fatalf("published body type = %T, want InventoryReservedEvent", messages[0].Body)
	}
	if reserved.OrderID != orderEvent.OrderID {
		t.Fatalf("reserved order id = %q, want %q", reserved.OrderID, orderEvent.OrderID)
	}
	if reserved.ReservationID != "res-1" {
		t.Fatalf("reservation id = %q, want %q", reserved.ReservationID, "res-1")
	}
	if len(reserved.ReservedItems) != 1 || reserved.ReservedItems[0].ProductID != "PROD-001" || reserved.ReservedItems[0].Quantity != 2 {
		t.Fatalf("reserved items = %#v, want PROD-001 x2", reserved.ReservedItems)
	}
	product, ok, err := repo.Product(context.Background(), "PROD-001")
	if err != nil {
		t.Fatalf("lookup product: %v", err)
	}
	if !ok {
		t.Fatalf("product PROD-001 missing after reservation")
	}
	if product.QuantityAvailable != 98 || product.QuantityReserved != 2 {
		t.Fatalf("product quantities = available:%d reserved:%d, want 98/2", product.QuantityAvailable, product.QuantityReserved)
	}
}

func TestPaymentCompletedWaitsForOrderCreated(t *testing.T) {
	consumer, _, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 17, 30, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-INV-RACE",
		"CUST-INV-RACE",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-inventory-race",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1, Price: json.Number("15999000")}},
		json.Number("15999000"),
		now,
	)
	paymentEvent := events.NewPaymentCompletedEvent("PAY-INV-RACE", orderEvent.OrderID, json.Number("15999000"), "TX-INV-RACE", now.Add(time.Minute), orderEvent.CorrelationID, now.Add(time.Minute))
	errCh := make(chan error, 1)

	go func() {
		errCh <- deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderEvent.OrderID, paymentEvent)
	}()
	time.Sleep(30 * time.Millisecond)
	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderEvent.OrderID, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("consume payment completed: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published message count = %d, want 1", len(messages))
	}
	if _, ok := messages[0].Body.(events.InventoryReservedEvent); !ok {
		t.Fatalf("published body type = %T, want InventoryReservedEvent", messages[0].Body)
	}
}

func TestInsufficientStockPublishesReservationFailed(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 18, 0, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-INV-OVER",
		"CUST-OVER",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-low-stock",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1000, Price: json.Number("15999000")}},
		json.Number("15999000000"),
		now,
	)
	paymentEvent := events.NewPaymentCompletedEvent("PAY-OVER-1", orderEvent.OrderID, json.Number("15999000000"), "TX-OVER-1", now.Add(time.Minute), orderEvent.CorrelationID, now.Add(time.Minute))

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderEvent.OrderID, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderEvent.OrderID, paymentEvent); err != nil {
		t.Fatalf("consume payment completed: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published message count = %d, want 1", len(messages))
	}
	failed, ok := messages[0].Body.(events.InventoryReservationFailedEvent)
	if !ok {
		t.Fatalf("published body type = %T, want InventoryReservationFailedEvent", messages[0].Body)
	}
	if failed.ProductID != "PROD-001" {
		t.Fatalf("failed product id = %q, want %q", failed.ProductID, "PROD-001")
	}
	product, ok, err := repo.Product(context.Background(), "PROD-001")
	if err != nil {
		t.Fatalf("lookup product: %v", err)
	}
	if !ok {
		t.Fatalf("product PROD-001 missing after failed reservation")
	}
	if product.QuantityAvailable != 100 || product.QuantityReserved != 0 {
		t.Fatalf("product quantities = available:%d reserved:%d, want 100/0", product.QuantityAvailable, product.QuantityReserved)
	}
}

func TestReleaseCompensationIsIdempotent(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 19, 0, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-INV-REL",
		"CUST-REL",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-release",
		[]dto.OrderItemRequest{{ProductID: "PROD-002", ProductName: "Phone", Quantity: 3, Price: json.Number("2399000")}},
		json.Number("7197000"),
		now,
	)
	paymentEvent := events.NewPaymentCompletedEvent("PAY-REL-1", orderEvent.OrderID, json.Number("7197000"), "TX-REL-1", now.Add(time.Minute), orderEvent.CorrelationID, now.Add(time.Minute))
	shippingFailed := events.NewShippingFailedEvent(orderEvent.OrderID, "carrier unavailable", now.Add(2*time.Minute), orderEvent.CorrelationID, now.Add(2*time.Minute))

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderEvent.OrderID, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderEvent.OrderID, paymentEvent); err != nil {
		t.Fatalf("consume payment completed: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultShippingEventsTopic, orderEvent.OrderID, shippingFailed); err != nil {
		t.Fatalf("first shipping failed consume: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultShippingEventsTopic, orderEvent.OrderID, shippingFailed); err != nil {
		t.Fatalf("duplicate shipping failed should be ignored: %v", err)
	}

	messages := publisher.Messages()
	if len(messages) != 2 {
		t.Fatalf("published message count = %d, want 2", len(messages))
	}
	released, ok := messages[1].Body.(events.InventoryReleasedEvent)
	if !ok {
		t.Fatalf("published body type = %T, want InventoryReleasedEvent", messages[1].Body)
	}
	if released.ReservationID != "res-1" {
		t.Fatalf("released reservation id = %q, want %q", released.ReservationID, "res-1")
	}
	product, ok, err := repo.Product(context.Background(), "PROD-002")
	if err != nil {
		t.Fatalf("lookup compensated product: %v", err)
	}
	if !ok {
		t.Fatalf("product PROD-002 missing after release")
	}
	if product.QuantityAvailable != 200 || product.QuantityReserved != 0 {
		t.Fatalf("product quantities after release = available:%d reserved:%d, want 200/0", product.QuantityAvailable, product.QuantityReserved)
	}
}

func TestDuplicatePaymentCompletedReplayIsSafe(t *testing.T) {
	consumer, _, publisher := newTestService(t)
	now := time.Date(2026, 4, 13, 20, 0, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-INV-DUPE",
		"CUST-DUPE",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-dupe",
		[]dto.OrderItemRequest{{ProductID: "PROD-003", ProductName: "Headphones", Quantity: 1, Price: json.Number("1299000")}},
		json.Number("1299000"),
		now,
	)
	paymentEvent := events.NewPaymentCompletedEvent("PAY-DUPE-1", orderEvent.OrderID, json.Number("1299000"), "TX-DUPE-1", now.Add(time.Minute), orderEvent.CorrelationID, now.Add(time.Minute))

	if err := deliverEvent(consumer, commonkafka.DefaultOrderEventsTopic, orderEvent.OrderID, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderEvent.OrderID, paymentEvent); err != nil {
		t.Fatalf("first payment completed consume: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderEvent.OrderID, paymentEvent); err != nil {
		t.Fatalf("duplicate payment completed should be ignored: %v", err)
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(publisher.Messages()))
	}
}

func newTestService(t *testing.T) (*messaging.DownstreamConsumer, repository.Repository, *testutil.RecordingPublisher) {
	t.Helper()
	consumer, repo, publisher, _ := newTestServiceWithService(t)
	return consumer, repo, publisher
}

func newTestServiceWithService(t *testing.T) (*messaging.DownstreamConsumer, repository.Repository, *testutil.RecordingPublisher, *Service) {
	t.Helper()
	db := testutil.OpenPostgres(t, testutil.DefaultChoreographyInventoryDatabaseURL, "choreography_inventory_service_test", testutil.Migration{Scope: "choreography-inventory-service", Dir: "choreography-saga/inventory-service/db/migrations"})
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	publisher := testutil.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewInventoryTopicPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 13, 21, 0, 0, 0, time.UTC) })
	service.WithIDGenerator(sequenceIDs("res-1", "res-2", "res-3"))
	consumer, err := messaging.NewDownstreamConsumer(service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new downstream consumer: %v", err)
	}
	return consumer, repo, publisher, service
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

func TestRepositorySeedsCatalogProducts(t *testing.T) {
	db := testutil.OpenPostgres(t, testutil.DefaultChoreographyInventoryDatabaseURL, "choreography_inventory_fixture_test", testutil.Migration{Scope: "choreography-inventory-service", Dir: "choreography-saga/inventory-service/db/migrations"})
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	for _, fixture := range []struct {
		id        string
		available int
	}{
		{id: "PROD-001", available: 100},
		{id: "PROD-005", available: 120},
	} {
		product, ok, err := repo.Product(context.Background(), fixture.id)
		if err != nil {
			t.Fatalf("lookup product %s: %v", fixture.id, err)
		}
		if !ok {
			t.Fatalf("seed product %s missing", fixture.id)
		}
		if product.QuantityAvailable != fixture.available {
			t.Fatalf("seed product %s available = %d, want %d", fixture.id, product.QuantityAvailable, fixture.available)
		}
	}
}
