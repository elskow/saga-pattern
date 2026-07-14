package inventory

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/testutil"
)

type recordedEvent struct {
	Topic     string
	Key       string
	EventType string
	Payload   any
}

type recordingParticipant struct {
	mu     sync.Mutex
	events []recordedEvent
}

func (p *recordingParticipant) EnqueueEvent(_ context.Context, _ *sql.Tx, topic, key, eventType string, payload any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, recordedEvent{Topic: topic, Key: key, EventType: eventType, Payload: payload})
	return nil
}

func (p *recordingParticipant) TriggerImmediatePublish(_ context.Context) {}

func (p *recordingParticipant) Events() []recordedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	cpy := make([]recordedEvent, len(p.events))
	copy(cpy, p.events)
	return cpy
}

func TestReserveInventoryPublishesReservedEvent(t *testing.T) {
	consumer, repo, participant := newTestService(t)
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

	evts := participant.Events()
	if len(evts) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(evts))
	}
	if evts[0].EventType != events.TypeInventoryReserved {
		t.Fatalf("enqueued event type = %q, want %q", evts[0].EventType, events.TypeInventoryReserved)
	}
	reserved, ok := evts[0].Payload.(events.InventoryReservedEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want InventoryReservedEvent", evts[0].Payload)
	}
	if reserved.OrderID != orderEvent.OrderID {
		t.Fatalf("reserved order id = %q, want %q", reserved.OrderID, orderEvent.OrderID)
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
	consumer, _, participant := newTestService(t)
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

	evts := participant.Events()
	if len(evts) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(evts))
	}
	if evts[0].EventType != events.TypeInventoryReserved {
		t.Fatalf("enqueued event type = %q, want %q", evts[0].EventType, events.TypeInventoryReserved)
	}
}

func TestInsufficientStockPublishesReservationFailed(t *testing.T) {
	consumer, repo, participant := newTestService(t)
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

	evts := participant.Events()
	if len(evts) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(evts))
	}
	if evts[0].EventType != events.TypeInventoryReservationFailed {
		t.Fatalf("enqueued event type = %q, want %q", evts[0].EventType, events.TypeInventoryReservationFailed)
	}
	failed, ok := evts[0].Payload.(events.InventoryReservationFailedEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want InventoryReservationFailedEvent", evts[0].Payload)
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
	consumer, repo, participant := newTestService(t)
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

	evts := participant.Events()
	if len(evts) != 2 {
		t.Fatalf("enqueued event count = %d, want 2", len(evts))
	}
	if evts[0].EventType != events.TypeInventoryReserved {
		t.Fatalf("first event type = %q, want %q", evts[0].EventType, events.TypeInventoryReserved)
	}
	if evts[1].EventType != events.TypeInventoryReleased {
		t.Fatalf("second event type = %q, want %q", evts[1].EventType, events.TypeInventoryReleased)
	}
	released, ok := evts[1].Payload.(events.InventoryReleasedEvent)
	if !ok {
		t.Fatalf("released payload type = %T, want InventoryReleasedEvent", evts[1].Payload)
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
	consumer, _, participant := newTestService(t)
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
	if len(participant.Events()) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.Events()))
	}
}

func newTestService(t *testing.T) (*recordingConsumer, repository.Repository, *recordingParticipant) {
	t.Helper()
	consumer, repo, participant, _ := newTestServiceWithService(t)
	return consumer, repo, participant
}

func newTestServiceWithService(t *testing.T) (*recordingConsumer, repository.Repository, *recordingParticipant, *Service) {
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
	participant := &recordingParticipant{}
	service, err := NewService(repo, participant, metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 13, 21, 0, 0, 0, time.UTC) })
	service.WithIDGenerator(sequenceIDs("res-1", "res-2", "res-3"))
	consumer := newRecordingConsumer(service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return consumer, repo, participant, service
}

// recordingConsumer wraps the service as an event consumer for tests.
type recordingConsumer struct {
	handler interface{ HandleEvent(context.Context, events.ChoreographyEvent) error }
	logger  *slog.Logger
}

func newRecordingConsumer(handler interface{ HandleEvent(context.Context, events.ChoreographyEvent) error }, logger *slog.Logger) *recordingConsumer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &recordingConsumer{handler: handler, logger: logger}
}

func (c *recordingConsumer) Consume(ctx context.Context, topic, key string, event events.ChoreographyEvent) error {
	if delay := simulatedDelayMsForTest.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	return c.handler.HandleEvent(ctx, event)
}

// simulatedDelayMsForTest mirrors domain.SimulatedDelayMs for test consumer.
var simulatedDelayMsForTest atomicInt32

type atomicInt32 struct{ v int32 }

func (a *atomicInt32) Load() int32   { return a.v }
func (a *atomicInt32) Store(v int32) { a.v = v }

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

func deliverEvent(consumer *recordingConsumer, topic string, key string, event events.ChoreographyEvent) error {
	return consumer.Consume(context.Background(), topic, key, event)
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
