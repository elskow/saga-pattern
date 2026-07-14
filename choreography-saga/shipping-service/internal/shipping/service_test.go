package shipping

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/choreography-saga/shipping-service/internal/observability"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/testutil"
)

func TestScheduleShippingPublishesShippingScheduled(t *testing.T) {
	service, repo, participant := newTestService(t)
	now := time.Date(2026, 4, 14, 9, 0, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-1",
		"CUST-1",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-ship-1",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Widget", Quantity: 1, Price: json.Number("799000")}},
		json.Number("799000"),
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

	if err := deliverEvent(service, orderCreated); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(service, inventoryReserved); err != nil {
		t.Fatalf("consume inventory reserved: %v", err)
	}

	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
	}
	if participant.enqueued[0].Topic != commonkafka.DefaultShippingEventsTopic {
		t.Fatalf("enqueued topic = %q, want %q", participant.enqueued[0].Topic, commonkafka.DefaultShippingEventsTopic)
	}
	scheduled, ok := participant.enqueued[0].Payload.(events.ShippingScheduledEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want ShippingScheduledEvent", participant.enqueued[0].Payload)
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
	if _, ok, err := repo.LoadPendingShippingAddress(context.Background(), orderCreated.OrderID); err != nil {
		t.Fatalf("pending address lookup: %v", err)
	} else if ok {
		t.Fatalf("pending address should be deleted after scheduling")
	}
}

func TestInventoryReservedWaitsForOrderCreated(t *testing.T) {
	service, _, participant := newTestService(t)
	now := time.Date(2026, 4, 14, 9, 30, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-RACE",
		"CUST-SHIP-RACE",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-ship-race",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Widget", Quantity: 1, Price: json.Number("799000")}},
		json.Number("799000"),
		now,
	)
	inventoryReserved := events.NewInventoryReservedEvent(
		"RES-SHIP-RACE",
		orderCreated.OrderID,
		[]events.InventoryReservedItem{{ProductID: "PROD-001", Quantity: 1}},
		now.Add(time.Minute),
		orderCreated.CorrelationID,
		now.Add(time.Minute),
	)
	errCh := make(chan error, 1)

	go func() {
		errCh <- deliverEvent(service, inventoryReserved)
	}()
	time.Sleep(30 * time.Millisecond)
	if err := deliverEvent(service, orderCreated); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("consume inventory reserved: %v", err)
	}

	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
	}
	if _, ok := participant.enqueued[0].Payload.(events.ShippingScheduledEvent); !ok {
		t.Fatalf("enqueued payload type = %T, want ShippingScheduledEvent", participant.enqueued[0].Payload)
	}
}

func TestCancelShippingCompensationIsIdempotent(t *testing.T) {
	service, repo, participant := newTestService(t)
	now := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-CANCEL",
		"CUST-2",
		"8 Refund Lane",
		"corr-ship-cancel",
		[]dto.OrderItemRequest{{ProductID: "PROD-002", ProductName: "Phone", Quantity: 1, Price: json.Number("2399000")}},
		json.Number("2399000"),
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
		json.Number("2399000"),
		now.Add(2*time.Minute),
		orderCreated.CorrelationID,
		now.Add(2*time.Minute),
	)

	for _, delivery := range []struct {
		topic string
		event events.ChoreographyEvent
	}{
		{topic: commonkafka.DefaultOrderEventsTopic, event: orderCreated},
		{topic: commonkafka.DefaultInventoryEventsTopic, event: inventoryReserved},
		{topic: commonkafka.DefaultPaymentEventsTopic, event: refunded},
		{topic: commonkafka.DefaultPaymentEventsTopic, event: refunded},
	} {
		if err := deliverEvent(service, delivery.event); err != nil {
			t.Fatalf("consume %s: %v", delivery.topic, err)
		}
	}

	if len(participant.enqueued) != 2 {
		t.Fatalf("enqueued event count = %d, want 2", len(participant.enqueued))
	}
	cancelled, ok := participant.enqueued[1].Payload.(events.ShippingCancelledEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want ShippingCancelledEvent", participant.enqueued[1].Payload)
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
	service, _, participant := newTestService(t)
	now := time.Date(2026, 4, 14, 11, 0, 0, 0, time.UTC)
	orderCreated := events.NewOrderCreatedEvent(
		"ORDER-SHIP-DUPE",
		"CUST-3",
		"12 Replay Road",
		"corr-ship-dupe",
		[]dto.OrderItemRequest{{ProductID: "PROD-003", ProductName: "Headphones", Quantity: 2, Price: json.Number("1299000")}},
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

	if err := deliverEvent(service, orderCreated); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	if err := deliverEvent(service, inventoryReserved); err != nil {
		t.Fatalf("first inventory reserved consume: %v", err)
	}
	if err := deliverEvent(service, inventoryReserved); err != nil {
		t.Fatalf("duplicate inventory reserved should be ignored: %v", err)
	}
	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
	}
}

type enqueuedEvent struct {
	Topic     string
	Key       string
	EventType string
	Payload   any
}

type stubParticipant struct {
	enqueued []enqueuedEvent
}

func (p *stubParticipant) EnqueueEvent(_ context.Context, _ *sql.Tx, topic, key, eventType string, payload any) error {
	p.enqueued = append(p.enqueued, enqueuedEvent{Topic: topic, Key: key, EventType: eventType, Payload: payload})
	return nil
}

func (p *stubParticipant) TriggerImmediatePublish(_ context.Context) {}

var _ participantAdapter = (*stubParticipant)(nil)

func newTestService(t *testing.T) (*Service, repository.Repository, *stubParticipant) {
	t.Helper()
	db := testutil.OpenPostgres(t, testutil.DefaultChoreographyShippingDatabaseURL, "choreography_shipping_service_test", testutil.Migration{Scope: "choreography-shipping-service", Dir: "choreography-saga/shipping-service/db/migrations"})
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	participant := &stubParticipant{}
	service, err := NewService(repo, participant, metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC) })
	service.WithIDGenerator(sequenceIDs("ship-1", "ship-2", "ship-3"))
	return service, repo, participant
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

func deliverEvent(service *Service, event events.ChoreographyEvent) error {
	return service.HandleEvent(context.Background(), event)
}
