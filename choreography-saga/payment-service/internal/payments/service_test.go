package payments

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/big"
	"sync"
	"testing"
	"time"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
	"saga-pattern/choreography-saga/payment-service/internal/observability"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/testutil"
)

func TestOrderCreatedPublishesPaymentCompleted(t *testing.T) {
	service, repo, participant := newTestService(t)
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)

	event := events.NewOrderCreatedEvent(
		"ORDER-123",
		"CUST-001",
		"Jl. Ketintang Wiyata, Surabaya 60231",
		"corr-123",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Widget", Quantity: 1, Price: json.Number("1599000")}},
		json.Number("1599000"),
		now,
	)

	if err := deliverEvent(service, event); err != nil {
		t.Fatalf("consume order created: %v", err)
	}

	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
	}
	if participant.enqueued[0].Topic != commonkafka.DefaultPaymentEventsTopic {
		t.Fatalf("enqueued topic = %q, want %q", participant.enqueued[0].Topic, commonkafka.DefaultPaymentEventsTopic)
	}
	completed, ok := participant.enqueued[0].Payload.(events.PaymentCompletedEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want PaymentCompletedEvent", participant.enqueued[0].Payload)
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

func TestHighAmountOrderPublishesPaymentCompleted(t *testing.T) {
	service, repo, participant := newTestService(t)
	now := time.Date(2026, 4, 13, 13, 0, 0, 0, time.UTC)

	event := events.NewOrderCreatedEvent(
		"ORDER-HIGH",
		"CUST-999",
		"9 Premium Avenue",
		"corr-high",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 10, Price: json.Number("1000.00")}},
		json.Number("10000.00"),
		now,
	)

	if err := deliverEvent(service, event); err != nil {
		t.Fatalf("consume high amount order created: %v", err)
	}

	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
	}
	if _, ok := participant.enqueued[0].Payload.(events.PaymentCompletedEvent); !ok {
		t.Fatalf("enqueued payload type = %T, want PaymentCompletedEvent", participant.enqueued[0].Payload)
	}
	payment, ok, err := repo.GetByOrderID(context.Background(), event.OrderID)
	if err != nil {
		t.Fatalf("get payment by order id: %v", err)
	}
	if !ok || payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment = %+v, found=%v", payment, ok)
	}
}

func TestInsufficientDepositBalancePublishesPaymentFailed(t *testing.T) {
	service, repo, participant := newTestService(t)
	service.SetDepositBalance(big.NewRat(50, 1))
	now := time.Date(2026, 4, 13, 13, 30, 0, 0, time.UTC)
	event := events.NewOrderCreatedEvent(
		"ORDER-BALANCE",
		"CUST-BALANCE",
		"9 Balance Avenue",
		"corr-balance",
		[]dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1, Price: json.Number("1599000")}},
		json.Number("1599000"),
		now,
	)

	if err := deliverEvent(service, event); err != nil {
		t.Fatalf("consume order created: %v", err)
	}

	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
	}
	failed, ok := participant.enqueued[0].Payload.(events.PaymentFailedEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want PaymentFailedEvent", participant.enqueued[0].Payload)
	}
	if failed.Reason != "insufficient deposit balance" {
		t.Fatalf("failure reason = %q", failed.Reason)
	}
	payment, ok, err := repo.GetByOrderID(context.Background(), event.OrderID)
	if err != nil {
		t.Fatalf("get payment by order id: %v", err)
	}
	if !ok || payment.Status != domain.PaymentStatusFailed {
		t.Fatalf("payment = %+v, found=%v", payment, ok)
	}
}

func TestConcurrentDepositDeductionsDeductOnlySuccessfulPayment(t *testing.T) {
	service, _, _ := newTestService(t)
	service.SetDepositBalance(big.NewRat(100, 1))

	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- service.CheckAndDeductBalance(big.NewRat(75, 1))
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	for ok := range results {
		if ok {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful deductions = %d, want 1", successes)
	}
	if got := service.GetDepositBalance(); got.Cmp(big.NewRat(25, 1)) != 0 {
		t.Fatalf("deposit balance = %s, want 25", got.RatString())
	}
}

func TestDuplicateOrderCreatedReplayIsSafe(t *testing.T) {
	service, repo, participant := newTestService(t)
	now := time.Date(2026, 4, 13, 14, 0, 0, 0, time.UTC)

	event := events.NewOrderCreatedEvent(
		"ORDER-DUPE",
		"CUST-002",
		"22 Replay Road",
		"corr-dupe",
		[]dto.OrderItemRequest{{ProductID: "PROD-002", ProductName: "Basic", Quantity: 2, Price: json.Number("2399000")}},
		json.Number("299.98"),
		now,
	)

	if err := deliverEvent(service, event); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if err := deliverEvent(service, event); err != nil {
		t.Fatalf("duplicate consume should be ignored: %v", err)
	}

	if len(participant.enqueued) != 1 {
		t.Fatalf("enqueued event count = %d, want 1", len(participant.enqueued))
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
	service, repo, participant := newTestService(t)
	now := time.Date(2026, 4, 13, 15, 0, 0, 0, time.UTC)
	orderEvent := events.NewOrderCreatedEvent(
		"ORDER-REFUND",
		"CUST-003",
		"7 Refund Street",
		"corr-refund",
		[]dto.OrderItemRequest{{ProductID: "PROD-003", ProductName: "Refundable", Quantity: 1, Price: json.Number("1299000")}},
		json.Number("1299000"),
		now,
	)

	if err := deliverEvent(service, orderEvent); err != nil {
		t.Fatalf("consume order created: %v", err)
	}
	refundEvent := events.NewInventoryReservationFailedEvent(orderEvent.OrderID, "PROD-003", "out of stock", now.Add(time.Minute), "corr-refund", now.Add(time.Minute))
	if err := deliverEvent(service, refundEvent); err != nil {
		t.Fatalf("consume inventory failure: %v", err)
	}

	if len(participant.enqueued) != 2 {
		t.Fatalf("enqueued event count = %d, want 2", len(participant.enqueued))
	}
	refunded, ok := participant.enqueued[1].Payload.(events.PaymentRefundedEvent)
	if !ok {
		t.Fatalf("enqueued payload type = %T, want PaymentRefundedEvent", participant.enqueued[1].Payload)
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
	db := testutil.OpenPostgres(t, testutil.DefaultChoreographyPaymentDatabaseURL, "choreography_payment_service_test", testutil.Migration{Scope: "choreography-payment-service", Dir: "choreography-saga/payment-service/db/migrations"})
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
	service.WithClock(func() time.Time { return time.Date(2026, 4, 13, 16, 0, 0, 0, time.UTC) })
	service.WithIDGenerator(sequenceIDs("pay-1", "tx-seed-1", "pay-2", "tx-seed-2", "pay-3", "tx-seed-3", "pay-4", "tx-seed-4"))
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
