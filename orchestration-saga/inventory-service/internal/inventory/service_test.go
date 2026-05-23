package inventory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/commands"
	"saga-pattern/common/dto"
	"saga-pattern/common/faultinjection"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/common/testutil"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
	"saga-pattern/orchestration-saga/inventory-service/internal/messaging"
	"saga-pattern/orchestration-saga/inventory-service/internal/observability"
	"saga-pattern/orchestration-saga/inventory-service/internal/repository"
)

func TestReserveInventoryPublishesReservedReply(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	command := commands.NewReserveInventoryCommand("RES-1", "ORDER-1", []dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 2, Price: json.Number("15999000")}})

	if err := deliverCommand(consumer, command.OrderID, command); err != nil {
		t.Fatalf("deliver reserve inventory command: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultInventoryRepliesTopic, command.OrderID)
	reply, ok := publisher.Messages()[0].Body.(commonreplies.InventoryReservedReply)
	if !ok {
		t.Fatalf("reply type = %T, want InventoryReservedReply", publisher.Messages()[0].Body)
	}
	if reply.ReservationID != command.ReservationID || reply.OrderID != command.OrderID {
		t.Fatalf("reserved reply = %+v", reply)
	}
	product, found, err := repo.Product(context.Background(), "PROD-001")
	if err != nil {
		t.Fatalf("get product: %v", err)
	}
	if !found || product.AvailableQuantity() != 98 || product.ReservedQuantity != 2 {
		t.Fatalf("product = %+v, found=%v", product, found)
	}
	reservation, found, err := repo.GetReservation(context.Background(), command.ReservationID)
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if !found || reservation.Status != domain.ReservationStatusReserved {
		t.Fatalf("reservation = %+v, found=%v", reservation, found)
	}
}

func TestInsufficientStockPublishesInventoryFailedReply(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	command := commands.NewReserveInventoryCommand("RES-LOW-1", "ORDER-LOW-1", []dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1000, Price: json.Number("15999000")}})

	if err := deliverCommand(consumer, command.OrderID, command); err != nil {
		t.Fatalf("deliver insufficient stock reserve command: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultInventoryRepliesTopic, command.OrderID)
	reply, ok := publisher.Messages()[0].Body.(commonreplies.InventoryFailedReply)
	if !ok {
		t.Fatalf("reply type = %T, want InventoryFailedReply", publisher.Messages()[0].Body)
	}
	if reply.OrderID != command.OrderID || reply.ReservationID != command.ReservationID {
		t.Fatalf("failed reply = %+v", reply)
	}
	if reply.Reason != "insufficient stock for product PROD-001: requested 1000, available 100" {
		t.Fatalf("failure reason = %q", reply.Reason)
	}
	product, found, err := repo.Product(context.Background(), "PROD-001")
	if err != nil {
		t.Fatalf("get insufficient stock product: %v", err)
	}
	if !found || product.AvailableQuantity() != 100 || product.ReservedQuantity != 0 {
		t.Fatalf("product = %+v, found=%v", product, found)
	}
	reservation, found, err := repo.GetReservation(context.Background(), command.ReservationID)
	if err != nil {
		t.Fatalf("get failed reservation: %v", err)
	}
	if !found || reservation.Status != domain.ReservationStatusFailed {
		t.Fatalf("reservation = %+v, found=%v", reservation, found)
	}
}

func TestReserveInventoryFailureModeFailNextAutoDisables(t *testing.T) {
	consumer, repo, publisher, service := newTestServiceWithService(t)
	state := service.ConfigureFailureMode(faultinjection.Config{Enabled: true, RunLabel: "thesis-run-1", FailNext: 1})
	if !state.Enabled || state.RunLabel != "thesis-run-1" || state.Remaining != 1 {
		t.Fatalf("failure state = %+v", state)
	}

	first := commands.NewReserveInventoryCommand("RES-FAIL-NEXT", "ORDER-FAIL-NEXT", []dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1, Price: json.Number("15999000")}})
	second := commands.NewReserveInventoryCommand("RES-SUCCESS-NEXT", "ORDER-SUCCESS-NEXT", []dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1, Price: json.Number("15999000")}})
	if err := deliverCommand(consumer, first.OrderID, first); err != nil {
		t.Fatalf("deliver first reserve inventory command: %v", err)
	}
	if err := deliverCommand(consumer, second.OrderID, second); err != nil {
		t.Fatalf("deliver second reserve inventory command: %v", err)
	}

	if service.FailureModeEnabled() {
		t.Fatalf("failure mode should auto-disable after failNext is exhausted")
	}
	if len(publisher.Messages()) != 2 {
		t.Fatalf("published messages = %d, want 2", len(publisher.Messages()))
	}
	if _, ok := publisher.Messages()[0].Body.(commonreplies.InventoryFailedReply); !ok {
		t.Fatalf("first reply type = %T, want InventoryFailedReply", publisher.Messages()[0].Body)
	}
	if _, ok := publisher.Messages()[1].Body.(commonreplies.InventoryReservedReply); !ok {
		t.Fatalf("second reply type = %T, want InventoryReservedReply", publisher.Messages()[1].Body)
	}
	reservation, found, err := repo.GetReservation(context.Background(), second.ReservationID)
	if err != nil {
		t.Fatalf("get second reservation: %v", err)
	}
	if !found || reservation.Status != domain.ReservationStatusReserved {
		t.Fatalf("second reservation = %+v, found=%v", reservation, found)
	}
}

func TestReleaseInventoryCompensationIsIdempotent(t *testing.T) {
	consumer, repo, publisher := newSequencedClockService(t)
	reserve := commands.NewReserveInventoryCommand("RES-REL-1", "ORDER-REL-1", []dto.OrderItemRequest{{ProductID: "PROD-002", ProductName: "Smartphone", Quantity: 3, Price: json.Number("2399000")}})
	release := commands.NewReleaseInventoryCommand(reserve.ReservationID, reserve.OrderID)

	if err := deliverCommand(consumer, reserve.OrderID, reserve); err != nil {
		t.Fatalf("deliver reserve inventory command: %v", err)
	}
	if err := deliverCommand(consumer, release.OrderID, release); err != nil {
		t.Fatalf("deliver first release command: %v", err)
	}
	firstReservation, found, err := repo.GetReservation(context.Background(), reserve.ReservationID)
	if err != nil {
		t.Fatalf("get reservation after first release: %v", err)
	}
	if !found || firstReservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation after first release = %+v, found=%v", firstReservation, found)
	}
	if err := deliverCommand(consumer, release.OrderID, release); err != nil {
		t.Fatalf("deliver duplicate release command: %v", err)
	}
	secondReservation, found, err := repo.GetReservation(context.Background(), reserve.ReservationID)
	if err != nil {
		t.Fatalf("get reservation after duplicate release: %v", err)
	}
	if !found || secondReservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation after duplicate release = %+v, found=%v", secondReservation, found)
	}
	if !secondReservation.ReleasedAt.Equal(firstReservation.ReleasedAt) {
		t.Fatalf("released timestamp changed on duplicate release: first=%s second=%s", firstReservation.ReleasedAt, secondReservation.ReleasedAt)
	}
	if len(publisher.Messages()) != 3 {
		t.Fatalf("published messages = %d, want 3", len(publisher.Messages()))
	}
	for i := 1; i <= 2; i++ {
		reply, ok := publisher.Messages()[i].Body.(commonreplies.InventoryReleasedReply)
		if !ok {
			t.Fatalf("reply[%d] type = %T, want InventoryReleasedReply", i, publisher.Messages()[i].Body)
		}
		if !reply.Success {
			t.Fatalf("reply[%d] success = false, reason=%q", i, reply.Reason)
		}
	}
	product, found, err := repo.Product(context.Background(), "PROD-002")
	if err != nil {
		t.Fatalf("get product after release: %v", err)
	}
	if !found || product.AvailableQuantity() != 200 || product.ReservedQuantity != 0 {
		t.Fatalf("product after release = %+v, found=%v", product, found)
	}
}

func newTestService(t *testing.T) (*messaging.CommandConsumer, repository.Repository, *testutil.RecordingPublisher) {
	t.Helper()
	consumer, repo, publisher, _ := newTestServiceWithService(t)
	return consumer, repo, publisher
}

func newTestServiceWithService(t *testing.T) (*messaging.CommandConsumer, repository.Repository, *testutil.RecordingPublisher, *Service) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	db := testutil.OpenPostgres(t, testutil.DefaultInventoryDatabaseURL, "inventory_service_test", testutil.Migration{Scope: "orchestration-inventory-service", Dir: "orchestration-saga/inventory-service/db/migrations"})
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	publisher := testutil.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewReplyPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC) })
	consumer, err := messaging.NewCommandConsumer(service)
	if err != nil {
		t.Fatalf("new command consumer: %v", err)
	}
	return consumer, repo, publisher, service
}

func newSequencedClockService(t *testing.T) (*messaging.CommandConsumer, repository.Repository, *testutil.RecordingPublisher) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	db := testutil.OpenPostgres(t, testutil.DefaultInventoryDatabaseURL, "inventory_service_sequence_test", testutil.Migration{Scope: "orchestration-inventory-service", Dir: "orchestration-saga/inventory-service/db/migrations"})
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	publisher := testutil.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewReplyPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ticks := []time.Time{
		time.Date(2026, 4, 14, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 14, 13, 1, 0, 0, time.UTC),
		time.Date(2026, 4, 14, 13, 2, 0, 0, time.UTC),
		time.Date(2026, 4, 14, 13, 3, 0, 0, time.UTC),
		time.Date(2026, 4, 14, 13, 4, 0, 0, time.UTC),
	}
	index := 0
	service.WithClock(func() time.Time {
		if index >= len(ticks) {
			return ticks[len(ticks)-1]
		}
		value := ticks[index]
		index++
		return value
	})
	consumer, err := messaging.NewCommandConsumer(service)
	if err != nil {
		t.Fatalf("new command consumer: %v", err)
	}
	return consumer, repo, publisher
}

func deliverCommand(consumer *messaging.CommandConsumer, key string, command any) error {
	payload, err := json.Marshal(command)
	if err != nil {
		return err
	}
	return consumer.Consume(context.Background(), messaging.CommandEnvelope{Topic: commonkafka.DefaultInventoryCommandsTopic, Key: key, Value: payload})
}

func assertSingleReplyMessage(t *testing.T, publisher *testutil.RecordingPublisher, topic, key string) {
	t.Helper()
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published messages = %d, want 1", len(publisher.Messages()))
	}
	message := publisher.Messages()[0]
	if message.Topic != topic || message.Key != key {
		t.Fatalf("published message = (%s,%s), want (%s,%s)", message.Topic, message.Key, topic, key)
	}
}
