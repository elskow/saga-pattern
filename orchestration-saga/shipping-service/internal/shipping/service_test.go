package shipping

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
	"saga-pattern/orchestration-saga/shipping-service/internal/messaging"
	"saga-pattern/orchestration-saga/shipping-service/internal/observability"
	"saga-pattern/orchestration-saga/shipping-service/internal/repository"
)

func TestScheduleShippingPublishesSuccessReply(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	command := commands.NewScheduleShippingCommand("SHIP-1", "ORDER-1", "123 Main Street")

	if err := deliverCommand(consumer, command.OrderID, command); err != nil {
		t.Fatalf("deliver schedule shipping command: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultShippingRepliesTopic, command.OrderID)
	reply, ok := publisher.Messages()[0].Body.(commonreplies.ShippingScheduledReply)
	if !ok {
		t.Fatalf("reply type = %T, want ShippingScheduledReply", publisher.Messages()[0].Body)
	}
	if reply.ShippingID != command.ShippingID || reply.OrderID != command.OrderID {
		t.Fatalf("scheduled reply = %+v", reply)
	}
	shipment, found, err := repo.GetByShippingID(context.Background(), command.ShippingID)
	if err != nil {
		t.Fatalf("get shipment: %v", err)
	}
	if !found || shipment.Status != domain.ShipmentStatusScheduled {
		t.Fatalf("shipment = %+v, found=%v", shipment, found)
	}
}

func TestScheduleShippingFailurePublishesFailedReply(t *testing.T) {
	consumer, repo, publisher := newTestService(t)
	command := commands.NewScheduleShippingCommand("SHIP-FAIL-1", "ORDER-FAIL-1", "FAIL_SHIPPING requested")

	if err := deliverCommand(consumer, command.OrderID, command); err != nil {
		t.Fatalf("deliver failing schedule shipping command: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultShippingRepliesTopic, command.OrderID)
	reply, ok := publisher.Messages()[0].Body.(commonreplies.ShippingFailedReply)
	if !ok {
		t.Fatalf("reply type = %T, want ShippingFailedReply", publisher.Messages()[0].Body)
	}
	if reply.ShippingID != command.ShippingID || reply.OrderID != command.OrderID {
		t.Fatalf("failed reply = %+v", reply)
	}
	if reply.Reason != "shipping simulation requested failure" {
		t.Fatalf("failure reason = %q", reply.Reason)
	}
	shipment, found, err := repo.GetByShippingID(context.Background(), command.ShippingID)
	if err != nil {
		t.Fatalf("get failed shipment: %v", err)
	}
	if !found || shipment.Status != domain.ShipmentStatusFailed {
		t.Fatalf("shipment = %+v, found=%v", shipment, found)
	}
}

func TestCancelShippingCompensationIsIdempotent(t *testing.T) {
	consumer, repo, publisher := newSequencedClockService(t)
	schedule := commands.NewScheduleShippingCommand("SHIP-CANCEL-1", "ORDER-CANCEL-1", "123 Main Street")
	cancel := commands.NewCancelShippingCommand(schedule.ShippingID, schedule.OrderID)

	if err := deliverCommand(consumer, schedule.OrderID, schedule); err != nil {
		t.Fatalf("deliver schedule shipping command: %v", err)
	}
	if err := deliverCommand(consumer, cancel.OrderID, cancel); err != nil {
		t.Fatalf("deliver first cancel shipping command: %v", err)
	}
	firstShipment, found, err := repo.GetByShippingID(context.Background(), schedule.ShippingID)
	if err != nil {
		t.Fatalf("get shipment after first cancel: %v", err)
	}
	if !found || firstShipment.Status != domain.ShipmentStatusCancelled {
		t.Fatalf("shipment after first cancel = %+v, found=%v", firstShipment, found)
	}
	if firstShipment.CancelledAt.IsZero() {
		t.Fatalf("expected cancelled timestamp after first cancel")
	}
	if err := deliverCommand(consumer, cancel.OrderID, cancel); err != nil {
		t.Fatalf("deliver duplicate cancel shipping command: %v", err)
	}
	secondShipment, found, err := repo.GetByShippingID(context.Background(), schedule.ShippingID)
	if err != nil {
		t.Fatalf("get shipment after duplicate cancel: %v", err)
	}
	if !found || secondShipment.Status != domain.ShipmentStatusCancelled {
		t.Fatalf("shipment after duplicate cancel = %+v, found=%v", secondShipment, found)
	}
	if !secondShipment.CancelledAt.Equal(firstShipment.CancelledAt) {
		t.Fatalf("cancelled timestamp changed on duplicate cancel: first=%s second=%s", firstShipment.CancelledAt, secondShipment.CancelledAt)
	}
	if len(publisher.Messages()) != 3 {
		t.Fatalf("published messages = %d, want 3", len(publisher.Messages()))
	}
	for i := 1; i <= 2; i++ {
		reply, ok := publisher.Messages()[i].Body.(commonreplies.ShippingCancelledReply)
		if !ok {
			t.Fatalf("reply[%d] type = %T, want ShippingCancelledReply", i, publisher.Messages()[i].Body)
		}
		if !reply.Success {
			t.Fatalf("reply[%d] success = false, reason=%q", i, reply.Reason)
		}
	}
}

func newTestService(t *testing.T) (*messaging.CommandConsumer, repository.Repository, *messaging.RecordingPublisher) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	repo := repository.NewMemoryRepository()
	publisher := messaging.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewReplyPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC) })
	consumer, err := messaging.NewCommandConsumer(service)
	if err != nil {
		t.Fatalf("new command consumer: %v", err)
	}
	return consumer, repo, publisher
}

func newSequencedClockService(t *testing.T) (*messaging.CommandConsumer, repository.Repository, *messaging.RecordingPublisher) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	repo := repository.NewMemoryRepository()
	publisher := messaging.NewRecordingPublisher()
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
	return consumer.Consume(context.Background(), messaging.CommandEnvelope{Topic: commonkafka.DefaultShippingCommandsTopic, Key: key, Value: payload})
}

func assertSingleReplyMessage(t *testing.T, publisher *messaging.RecordingPublisher, topic, key string) {
	t.Helper()
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published messages = %d, want 1", len(publisher.Messages()))
	}
	message := publisher.Messages()[0]
	if message.Topic != topic || message.Key != key {
		t.Fatalf("published message = (%s,%s), want (%s,%s)", message.Topic, message.Key, topic, key)
	}
}
