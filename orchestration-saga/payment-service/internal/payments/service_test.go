package payments

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/commands"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
	"saga-pattern/orchestration-saga/payment-service/internal/messaging"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
	"saga-pattern/orchestration-saga/payment-service/internal/repository"
)

func TestProcessPaymentPublishesSuccessReply(t *testing.T) {
	service, repo, publisher := newTestService(t)
	command := commands.NewProcessPaymentCommand("PAY-1", "ORDER-1", "CUST-1", json.Number("99.99"))

	if err := service.HandleProcessPayment(context.Background(), command); err != nil {
		t.Fatalf("handle process payment: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultPaymentRepliesTopic, "ORDER-1")
	reply, ok := publisher.Messages()[0].Body.(commonreplies.PaymentCompletedReply)
	if !ok {
		t.Fatalf("reply type = %T, want PaymentCompletedReply", publisher.Messages()[0].Body)
	}
	if reply.PaymentID != "PAY-1" || reply.OrderID != "ORDER-1" {
		t.Fatalf("completed reply = %+v", reply)
	}
	payment, found, err := repo.GetByPaymentID(context.Background(), "PAY-1")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if !found || payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment = %+v, found=%v", payment, found)
	}
}

func TestPremiumProductPublishesPaymentFailedReply(t *testing.T) {
	service, repo, publisher := newTestService(t)
	command := commands.NewProcessPaymentCommand("PAY-PREMIUM", "ORDER-PREMIUM", "CUST-1", json.Number("10000.00"))

	if err := service.HandleProcessPayment(context.Background(), command); err != nil {
		t.Fatalf("handle premium process payment: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultPaymentRepliesTopic, "ORDER-PREMIUM")
	reply, ok := publisher.Messages()[0].Body.(commonreplies.PaymentFailedReply)
	if !ok {
		t.Fatalf("reply type = %T, want PaymentFailedReply", publisher.Messages()[0].Body)
	}
	if reply.OrderID != "ORDER-PREMIUM" || reply.PaymentID != "PAY-PREMIUM" {
		t.Fatalf("failed reply = %+v", reply)
	}
	if reply.Reason != "Payment amount exceeds maximum allowed limit of 10000.00" {
		t.Fatalf("failure reason = %q", reply.Reason)
	}
	payment, found, err := repo.GetByPaymentID(context.Background(), "PAY-PREMIUM")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if !found || payment.Status != domain.PaymentStatusFailed {
		t.Fatalf("payment = %+v, found=%v", payment, found)
	}
}

func TestRefundPaymentIsIdempotent(t *testing.T) {
	service, repo, publisher := newTestService(t)
	process := commands.NewProcessPaymentCommand("PAY-REFUND", "ORDER-REFUND", "CUST-1", json.Number("49.99"))
	if err := service.HandleProcessPayment(context.Background(), process); err != nil {
		t.Fatalf("handle process payment: %v", err)
	}
	refund := commands.NewRefundPaymentCommand("PAY-REFUND", "ORDER-REFUND")
	if err := service.HandleRefundPayment(context.Background(), refund); err != nil {
		t.Fatalf("first refund: %v", err)
	}
	firstState, found, err := repo.GetByPaymentID(context.Background(), "PAY-REFUND")
	if err != nil {
		t.Fatalf("get payment after first refund: %v", err)
	}
	if !found || firstState.Status != domain.PaymentStatusRefunded {
		t.Fatalf("payment after first refund = %+v, found=%v", firstState, found)
	}
	if firstState.RefundedAt.IsZero() {
		t.Fatalf("expected refunded timestamp after first refund")
	}
	if err := service.HandleRefundPayment(context.Background(), refund); err != nil {
		t.Fatalf("second refund: %v", err)
	}
	secondState, found, err := repo.GetByPaymentID(context.Background(), "PAY-REFUND")
	if err != nil {
		t.Fatalf("get payment after second refund: %v", err)
	}
	if !found || secondState.Status != domain.PaymentStatusRefunded {
		t.Fatalf("payment after second refund = %+v, found=%v", secondState, found)
	}
	if !secondState.RefundedAt.Equal(firstState.RefundedAt) {
		t.Fatalf("refunded timestamp changed on duplicate refund: first=%s second=%s", firstState.RefundedAt, secondState.RefundedAt)
	}
	if len(publisher.Messages()) != 3 {
		t.Fatalf("published messages = %d, want 3", len(publisher.Messages()))
	}
	for i := 1; i <= 2; i++ {
		reply, ok := publisher.Messages()[i].Body.(commonreplies.PaymentRefundedReply)
		if !ok {
			t.Fatalf("reply[%d] type = %T, want PaymentRefundedReply", i, publisher.Messages()[i].Body)
		}
		if !reply.Success {
			t.Fatalf("reply[%d] success = false", i)
		}
	}
}

func newTestService(t *testing.T) (*Service, *repository.MemoryRepository, *messaging.RecordingPublisher) {
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
	return service, repo, publisher
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
