package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	commonconfig "saga-pattern/common/config"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	frameworkruntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/messaging"
	"saga-pattern/orchestration-saga/order-service/internal/observability"
	ordersvc "saga-pattern/orchestration-saga/order-service/internal/orders"
	"saga-pattern/orchestration-saga/order-service/internal/repository"
)

func TestCreateOrderStartsSagaAndCompletes(t *testing.T) {
	server, service, consumer, publisher := newTestServer(t)

	createResp := createOrder(t, server)
	if createResp.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, want %d, body=%s", createResp.Code, http.StatusAccepted, createResp.Body.String())
	}
	var accepted struct {
		OrderID string `json:"orderId"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(createResp.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("decode accepted response: %v", err)
	}
	if accepted.Status != "SAGA_STARTED" {
		t.Fatalf("accepted status = %q", accepted.Status)
	}

	assertPublishStep(t, service, publisher, 0, commonkafka.DefaultPaymentCommandsTopic, "PROCESS_PAYMENT")
	deliverReply(t, consumer, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, consumer, accepted.OrderID, "reply-inventory", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReservedReply("RES-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultShippingCommandsTopic, "SCHEDULE_SHIPPING")
	deliverReply(t, consumer, accepted.OrderID, "reply-shipping", commonkafka.DefaultShippingRepliesTopic, commonreplies.NewShippingScheduledReply("SHIP-1", accepted.OrderID))

	getResp := httptest.NewRecorder()
	server.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if getResp.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d body=%s", getResp.Code, http.StatusOK, getResp.Body.String())
	}
	if !strings.Contains(getResp.Body.String(), `"status":"COMPLETED"`) {
		t.Fatalf("expected completed order, body=%s", getResp.Body.String())
	}

	promResp := httptest.NewRecorder()
	server.ServeHTTP(promResp, httptest.NewRequest(http.MethodGet, "/actuator/prometheus", nil))
	if promResp.Code != http.StatusOK {
		t.Fatalf("prometheus status = %d", promResp.Code)
	}
	for _, metricName := range []string{
		"saga_orders_created_total",
		"saga_orders_completed_total",
		"saga_orders_failed_total",
		"saga_total_duration_seconds",
		"saga_compensations_total",
		"saga_compensations_payment_total",
		"saga_compensations_inventory_total",
		"saga_compensations_shipping_total",
		"saga_framework_duration_seconds",
		"saga_framework_step_duration_seconds",
		"saga_framework_compensation_started_total",
		"saga_framework_compensation_completed_total",
	} {
		if !strings.Contains(promResp.Body.String(), metricName) {
			t.Fatalf("missing metric %s", metricName)
		}
	}
}

func TestShippingFailureTriggersCompensationSequence(t *testing.T) {
	server, service, consumer, publisher := newTestServer(t)
	accepted := decodeAcceptedResponse(t, createOrder(t, server).Body.Bytes())

	assertPublishStep(t, service, publisher, 0, commonkafka.DefaultPaymentCommandsTopic, "PROCESS_PAYMENT")
	deliverReply(t, consumer, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, consumer, accepted.OrderID, "reply-inventory", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReservedReply("RES-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultShippingCommandsTopic, "SCHEDULE_SHIPPING")
	deliverReply(t, consumer, accepted.OrderID, "reply-shipping-failed", commonkafka.DefaultShippingRepliesTopic, commonreplies.NewShippingFailedReply("SHIP-1", accepted.OrderID, "courier rejected route"))
	assertPublishStep(t, service, publisher, 3, commonkafka.DefaultInventoryCommandsTopic, "RELEASE_INVENTORY")
	deliverReply(t, consumer, accepted.OrderID, "reply-inventory-released", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReleasedReply("RES-1", accepted.OrderID, true, ""))
	assertPublishStep(t, service, publisher, 4, commonkafka.DefaultPaymentCommandsTopic, "REFUND_PAYMENT")
	deliverReply(t, consumer, accepted.OrderID, "reply-payment-refunded", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentRefundedReply("PAY-1", accepted.OrderID, true, ""))

	getResp := httptest.NewRecorder()
	server.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if getResp.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d body=%s", getResp.Code, http.StatusOK, getResp.Body.String())
	}
	if !strings.Contains(getResp.Body.String(), `"status":"CANCELLED"`) {
		t.Fatalf("expected cancelled order, body=%s", getResp.Body.String())
	}

	messageTypes := make([]string, 0, len(publisher.Messages()))
	for _, message := range publisher.Messages() {
		messageTypes = append(messageTypes, message.Message.MessageType)
	}
	want := []string{"PROCESS_PAYMENT", "RESERVE_INVENTORY", "SCHEDULE_SHIPPING", "RELEASE_INVENTORY", "REFUND_PAYMENT"}
	if strings.Join(messageTypes, ",") != strings.Join(want, ",") {
		t.Fatalf("message types = %v, want %v", messageTypes, want)
	}
}

func TestTemporary404PollingStillSettlesToTerminalState(t *testing.T) {
	server, service, consumer, publisher := newTestServer(t)
	accepted := decodeAcceptedResponse(t, createOrder(t, server).Body.Bytes())

	missingResp := httptest.NewRecorder()
	server.ServeHTTP(missingResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if missingResp.Code != http.StatusNotFound {
		t.Fatalf("initial poll status = %d, want %d body=%s", missingResp.Code, http.StatusNotFound, missingResp.Body.String())
	}

	assertPublishStep(t, service, publisher, 0, commonkafka.DefaultPaymentCommandsTopic, "PROCESS_PAYMENT")
	deliverReply(t, consumer, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, consumer, accepted.OrderID, "reply-inventory", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReservedReply("RES-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultShippingCommandsTopic, "SCHEDULE_SHIPPING")
	deliverReply(t, consumer, accepted.OrderID, "reply-shipping", commonkafka.DefaultShippingRepliesTopic, commonreplies.NewShippingScheduledReply("SHIP-1", accepted.OrderID))

	finalResp := httptest.NewRecorder()
	server.ServeHTTP(finalResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if finalResp.Code != http.StatusOK {
		t.Fatalf("final poll status = %d, want %d body=%s", finalResp.Code, http.StatusOK, finalResp.Body.String())
	}
	if !strings.Contains(finalResp.Body.String(), `"status":"COMPLETED"`) {
		t.Fatalf("expected completed terminal state, body=%s", finalResp.Body.String())
	}
}

func newTestServer(t *testing.T) (http.Handler, *ordersvc.Service, *messaging.ReplyConsumer, *messaging.RecordingPublisher) {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	publisher := messaging.NewRecordingPublisher()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	runtimeIDs := []string{"worker-1", "hist-1", "outbox-1", "hist-2", "outbox-2", "hist-3", "outbox-3", "hist-4", "outbox-4", "hist-5", "outbox-5", "hist-6", "outbox-6", "hist-7", "outbox-7"}
	runtimeIdx := 0
	runtime := frameworkruntime.NewInMemory(frameworkruntime.OrderDefinition(), frameworkruntime.InMemoryDependencies{
		Publisher:       publisher,
		MetricsRegistry: registry,
		Clock:           func() time.Time { return now },
		IDGenerator: func() string {
			value := runtimeIDs[runtimeIdx]
			runtimeIdx++
			return value
		},
		WorkerID: "worker-a",
	})
	service, err := ordersvc.NewService(repository.NewMemoryRepository(), runtime, metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return now })
	ids := []string{"ORDER-1", "PAY-1", "RES-1", "SHIP-1"}
	idx := 0
	service.WithIDGenerator(func() string {
		value := ids[idx]
		idx++
		return value
	})
	consumer, err := messaging.NewReplyConsumer(service, []string{commonkafka.DefaultPaymentRepliesTopic, commonkafka.DefaultInventoryRepliesTopic, commonkafka.DefaultShippingRepliesTopic})
	if err != nil {
		t.Fatalf("new reply consumer: %v", err)
	}
	handler := NewHandler(HandlerDependencies{
		Config:   commonconfig.ServiceConfig{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Orders:   service,
		Registry: metrics.Registry(),
	})
	return handler, service, consumer, publisher
}

func createOrder(t *testing.T, server http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	body := []byte(`{"customerId":"CUST-001","totalAmount":99.99,"shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":99.99}]}`)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/orders", bytes.NewReader(body)))
	return resp
}

func decodeAcceptedResponse(t *testing.T, payload []byte) struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
} {
	t.Helper()
	var accepted struct {
		OrderID string `json:"orderId"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(payload, &accepted); err != nil {
		t.Fatalf("decode accepted response: %v", err)
	}
	return accepted
}

func assertPublishStep(t *testing.T, service *ordersvc.Service, publisher *messaging.RecordingPublisher, index int, topic string, messageType string) {
	t.Helper()
	if err := service.PublishPending(context.Background()); err != nil {
		t.Fatalf("publish pending: %v", err)
	}
	messages := publisher.Messages()
	if len(messages) <= index {
		t.Fatalf("published messages len = %d, want > %d", len(messages), index)
	}
	message := messages[index].Message
	if message.Topic != topic || message.MessageType != messageType {
		t.Fatalf("published message[%d] = (%s,%s), want (%s,%s)", index, message.Topic, message.MessageType, topic, messageType)
	}
}

func deliverReply(t *testing.T, consumer *messaging.ReplyConsumer, sagaID string, replyID string, topic string, reply commonreplies.SagaReply) {
	t.Helper()
	payload, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	if err := consumer.Consume(context.Background(), messaging.ReplyEnvelope{ReplyID: replyID, SagaID: sagaID, Topic: topic, ReceivedAt: time.Now().UTC(), Payload: payload}); err != nil {
		t.Fatalf("deliver reply: %v", err)
	}
}
