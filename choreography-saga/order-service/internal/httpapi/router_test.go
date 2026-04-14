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

	"saga-pattern/choreography-saga/order-service/internal/messaging"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	ordersvc "saga-pattern/choreography-saga/order-service/internal/orders"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

func TestCreateAndPollOrderLifecycle(t *testing.T) {
	server, consumer, _, publisher := newTestServer(t)

	body := `{"customerId":"CUST-001","shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Widget","quantity":2,"price":49.99}]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	server.ServeHTTP(createResp, createReq)

	if createResp.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d, body=%s", createResp.Code, http.StatusCreated, createResp.Body.String())
	}

	var created dto.OrderResponse
	if err := json.Unmarshal(createResp.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.Status != string(dto.OrderStatusPending) {
		t.Fatalf("created status = %q, want %q", created.Status, dto.OrderStatusPending)
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(publisher.Messages()))
	}
	msg := publisher.Messages()[0]
	if msg.Topic != messaging.NewOrderTopicPublisher(publisher).Topic() {
		t.Fatalf("published topic = %q", msg.Topic)
	}

	now := time.Now().UTC()
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, created.OrderID, events.NewPaymentCompletedEvent("PAY-1", created.OrderID, json.Number("99.98"), "TX-1", now, "corr-1", now)); err != nil {
		t.Fatalf("handle payment completed: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultInventoryEventsTopic, created.OrderID, events.NewInventoryReservedEvent("RES-1", created.OrderID, []events.InventoryReservedItem{{ProductID: "PROD-001", Quantity: 2}}, now, "corr-1", now)); err != nil {
		t.Fatalf("handle inventory reserved: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultShippingEventsTopic, created.OrderID, events.NewShippingScheduledEvent("SHIP-1", created.OrderID, "TRACK-1", "123 Main Street", now.Add(24*time.Hour), now, "corr-1", now)); err != nil {
		t.Fatalf("handle shipping scheduled: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/orders/"+created.OrderID, nil)
	getResp := httptest.NewRecorder()
	server.ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getResp.Code, http.StatusOK)
	}
	var fetched dto.OrderResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if fetched.Status != string(dto.OrderStatusCompleted) {
		t.Fatalf("final status = %q, want %q", fetched.Status, dto.OrderStatusCompleted)
	}
}

func TestDuplicateCreateOrderUsesIdempotencyKey(t *testing.T) {
	server, _, _, publisher := newTestServer(t)
	body := `{"customerId":"CUST-001","shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":49.99}]}`

	first := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	first.Header.Set("Content-Type", "application/json")
	first.Header.Set(idempotencyHeader, "IDEM-123")
	firstResp := httptest.NewRecorder()
	server.ServeHTTP(firstResp, first)

	second := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	second.Header.Set("Content-Type", "application/json")
	second.Header.Set(idempotencyHeader, "IDEM-123")
	secondResp := httptest.NewRecorder()
	server.ServeHTTP(secondResp, second)

	if firstResp.Code != http.StatusCreated {
		t.Fatalf("first create status = %d", firstResp.Code)
	}
	if secondResp.Code != http.StatusOK {
		t.Fatalf("second create status = %d", secondResp.Code)
	}

	var firstOrder dto.OrderResponse
	if err := json.Unmarshal(firstResp.Body.Bytes(), &firstOrder); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	var secondOrder dto.OrderResponse
	if err := json.Unmarshal(secondResp.Body.Bytes(), &secondOrder); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if firstOrder.OrderID != secondOrder.OrderID {
		t.Fatalf("duplicate order ids differ: %q != %q", firstOrder.OrderID, secondOrder.OrderID)
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(publisher.Messages()))
	}
}

func TestDownstreamTerminalEventsUpdateOrders(t *testing.T) {
	server, consumer, _, _ := newTestServer(t)
	orderID := createOrderForTest(t, server)
	now := time.Now().UTC()

	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderID, events.NewPaymentFailedEvent("", orderID, "card declined", now, "corr-2", now)); err != nil {
		t.Fatalf("handle payment failed: %v", err)
	}
	if err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, orderID, events.NewPaymentFailedEvent("", orderID, "card declined", now, "corr-2", now)); err != nil {
		t.Fatalf("replay payment failed should be ignored, got %v", err)
	}

	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/orders/"+orderID, nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("get status = %d", resp.Code)
	}
	var payload dto.OrderResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Status != string(dto.OrderStatusCancelled) {
		t.Fatalf("status = %q, want %q", payload.Status, dto.OrderStatusCancelled)
	}
}

func TestPrometheusEndpointExposesRequiredOrderMetrics(t *testing.T) {
	server, consumer, _, _ := newTestServer(t)
	orderID := createOrderForTest(t, server)
	now := time.Now().UTC()
	if err := deliverEvent(consumer, commonkafka.DefaultShippingEventsTopic, orderID, events.NewShippingFailedEvent(orderID, "courier error", now, "corr-3", now)); err != nil {
		t.Fatalf("handle shipping failed: %v", err)
	}

	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/actuator/prometheus", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("prometheus status = %d", resp.Code)
	}
	body := resp.Body.String()
	for _, metricName := range []string{
		"saga_orders_created_total",
		"saga_orders_completed_total",
		"saga_orders_failed_total",
		"saga_compensations_total",
		"saga_order_processing_time_seconds",
	} {
		if !strings.Contains(body, metricName) {
			t.Fatalf("metrics body missing %s", metricName)
		}
	}
}

func TestDownstreamConsumerRejectsTopicEventMismatch(t *testing.T) {
	_, consumer, _, _ := newTestServer(t)
	now := time.Now().UTC()
	err := deliverEvent(consumer, commonkafka.DefaultPaymentEventsTopic, "ORDER-1", events.NewShippingFailedEvent("ORDER-1", "bad route", now, "corr-4", now))
	if err == nil || !strings.Contains(err.Error(), "not allowed on topic") {
		t.Fatalf("topic/event mismatch error = %v, want mismatch rejection", err)
	}
}

func newTestServer(t *testing.T) (http.Handler, *messaging.DownstreamConsumer, *ordersvc.Service, *messaging.RecordingPublisher) {
	t.Helper()
	repo := repository.NewMemoryRepository()
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	publisher := messaging.NewRecordingPublisher()
	service, err := ordersvc.NewService(repo, messaging.NewOrderTopicPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer, err := messaging.NewDownstreamConsumer(service, logger)
	if err != nil {
		t.Fatalf("new downstream consumer: %v", err)
	}
	handler := NewHandler(HandlerDependencies{
		Config:   commonconfig.ServiceConfig{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger:   logger,
		Orders:   service,
		Registry: metrics.Registry(),
	})
	return handler, consumer, service, publisher
}

func createOrderForTest(t *testing.T, server http.Handler) string {
	t.Helper()
	body := []byte(`{"customerId":"CUST-001","shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":49.99}]}`)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/orders", bytes.NewReader(body)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	var payload dto.OrderResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	return payload.OrderID
}

func deliverEvent(consumer *messaging.DownstreamConsumer, topic string, key string, event events.ChoreographyEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return consumer.Consume(context.Background(), messaging.DownstreamEnvelope{Topic: topic, Key: key, Value: payload})
}
