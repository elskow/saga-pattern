package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	choreoruntime "saga-pattern/choreography-framework/runtime"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	ordersvc "saga-pattern/choreography-saga/order-service/internal/orders"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commonconfig "saga-pattern/common/config"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	"saga-pattern/common/inventorycatalog"
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/common/testutil"
)

func TestCreateAndPollOrderLifecycle(t *testing.T) {
	server, service, publisher := newTestServer(t)

	body := `{"customerId":"CUST-001","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":2,"price":799000}]}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq = createReq.WithContext(commoncontext.With(createReq.Context(), commoncontext.Data{RequestID: "request-1", CorrelationID: "http-correlation"}))
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
	if msg.Topic != commonkafka.DefaultOrderEventsTopic {
		t.Fatalf("published topic = %q, want %q", msg.Topic, commonkafka.DefaultOrderEventsTopic)
	}
	if msg.Key != created.OrderID {
		t.Fatalf("published key = %q, want %q", msg.Key, created.OrderID)
	}
	if msg.EventType != events.TypeOrderCreated {
		t.Fatalf("published event type = %q, want %q", msg.EventType, events.TypeOrderCreated)
	}
	var publishedEvent events.OrderCreatedEvent
	if err := json.Unmarshal(msg.Payload, &publishedEvent); err != nil {
		t.Fatalf("unmarshal published event: %v", err)
	}
	if publishedEvent.OrderID != created.OrderID {
		t.Fatalf("published order id = %q, want %q", publishedEvent.OrderID, created.OrderID)
	}

	now := time.Now().UTC()
	deliverEvent(t, service, events.NewPaymentCompletedEvent("PAY-1", created.OrderID, json.Number("99.98"), "TX-1", now, "corr-1", now))
	deliverEvent(t, service, events.NewInventoryReservedEvent("RES-1", created.OrderID, []events.InventoryReservedItem{{ProductID: "PROD-001", Quantity: 2}}, now, "corr-1", now))
	deliverEvent(t, service, events.NewShippingScheduledEvent("SHIP-1", created.OrderID, "TRACK-1", "Jl. Ketintang Wiyata, Surabaya 60231", now.Add(24*time.Hour), now, "corr-1", now))

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
	server, _, publisher := newTestServer(t)
	body := `{"customerId":"CUST-001","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`

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

func TestDuplicateCreateOrderRejectsDifferentPayloadForSameIdempotencyKey(t *testing.T) {
	server, _, publisher := newTestServer(t)
	firstBody := `{"customerId":"CUST-001","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`
	secondBody := `{"customerId":"CUST-002","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`

	first := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(firstBody))
	first.Header.Set("Content-Type", "application/json")
	first.Header.Set(idempotencyHeader, "IDEM-CONFLICT")
	firstResp := httptest.NewRecorder()
	server.ServeHTTP(firstResp, first)

	second := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(secondBody))
	second.Header.Set("Content-Type", "application/json")
	second.Header.Set(idempotencyHeader, "IDEM-CONFLICT")
	secondResp := httptest.NewRecorder()
	server.ServeHTTP(secondResp, second)

	if firstResp.Code != http.StatusCreated {
		t.Fatalf("first create status = %d", firstResp.Code)
	}
	if secondResp.Code != http.StatusConflict {
		t.Fatalf("second create status = %d, want %d, body=%s", secondResp.Code, http.StatusConflict, secondResp.Body.String())
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(publisher.Messages()))
	}
}

func TestCreateOrderRejectsInsufficientAvailabilityBeforePublishing(t *testing.T) {
	server, _, publisher := newTestServerWithCatalog(t, fakeCatalogResolver{err: inventorycatalog.InsufficientAvailabilityError{ProductID: "PROD-001", Requested: 1, Available: 0}})
	body := `{"customerId":"CUST-001","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`

	req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	if resp.Code != http.StatusConflict {
		t.Fatalf("create status = %d, want %d, body=%s", resp.Code, http.StatusConflict, resp.Body.String())
	}
	if len(publisher.Messages()) != 0 {
		t.Fatalf("published message count = %d, want 0", len(publisher.Messages()))
	}
}

func TestCreateOrderRejectsJudgeCustomerInsufficientAvailabilityBeforePublishing(t *testing.T) {
	server, _, publisher := newTestServerWithCatalog(t, fakeCatalogResolver{err: inventorycatalog.InsufficientAvailabilityError{ProductID: "PROD-001", Requested: 1, Available: 0}})
	body := `{"customerId":"CUST-JUDGE-001","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`

	req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	if resp.Code != http.StatusConflict {
		t.Fatalf("create status = %d, want %d, body=%s", resp.Code, http.StatusConflict, resp.Body.String())
	}
	if len(publisher.Messages()) != 0 {
		t.Fatalf("published message count = %d, want 0", len(publisher.Messages()))
	}
}

func TestDownstreamTerminalEventsUpdateOrders(t *testing.T) {
	server, service, _ := newTestServer(t)
	orderID := createOrderForTest(t, server)
	now := time.Now().UTC()

	deliverEvent(t, service, events.NewPaymentFailedEvent("", orderID, "card declined", now, "corr-2", now))
	deliverEvent(t, service, events.NewPaymentFailedEvent("", orderID, "card declined", now, "corr-2", now))

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
	if payload.FailureStep != "payment" {
		t.Fatalf("failure step = %q, want payment", payload.FailureStep)
	}
}

func TestInventoryFailureResponseShowsPaymentCompensation(t *testing.T) {
	server, service, _ := newTestServer(t)
	orderID := createOrderForTest(t, server)
	now := time.Now().UTC()

	deliverEvent(t, service, events.NewPaymentCompletedEvent("PAY-1", orderID, json.Number("799000"), "TX-1", now, "corr-5", now))
	deliverEvent(t, service, events.NewInventoryReservationFailedEvent(orderID, "PROD-001", "out of stock", now, "corr-5", now))

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
	if payload.PaymentID != "PAY-1" {
		t.Fatalf("payment id = %q, want PAY-1", payload.PaymentID)
	}
	if payload.FailureStep != "inventory" {
		t.Fatalf("failure step = %q, want inventory", payload.FailureStep)
	}
	if strings.Join(payload.CompensatedSteps, ",") != "payment" {
		t.Fatalf("compensated steps = %v, want [payment]", payload.CompensatedSteps)
	}
}

func TestPrometheusEndpointExposesRequiredOrderMetrics(t *testing.T) {
	server, service, _ := newTestServer(t)
	orderID := createOrderForTest(t, server)
	now := time.Now().UTC()
	deliverEvent(t, service, events.NewShippingFailedEvent(orderID, "courier error", now, "corr-3", now))

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
		"saga_total_duration_seconds",
	} {
		if !strings.Contains(body, metricName) {
			t.Fatalf("metrics body missing %s", metricName)
		}
	}
}

func newTestServer(t *testing.T) (http.Handler, *ordersvc.Service, *testFrameworkPublisher) {
	t.Helper()
	return newTestServerWithCatalog(t, fakeCatalogResolver{})
}

func newTestServerWithCatalog(t *testing.T, catalog fakeCatalogResolver) (http.Handler, *ordersvc.Service, *testFrameworkPublisher) {
	t.Helper()
	db := testutil.OpenPostgres(t, testutil.DefaultChoreographyOrderDatabaseURL, "choreography_order_service_test",
		testutil.Migration{Scope: "choreography-order-service", Dir: "choreography-saga/order-service/db/migrations"},
		testutil.Migration{Scope: "choreography-framework", Dir: "choreography-framework/db/migrations"},
	)
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}

	recording := &testFrameworkPublisher{}
	holder := &eventHandlerHolder{}

	participant, err := choreoruntime.NewPostgres(db,
		choreoruntime.EventRegistry{
			Subscriptions: []choreoruntime.TopicSubscription{
				{Topic: commonkafka.DefaultPaymentEventsTopic, EventTypes: []string{events.TypePaymentCompleted, events.TypePaymentFailed, events.TypePaymentRefunded}},
				{Topic: commonkafka.DefaultInventoryEventsTopic, EventTypes: []string{events.TypeInventoryReserved, events.TypeInventoryReservationFailed, events.TypeInventoryReleased}},
				{Topic: commonkafka.DefaultShippingEventsTopic, EventTypes: []string{events.TypeShippingScheduled, events.TypeShippingFailed}},
			},
			OnUnknownEvent: choreoruntime.RejectUnknown,
		},
		holder,
		recording,
		choreoruntime.Config{ServiceName: "test-order-service", ImmediateOutboxPublish: true},
	)
	if err != nil {
		t.Fatalf("new choreography participant: %v", err)
	}

	frameworkParticipant := ordersvc.NewFrameworkParticipant(participant)
	service, err := ordersvc.NewService(repo, frameworkParticipant, catalog, metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	holder.handler = service

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(HandlerDependencies{
		Config:   commonconfig.ServiceConfig{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger:   logger,
		Orders:   service,
		Registry: metrics.Registry(),
	})
	return handler, service, recording
}

type eventHandlerHolder struct {
	handler choreoruntime.EventHandler
}

func (h *eventHandlerHolder) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	if h.handler == nil {
		return fmt.Errorf("event handler not wired yet")
	}
	return h.handler.HandleEvent(ctx, event)
}

type testFrameworkPublisher struct {
	mu       sync.Mutex
	messages []choreoruntime.Message
}

func (p *testFrameworkPublisher) Publish(_ context.Context, message choreoruntime.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	return nil
}

func (p *testFrameworkPublisher) Messages() []choreoruntime.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	items := make([]choreoruntime.Message, len(p.messages))
	copy(items, p.messages)
	return items
}

type fakeCatalogResolver struct {
	err error
}

func (r fakeCatalogResolver) NormalizeOrderItems(_ context.Context, items []dto.OrderItemRequest) ([]dto.OrderItemRequest, json.Number, error) {
	if r.err != nil {
		return nil, "", r.err
	}
	normalized := make([]dto.OrderItemRequest, len(items))
	copy(normalized, items)
	total := 0.0
	for _, item := range items {
		price, _ := item.Price.Float64()
		total += price * float64(item.Quantity)
	}
	return normalized, json.Number(fmt.Sprintf("%.2f", total)), nil
}

func createOrderForTest(t *testing.T, server http.Handler) string {
	t.Helper()
	body := []byte(`{"customerId":"CUST-001","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`)
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

func deliverEvent(t *testing.T, service *ordersvc.Service, event events.ChoreographyEvent) {
	t.Helper()
	if err := service.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("handle event %T: %v", event, err)
	}
}
