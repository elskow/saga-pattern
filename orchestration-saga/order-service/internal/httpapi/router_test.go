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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/commands"
	commonconfig "saga-pattern/common/config"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/inventorycatalog"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/common/testutil"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/messaging"
	"saga-pattern/orchestration-saga/order-service/internal/observability"
	ordersvc "saga-pattern/orchestration-saga/order-service/internal/orders"
	"saga-pattern/orchestration-saga/order-service/internal/repository"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
)

func TestCreateOrderStartsSagaAndCompletes(t *testing.T) {
	server, service, publisher := newTestServer(t)

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
	paymentCommand := decodePublishedCommand[commands.ProcessPaymentCommand](t, publisher, 0)
	if len(paymentCommand.Items) != 1 || paymentCommand.Items[0].ProductID != "PROD-001" {
		t.Fatalf("payment command items = %+v, want order items", paymentCommand.Items)
	}
	deliverReply(t, service, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReservedReply("RES-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultShippingCommandsTopic, "SCHEDULE_SHIPPING")
	deliverReply(t, service, accepted.OrderID, "reply-shipping", commonkafka.DefaultShippingRepliesTopic, commonreplies.NewShippingScheduledReply("SHIP-1", accepted.OrderID, "TRK-SHIP1"))
	assertPublishStep(t, service, publisher, 3, commonkafka.DefaultInventoryCommandsTopic, "COMMIT_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory-commit", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryCommittedReply("RES-1", accepted.OrderID))

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
	server, service, publisher := newTestServer(t)
	accepted := decodeAcceptedResponse(t, createOrder(t, server).Body.Bytes())

	assertPublishStep(t, service, publisher, 0, commonkafka.DefaultPaymentCommandsTopic, "PROCESS_PAYMENT")
	deliverReply(t, service, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReservedReply("RES-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultShippingCommandsTopic, "SCHEDULE_SHIPPING")
	deliverReply(t, service, accepted.OrderID, "reply-shipping-failed", commonkafka.DefaultShippingRepliesTopic, commonreplies.NewShippingFailedReply("SHIP-1", accepted.OrderID, "courier rejected route"))
	assertPublishStep(t, service, publisher, 3, commonkafka.DefaultInventoryCommandsTopic, "RELEASE_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory-released", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReleasedReply("RES-1", accepted.OrderID, true, ""))
	assertPublishStep(t, service, publisher, 4, commonkafka.DefaultPaymentCommandsTopic, "REFUND_PAYMENT")
	deliverReply(t, service, accepted.OrderID, "reply-payment-refunded", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentRefundedReply("PAY-1", accepted.OrderID, true, ""))

	getResp := httptest.NewRecorder()
	server.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if getResp.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d body=%s", getResp.Code, http.StatusOK, getResp.Body.String())
	}
	if !strings.Contains(getResp.Body.String(), `"status":"CANCELLED"`) {
		t.Fatalf("expected cancelled order, body=%s", getResp.Body.String())
	}
	var cancelled dto.OrderResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &cancelled); err != nil {
		t.Fatalf("decode cancelled response: %v", err)
	}
	if cancelled.FailureStep != "shipping" {
		t.Fatalf("failure step = %q, want shipping", cancelled.FailureStep)
	}
	if strings.Join(cancelled.CompensatedSteps, ",") != "inventory,payment" {
		t.Fatalf("compensated steps = %v, want [inventory payment]", cancelled.CompensatedSteps)
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

func TestInventoryFailureShowsPaymentCompensation(t *testing.T) {
	server, service, publisher := newTestServer(t)
	accepted := decodeAcceptedResponse(t, createOrder(t, server).Body.Bytes())

	assertPublishStep(t, service, publisher, 0, commonkafka.DefaultPaymentCommandsTopic, "PROCESS_PAYMENT")
	deliverReply(t, service, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory-failed", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryFailedReply("RES-1", accepted.OrderID, "out of stock"))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultPaymentCommandsTopic, "REFUND_PAYMENT")
	deliverReply(t, service, accepted.OrderID, "reply-payment-refunded", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentRefundedReply("PAY-1", accepted.OrderID, true, ""))

	getResp := httptest.NewRecorder()
	server.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if getResp.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d body=%s", getResp.Code, http.StatusOK, getResp.Body.String())
	}
	var cancelled dto.OrderResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &cancelled); err != nil {
		t.Fatalf("decode cancelled response: %v", err)
	}
	if cancelled.Status != "CANCELLED" {
		t.Fatalf("status = %q, want CANCELLED", cancelled.Status)
	}
	if cancelled.FailureStep != "inventory" {
		t.Fatalf("failure step = %q, want inventory", cancelled.FailureStep)
	}
	if strings.Join(cancelled.CompensatedSteps, ",") != "payment" {
		t.Fatalf("compensated steps = %v, want [payment]", cancelled.CompensatedSteps)
	}
}

func TestAcceptedOrderShowsInProgressSagaProgress(t *testing.T) {
	server, service, publisher := newTestServer(t)
	accepted := decodeAcceptedResponse(t, createOrder(t, server).Body.Bytes())

	inProgressResp := httptest.NewRecorder()
	server.ServeHTTP(inProgressResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if inProgressResp.Code != http.StatusOK {
		t.Fatalf("in-progress poll status = %d, want %d body=%s", inProgressResp.Code, http.StatusOK, inProgressResp.Body.String())
	}
	var inProgress dto.OrderResponse
	if err := json.Unmarshal(inProgressResp.Body.Bytes(), &inProgress); err != nil {
		t.Fatalf("decode in-progress response: %v", err)
	}
	if inProgress.Status != "PAYMENT_PENDING" {
		t.Fatalf("in-progress status = %q, want PAYMENT_PENDING", inProgress.Status)
	}
	if inProgress.CurrentStep != "payment" {
		t.Fatalf("in-progress currentStep = %q, want payment", inProgress.CurrentStep)
	}

	assertPublishStep(t, service, publisher, 0, commonkafka.DefaultPaymentCommandsTopic, "PROCESS_PAYMENT")
	deliverReply(t, service, accepted.OrderID, "reply-payment", commonkafka.DefaultPaymentRepliesTopic, commonreplies.NewPaymentCompletedReply("PAY-1", accepted.OrderID))

	afterPayment := httptest.NewRecorder()
	server.ServeHTTP(afterPayment, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if afterPayment.Code != http.StatusOK {
		t.Fatalf("after-payment poll status = %d, want %d body=%s", afterPayment.Code, http.StatusOK, afterPayment.Body.String())
	}
	var paid dto.OrderResponse
	if err := json.Unmarshal(afterPayment.Body.Bytes(), &paid); err != nil {
		t.Fatalf("decode after-payment response: %v", err)
	}
	if paid.Status != "PAYMENT_COMPLETED" {
		t.Fatalf("after-payment status = %q, want PAYMENT_COMPLETED", paid.Status)
	}
	if paid.CurrentStep != "inventory" {
		t.Fatalf("after-payment currentStep = %q, want inventory", paid.CurrentStep)
	}
	if len(paid.CompletedSteps) != 1 || paid.CompletedSteps[0] != "payment" {
		t.Fatalf("after-payment completedSteps = %v, want [payment]", paid.CompletedSteps)
	}

	assertPublishStep(t, service, publisher, 1, commonkafka.DefaultInventoryCommandsTopic, "RESERVE_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryReservedReply("RES-1", accepted.OrderID))
	assertPublishStep(t, service, publisher, 2, commonkafka.DefaultShippingCommandsTopic, "SCHEDULE_SHIPPING")
	deliverReply(t, service, accepted.OrderID, "reply-shipping", commonkafka.DefaultShippingRepliesTopic, commonreplies.NewShippingScheduledReply("SHIP-1", accepted.OrderID, "TRK-SHIP1"))
	assertPublishStep(t, service, publisher, 3, commonkafka.DefaultInventoryCommandsTopic, "COMMIT_INVENTORY")
	deliverReply(t, service, accepted.OrderID, "reply-inventory-commit", commonkafka.DefaultInventoryRepliesTopic, commonreplies.NewInventoryCommittedReply("RES-1", accepted.OrderID))

	finalResp := httptest.NewRecorder()
	server.ServeHTTP(finalResp, httptest.NewRequest(http.MethodGet, "/api/orders/"+accepted.OrderID, nil))
	if finalResp.Code != http.StatusOK {
		t.Fatalf("final poll status = %d, want %d body=%s", finalResp.Code, http.StatusOK, finalResp.Body.String())
	}
	if !strings.Contains(finalResp.Body.String(), `"status":"COMPLETED"`) {
		t.Fatalf("expected completed terminal state, body=%s", finalResp.Body.String())
	}
}

func TestDuplicateCreateOrderUsesIdempotencyKey(t *testing.T) {
	server, _, publisher := newTestServer(t)

	first := createOrderWithIdempotency(t, server, "IDEM-123")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first create status = %d, want %d, body=%s", first.Code, http.StatusAccepted, first.Body.String())
	}
	second := createOrderWithIdempotency(t, server, "IDEM-123")
	if second.Code != http.StatusAccepted {
		t.Fatalf("second create status = %d, want %d, body=%s", second.Code, http.StatusAccepted, second.Body.String())
	}

	firstAccepted := decodeAcceptedResponse(t, first.Body.Bytes())
	secondAccepted := decodeAcceptedResponse(t, second.Body.Bytes())
	if firstAccepted.OrderID != secondAccepted.OrderID {
		t.Fatalf("duplicate order ids differ: %q != %q", firstAccepted.OrderID, secondAccepted.OrderID)
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("duplicate idempotent create published %d messages, want only the initial command", len(publisher.Messages()))
	}
	if len(firstAccepted.OrderID) != 32 {
		t.Fatalf("accepted order id = %q, want 32-char deterministic hash", firstAccepted.OrderID)
	}
}

func TestDuplicateCreateOrderRejectsDifferentPayloadForSameIdempotencyKey(t *testing.T) {
	server, _, publisher := newTestServer(t)
	first := createOrderWithIdempotency(t, server, "IDEM-CONFLICT")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first create status = %d, want %d, body=%s", first.Code, http.StatusAccepted, first.Body.String())
	}

	body := []byte(`{"customerId":"CUST-002","totalAmount":1599000,"shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":1599000}]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/orders", bytes.NewReader(body))
	req.Header.Set(commoncontext.HeaderIdempotencyKey, "IDEM-CONFLICT")
	second := httptest.NewRecorder()
	server.ServeHTTP(second, req)

	if second.Code != http.StatusConflict {
		t.Fatalf("second create status = %d, want %d, body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published messages = %d, want only the initial command", len(publisher.Messages()))
	}
}

func TestCreateOrderRejectsInsufficientAvailabilityBeforeStartingSaga(t *testing.T) {
	server, service, publisher := newTestServerWithCatalog(t, fakeCatalogResolver{err: inventorycatalog.InsufficientAvailabilityError{ProductID: "PROD-001", Requested: 1, Available: 0}})

	resp := createOrder(t, server)
	if resp.Code != http.StatusConflict {
		t.Fatalf("create status = %d, want %d, body=%s", resp.Code, http.StatusConflict, resp.Body.String())
	}
	if err := service.PublishPending(context.Background()); err != nil {
		t.Fatalf("publish pending: %v", err)
	}
	if len(publisher.Messages()) != 0 {
		t.Fatalf("published message count = %d, want 0", len(publisher.Messages()))
	}
}

func TestCreateOrderRejectsJudgeCustomerInsufficientAvailabilityBeforeStartingSaga(t *testing.T) {
	server, service, publisher := newTestServerWithCatalog(t, fakeCatalogResolver{err: inventorycatalog.InsufficientAvailabilityError{ProductID: "PROD-001", Requested: 1, Available: 0}})
	body := `{"customerId":"CUST-JUDGE-001","totalAmount":799000,"shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":799000}]}`

	req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	if resp.Code != http.StatusConflict {
		t.Fatalf("create status = %d, want %d, body=%s", resp.Code, http.StatusConflict, resp.Body.String())
	}
	if err := service.PublishPending(context.Background()); err != nil {
		t.Fatalf("publish pending: %v", err)
	}
	if len(publisher.Messages()) != 0 {
		t.Fatalf("published message count = %d, want 0", len(publisher.Messages()))
	}
}

func newTestServer(t *testing.T) (http.Handler, *ordersvc.Service, *messaging.RecordingPublisher) {
	t.Helper()
	return newTestServerWithCatalog(t, fakeCatalogResolver{})
}

func newTestServerWithCatalog(t *testing.T, catalog fakeCatalogResolver) (http.Handler, *ordersvc.Service, *messaging.RecordingPublisher) {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	publisher := messaging.NewRecordingPublisher()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	db := testutil.OpenPostgres(t, testutil.DefaultOrderDatabaseURL, "order_service_router_test",
		testutil.Migration{Scope: "orchestration-framework", Dir: "orchestration-framework/db/migrations"},
		testutil.Migration{Scope: "orchestration-order-service", Dir: "orchestration-saga/order-service/db/migrations"},
	)
	runtimeIDs := []string{"worker-1", "hist-1", "outbox-1", "hist-2", "outbox-2", "hist-3", "outbox-3", "hist-4", "outbox-4", "hist-5", "outbox-5", "hist-6", "outbox-6", "hist-7", "outbox-7"}
	runtimeIdx := 0
	runtime, err := sagaRuntime.NewPostgres(ordersaga.Definition(commonkafka.DefaultTopics()), db, sagaRuntime.PostgresDependencies{
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
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	service, err := ordersvc.NewService(repo, runtime, catalog, metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return now })
	ids := []string{"ORDER-1", "PAY-1", "RES-1", "SHIP-1", "ORDER-2", "PAY-2", "RES-2", "SHIP-2"}
	idx := 0
	service.WithIDGenerator(func() string {
		value := ids[idx]
		idx++
		return value
	})
	handler := NewHandler(HandlerDependencies{
		Config:   commonconfig.ServiceConfig{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Orders:   service,
		Registry: metrics.Registry(),
	})
	return handler, service, publisher
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

func createOrder(t *testing.T, server http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	body := []byte(`{"customerId":"CUST-001","totalAmount":1599000,"shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":1599000}]}`)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/orders", bytes.NewReader(body)))
	return resp
}

func createOrderWithIdempotency(t *testing.T, server http.Handler, idempotencyKey string) *httptest.ResponseRecorder {
	t.Helper()
	body := []byte(`{"customerId":"CUST-001","totalAmount":1599000,"shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Widget","quantity":1,"price":1599000}]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/orders", bytes.NewReader(body))
	req.Header.Set(commoncontext.HeaderIdempotencyKey, idempotencyKey)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
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

func decodePublishedCommand[T any](t *testing.T, publisher *messaging.RecordingPublisher, index int) T {
	t.Helper()
	messages := publisher.Messages()
	if len(messages) <= index {
		t.Fatalf("published messages len = %d, want > %d", len(messages), index)
	}
	var command T
	if err := json.Unmarshal(messages[index].Message.Payload, &command); err != nil {
		t.Fatalf("decode published command[%d]: %v", index, err)
	}
	return command
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

func deliverReply(t *testing.T, service *ordersvc.Service, sagaID string, replyID string, topic string, reply commonreplies.SagaReply) {
	t.Helper()
	payload, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	if err := service.HandleReply(context.Background(), sagaRuntime.ReplyEnvelope{ReplyID: replyID, SagaID: sagaID, Topic: topic, ReceivedAt: time.Now().UTC(), Payload: payload}); err != nil {
		t.Fatalf("deliver reply: %v", err)
	}
}
