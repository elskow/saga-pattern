package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/commands"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/common/testutil"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

type testSagaData struct {
	OrderID         string
	CustomerID      string
	PaymentID       string
	ReservationID   string
	ShippingID      string
	CorrelationID   string
	ShippingAddress string
	TotalAmount     json.Number
	Items           []dto.OrderItemRequest
}

func (d testSagaData) Validate() error {
	if d.OrderID == "" || d.CustomerID == "" || d.PaymentID == "" || d.ReservationID == "" || d.ShippingID == "" || d.TotalAmount == "" || d.ShippingAddress == "" || len(d.Items) == 0 {
		return fmt.Errorf("test saga data is incomplete")
	}
	return nil
}

func testDefinition() Definition[testSagaData] {
	topics := commonkafka.DefaultTopics()
	return Definition[testSagaData]{
		SagaType:          "OrderSaga",
		CompensatingState: "COMPENSATING",
		TerminalStates:    []string{string(model.SagaStateCompleted), string(model.SagaStateCancelled)},
		DataCodec:         JSONCodec[testSagaData]{},
		ValidateData:      func(data testSagaData) error { return data.Validate() },
		Steps: []Step[testSagaData]{
			{
				Name:         "Payment",
				PendingState: "PAYMENT_PENDING",
				Forward: CommandSpec[testSagaData]{Topic: topics.PaymentCommands, CommandType: commands.CommandProcessPayment, BuildPayload: func(data testSagaData) (any, error) {
					return commands.NewProcessPaymentCommand(data.PaymentID, data.OrderID, data.CustomerID, data.TotalAmount, data.Items), nil
				}},
				Compensation: CommandSpec[testSagaData]{Topic: topics.PaymentCommands, CommandType: commands.CommandRefundPayment, BuildPayload: func(data testSagaData) (any, error) {
					return commands.NewRefundPaymentCommand(data.PaymentID, data.OrderID), nil
				}},
				ForwardReplies: []ReplyCase[testSagaData]{
					TypedReplyCase(topics.PaymentReplies, commonreplies.TypePaymentCompleted, decodeTestReply[commonreplies.PaymentCompletedReply], func(_ testSagaData, _ commonreplies.PaymentCompletedReply) (Decision, error) {
						return Advance("Inventory", "INVENTORY_PENDING"), nil
					}),
					TypedReplyCase(topics.PaymentReplies, commonreplies.TypePaymentFailed, decodeTestReply[commonreplies.PaymentFailedReply], func(_ testSagaData, reply commonreplies.PaymentFailedReply) (Decision, error) {
						return Cancel(string(model.SagaStateCancelled), reply.Reason), nil
					}),
				},
				CompensateReplies: []ReplyCase[testSagaData]{
					TypedReplyCase(topics.PaymentReplies, commonreplies.TypePaymentRefunded, decodeTestReply[commonreplies.PaymentRefundedReply], func(_ testSagaData, reply commonreplies.PaymentRefundedReply) (Decision, error) {
						if !reply.Success {
							return Ignore(), nil
						}
						return Compensated(), nil
					}),
				},
			},
			{
				Name:         "Inventory",
				PendingState: "INVENTORY_PENDING",
				Forward: CommandSpec[testSagaData]{Topic: topics.InventoryCommands, CommandType: commands.CommandReserveInventory, BuildPayload: func(data testSagaData) (any, error) {
					return commands.NewReserveInventoryCommand(data.ReservationID, data.OrderID, data.Items), nil
				}},
				Compensation: CommandSpec[testSagaData]{Topic: topics.InventoryCommands, CommandType: commands.CommandReleaseInventory, BuildPayload: func(data testSagaData) (any, error) {
					return commands.NewReleaseInventoryCommand(data.ReservationID, data.OrderID), nil
				}},
				ForwardReplies: []ReplyCase[testSagaData]{
					TypedReplyCase(topics.InventoryReplies, commonreplies.TypeInventoryReserved, decodeTestReply[commonreplies.InventoryReservedReply], func(_ testSagaData, _ commonreplies.InventoryReservedReply) (Decision, error) {
						return Advance("Shipping", "SHIPPING_PENDING"), nil
					}),
					TypedReplyCase(topics.InventoryReplies, commonreplies.TypeInventoryFailed, decodeTestReply[commonreplies.InventoryFailedReply], func(_ testSagaData, reply commonreplies.InventoryFailedReply) (Decision, error) {
						return BeginCompensation(reply.Reason), nil
					}),
				},
				CompensateReplies: []ReplyCase[testSagaData]{
					TypedReplyCase(topics.InventoryReplies, commonreplies.TypeInventoryReleased, decodeTestReply[commonreplies.InventoryReleasedReply], func(_ testSagaData, reply commonreplies.InventoryReleasedReply) (Decision, error) {
						if !reply.Success {
							return Ignore(), nil
						}
						return Compensated(), nil
					}),
				},
			},
			{
				Name:         "Shipping",
				PendingState: "SHIPPING_PENDING",
				Forward: CommandSpec[testSagaData]{Topic: topics.ShippingCommands, CommandType: commands.CommandScheduleShipping, BuildPayload: func(data testSagaData) (any, error) {
					return commands.NewScheduleShippingCommand(data.ShippingID, data.OrderID, data.ShippingAddress), nil
				}},
				Compensation: CommandSpec[testSagaData]{Topic: topics.ShippingCommands, CommandType: commands.CommandCancelShipping, BuildPayload: func(data testSagaData) (any, error) {
					return commands.NewCancelShippingCommand(data.ShippingID, data.OrderID), nil
				}},
				ForwardReplies: []ReplyCase[testSagaData]{
					TypedReplyCase(topics.ShippingReplies, commonreplies.TypeShippingScheduled, decodeTestReply[commonreplies.ShippingScheduledReply], func(_ testSagaData, _ commonreplies.ShippingScheduledReply) (Decision, error) {
						return Complete(string(model.SagaStateCompleted)), nil
					}),
					TypedReplyCase(topics.ShippingReplies, commonreplies.TypeShippingFailed, decodeTestReply[commonreplies.ShippingFailedReply], func(_ testSagaData, reply commonreplies.ShippingFailedReply) (Decision, error) {
						return BeginCompensation(reply.Reason), nil
					}),
				},
				CompensateReplies: []ReplyCase[testSagaData]{
					TypedReplyCase(topics.ShippingReplies, commonreplies.TypeShippingCancelled, decodeTestReply[commonreplies.ShippingCancelledReply], func(_ testSagaData, reply commonreplies.ShippingCancelledReply) (Decision, error) {
						if !reply.Success {
							return Ignore(), nil
						}
						return Compensated(), nil
					}),
				},
			},
		},
	}
}

func TestCommandEnqueuedAttributesAreQueryable(t *testing.T) {
	attrs := runtimeAttributesByKey(commandEnqueuedAttributes(model.OutboxRow{
		SagaID:         "order-1",
		SagaType:       "OrderSaga",
		Step:           "Payment",
		Direction:      model.StepDirectionForward,
		Topic:          "orchestration.payment.commands",
		MessageType:    commands.CommandProcessPayment,
		Payload:        []byte(`{"orderId":"order-1"}`),
		RequestID:      "request-1",
		CorrelationID:  "correlation-1",
		BenchmarkRun:   "run-1",
		BenchmarkScene: "happy-path",
		BenchmarkPhase: "create",
	}))

	assertRuntimeAttr(t, attrs, "saga.command.event", "saga.command.enqueued")
	assertRuntimeAttr(t, attrs, "saga.command.enqueued", "true")
	assertRuntimeAttr(t, attrs, "saga.id", "order-1")
	assertRuntimeAttr(t, attrs, "saga.type", "OrderSaga")
	assertRuntimeAttr(t, attrs, "saga.step", "Payment")
	assertRuntimeAttr(t, attrs, "saga.pending.direction", "forward")
	assertRuntimeAttr(t, attrs, "topic", "orchestration.payment.commands")
	assertRuntimeAttr(t, attrs, "saga.command.type", commands.CommandProcessPayment)
	assertRuntimeAttr(t, attrs, "messaging.message.payload_size_bytes", "21")
	assertRuntimeAttr(t, attrs, "request.id", "request-1")
	assertRuntimeAttr(t, attrs, "correlation.id", "correlation-1")
	assertRuntimeAttr(t, attrs, "benchmark.run_label", "run-1")
	assertRuntimeAttr(t, attrs, "benchmark.scenario", "happy-path")
	assertRuntimeAttr(t, attrs, "benchmark.phase", "create")
}

func TestTraceHeadersFromContextPersistsOnlyW3CTraceContext(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	traceID, err := trace.TraceIDFromHex("958b17a8fdf7a6b3efa0d85c7685cda4")
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("ac7a5dd8008f57cb")
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})

	headers := traceHeadersFromContext(trace.ContextWithSpanContext(context.Background(), spanContext))

	if headers["traceparent"] == "" {
		t.Fatalf("missing traceparent in %#v", headers)
	}
	if _, ok := headers["baggage"]; ok {
		t.Fatalf("baggage should not be persisted in trace headers: %#v", headers)
	}
	if len(headers) != 1 {
		t.Fatalf("headers = %#v, want only traceparent", headers)
	}
}

func TestStartSagaPersistsTraceContextOnOutbox(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	traceID, err := trace.TraceIDFromHex("958b17a8fdf7a6b3efa0d85c7685cda4")
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("ac7a5dd8008f57cb")
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	fixture := newFixture(t)

	sagaID := fixture.startSagaWithContext(t, trace.ContextWithSpanContext(context.Background(), spanContext))
	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(outbox))
	}
	if got := outbox[0].TraceHeaders["traceparent"]; got != "00-958b17a8fdf7a6b3efa0d85c7685cda4-ac7a5dd8008f57cb-01" {
		t.Fatalf("traceparent = %q", got)
	}
}

func TestStartSagaUsesConfiguredStepDeadline(t *testing.T) {
	fixture := newFixture(t)
	fixture.runtime.config.PendingCommandRetryDelay = 45 * time.Second

	sagaID := fixture.startSaga(t)
	sagaRow, ok, err := fixture.store.GetSaga(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("get saga: %v", err)
	}
	if !ok {
		t.Fatalf("saga %s not found", sagaID)
	}
	want := fixture.now.Add(45 * time.Second)
	if !sagaRow.StepDeadlineAt.Equal(want) {
		t.Fatalf("step deadline = %s, want %s", sagaRow.StepDeadlineAt, want)
	}
}

func runtimeAttributesByKey(attrs []attribute.KeyValue) map[string]string {
	byKey := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		byKey[string(attr.Key)] = attr.Value.Emit()
	}
	return byKey
}

func assertRuntimeAttr(t *testing.T, attrs map[string]string, key string, want string) {
	t.Helper()
	got, ok := attrs[key]
	if !ok {
		t.Fatalf("missing attribute %q in %#v", key, attrs)
	}
	if got != want {
		t.Fatalf("attribute %q = %q, want %q", key, got, want)
	}
}

func decodeTestReply[R interface{ Validate() error }](payload []byte) (R, error) {
	var reply R
	if err := json.Unmarshal(payload, &reply); err != nil {
		return reply, err
	}
	return reply, reply.Validate()
}

func TestSagaStateMachineHappyPath(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)

	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("payment"), fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))
	assertSagaState(t, fixture.store, sagaID, model.SagaState("INVENTORY_PENDING"))

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("inventory"), fixture.inventoryRepliesTopic(), commonreplies.NewInventoryReservedReply("RES-1", "ORDER-1"))
	assertSagaState(t, fixture.store, sagaID, model.SagaState("SHIPPING_PENDING"))

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("shipping"), fixture.shippingRepliesTopic(), commonreplies.NewShippingScheduledReply("SHIP-1", "ORDER-1", "TRK-SHIP1"))
	assertSagaState(t, fixture.store, sagaID, model.SagaStateCompleted)

	assertMetricNames(t, fixture.runtime, []string{
		"saga_framework_duration_seconds",
		"saga_framework_step_duration_seconds",
		"saga_framework_compensation_started_total",
		"saga_framework_compensation_completed_total",
	})
	assertStepStatuses(t, fixture.store, sagaID, []model.StepStatus{
		model.StepStatusSucceeded,
		model.StepStatusSucceeded,
		model.StepStatusSucceeded,
	})
}

func TestInitialOutboxPublishPropagatesStoredRequestContextFromBackground(t *testing.T) {
	fixture := newFixture(t)
	ctx := commoncontext.With(context.Background(), commoncontext.Data{
		RequestID:      "request-1",
		CorrelationID:  "correlation-1",
		BenchmarkRun:   "run-1",
		BenchmarkScene: "happy-path",
		BenchmarkPhase: "create",
	})
	sagaID := fixture.startSagaWithContext(t, ctx)

	sagaRow, ok, err := fixture.store.GetSaga(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("get saga: %v", err)
	}
	if !ok {
		t.Fatalf("saga %s not found", sagaID)
	}
	if sagaRow.RequestID != "request-1" || sagaRow.CorrelationID != "correlation-1" || sagaRow.BenchmarkRun != "run-1" || sagaRow.BenchmarkScene != "happy-path" || sagaRow.BenchmarkPhase != "create" {
		t.Fatalf("saga context = (%q, %q, %q, %q, %q), want (request-1, correlation-1, run-1, happy-path, create)", sagaRow.RequestID, sagaRow.CorrelationID, sagaRow.BenchmarkRun, sagaRow.BenchmarkScene, sagaRow.BenchmarkPhase)
	}
	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(outbox))
	}
	if outbox[0].RequestID != "request-1" || outbox[0].CorrelationID != "correlation-1" || outbox[0].BenchmarkRun != "run-1" || outbox[0].BenchmarkScene != "happy-path" || outbox[0].BenchmarkPhase != "create" {
		t.Fatalf("outbox context = (%q, %q, %q, %q, %q), want (request-1, correlation-1, run-1, happy-path, create)", outbox[0].RequestID, outbox[0].CorrelationID, outbox[0].BenchmarkRun, outbox[0].BenchmarkScene, outbox[0].BenchmarkPhase)
	}

	messages := fixture.publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(messages))
	}
	assertPublishedContext(t, messages[0], "request-1", "correlation-1", "run-1", "happy-path", "create")
	assertBusinessPayloadHasNoRequestContext(t, messages[0])
}

func TestImmediateOutboxPublishFailureFallsBackToRetry(t *testing.T) {
	fixture := newFixture(t)
	fixture.publisher.failures[commands.CommandProcessPayment] = 1

	sagaID := fixture.startSaga(t)
	if len(fixture.publisher.Messages()) != 0 {
		t.Fatalf("published messages after forced failure = %d, want 0", len(fixture.publisher.Messages()))
	}
	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(outbox))
	}
	if outbox[0].Status != model.OutboxStatusPending {
		t.Fatalf("outbox status = %s, want %s", outbox[0].Status, model.OutboxStatusPending)
	}
	if outbox[0].AttemptCount != 1 {
		t.Fatalf("outbox attempt_count = %d, want 1", outbox[0].AttemptCount)
	}

	fixture.advance(fixture.runtime.config.OutboxRetryDelay + time.Second)
	if err := fixture.runtime.PublishPending(context.Background()); err != nil {
		t.Fatalf("publish pending fallback: %v", err)
	}
	if len(fixture.publisher.Messages()) != 1 {
		t.Fatalf("published messages after fallback = %d, want 1", len(fixture.publisher.Messages()))
	}
}

func TestProgressedCommandPublishPropagatesStoredRequestContextFromBackground(t *testing.T) {
	fixture := newFixture(t)
	ctx := commoncontext.With(context.Background(), commoncontext.Data{RequestID: "request-2", CorrelationID: "correlation-2", BenchmarkRun: "run-2", BenchmarkScene: "failure", BenchmarkPhase: "create"})
	sagaID := fixture.startSagaWithContext(t, ctx)

	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("payment"), fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))

	messages := fixture.publisher.Messages()
	if len(messages) != 2 {
		t.Fatalf("published messages = %d, want 2", len(messages))
	}
	assertPublishedContext(t, messages[1], "request-2", "correlation-2", "run-2", "failure", "create")
	assertBusinessPayloadHasNoRequestContext(t, messages[1])
}

func TestViewReturnsApplicationFacingSagaState(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)

	view, ok, err := fixture.runtime.View(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if !ok {
		t.Fatalf("View() found = false, want true")
	}
	if view.SagaID != sagaID {
		t.Fatalf("view.SagaID = %q, want %q", view.SagaID, sagaID)
	}
	if view.SagaType != "OrderSaga" {
		t.Fatalf("view.SagaType = %q, want %q", view.SagaType, "OrderSaga")
	}
	if view.State != "PAYMENT_PENDING" {
		t.Fatalf("view.State = %q, want %q", view.State, "PAYMENT_PENDING")
	}
	if view.CurrentStep != "Payment" {
		t.Fatalf("view.CurrentStep = %q, want %q", view.CurrentStep, "Payment")
	}
	if view.LastError != "" {
		t.Fatalf("view.LastError = %q, want empty", view.LastError)
	}
	if !view.StartedAt.Equal(fixture.now) {
		t.Fatalf("view.StartedAt = %v, want %v", view.StartedAt, fixture.now)
	}
	if !view.UpdatedAt.Equal(fixture.now) {
		t.Fatalf("view.UpdatedAt = %v, want %v", view.UpdatedAt, fixture.now)
	}
	if view.Data.OrderID != "ORDER-1" {
		t.Fatalf("view.Data.OrderID = %q, want %q", view.Data.OrderID, "ORDER-1")
	}
}

func TestDuplicateReplyAndTimeoutRecovery(t *testing.T) {
	fixture := newFixture(t)
	sagaID := fixture.startSaga(t)
	fixture.publishOutbox(t)

	paymentReplyID := fixture.replyID("payment")
	fixture.handleReply(t, sagaID, fixture.now, paymentReplyID, fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))
	fixture.handleReply(t, sagaID, fixture.now, paymentReplyID, fixture.paymentRepliesTopic(), commonreplies.NewPaymentCompletedReply("PAY-1", "ORDER-1"))
	fixture.publishOutbox(t)

	fixture.advance(11 * time.Second)
	if err := fixture.runtime.timeoutLoop.RunOnce(context.Background(), fixture.now); err != nil {
		t.Fatalf("timeout recovery: %v", err)
	}
	fixture.publishOutbox(t)

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("inventory-failed"), fixture.inventoryRepliesTopic(), commonreplies.NewInventoryFailedReply("RES-1", "ORDER-1", "out of stock"))
	fixture.publishOutbox(t)

	fixture.advance(2 * time.Second)
	fixture.handleReply(t, sagaID, fixture.now, fixture.replyID("payment-refunded"), fixture.paymentRepliesTopic(), commonreplies.NewPaymentRefundedReply("PAY-1", "ORDER-1", true, ""))
	assertSagaState(t, fixture.store, sagaID, model.SagaStateCancelled)

	history, err := fixture.store.ListStepHistory(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list step history: %v", err)
	}
	var timedOut, compensated bool
	for _, row := range history {
		if row.Step == "Inventory" && row.Status == model.StepStatusTimedOut {
			timedOut = true
		}
		if row.Step == "Payment" && row.Direction == model.StepDirectionCompensation && row.Status == model.StepStatusCompensated {
			compensated = true
		}
	}
	if !timedOut {
		t.Fatalf("expected inventory timeout to be recorded")
	}
	if !compensated {
		t.Fatalf("expected payment compensation to complete")
	}
	if len(fixture.publisher.Messages()) != 4 {
		t.Fatalf("published message count = %d, want 4", len(fixture.publisher.Messages()))
	}
}

func TestOutboxPublisherIsReplicaSafe(t *testing.T) {
	fixture := newFixture(t)
	fixture.runtime.config.ImmediateOutboxPublish = false
	sagaID := fixture.startSaga(t)

	other, err := NewWithStore(testDefinition(), AdvancedDependencies{
		Store:       fixture.store,
		Publisher:   fixture.publisher,
		Clock:       fixture.clock,
		IDGenerator: fixture.idGenerator,
		WorkerID:    "worker-b",
		Config:      fixture.runtime.config,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = fixture.runtime.outboxLoop.RunOnce(context.Background(), fixture.now)
	}()
	go func() {
		defer wg.Done()
		_ = other.outboxLoop.RunOnce(context.Background(), fixture.now)
	}()
	wg.Wait()

	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox row count = %d, want 1", len(outbox))
	}
	if outbox[0].Status != model.OutboxStatusSent {
		t.Fatalf("outbox status = %s, want %s", outbox[0].Status, model.OutboxStatusSent)
	}
	if len(fixture.publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(fixture.publisher.Messages()))
	}
}

func TestExpiredSendingOutboxRowIsReclaimed(t *testing.T) {
	fixture := newFixture(t)
	fixture.runtime.config.ImmediateOutboxPublish = false
	sagaID := fixture.startSaga(t)

	claimed, err := fixture.store.ClaimOutbox(context.Background(), "worker-a", fixture.now, fixture.runtime.config.OutboxBatchSize, fixture.runtime.config.OutboxRetryDelay)
	if err != nil {
		t.Fatalf("claim outbox: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed outbox rows = %d, want 1", len(claimed))
	}

	fixture.advance(fixture.runtime.config.OutboxRetryDelay + time.Second)
	fixture.publishOutbox(t)

	outbox, err := fixture.store.ListOutbox(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(outbox) != 1 {
		t.Fatalf("outbox row count = %d, want 1", len(outbox))
	}
	if outbox[0].Status != model.OutboxStatusSent {
		t.Fatalf("outbox status = %s, want %s", outbox[0].Status, model.OutboxStatusSent)
	}
	if outbox[0].AttemptCount != 2 {
		t.Fatalf("outbox attempt_count = %d, want 2", outbox[0].AttemptCount)
	}
	if len(fixture.publisher.Messages()) != 1 {
		t.Fatalf("published message count = %d, want 1", len(fixture.publisher.Messages()))
	}
}

type fixture struct {
	t           *testing.T
	store       store.Store
	publisher   *recordingPublisher
	runtime     *Runtime[testSagaData]
	now         time.Time
	clock       func() time.Time
	ids         []string
	idx         int
	idGenerator func() string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db := testutil.OpenPostgres(t, testutil.DefaultOrderDatabaseURL, "framework_runtime_test", testutil.Migration{Scope: "orchestration-framework", Dir: "orchestration-framework/db/migrations"})
	f := &fixture{
		t:         t,
		store:     store.NewPostgresStore(db),
		publisher: newRecordingPublisher(),
		now:       time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC),
		ids:       []string{"saga-1", "hist-1", "outbox-1", "reply-1", "hist-2", "outbox-2", "hist-3", "outbox-3", "hist-4", "outbox-4", "hist-5", "outbox-5", "hist-6", "outbox-6"},
	}
	f.clock = func() time.Time { return f.now }
	f.idGenerator = func() string {
		if f.idx >= len(f.ids) {
			return "generated-extra-id"
		}
		value := f.ids[f.idx]
		f.idx++
		return value
	}
	var err error
	f.runtime, err = NewWithStore(testDefinition(), AdvancedDependencies{
		Store:       f.store,
		Publisher:   f.publisher,
		Clock:       f.clock,
		IDGenerator: f.idGenerator,
		WorkerID:    "worker-a",
		Config:      DefaultConfig(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return f
}

func (f *fixture) startSaga(t *testing.T) string {
	t.Helper()
	return f.startSagaWithContext(t, context.Background())
}

func (f *fixture) startSagaWithContext(t *testing.T, ctx context.Context) string {
	t.Helper()
	sagaID, err := f.runtime.StartSaga(ctx, StartSagaInput[testSagaData]{Data: testSagaData{
		OrderID:         "ORDER-1",
		CustomerID:      "CUST-1",
		PaymentID:       "PAY-1",
		ReservationID:   "RES-1",
		ShippingID:      "SHIP-1",
		CorrelationID:   "corr-1",
		ShippingAddress: "Jl. Ketintang Wiyata, Surabaya 60231",
		TotalAmount:     json.Number("1599000"),
		Items:           []dto.OrderItemRequest{{ProductID: "PROD-1", ProductName: "Widget", Quantity: 1, Price: json.Number("1599000")}},
	}})
	if err != nil {
		t.Fatalf("start saga: %v", err)
	}
	return sagaID
}

func (f *fixture) publishOutbox(t *testing.T) {
	t.Helper()
	if err := f.runtime.outboxLoop.RunOnce(context.Background(), f.now); err != nil {
		t.Fatalf("publish outbox: %v", err)
	}
}

func (f *fixture) handleReply(t *testing.T, sagaID string, at time.Time, replyID string, topic string, reply commonreplies.SagaReply) {
	t.Helper()
	payload, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	if err := f.runtime.ConsumeReply(context.Background(), ReplyEnvelope{ReplyID: replyID, SagaID: sagaID, Topic: topic, ReceivedAt: at, Payload: payload}); err != nil {
		t.Fatalf("handle reply: %v", err)
	}
}

func (f *fixture) replyID(prefix string) string {
	return prefix + "-reply"
}

func (f *fixture) advance(delta time.Duration) {
	f.now = f.now.Add(delta)
}

func (f *fixture) paymentRepliesTopic() string   { return "orchestration.payment.replies" }
func (f *fixture) inventoryRepliesTopic() string { return "orchestration.inventory.replies" }
func (f *fixture) shippingRepliesTopic() string  { return "orchestration.shipping.replies" }

func assertSagaState(t *testing.T, s store.Store, sagaID string, want model.SagaState) {
	t.Helper()
	row, ok, err := s.GetSaga(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("get saga: %v", err)
	}
	if !ok {
		t.Fatalf("saga %s not found", sagaID)
	}
	if row.State != want {
		t.Fatalf("saga state = %s, want %s", row.State, want)
	}
}

func assertMetricNames(t *testing.T, runtime *Runtime[testSagaData], names []string) {
	t.Helper()
	families, err := runtime.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	seen := make(map[string]struct{}, len(families))
	for _, family := range families {
		seen[family.GetName()] = struct{}{}
	}
	for _, name := range names {
		if _, ok := seen[name]; !ok {
			t.Fatalf("missing metric %s", name)
		}
	}
}

func assertStepStatuses(t *testing.T, s store.Store, sagaID string, want []model.StepStatus) {
	t.Helper()
	rows, err := s.ListStepHistory(context.Background(), sagaID)
	if err != nil {
		t.Fatalf("list step history: %v", err)
	}
	got := make([]model.StepStatus, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.Status)
	}
	if len(got) != len(want) {
		t.Fatalf("step history len = %d, want %d (%v)", len(got), len(want), got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("step status[%d] = %s, want %s", idx, got[idx], want[idx])
		}
	}
}

type publishedMessage struct {
	Message        Message
	Body           any
	RequestID      string
	CorrelationID  string
	BenchmarkRun   string
	BenchmarkScene string
	BenchmarkPhase string
}

type recordingPublisher struct {
	mu       sync.Mutex
	messages []publishedMessage
	failures map[string]int
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{failures: make(map[string]int)}
}

func (p *recordingPublisher) Publish(ctx context.Context, message Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if remaining := p.failures[message.MessageType]; remaining > 0 {
		p.failures[message.MessageType] = remaining - 1
		return fmt.Errorf("forced publish failure for %s", message.MessageType)
	}
	var body any
	if len(message.Payload) > 0 {
		_ = json.Unmarshal(message.Payload, &body)
	}
	requestContext, _ := commoncontext.From(ctx)
	p.messages = append(p.messages, publishedMessage{
		Message:        message,
		Body:           body,
		RequestID:      requestContext.RequestID,
		CorrelationID:  requestContext.CorrelationID,
		BenchmarkRun:   requestContext.BenchmarkRun,
		BenchmarkScene: requestContext.BenchmarkScene,
		BenchmarkPhase: requestContext.BenchmarkPhase,
	})
	return nil
}

func (p *recordingPublisher) Messages() []publishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]publishedMessage(nil), p.messages...)
}

func assertPublishedContext(t *testing.T, message publishedMessage, requestID string, correlationID string, benchmarkRun string, benchmarkScene string, benchmarkPhase string) {
	t.Helper()
	if message.RequestID != requestID {
		t.Fatalf("published request id = %q, want %q", message.RequestID, requestID)
	}
	if message.CorrelationID != correlationID {
		t.Fatalf("published correlation id = %q, want %q", message.CorrelationID, correlationID)
	}
	if message.BenchmarkRun != benchmarkRun {
		t.Fatalf("published benchmark run = %q, want %q", message.BenchmarkRun, benchmarkRun)
	}
	if message.BenchmarkScene != benchmarkScene {
		t.Fatalf("published benchmark scene = %q, want %q", message.BenchmarkScene, benchmarkScene)
	}
	if message.BenchmarkPhase != benchmarkPhase {
		t.Fatalf("published benchmark phase = %q, want %q", message.BenchmarkPhase, benchmarkPhase)
	}
}

func assertBusinessPayloadHasNoRequestContext(t *testing.T, message publishedMessage) {
	t.Helper()
	body, ok := message.Body.(map[string]any)
	if !ok {
		t.Fatalf("published body has type %T, want map[string]any", message.Body)
	}
	if _, ok := body["requestId"]; ok {
		t.Fatalf("business payload unexpectedly contains requestId: %#v", body)
	}
	if _, ok := body["correlationId"]; ok {
		t.Fatalf("business payload unexpectedly contains correlationId: %#v", body)
	}
}
